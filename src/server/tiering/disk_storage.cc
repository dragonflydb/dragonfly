// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tiering/disk_storage.h"

#include <system_error>

#include "base/flags.h"
#include "base/logging.h"
#include "io/io_buf.h"
#include "server/error.h"
#include "server/tiering/common.h"
#include "server/tiering/external_alloc.h"
#include "util/fibers/uring_proactor.h"

using namespace ::dfly::tiering::literals;

#ifdef DIRECT_IO_SUPPORTED
constexpr int kDirectIoFlag = O_DIRECT;
constexpr bool kDirectIoDefault = true;
#else
constexpr int kDirectIoFlag = 0;
constexpr bool kDirectIoDefault = false;
#endif

ABSL_FLAG(bool, backing_file_direct, kDirectIoDefault,
          "If true uses O_DIRECT to open backing files");

ABSL_FLAG(uint64_t, registered_buffer_size, 512_KB,
          "Size of registered buffer for IoUring fixed read/writes");

namespace dfly::tiering {

using namespace std;
using namespace ::util::fb2;

namespace {

UringBuf AllocateTmpBuf(size_t size) {
  size = (size + kPageSize - 1) / kPageSize * kPageSize;
  VLOG(1) << "Fallback to temporary allocation: " << size;

  uint8_t* buf = new (align_val_t(kPageSize)) uint8_t[size];
  return UringBuf{{buf, size}, nullopt};
}

void DestroyTmpBuf(UringBuf buf) {
  DCHECK(!buf.buf_idx);
  ::operator delete[](buf.bytes.data(), align_val_t(kPageSize));
}

void ReturnBuf(UringBuf buf) {
  DCHECK_EQ(ProactorBase::me()->GetKind(), ProactorBase::IOURING);
  auto* up = static_cast<UringProactor*>(ProactorBase::me());

  if (buf.buf_idx)
    up->ReturnBuffer(buf);
  else
    DestroyTmpBuf(buf);
}

constexpr off_t kInitialSize = 1UL << 28;  // 256MB

template <typename... Ts> error_code DoFiberCall(void (SubmitEntry::*c)(Ts...), Ts... args) {
  auto* proactor = static_cast<UringProactor*>(ProactorBase::me());
  FiberCall fc(proactor);
  (fc.operator->()->*c)(std::forward<Ts>(args)...);
  FiberCall::IoResult io_res = fc.Get();
  return io_res < 0 ? error_code{-io_res, system_category()} : error_code{};
}

}  // anonymous namespace

DiskStorage::DiskStorage(size_t max_size) : max_size_(max_size) {
}

error_code DiskStorage::Open(string_view path) {
  DCHECK_EQ(ProactorBase::me()->GetKind(), ProactorBase::IOURING);
  CHECK(!backing_file_);

  int kFlags = O_CREAT | O_RDWR | O_TRUNC | O_CLOEXEC;
  if (absl::GetFlag(FLAGS_backing_file_direct))
    kFlags |= kDirectIoFlag;  // If supported, adds O_DIRECT. If not, adds 0 (no-op).

  backing_file_path_ = path;
  auto res = OpenLinux(path, kFlags, 0666);
  if (!res)
    return res.error();
  backing_file_ = std::move(res.value());

  int fd = backing_file_->fd();

  auto ec = DoFiberCall(&SubmitEntry::PrepFallocate, fd, 0, 0L, kInitialSize);
  VLOG_IF(1, ec) << "Fallocate not supported";

  RETURN_ON_ERR(DoFiberCall(&SubmitEntry::PrepFadvise, fd, 0L, 0L, POSIX_FADV_RANDOM));

  alloc_.AddStorage(0, kInitialSize);

  // TODO(vlad): Even though this is called only once for regular use,
  // the testing code runs this initializer every time, never unregistering previous buffers
  auto* up = static_cast<UringProactor*>(ProactorBase::me());
  auto registered_buffer_size = absl::GetFlag(FLAGS_registered_buffer_size);
  if (registered_buffer_size > 0) {
    if (int io_res = up->RegisterBuffers(registered_buffer_size); io_res < 0)
      return error_code{-io_res, system_category()};
  }
  return {};
}

void DiskStorage::Close() {
  using namespace chrono_literals;

  // TODO: to fix this polling.
  while (pending_ops_ > 0 || grow_.pending)
    util::ThisFiber::SleepFor(10ms);

  auto ec = backing_file_->Close();
  LOG_IF(ERROR, ec) << "Failed to close backing file: " << ec;
  backing_file_.reset();

  int errc = unlink(backing_file_path_.c_str());
  LOG_IF(ERROR, errc != 0) << "Failed to unlink backing file: "
                           << std::error_code{errc, std::system_category()};
}

void DiskStorage::Read(DiskSegment segment, ReadCb cb) {
  DCHECK_GT(segment.length, 0u);
  DCHECK_EQ(segment.offset % kPageSize, 0u);

  size_t len = segment.length;
  UringBuf buf = PrepareBuf(len);
  auto io_cb = [this, cb = std::move(cb), buf, len](int io_res) {
    if (io_res < 0) {
      cb(nonstd::make_unexpected(error_code{-io_res, system_category()}));
    } else {
      cb(string_view{reinterpret_cast<char*>(buf.bytes.data()), len});
    }
    ReturnBuf(buf);
    pending_ops_--;
  };

  pending_ops_++;
  if (buf.buf_idx)
    backing_file_->ReadFixedAsync(buf.bytes, segment.offset, *buf.buf_idx, std::move(io_cb));
  else
    backing_file_->ReadAsync(buf.bytes, segment.offset, std::move(io_cb));
}

void DiskStorage::MarkAsFree(DiskSegment segment) {
  DCHECK_GT(segment.length, 0u);
  DCHECK_EQ(segment.offset % kPageSize, 0u);

  alloc_.Free(segment.offset, segment.length);
}

io::Result<std::pair<size_t, UringBuf>> DiskStorage::PrepareStash(size_t length) {
  using namespace nonstd;

  int64_t offset = alloc_.Malloc(length);
  if (offset >= 0)
    return std::make_pair(offset, PrepareBuf(length));

  // If we don't have enough space, request grow and return to avoid blocking
  if (offset < 0) {
    auto ec = RequestGrow(-offset);
    return make_unexpected(ec ? ec : make_error_code(errc::operation_would_block));
  }

  offset = alloc_.Malloc(length);
  if (offset < 0)  // we can't fit it even after resizing
    return make_unexpected(make_error_code(errc::file_too_large));

  return std::make_pair(offset, PrepareBuf(length));
}

void DiskStorage::Stash(DiskSegment segment, UringBuf buf, StashCb cb) {
  auto io_cb = [this, cb, buf, segment](int io_res) {
    if (io_res < 0) {
      MarkAsFree(segment);
      cb(error_code{-io_res, std::system_category()});
    } else {
      cb({});
    }
    ReturnBuf(buf);
    pending_ops_--;
  };

  pending_ops_++;
  size_t offset = segment.offset;
  if (buf.buf_idx)
    backing_file_->WriteFixedAsync(buf.bytes, offset, *buf.buf_idx, std::move(io_cb));
  else
    backing_file_->WriteAsync(buf.bytes, offset, std::move(io_cb));

  // Grow in advance if needed and possible
  size_t capacity = alloc_.capacity();
  size_t available = capacity - alloc_.allocated_bytes();
  if ((available < 256_MB) && (available < capacity * 0.15) && !grow_.pending) {
    auto ec = RequestGrow(256_MB);
    LOG_IF(ERROR, ec && ec != errc::file_too_large) << "Could not call grow :" << ec.message();
  }
}

DiskStorage::Stats DiskStorage::GetStats() const {
  return {
      alloc_.allocated_bytes(),       alloc_.capacity(), heap_buf_alloc_cnt_, reg_buf_alloc_cnt_,
      static_cast<size_t>(max_size_), pending_ops_};
}

error_code DiskStorage::RequestGrow(off_t grow_size) {
  DCHECK_EQ(grow_size % ExternalAllocator::kExtAlignment, 0u);
  if (alloc_.capacity() + grow_size >= static_cast<size_t>(max_size_))
    return make_error_code(errc::file_too_large);

  // Don't try again immediately, most likely it won't succeed ever.
  const uint64_t kCooldownTime = 100'000'000;  // 100ms
  if (grow_.last_err && (ProactorBase::GetMonotonicTimeNs() - grow_.timestamp_ns) < kCooldownTime)
    return make_error_code(errc::operation_canceled);

  if (std::exchange(grow_.pending, true)) {
    LOG_EVERY_T(WARNING, 1) << "Blocked on concurrent grow";
    return make_error_code(errc::operation_in_progress);
  }

  off_t end = alloc_.capacity();
  backing_file_->FallocateAsync(0, end, grow_size, [=](int res) {
    auto ec = (res < 0) ? std::error_code{-res, std::system_category()} : std::error_code{};
    grow_.pending = false;
    grow_.last_err = ec;
    grow_.timestamp_ns = ProactorBase::GetMonotonicTimeNs();
    if (!ec)
      alloc_.AddStorage(end, grow_size);
  });

  return {};
}

UringBuf DiskStorage::PrepareBuf(size_t size) {
  DCHECK_EQ(ProactorBase::me()->GetKind(), ProactorBase::IOURING);
  auto* up = static_cast<UringProactor*>(ProactorBase::me());

  if (auto borrowed = up->RequestBuffer(size); borrowed) {
    ++reg_buf_alloc_cnt_;
    return *borrowed;
  }
  ++heap_buf_alloc_cnt_;
  return AllocateTmpBuf(size);
}

}  // namespace dfly::tiering
