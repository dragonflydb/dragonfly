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

ABSL_FLAG(bool, backing_file_direct, true, "If true uses O_DIRECT to open backing files");

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
    kFlags |= O_DIRECT;

  auto res = OpenLinux(path, kFlags, 0666);
  if (!res)
    return res.error();
  backing_file_ = std::move(res.value());

  int fd = backing_file_->fd();
  RETURN_ON_ERR(DoFiberCall(&SubmitEntry::PrepFallocate, fd, 0, 0L, kInitialSize));
  RETURN_ON_ERR(DoFiberCall(&SubmitEntry::PrepFadvise, fd, 0L, 0L, POSIX_FADV_RANDOM));

  alloc_.AddStorage(0, kInitialSize);

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
  while (pending_ops_ > 0 || grow_pending_)
    util::ThisFiber::SleepFor(10ms);

  backing_file_->Close();
  backing_file_.reset();
}

void DiskStorage::Read(DiskSegment segment, ReadCb cb) {
  DCHECK_GT(segment.length, 0u);
  DCHECK_EQ(segment.offset % kPageSize, 0u);

  size_t len = segment.length;
  UringBuf buf = PrepareBuf(len);
  auto io_cb = [this, cb = std::move(cb), buf, len](int io_res) {
    if (io_res < 0) {
      cb(nonstd::make_unexpected(error_code{-io_res, system_category()}));
      return;
    }

    cb(string_view{reinterpret_cast<char*>(buf.bytes.data()), len});
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

std::error_code DiskStorage::Stash(io::Bytes bytes, io::Bytes footer, StashCb cb) {
  DCHECK_GT(bytes.length(), 0u);

  size_t len = bytes.size() + footer.size();
  int64_t offset = alloc_.Malloc(len);

  // If we've run out of space, block and grow as much as needed
  if (offset < 0) {
    if (CanGrow()) {
      // TODO: To introduce asynchronous call that starts resizing before we reach this step.
      // Right now we do it synchronously as well (see Grow(256MB) call.)
      RETURN_ON_ERR(Grow(-offset));
    } else {
      return make_error_code(errc::file_too_large);
    }
    offset = alloc_.Malloc(len);
    if (offset < 0)  // we can't fit it even after resizing
      return make_error_code(errc::file_too_large);
  }

  UringBuf buf = PrepareBuf(len);
  memcpy(buf.bytes.data(), bytes.data(), bytes.length());
  if (!footer.empty())
    memcpy(buf.bytes.data() + bytes.length(), footer.data(), footer.length());

  auto io_cb = [this, cb, offset, buf, len](int io_res) {
    if (io_res < 0) {
      MarkAsFree({size_t(offset), len});
      cb(nonstd::make_unexpected(error_code{-io_res, std::system_category()}));
    } else {
      cb(DiskSegment{size_t(offset), len});
    }
    ReturnBuf(buf);
    pending_ops_--;
  };

  pending_ops_++;
  if (buf.buf_idx)
    backing_file_->WriteFixedAsync(buf.bytes, offset, *buf.buf_idx, std::move(io_cb));
  else
    backing_file_->WriteAsync(buf.bytes, offset, std::move(io_cb));

  // Grow in advance if needed and possible
  size_t capacity = alloc_.capacity();
  size_t available = capacity - alloc_.allocated_bytes();
  if ((available < 256_MB) && (available < capacity * 0.15) && !grow_pending_ && CanGrow()) {
    auto ec = Grow(256_MB);
    LOG_IF(ERROR, ec) << "Could not call grow :" << ec.message();
    return ec;
  }
  return {};
}

DiskStorage::Stats DiskStorage::GetStats() const {
  return {
      alloc_.allocated_bytes(),       alloc_.capacity(), heap_buf_alloc_cnt_, reg_buf_alloc_cnt_,
      static_cast<size_t>(max_size_), pending_ops_};
}

bool DiskStorage::CanGrow() const {
  return alloc_.capacity() + ExternalAllocator::kExtAlignment <= static_cast<size_t>(max_size_);
}

error_code DiskStorage::Grow(off_t grow_size) {
  DCHECK(CanGrow());

  if (std::exchange(grow_pending_, true)) {
    // TODO: to introduce future like semantics where multiple flow can block on the
    // ongoing Grow operation.
    LOG(WARNING) << "Concurrent grow request detected ";
    return make_error_code(errc::operation_in_progress);
  }
  off_t end = alloc_.capacity();
  auto err = DoFiberCall(&SubmitEntry::PrepFallocate, backing_file_->fd(), 0, end, grow_size);
  grow_pending_ = false;
  RETURN_ON_ERR(err);

  alloc_.AddStorage(end, grow_size);

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
