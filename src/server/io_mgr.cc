// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/io_mgr.h"

#include <fcntl.h>
#include <mimalloc.h>

#include "base/flags.h"
#include "base/logging.h"
#include "facade/facade_types.h"
#include "util/fibers/uring_proactor.h"

ABSL_FLAG(bool, backing_file_direct, false, "If true uses O_DIRECT to open backing files");

namespace dfly {

using namespace std;
using namespace util;
using namespace facade;

using Proactor = fb2::UringProactor;
using fb2::ProactorBase;
using fb2::SubmitEntry;

namespace {

constexpr inline size_t alignup(size_t num, size_t align) {
  size_t amask = align - 1;
  return (num + amask) & (~amask);
}

}  // namespace

constexpr size_t kInitialSize = 1UL << 28;  // 256MB

error_code IoMgr::Open(std::string_view path) {
  CHECK(!backing_file_);

  int kFlags = O_CREAT | O_RDWR | O_TRUNC | O_CLOEXEC;
  if (absl::GetFlag(FLAGS_backing_file_direct)) {
    kFlags |= O_DIRECT;
  }
  auto res = fb2::OpenLinux(path, kFlags, 0666);
  if (!res)
    return res.error();
  backing_file_ = std::move(res.value());
  Proactor* proactor = (Proactor*)ProactorBase::me();
  {
    fb2::FiberCall fc(proactor);
    fc->PrepFallocate(backing_file_->fd(), 0, 0, kInitialSize);
    fb2::FiberCall::IoResult io_res = fc.Get();
    if (io_res < 0) {
      return error_code{-io_res, system_category()};
    }
  }
  {
    fb2::FiberCall fc(proactor);
    fc->PrepFadvise(backing_file_->fd(), 0, 0, POSIX_FADV_RANDOM);
    fb2::FiberCall::IoResult io_res = fc.Get();
    if (io_res < 0) {
      return error_code{-io_res, system_category()};
    }
  }
  sz_ = kInitialSize;
  return error_code{};
}

error_code IoMgr::Grow(size_t len) {
  Proactor* proactor = (Proactor*)ProactorBase::me();

  if (exchange(grow_progress, true))
    return make_error_code(errc::operation_in_progress);

  fb2::FiberCall fc(proactor);
  fc->PrepFallocate(backing_file_->fd(), 0, sz_, len);
  Proactor::IoResult res = fc.Get();

  grow_progress = false;

  if (res == 0) {
    sz_ += len;
    return {};
  } else {
    return std::error_code(-res, std::iostream_category());
  }
}

error_code IoMgr::GrowAsync(size_t len, GrowCb cb) {
  DCHECK_EQ(0u, len % (1 << 20));

  if (exchange(grow_progress, true)) {
    return make_error_code(errc::operation_in_progress);
  }

  Proactor* proactor = (Proactor*)ProactorBase::me();

  SubmitEntry entry = proactor->GetSubmitEntry(
      [this, len, cb = std::move(cb)](auto*, Proactor::IoResult res, uint32_t) {
        this->grow_progress = false;
        sz_ += (res == 0 ? len : 0);
        cb(res);
      },
      0);

  entry.PrepFallocate(backing_file_->fd(), 0, sz_, len);

  return error_code{};
}

error_code IoMgr::WriteAsync(size_t offset, string_view blob, WriteCb cb) {
  DCHECK(!blob.empty());
  VLOG(1) << "WriteAsync " << offset << "/" << blob.size();

  Proactor* proactor = (Proactor*)ProactorBase::me();

  auto ring_cb = [cb = std::move(cb)](auto*, Proactor::IoResult res, uint32_t flags) { cb(res); };

  SubmitEntry se = proactor->GetSubmitEntry(std::move(ring_cb), 0);
  se.PrepWrite(backing_file_->fd(), blob.data(), blob.size(), offset);

  return error_code{};
}

error_code IoMgr::Read(size_t offset, io::MutableBytes dest) {
  DCHECK(!dest.empty());

  if (absl::GetFlag(FLAGS_backing_file_direct)) {
    size_t read_offs = offset & ~4095ULL;
    size_t end_range = alignup(offset + dest.size(), 4096);
    size_t space_needed = end_range - read_offs;
    DCHECK_EQ(0u, space_needed % 4096);

    uint8_t* space = (uint8_t*)mi_malloc_aligned(space_needed, 4096);
    iovec v{.iov_base = space, .iov_len = space_needed};
    uint64_t from_ts = ProactorBase::GetMonotonicTimeNs();
    error_code ec = backing_file_->Read(&v, 1, read_offs, 0);
    uint64_t end_ts = ProactorBase::GetMonotonicTimeNs();

    stats_.read_delay_usec += (end_ts - from_ts) / 1000;
    ++stats_.read_total;

    if (ec) {
      mi_free(space);
      return ec;
    }

    memcpy(dest.data(), space + offset - read_offs, dest.size());
    mi_free_size_aligned(space, space_needed, 4096);
    return ec;
  }

  iovec v{.iov_base = dest.data(), .iov_len = dest.size()};
  uint64_t from_ts = ProactorBase::GetMonotonicTimeNs();
  auto ec = backing_file_->Read(&v, 1, offset, 0);
  uint64_t end_ts = ProactorBase::GetMonotonicTimeNs();

  stats_.read_delay_usec += (end_ts - from_ts) / 1000;
  ++stats_.read_total;
  return ec;
}

std::error_code IoMgr::ReadAsync(size_t offset, absl::Span<uint8_t> buffer, ReadCb cb) {
  DCHECK(!buffer.empty());
  VLOG(1) << "Read " << offset << "/" << buffer.size();

  Proactor* proactor = (Proactor*)ProactorBase::me();

  auto ring_cb = [cb = std::move(cb)](auto*, Proactor::IoResult res, uint32_t flags) { cb(res); };

  SubmitEntry se = proactor->GetSubmitEntry(std::move(ring_cb), 0);
  se.PrepRead(backing_file_->fd(), buffer.data(), buffer.size(), offset);

  return error_code{};
}

void IoMgr::Shutdown() {
  while (grow_progress) {
    ThisFiber::SleepFor(200us);  // TODO: hacky for now.
  }
}

}  // namespace dfly
