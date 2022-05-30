// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/io_mgr.h"

#include <fcntl.h>
#include <mimalloc.h>

#include "base/flags.h"
#include "base/logging.h"
#include "facade/facade_types.h"
#include "util/uring/proactor.h"

ABSL_FLAG(bool, backing_file_direct, false, "If true uses O_DIRECT to open backing files");

namespace dfly {

using namespace std;
using namespace util;
using namespace facade;
using uring::FiberCall;
using uring::Proactor;
namespace this_fiber = ::boost::this_fiber;

namespace {

constexpr inline size_t alignup(size_t num, size_t align) {
  size_t amask = align - 1;
  return (num + amask) & (~amask);
}

}  // namespace

IoMgr::IoMgr() {
  flags_val = 0;
}

constexpr size_t kInitialSize = 1UL << 28;  // 256MB

error_code IoMgr::Open(const string& path) {
  CHECK(!backing_file_);

  int kFlags = O_CREAT | O_RDWR | O_TRUNC | O_CLOEXEC;
  if (absl::GetFlag(FLAGS_backing_file_direct)) {
    kFlags |= O_DIRECT;
  }
  auto res = uring::OpenLinux(path, kFlags, 0666);
  if (!res)
    return res.error();
  backing_file_ = move(res.value());
  Proactor* proactor = (Proactor*)ProactorBase::me();
  {
    uring::FiberCall fc(proactor);
    fc->PrepFallocate(backing_file_->fd(), 0, 0, kInitialSize);
    FiberCall::IoResult io_res = fc.Get();
    if (io_res < 0) {
      return error_code{-io_res, system_category()};
    }
  }
  {
    uring::FiberCall fc(proactor);
    fc->PrepFadvise(backing_file_->fd(), 0, 0, POSIX_FADV_RANDOM);
    FiberCall::IoResult io_res = fc.Get();
    if (io_res < 0) {
      return error_code{-io_res, system_category()};
    }
  }
  sz_ = kInitialSize;
  return error_code{};
}

error_code IoMgr::GrowAsync(size_t len, GrowCb cb) {
  DCHECK_EQ(0u, len % (1 << 20));

  if (flags.grow_progress) {
    return make_error_code(errc::operation_in_progress);
  }

  Proactor* proactor = (Proactor*)ProactorBase::me();

  uring::SubmitEntry entry = proactor->GetSubmitEntry(
      [this, cb = move(cb)](Proactor::IoResult res, uint32_t, int64_t arg) {
        this->flags.grow_progress = 0;
        sz_ += (res == 0 ? arg : 0);
        cb(res);
      },
      len);

  entry.PrepFallocate(backing_file_->fd(), 0, sz_, len);
  flags.grow_progress = 1;

  return error_code{};
}

error_code IoMgr::WriteAsync(size_t offset, string_view blob, WriteCb cb) {
  DCHECK(!blob.empty());
  VLOG(1) << "WriteAsync " << offset << "/" << blob.size();

  Proactor* proactor = (Proactor*)ProactorBase::me();

  auto ring_cb = [cb = move(cb)](Proactor::IoResult res, uint32_t flags, int64_t payload) {
    cb(res);
  };

  uring::SubmitEntry se = proactor->GetSubmitEntry(move(ring_cb), 0);
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
    error_code ec = backing_file_->Read(&v, 1, read_offs, 0);
    if (ec) {
      mi_free(space);
      return ec;
    }

    memcpy(dest.data(), space + offset - read_offs, dest.size());
    mi_free_size_aligned(space, space_needed, 4096);
    return ec;
  }

  iovec v{.iov_base = dest.data(), .iov_len = dest.size()};
  return backing_file_->Read(&v, 1, offset, 0);
}

void IoMgr::Shutdown() {
  while (flags_val) {
    this_fiber::sleep_for(200us);  // TODO: hacky for now.
  }
}

}  // namespace dfly
