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

IoMgr::IoMgr() {
  flags_val_ = 0;

  Proactor* proactor = (Proactor*)ProactorBase::me();
  CHECK_NOTNULL(proactor);

  constexpr size_t kNumPages = 2048;
  void* ptr = nullptr;
  size_t size = kNumPages * 4096;
  CHECK_EQ(0, posix_memalign(&ptr, 4096, size));
  reg_buf_ = (uint8_t*)ptr;
  iovec vecs[1];
  vecs[0].iov_base = ptr;
  vecs[0].iov_len = size;
  CHECK_EQ(0, proactor->RegisterBuffers(vecs, 1));
  page_mask_.resize(kNumPages, false);
}

IoMgr::~IoMgr() {
  if (reg_buf_) {
    Proactor* proactor = (Proactor*)ProactorBase::me();
    CHECK_NOTNULL(proactor);
    CHECK_EQ(0, proactor->UnregisterBuffers());
    free(reg_buf_);
  }
}

constexpr size_t kInitialSize = 1UL << 28;  // 256MB

error_code IoMgr::Open(const string& path) {
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

error_code IoMgr::GrowAsync(size_t len, GrowCb cb) {
  DCHECK_EQ(0u, len % (1 << 20));

  if (flags.grow_progress) {
    return make_error_code(errc::operation_in_progress);
  }

  Proactor* proactor = (Proactor*)ProactorBase::me();

  SubmitEntry entry = proactor->GetSubmitEntry(
      [this, len, cb = std::move(cb)](auto*, Proactor::IoResult res, uint32_t) {
        this->flags.grow_progress = 0;
        sz_ += (res == 0 ? len : 0);
        cb(res);
      },
      0);

  entry.PrepFallocate(backing_file_->fd(), 0, sz_, len);
  flags.grow_progress = 1;

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
    size_t io_len = end_range - read_offs;
    CHECK_EQ(io_len, 4096u) << "TODO";
    auto it = std::find(page_mask_.begin(), page_mask_.end(), false);
    CHECK(it != page_mask_.end()) << "TODO";
    size_t page_index = it - page_mask_.begin();

    io::MutableBytes direct(reg_buf_ + page_index * 4096, 4096);
    *it = true;
    error_code ec = backing_file_->ReadFixed(direct, read_offs, 0);
    *it = false;
    if (ec) {
      return ec;
    }
    memcpy(dest.data(), direct.data() + (offset - read_offs), dest.size());
#if 0
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
    mi_free_size_aligned(space, space_needed, 4096);
#endif
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

void IoMgr::Shutdown() {
  while (flags_val_) {
    ThisFiber::SleepFor(200us);  // TODO: hacky for now.
  }
}

}  // namespace dfly
