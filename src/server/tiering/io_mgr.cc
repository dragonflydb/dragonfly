// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tiering/io_mgr.h"

#include <fcntl.h>
#include <mimalloc.h>

#include "base/flags.h"
#include "base/logging.h"
#include "facade/facade_types.h"
#include "server/tiering/common.h"
#include "util/fibers/uring_proactor.h"

ABSL_FLAG(bool, backing_file_direct, false, "If true uses O_DIRECT to open backing files");

namespace dfly::tiering {

using namespace std;
using namespace util;
using namespace facade;

using Proactor = fb2::UringProactor;
using fb2::ProactorBase;
using fb2::SubmitEntry;

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

  if (exchange(grow_progress_, true))
    return make_error_code(errc::operation_in_progress);

  fb2::FiberCall fc(proactor);
  fc->PrepFallocate(backing_file_->fd(), 0, sz_, len);
  Proactor::IoResult res = fc.Get();

  grow_progress_ = false;

  if (res == 0) {
    sz_ += len;
    return {};
  } else {
    return std::error_code(-res, std::iostream_category());
  }
}

void IoMgr::WriteAsync(size_t offset, util::fb2::UringBuf buf, WriteCb cb) {
  DCHECK(!buf.bytes.empty());

  auto* proactor = static_cast<Proactor*>(ProactorBase::me());
  auto ring_cb = [cb = std::move(cb)](auto*, Proactor::IoResult res, uint32_t flags) { cb(res); };

  SubmitEntry se = proactor->GetSubmitEntry(std::move(ring_cb), 0);
  if (buf.buf_idx)
    se.PrepWriteFixed(backing_file_->fd(), buf.bytes.data(), buf.bytes.size(), offset,
                      *buf.buf_idx);
  else
    se.PrepWrite(backing_file_->fd(), buf.bytes.data(), buf.bytes.size(), offset);
}

void IoMgr::ReadAsync(size_t offset, util::fb2::UringBuf buf, ReadCb cb) {
  DCHECK(!buf.bytes.empty());

  auto* proactor = static_cast<Proactor*>(ProactorBase::me());
  auto ring_cb = [cb = std::move(cb)](auto*, Proactor::IoResult res, uint32_t flags) { cb(res); };

  SubmitEntry se = proactor->GetSubmitEntry(std::move(ring_cb), 0);
  if (buf.buf_idx)
    se.PrepReadFixed(backing_file_->fd(), buf.bytes.data(), buf.bytes.size(), offset, *buf.buf_idx);
  else
    se.PrepRead(backing_file_->fd(), buf.bytes.data(), buf.bytes.size(), offset);
}

void IoMgr::Shutdown() {
  while (grow_progress_) {
    ThisFiber::SleepFor(200us);  // TODO: hacky for now.
  }
  backing_file_->Close();
  backing_file_.reset();
}

}  // namespace dfly::tiering
