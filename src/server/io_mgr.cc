// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/io_mgr.h"

#include <fcntl.h>

#include "base/logging.h"
#include "facade/facade_types.h"
#include "util/uring/proactor.h"

namespace dfly {

using namespace std;
using namespace util;
using namespace facade;
using uring::FiberCall;
using uring::Proactor;
namespace this_fiber = ::boost::this_fiber;

IoMgr::IoMgr() {
  flags_val = 0;
}

constexpr size_t kInitialSize = 1UL << 28;  // 256MB

error_code IoMgr::Open(const string& path) {
  CHECK(!backing_file_);

  auto res = uring::OpenLinux(path, O_CREAT | O_WRONLY | O_TRUNC | O_CLOEXEC, 0666);
  if (!res)
    return res.error();
  backing_file_ = move(res.value());
  Proactor* proactor = (Proactor*)ProactorBase::me();
  uring::FiberCall fc(proactor);
  fc->PrepFallocate(backing_file_->fd(), 0, 0, kInitialSize);
  FiberCall::IoResult io_res = fc.Get();
  if (io_res < 0) {
    return error_code{-io_res, system_category()};
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

  uring::Proactor* proactor = (uring::Proactor*)ProactorBase::me();

  uint8_t* ptr = new uint8_t[blob.size()];
  memcpy(ptr, blob.data(), blob.size());

  auto ring_cb = [ptr, cb = move(cb)](uring::Proactor::IoResult res, uint32_t flags,
                                      int64_t payload) {
    cb(res);
    delete[] ptr;
  };

  uring::SubmitEntry se = proactor->GetSubmitEntry(move(ring_cb), 0);
  se.PrepWrite(backing_file_->fd(), ptr, blob.size(), offset);

  return error_code{};
}

void IoMgr::Shutdown() {
  while (flags_val) {
    this_fiber::sleep_for(20us);  // TODO: hacky for now.
  }
}

}  // namespace dfly
