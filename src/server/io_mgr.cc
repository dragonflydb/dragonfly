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

error_code IoMgr::Open(const string& path) {
  CHECK(!backing_file_);

  auto res = uring::OpenLinux(path, O_CREAT | O_WRONLY | O_TRUNC | O_CLOEXEC, 0666);
  if (!res)
    return res.error();
  backing_file_ = move(res.value());

  return error_code{};
}

error_code IoMgr::GrowAsync(size_t len, GrowCb cb) {
  DCHECK_EQ(0u, len % (1 << 20));

  if (flags.grow_progress) {
    return make_error_code(errc::operation_in_progress);
  }

  Proactor* proactor = (Proactor*)ProactorBase::me();

  uring::SubmitEntry entry = proactor->GetSubmitEntry(
      [this, cb = move(cb)](Proactor::IoResult res, uint32_t , int64_t arg) {
        this->flags.grow_progress = 0;
        sz_ += (res == 0 ? arg : 0);
        cb(res);
      }, len);

  entry.PrepFallocate(backing_file_->fd(), 0, sz_, len);
  flags.grow_progress = 1;

  return error_code{};
}

error_code IoMgr::GetBlockAsync(string_view buf, int64_t arg, CbType cb) {
  /*uring::Proactor* proactor = (uring::Proactor*)ProactorBase::me();

  auto mgr_cb = [cb = move(cb)](uring::Proactor::IoResult res, uint32_t flags, int64_t payload) {
    cb(res, 4096);
  };

  uring::SubmitEntry se = proactor->GetSubmitEntry(move(mgr_cb), 0);
  se.PrepWrite(backing_file_->fd(), str_.data(), str_.size(), 4096);
*/
  return error_code{};
}

void IoMgr::Shutdown() {
  while (flags_val) {
    this_fiber::sleep_for(20us);  // TODO: hacky for now.
  }
}

}  // namespace dfly
