// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <functional>
#include <string>

#include "server/common.h"
#include "util/fibers/uring_file.h"
#include "util/fibers/uring_proactor.h"

namespace dfly::tiering {

class IoMgr {
 public:
  // first arg - io result.
  // using WriteCb = fu2::function_base<true, false, fu2::capacity_default, false, false,
  // void(int)>;
  using WriteCb = std::function<void(int)>;

  using ReadCb = std::function<void(int)>;

  // blocks until all the pending requests are finished.
  void Shutdown();

  std::error_code Open(std::string_view path);

  // Try growing file by that length. Return error if growth failed.
  std::error_code Grow(size_t len);

  // Write into offset from src and call cb once done. The callback is guaranteed to be invoked in
  // any error case for cleanup. The src buffer must outlive the call, until cb is resolved.
  void WriteAsync(size_t offset, util::fb2::UringBuf src, WriteCb cb);

  // Read into dest and call cb once read. The callback is guaranteed to be invoked in any error
  // case for cleanup. The dest buffer must outlive the call, until cb is resolved.
  void ReadAsync(size_t offset, util::fb2::UringBuf dest, ReadCb cb);

  // Total file span
  size_t Span() const {
    return sz_;
  }

  bool grow_pending() const {
    return grow_progress_;
  }

  const IoMgrStats& GetStats() const {
    return stats_;
  }

 private:
  std::unique_ptr<util::fb2::LinuxFile> backing_file_;
  size_t sz_ = 0;

  bool grow_progress_ = false;
  IoMgrStats stats_;
};

}  // namespace dfly::tiering
