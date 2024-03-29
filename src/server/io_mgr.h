// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <functional>
#include <string>

#include "server/common.h"
#include "util/fibers/uring_file.h"

namespace dfly {

class IoMgr {
 public:
  // first arg - io result.
  // using WriteCb = fu2::function_base<true, false, fu2::capacity_default, false, false,
  // void(int)>;
  using WriteCb = std::function<void(int)>;

  // (io_res, )
  using GrowCb = std::function<void(int)>;

  using ReadCb = std::function<void(int)>;

  // blocks until all the pending requests are finished.
  void Shutdown();

  std::error_code Open(std::string_view path);

  // Try growing file by that length. Return error if growth failed.
  std::error_code Grow(size_t len);

  // Grows file by that length. len must be divided by 1MB.
  // passing other values will check-fail.
  std::error_code GrowAsync(size_t len, GrowCb cb);

  // Returns error if submission failed. Otherwise - returns the io result
  // via cb. A caller must make sure that the blob exists until cb is called.
  std::error_code WriteAsync(size_t offset, std::string_view blob, WriteCb cb);

  // Read synchronously into dest
  std::error_code Read(size_t offset, io::MutableBytes dest);

  // Read into dest and call cb once read
  std::error_code ReadAsync(size_t offset, io::MutableBytes dest, ReadCb cb);

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

}  // namespace dfly
