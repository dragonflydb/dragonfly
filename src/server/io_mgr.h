// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <functional>
#include <string>

#include "core/uring.h"

namespace dfly {

class IoMgr {
 public:
  // first arg - io result.
  // using WriteCb = fu2::function_base<true, false, fu2::capacity_default, false, false,
  // void(int)>;
  using WriteCb = std::function<void(int)>;

  // (io_res, )
  using GrowCb = std::function<void(int)>;

  IoMgr();

  // blocks until all the pending requests are finished.
  void Shutdown();

  std::error_code Open(const std::string& path);

  // Grows file by that length. len must be divided by 1MB.
  // passing other values will check-fail.
  std::error_code GrowAsync(size_t len, GrowCb cb);

  // Returns error if submission failed. Otherwise - returns the io result
  // via cb. A caller must make sure that the blob exists until cb is called.
  std::error_code WriteAsync(size_t offset, std::string_view blob, WriteCb cb);
  std::error_code Read(size_t offset, io::MutableBytes dest);

  // Total file span
  size_t Span() const {
    return sz_;
  }

  bool grow_pending() const {
    return flags.grow_progress;
  }

 private:
  std::unique_ptr<LinuxFile> backing_file_;
  size_t sz_ = 0;

  union {
    uint8_t flags_val;
    struct {
      uint8_t grow_progress : 1;
    } flags;
  };
};

}  // namespace dfly
