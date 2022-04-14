// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <functional>
#include <string>

#include "util/uring/uring_file.h"

namespace dfly {

class IoMgr {
 public:
  // first arg - io result.
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

  std::error_code Write(size_t offset, std::string_view blob) {
    return backing_file_->Write(io::Buffer(blob), offset, 0);
  }

  // Returns error if submission failed. Otherwise - returns the io result
  // via cb.
  std::error_code WriteAsync(size_t offset, std::string_view blob, WriteCb cb);

  size_t Size() const { return sz_; }

  bool grow_pending() const { return flags.grow_progress;}

 private:
  std::unique_ptr<util::uring::LinuxFile> backing_file_;
  size_t sz_ = 0;

  union {
    uint8_t flags_val;
    struct {
      uint8_t grow_progress : 1;
    } flags;
  };
};

}  // namespace dfly
