// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <system_error>

#include "io/io.h"
#include "server/tiering/common.h"
#include "server/tiering/external_alloc.h"
#include "util/fibers/uring_file.h"

namespace dfly::tiering {

// Disk storage controlled by asynchronous operations.
class DiskStorage {
 public:
  struct Stats {
    size_t allocated_bytes = 0;
    size_t capacity_bytes = 0;
  };

  using ReadCb = std::function<void(std::string_view, std::error_code)>;
  using StashCb = std::function<void(DiskSegment, std::error_code)>;

  explicit DiskStorage(size_t max_size);

  std::error_code Open(std::string_view path);
  void Close();

  // Request read for segment, cb will be called on completion with read value
  void Read(DiskSegment segment, ReadCb cb);

  // Mark segment as free, performed immediately
  void MarkAsFree(DiskSegment segment);

  // Request bytes to be stored, cb will be called with assigned segment on completion. Can block to
  // grow backing file. Returns error code if operation failed  immediately (most likely it failed
  // to grow the backing file) or passes an empty segment if the final write operation failed.
  // Bytes are copied and can be dropped before cb is resolved
  std::error_code Stash(io::Bytes bytes, StashCb cb);

  Stats GetStats() const;

 private:
  std::error_code Grow(off_t grow_size);

 private:
  off_t size_, max_size_;
  size_t pending_ops_ = 0;  // number of ongoing ops for safe shutdown
  bool grow_pending_ = false;
  std::unique_ptr<util::fb2::LinuxFile> backing_file_;

  ExternalAllocator alloc_;
};

};  // namespace dfly::tiering
