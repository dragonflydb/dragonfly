// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string>
#include <system_error>

#include "core/external_alloc.h"
#include "io/io.h"
#include "server/io_mgr.h"

namespace dfly::tiering {

struct DiskSegment {
  // Mesured in bytes, offset should be aligned to page boundaries (4kb)
  size_t offset, length;
};

// Disk storage controlled by asynchronous operations.
class DiskStorage {
 public:
  using ReadCb = std::function<void(std::string_view)>;
  using StashCb = std::function<void(DiskSegment)>;

  std::error_code Open(std::string_view path);
  void Close();

  // Request read for segment, cb will be called on completion with read value
  void Read(DiskSegment segment, ReadCb cb);

  // Mark segment as free, performed immediately
  void MarkAsFree(DiskSegment segment);

  // Request bytes to be stored, cb will be called with assigned segment on completion. Can block to
  // grow backing file, thus safe to call only from single fiber.
  // Returns error code if operation failed  immediately (most likely it failed to grow the backing
  // file) or passes an empty segment if the final write operation failed.
  std::error_code Stash(io::Bytes bytes, StashCb cb);

 private:
  IoMgr io_mgr_;
  ExternalAllocator alloc_;
};

};  // namespace dfly::tiering
