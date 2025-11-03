// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <system_error>

#include "io/io.h"
#include "server/tiering/common.h"
#include "server/tiering/external_alloc.h"
#include "util/fibers/uring_file.h"
#include "util/fibers/uring_proactor.h"  // for UringBuf

namespace dfly::tiering {

// Disk storage controlled by asynchronous operations.
// Provides Random Access Read/Stash asynchronous interface around low level linux file.
// Handles ranges management and file growth via underlying ExternalAllocator.
class DiskStorage {
 public:
  struct Stats {
    size_t allocated_bytes = 0;
    size_t capacity_bytes = 0;
    uint64_t heap_buf_alloc_count = 0;
    uint64_t registered_buf_alloc_count = 0;
    size_t max_file_size = 0;
    size_t pending_ops = 0;
  };

  using ReadCb = std::function<void(io::Result<std::string_view>)>;
  using StashCb = std::function<void(std::error_code)>;

  explicit DiskStorage(size_t max_size);

  std::error_code Open(std::string_view path);
  void Close();

  // Request read for segment, cb will be called on completion with read value
  void Read(DiskSegment segment, ReadCb cb);

  // Mark segment as free, performed immediately
  void MarkAsFree(DiskSegment segment);

  // Allocate segment of at least given length and prepare buffer. Migh block to grow backing.
  // Return error if not enough space is available or growing failed.
  // Every successful preparation must end in a Stash(), otherwise resources are leaked.
  io::Result<std::pair<size_t /* offset */, util::fb2::UringBuf>> PrepareStash(size_t length);

  // Write prepared buffer to given segment and resolve completion callback when write is done.
  void Stash(DiskSegment segment, util::fb2::UringBuf buf, StashCb cb);

  Stats GetStats() const;

 private:
  // Try asynchronously growing backing file by requested size
  std::error_code RequestGrow(off_t grow_size);

  // Returns a buffer with size greater or equal to len.
  util::fb2::UringBuf PrepareBuf(size_t len);

  off_t max_size_;
  size_t pending_ops_ = 0;  // number of ongoing ops for safe shutdown

  // how many times we allocate registered/heap buffers.
  uint64_t heap_buf_alloc_cnt_ = 0, reg_buf_alloc_cnt_ = 0;

  struct {
    bool pending = false;  // currently in progress
    std::error_code last_err;
    uint64_t timestamp_ns;  // last grow finished
  } grow_;                  // status of last RequestGrow() operation

  std::string backing_file_path_;
  std::unique_ptr<util::fb2::LinuxFile> backing_file_;
  ExternalAllocator alloc_;
};

};  // namespace dfly::tiering
