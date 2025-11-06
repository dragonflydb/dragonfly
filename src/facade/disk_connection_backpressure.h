// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <util/fibers/uring_file.h>

#include <deque>
#include <functional>
#include <memory>
#include <string_view>
#include <system_error>

#include "io/io.h"

namespace facade {

class DiskBackedBackpressureQueue {
 public:
  explicit DiskBackedBackpressureQueue(uint32_t conn_id);
  ~DiskBackedBackpressureQueue();

  std::error_code Init();

  // Check if we can offload bytes to backing file.
  bool HasEnoughBackingSpaceFor(size_t bytes) const;

  // Total size of internal buffers/structures.
  size_t TotalInMemoryBytes() const;

  void OffloadToBacking(std::string_view blob);

  // For each item loaded from disk it calls f(item) to consume it.
  // Reads up to max_queue_load_size_ items on each call
  void LoadFromBacking(std::function<void(io::MutableBytes)> f);

  // Check if backing file is empty, i.e. backing file has 0 bytes.
  bool Empty() const;

 private:
  // File Reader/Writer
  std::unique_ptr<io::WriteFile> writer_;
  std::unique_ptr<io::ReadonlyFile> reader_;

  // In memory backed file map
  struct ItemOffset {
    size_t offset = 0;
    size_t total_bytes = 0;
  };

  std::deque<ItemOffset> offsets_;

  size_t total_backing_bytes_ = 0;
  size_t next_offset_ = 0;

  // Read only constants
  const size_t max_backing_size_ = 0;
  const size_t max_queue_load_size_ = 0;

  // unique id for the file backed
  const size_t id_ = 0;
};

}  // namespace facade
