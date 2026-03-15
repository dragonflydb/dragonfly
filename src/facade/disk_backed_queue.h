// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <functional>
#include <memory>
#include <string_view>
#include <system_error>

#include "io/io.h"
#include "util/fibers/uring_file.h"

namespace facade {

class DiskBackedQueue {
 public:
  explicit DiskBackedQueue(uint32_t conn_id);
  ~DiskBackedQueue();

  std::error_code Init();

  // Check if we can offload bytes to backing file.
  bool HasEnoughBackingSpaceFor(size_t bytes) const;

  using AsyncPushCallback = std::function<void(std::error_code)>;

  void PushAsync(io::Bytes bytes, AsyncPushCallback cb);

  using AsyncPopCallback = std::function<void(io::Result<size_t>)>;

  // Async read variant. Callback is invoked with Result containing bytes read or error.
  void PopAsync(io::MutableBytes out, AsyncPopCallback cb);

  // Check if backing file is empty, i.e. backing file has 0 bytes.
  bool Empty() const;

  // Total bytes currently buffered on disk (not yet consumed).
  size_t TotalBytes() const;

  std::error_code Close();

 private:
  // Punch holes over the aligned region we have fully read past so the OS can reclaim pages.
  void MaybePunchHole();

  // Single O_RDWR file used for both writes and reads, avoiding a separate fd for fallocate.
  std::unique_ptr<util::fb2::LinuxFile> file_;

  size_t write_offset_ = 0;
  size_t total_backing_bytes_ = 0;
  size_t next_read_offset_ = 0;
  // Tracks how far into the file holes have been punched (always 4096-aligned).
  size_t punch_offset_ = 0;

  // Read only constants
  const size_t max_backing_size_ = 0;

  // same as connection id. Used to uniquely identify the backed file
  const size_t id_ = 0;
  size_t in_flight_callbacks_ = 0;
};

}  // namespace facade
