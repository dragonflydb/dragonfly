// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <functional>
#include <memory>
#include <system_error>

#include "io/io.h"

namespace facade {

// Disk-backed queue for offloading connection backpressure to disk.
// On non-Linux platforms this is a no-op stub: Init() returns ENOSYS and
// HasEnoughBackingSpaceFor() always returns false, so callers never push/pop.
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
  struct Impl;

  std::unique_ptr<Impl> impl_;

  // Read only constants
  const size_t max_backing_size_;

  // same as connection id. Used to uniquely identify the backed file
  const size_t id_;
};

}  // namespace facade
