// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <functional>
#include <memory>
#include <system_error>
#include <vector>

#include "io/io.h"

namespace facade {

// Disk-backed queue for offloading connection backpressure to disk.
// On non-Linux platforms this is a no-op stub: Init() returns ENOSYS and
// HasEnoughBackingSpaceFor() always returns false, so callers never push/pop.
class DiskBackedQueue {
 public:
  // Maximum number of bytes per chunk. Recvs read into a Chunk-sized buffer;
  // actual chunk size is the number of bytes received (may be smaller).
  static constexpr size_t kMaxChunkSize = 8192;

  struct Chunk {
    // 0 < data.size() * 8 < kMaxChunkSize
    std::vector<uint8_t> data;
  };

  explicit DiskBackedQueue(uint32_t conn_id);
  ~DiskBackedQueue();

  std::error_code Init();

  // Returns true if the queue has any pending work: data on disk, chunks
  // waiting to be written, a write CQE in-flight, or a pop CQE in-flight.
  bool IsActive() const;

  // Returns true when remaining bytes have dropped below the drain
  // threshold. The caller should block new socket reads to allow a clean
  // drain-to-memory transition (TCP backpressure builds during this window).
  bool IsDraining() const;

  // Returns true if there is a pop in flight
  bool IsPopInFlight() const;

  // Check if we can offload bytes to backing file.
  // Counts both on-disk bytes and bytes queued for writing (not yet on disk).
  bool HasEnoughBackingSpaceFor(size_t bytes) const;

  using AsyncPushCallback = std::function<void(std::error_code)>;

  // Takes ownership of chunk and enqueues it for async write to disk.
  // The caller's buffer is free the instant PushAsync returns.
  // cb is invoked after the chunk's write CQE fires.
  void PushAsync(Chunk chunk, AsyncPushCallback cb);

  using AsyncPopCallback = std::function<void(io::Result<size_t>)>;

  // Async read variant. Callback is invoked with Result containing bytes read or error.
  // Must only be called when no pop is already in-flight and the backing store is non-empty.
  void PopAsync(io::MutableBytes out, AsyncPopCallback cb);

  // Check if backing store (on-disk bytes) is empty.
  bool Empty() const;

  // Cancel all pending (not-yet-submitted) writes, invoking their callbacks with
  // operation_canceled.
  // Caller must also wait the in-flight operations to complete
  void Cancel();

  std::error_code Close();

 private:
  struct Impl;

  std::unique_ptr<Impl> impl_;

  // Read only constants
  const size_t max_backing_size_;
  const size_t drain_threshold_;

  // same as connection id. Used to uniquely identify the backed file
  const size_t id_;
};

}  // namespace facade
