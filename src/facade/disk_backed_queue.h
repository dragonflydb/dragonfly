// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <io/file.h>

#include <functional>
#include <memory>
#include <string_view>
#include <system_error>

namespace facade {

class DiskBackedQueue {
 public:
  explicit DiskBackedQueue(uint32_t conn_id);
  ~DiskBackedQueue();

  std::error_code Init();

  // Check if we can offload bytes to backing file.
  bool HasEnoughBackingSpaceFor(size_t bytes) const;

  std::error_code Push(std::string_view blob);

  std::error_code Pop(std::string* out);

  // Check if backing file is empty, i.e. backing file has 0 bytes.
  bool Empty() const;

  std::error_code CloseReader();
  std::error_code CloseWriter();

 private:
  // File Reader/Writer
  std::unique_ptr<io::WriteFile> writer_;
  std::unique_ptr<io::ReadonlyFile> reader_;

  size_t total_backing_bytes_ = 0;
  size_t total_backing_items_ = 0;

  size_t next_read_offset_ = 0;

  // Read only constants
  const size_t max_backing_size_ = 0;
  const size_t max_queue_load_size_ = 0;

  // same as connection id. Used to uniquely identify the backed file
  const size_t id_ = 0;

  std::string buffer;
};

}  // namespace facade
