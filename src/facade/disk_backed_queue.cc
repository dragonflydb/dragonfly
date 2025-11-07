// Copyright 2025, DragonflyDB authors.  All rights reserved.
//
// See LICENSE for licensing terms.
//

#include "facade/disk_backed_queue.h"

#include <absl/strings/str_cat.h>

#include <string>

#include "base/flags.h"
#include "base/logging.h"
#include "facade/facade_types.h"
#include "io/io.h"
#include "util/fibers/uring_file.h"

using facade::operator""_MB;

ABSL_FLAG(std::string, disk_backpressure_folder, "/tmp/",
          "Folder to store disk-backed connection backpressure");

ABSL_FLAG(size_t, disk_backpressure_file_max_bytes, 50_MB,
          "Maximum size of the backing file. When max size is reached, connection will "
          "stop offloading backpressure to disk and block on client read.");

ABSL_FLAG(size_t, disk_backpressure_load_size, 30,
          "How many items to load in dispatch queue from the disk-backed file.");

namespace facade {

DiskBackedQueue::DiskBackedQueue(uint32_t conn_id)
    : max_backing_size_(absl::GetFlag(FLAGS_disk_backpressure_file_max_bytes)),
      max_queue_load_size_(absl::GetFlag(FLAGS_disk_backpressure_load_size)),
      id_(conn_id) {
}

std::error_code DiskBackedQueue::Init() {
  std::string backing_name = absl::StrCat(absl::GetFlag(FLAGS_disk_backpressure_folder), id_);
  {
    // Kernel transparently handles buffering via the page cache.
    auto res = util::fb2::OpenWrite(backing_name, {} /* overwrite mode + non-direct io */);
    if (!res) {
      return res.error();
    }
    writer_.reset(*res);
  }

  auto res = util::fb2::OpenRead(backing_name);
  if (!res) {
    return res.error();
  }
  reader_.reset(*res);

  VLOG(3) << "Created backing for connection " << this << " " << backing_name;

  return {};
}

DiskBackedQueue::~DiskBackedQueue() {
  auto ec = writer_->Close();
  LOG_IF(WARNING, ec) << ec.message();
  ec = reader_->Close();
  LOG_IF(WARNING, ec) << ec.message();
}

// Check if backing file is empty, i.e. backing file has 0 bytes.
bool DiskBackedQueue::Empty() const {
  return total_backing_bytes_ == 0;
}

bool DiskBackedQueue::HasEnoughBackingSpaceFor(size_t bytes) const {
  return (bytes + total_backing_bytes_) < max_backing_size_;
}

std::error_code DiskBackedQueue::Push(std::string_view blob) {
  // TODO we should truncate as the file grows. That way we never end up with large files
  // on disk.
  uint32_t sz = blob.size();
  // We serialize the string as is and we prefix with 4 bytes denoting its size. The layout is:
  // 4bytes(str size) + followed by blob.size() bytes
  iovec offset_data[2]{{&sz, sizeof(uint32_t)}, {const_cast<char*>(blob.data()), blob.size()}};

  auto ec = writer_->Write(offset_data, 2);
  if (ec) {
    VLOG(2) << "Failed to offload blob of size " << sz << " to backing with error: " << ec;
    return ec;
  }

  total_backing_bytes_ += blob.size();
  ++total_backing_items_;

  if (next_item_total_bytes_ == 0) {
    next_item_total_bytes_ = blob.size();
  }

  VLOG(2) << "Offload connection " << this << " backpressure of " << blob.size();
  VLOG(3) << "Command offloaded: " << blob;
  return {};
}

std::error_code DiskBackedQueue::Pop(std::string* out) {
  // We read the next item and (if there are more) we also prefetch the next item's size.
  uint32_t read_sz = next_item_total_bytes_ + (total_backing_items_ > 1 ? sizeof(uint32_t) : 0);
  buffer.resize(read_sz);

  io::MutableBytes bytes{reinterpret_cast<uint8_t*>(buffer.data()), read_sz};
  auto result = reader_->Read(next_read_offset_, bytes);
  if (!result) {
    LOG(ERROR) << "Could not load item at offset " << next_read_offset_ << " of size " << read_sz
               << " from disk with error: " << result.error().value() << " "
               << result.error().message();
    return result.error();
  }

  VLOG(2) << "Loaded item with offset " << next_read_offset_ << " of size " << read_sz
          << " for connection " << this;

  next_read_offset_ += bytes.size();

  if (total_backing_items_ > 1) {
    auto buf = bytes.subspan(bytes.size() - sizeof(uint32_t));
    uint32_t val = ((uint32_t)buf[0]) | ((uint32_t)buf[1] << 8) | ((uint32_t)buf[2] << 16) |
                   ((uint32_t)buf[3] << 24);
    bytes = bytes.subspan(0, next_item_total_bytes_);
    next_item_total_bytes_ = val;
  } else {
    next_item_total_bytes_ = 0;
  }

  std::string read(reinterpret_cast<const char*>(bytes.data()), bytes.size());
  *out = std::move(read);

  total_backing_bytes_ -= next_item_total_bytes_;
  --total_backing_items_;

  return {};
}

}  // namespace facade
