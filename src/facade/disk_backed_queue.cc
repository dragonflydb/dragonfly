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
}

std::error_code DiskBackedQueue::CloseWriter() {
  if (writer_) {
    auto ec = writer_->Close();
    LOG_IF(WARNING, ec) << ec.message();
    return ec;
  }
  return {};
}

std::error_code DiskBackedQueue::CloseReader() {
  if (reader_) {
    auto ec = reader_->Close();
    LOG_IF(WARNING, ec) << ec.message();
    return ec;
  }
  return {};
}

// Check if backing file is empty, i.e. backing file has 0 bytes.
bool DiskBackedQueue::Empty() const {
  return total_backing_bytes_ == 0;
}

bool DiskBackedQueue::HasEnoughBackingSpaceFor(size_t bytes) const {
  return (bytes + total_backing_bytes_) < max_backing_size_;
}

std::error_code DiskBackedQueue::Push(std::string_view blob) {
  auto ec = writer_->Write(blob);
  if (ec) {
    VLOG(2) << "Failed to offload blob of size " << blob.size() << " to backing with error: " << ec;
    return ec;
  }

  total_backing_bytes_ += blob.size();

  VLOG(2) << "Offload connection " << this << " backpressure of " << blob.size();
  VLOG(3) << "Command offloaded: " << blob;
  return {};
}

std::error_code DiskBackedQueue::Pop(std::string* out) {
  const size_t k_read_size = 4096;
  const size_t to_read = std::min(k_read_size, total_backing_bytes_);
  out->resize(to_read);

  io::MutableBytes bytes{reinterpret_cast<uint8_t*>(out->data()), to_read};
  auto result = reader_->Read(next_read_offset_, bytes);
  if (!result) {
    LOG(ERROR) << "Could not load item at offset " << next_read_offset_ << " of size " << to_read
               << " from disk with error: " << result.error().value() << " "
               << result.error().message();
    return result.error();
  }

  VLOG(2) << "Loaded item with offset " << next_read_offset_ << " of size " << to_read
          << " for connection " << this;

  next_read_offset_ += bytes.size();
  total_backing_bytes_ -= bytes.size();

  return {};
}

}  // namespace facade
