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
}

std::error_code DiskBackedQueue::Close() {
  if (writer_ && reader_) {
    auto ec = writer_->Close();
    LOG_IF(WARNING, ec) << ec.message();

    auto ec2 = reader_->Close();
    LOG_IF(WARNING, ec2) << ec.message();
    // Return the first error.
    return ec ? ec : ec2;
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

std::error_code DiskBackedQueue::Write(io::Bytes bytes) {
  auto ec = writer_->Write(bytes);
  if (ec) {
    VLOG(2) << "Failed to offload blob of size " << bytes.size()
            << " to backing with error: " << ec;
    return ec;
  }

  total_backing_bytes_ += bytes.size();

  VLOG(2) << "Offload connection " << this << " backpressure of " << bytes.size();
  return {};
}

io::Result<size_t> DiskBackedQueue::ReadTo(io::MutableBytes out) {
  const size_t k_read_size = 4096;
  const size_t to_read = std::min(std::min(k_read_size, total_backing_bytes_), out.size());

  auto result = reader_->Read(next_read_offset_, out);
  if (!result) {
    LOG(ERROR) << "Could not load item at offset " << next_read_offset_ << " of size " << to_read
               << " from disk with error: " << result.error().value() << " "
               << result.error().message();
    return result;
  }

  VLOG(2) << "Loaded item with offset " << next_read_offset_ << " of size " << to_read
          << " for connection " << this;

  next_read_offset_ += to_read;
  total_backing_bytes_ -= to_read;

  return {to_read};
}

}  // namespace facade
