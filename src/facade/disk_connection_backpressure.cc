// Copyright 2025, DragonflyDB authors.  All rights reserved.
//
// See LICENSE for licensing terms.
//

#include "facade/disk_connection_backpressure.h"

#include <absl/strings/str_cat.h>

#include <string>

#include "base/flags.h"
#include "base/logging.h"
#include "facade/facade_types.h"
#include "io/io.h"
#include "util/fibers/uring_file.h"

using facade::operator""_MB;

ABSL_FLAG(std::string, disk_backpressure_folder, "/tmp/",
          "Folder to store "
          "disk backed connection backpressure");

ABSL_FLAG(size_t, disk_backpressure_file_max_bytes, 50_MB,
          "Maximum size of the backing file. When max size is reached, connection will "
          "stop offloading backpressure to disk and block on client read.");

ABSL_FLAG(size_t, disk_backpressure_load_size, 30,
          "How many items to load in dispatch queue from the disk backed file.");

namespace facade {

DiskBackedBackpressureQueue::DiskBackedBackpressureQueue(uint32_t conn_id)
    : max_backing_size_(absl::GetFlag(FLAGS_disk_backpressure_file_max_bytes)),
      max_queue_load_size_(absl::GetFlag(FLAGS_disk_backpressure_load_size)),
      id_(conn_id) {
}

std::error_code DiskBackedBackpressureQueue::Init() {
  std::string backing_name = absl::StrCat(absl::GetFlag(FLAGS_disk_backpressure_folder), id_);
  {
    // Kernel transparently handles buffering via the page cache.
    auto res = util::fb2::OpenWrite(backing_name, {} /* overwrite mode + non direct io */);
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

DiskBackedBackpressureQueue::~DiskBackedBackpressureQueue() {
  auto ec = writer_->Close();
  LOG_IF(WARNING, ec) << ec.message();
  ec = reader_->Close();
  LOG_IF(WARNING, ec) << ec.message();
}

// Check if backing file is empty, i.e. backing file has 0 bytes.
bool DiskBackedBackpressureQueue::Empty() const {
  return total_backing_bytes_ == 0;
}

bool DiskBackedBackpressureQueue::HasEnoughBackingSpaceFor(size_t bytes) const {
  return (bytes + total_backing_bytes_) < max_backing_size_;
}

size_t DiskBackedBackpressureQueue::TotalInMemoryBytes() const {
  return offsets_.size() * sizeof(ItemOffset);
}

void DiskBackedBackpressureQueue::OffloadToBacking(std::string_view blob) {
  ItemOffset item;
  item.offset = next_offset_;
  item.total_bytes = blob.size();

  // TODO we should truncate as the file grows. That way we never end up with large files
  // on disk.
  auto res = writer_->Write(blob);
  if (res) {
    VLOG(2) << "Failed to offload connection " << this << " backpressure with offset "
            << item.offset << " of size " << item.total_bytes << " to backing with error: " << res;
    return;
  }

  total_backing_bytes_ += blob.size();
  offsets_.push_back(item);
  next_offset_ += item.total_bytes;

  VLOG(2) << "Offload connection " << this << " backpressure of " << item.total_bytes
          << " bytes to disk at offset: " << item.offset;
  VLOG(3) << "Command offloaded: " << blob;
}

void DiskBackedBackpressureQueue::LoadFromBacking(std::function<void(io::MutableBytes)> f) {
  std::string buffer;
  size_t up_to = max_queue_load_size_;

  while (!offsets_.empty() && up_to--) {
    ItemOffset item = offsets_.front();

    buffer.resize(item.total_bytes);

    io::MutableBytes bytes{reinterpret_cast<uint8_t*>(buffer.data()), item.total_bytes};
    auto result = reader_->Read(item.offset, bytes);
    if (!result) {
      LOG(ERROR) << "Could not load item at offset " << item.offset << " of size "
                 << item.total_bytes << " from disk with error: " << result.error().value() << " "
                 << result.error().message();
      return;
    }

    VLOG(2) << "Loaded item with offset " << item.offset << " of size " << item.total_bytes
            << " for connection " << this;

    f(bytes);

    offsets_.pop_front();
    total_backing_bytes_ -= item.total_bytes;
  }
}

}  // namespace facade
