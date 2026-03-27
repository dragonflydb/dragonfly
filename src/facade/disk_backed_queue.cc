// Copyright 2026, DragonflyDB authors.  All rights reserved.
//
// See LICENSE for licensing terms.
//

#include "facade/disk_backed_queue.h"

#include "base/logging.h"

#ifdef __linux__

#include <absl/strings/str_cat.h>
#include <fcntl.h>

#include <cerrno>
#include <cstring>
#include <string>

#include "base/flags.h"
#include "facade/facade_types.h"
#include "util/fibers/uring_file.h"
#include "util/fibers/uring_proactor.h"

using facade::operator""_MB;

ABSL_FLAG(std::string, disk_backpressure_folder, "/tmp/",
          "Folder to store disk-backed connection backpressure");

ABSL_FLAG(size_t, disk_backpressure_file_max_bytes, 50_MB,
          "Maximum size of the backing file. When max size is reached, connection will "
          "stop offloading backpressure to disk and block on client read.");

namespace facade {

struct DiskBackedQueue::Impl {
  std::unique_ptr<util::fb2::LinuxFile> file;
  size_t write_offset = 0;
  size_t total_backing_bytes = 0;
  size_t next_read_offset = 0;
  // Tracks how far into the file holes have been punched (always 4096-aligned).
  size_t punch_offset = 0;
  size_t in_flight_callbacks = 0;

  // Punch holes over the aligned region we have fully read past so the OS can reclaim pages.
  void MaybePunchHole() {
    const size_t aligned_end = (next_read_offset / 4096) * 4096;
    if (aligned_end > punch_offset) {
      int res = fallocate(file->GetFd(), FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, punch_offset,
                          aligned_end - punch_offset);
      DCHECK_EQ(res, 0) << "fallocate punch failed: " << strerror(errno);
      punch_offset = aligned_end;
    }
  }
};

DiskBackedQueue::DiskBackedQueue(uint32_t conn_id)
    : impl_(std::make_unique<Impl>()),
      max_backing_size_(absl::GetFlag(FLAGS_disk_backpressure_file_max_bytes)),
      id_(conn_id) {
}

DiskBackedQueue::~DiskBackedQueue() {
  DCHECK_EQ(impl_->in_flight_callbacks, 0ul);
}

std::error_code DiskBackedQueue::Init() {
  std::string backing_name = absl::StrCat(absl::GetFlag(FLAGS_disk_backpressure_folder), id_);
  // Open a single O_RDWR file so the same fd serves writes, reads, and fallocate punch holes.
  auto res = util::fb2::OpenLinux(backing_name, O_RDWR | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
  if (!res) {
    return res.error();
  }
  impl_->file = std::move(*res);

  VLOG(3) << "Created backing for connection " << this << " " << backing_name;

  return {};
}

std::error_code DiskBackedQueue::Close() {
  if (impl_->file) {
    auto ec = impl_->file->Close();
    LOG_IF(WARNING, ec) << ec.message();

    std::string backing = absl::StrCat(absl::GetFlag(FLAGS_disk_backpressure_folder), id_);
    int errc = unlink(backing.c_str());
    LOG_IF(ERROR, errc == -1) << "Failed to unlink backing file: "
                              << std::error_code{errno, std::system_category()};
    return ec;
  }

  return {};
}

bool DiskBackedQueue::Empty() const {
  return impl_->total_backing_bytes == 0;
}

size_t DiskBackedQueue::TotalBytes() const {
  return total_backing_bytes_;
}

bool DiskBackedQueue::HasEnoughBackingSpaceFor(size_t bytes) const {
  return (bytes + impl_->total_backing_bytes) < max_backing_size_;
}

void DiskBackedQueue::PushAsync(io::Bytes bytes, AsyncPushCallback cb) {
  const size_t offset = impl_->write_offset;
  const size_t size = bytes.size();
  ++impl_->in_flight_callbacks;

  impl_->file->WriteAsync(bytes, offset, [this, size, cb = std::move(cb)](int res) {
    --impl_->in_flight_callbacks;
    if (res < 0) {
      std::error_code ec{-res, std::system_category()};
      VLOG(2) << "Failed to offload blob of size " << size << " to backing with error: " << ec;
      cb(ec);
      return;
    }

    impl_->write_offset += size;
    impl_->total_backing_bytes += size;
    VLOG(2) << "Offload connection " << this << " backpressure of " << size;
    cb({});
  });
}

void DiskBackedQueue::PopAsync(io::MutableBytes out, AsyncPopCallback cb) {
  const size_t to_read = std::min(impl_->total_backing_bytes, out.size());
  const size_t offset = impl_->next_read_offset;
  ++impl_->in_flight_callbacks;

  io::MutableBytes read_buf = out.subspan(0, to_read);

  impl_->file->ReadAsync(read_buf, offset, [this, to_read, offset, cb = std::move(cb)](int res) {
    --impl_->in_flight_callbacks;
    if (res < 0) {
      std::error_code ec{-res, std::system_category()};
      LOG(ERROR) << "Could not load item at offset " << offset << " of size " << to_read
                 << " from disk with error: " << ec.value() << " " << ec.message();
      cb(nonstd::make_unexpected(ec));
      return;
    }

    size_t bytes_read = static_cast<size_t>(res);
    impl_->next_read_offset += bytes_read;
    impl_->total_backing_bytes -= bytes_read;

    VLOG(2) << "Loaded item with offset " << offset << " of size " << bytes_read
            << " for connection " << this;

    impl_->MaybePunchHole();

    cb(bytes_read);
  });
}

}  // namespace facade

#else  // __linux__

namespace facade {

struct DiskBackedQueue::Impl {};

DiskBackedQueue::DiskBackedQueue(uint32_t conn_id)
    : impl_(std::make_unique<Impl>()), max_backing_size_(0), id_(conn_id) {
}

DiskBackedQueue::~DiskBackedQueue() = default;

std::error_code DiskBackedQueue::Init() {
  return std::make_error_code(std::errc::function_not_supported);
}

bool DiskBackedQueue::HasEnoughBackingSpaceFor(size_t) const {
  return false;
}

bool DiskBackedQueue::Empty() const {
  return true;
}

std::error_code DiskBackedQueue::Close() {
  return {};
}

void DiskBackedQueue::PushAsync(io::Bytes, AsyncPushCallback cb) {
  cb(std::make_error_code(std::errc::function_not_supported));
}

void DiskBackedQueue::PopAsync(io::MutableBytes, AsyncPopCallback cb) {
  cb(nonstd::make_unexpected(std::make_error_code(std::errc::function_not_supported)));
}

}  // namespace facade

#endif  // __linux__
