// Copyright 2026, DragonflyDB authors.  All rights reserved.
//
// See LICENSE for licensing terms.
//

#include "facade/disk_backed_queue.h"

#include <absl/strings/str_cat.h>
#include <fcntl.h>

#include <cerrno>
#include <cstring>
#include <string>

#include "base/flags.h"
#include "base/logging.h"
#include "facade/facade_types.h"
#include "io/io.h"
#include "util/fibers/uring_file.h"
#include "util/fibers/uring_proactor.h"

using facade::operator""_MB;

ABSL_FLAG(std::string, disk_backpressure_folder, "/tmp/",
          "Folder to store disk-backed connection backpressure");

ABSL_FLAG(size_t, disk_backpressure_file_max_bytes, 50_MB,
          "Maximum size of the backing file. When max size is reached, connection will "
          "stop offloading backpressure to disk and block on client read.");

namespace facade {

DiskBackedQueue::DiskBackedQueue(uint32_t conn_id)
    : max_backing_size_(absl::GetFlag(FLAGS_disk_backpressure_file_max_bytes)), id_(conn_id) {
}

std::error_code DiskBackedQueue::Init() {
  std::string backing_name = absl::StrCat(absl::GetFlag(FLAGS_disk_backpressure_folder), id_);
  // Open a single O_RDWR file so the same fd serves writes, reads, and fallocate punch holes.
  // Kernel transparently handles buffering via the page cache.
  auto res = util::fb2::OpenLinux(backing_name, O_RDWR | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
  if (!res) {
    return res.error();
  }
  file_ = std::move(*res);

  VLOG(3) << "Created backing for connection " << this << " " << backing_name;

  return {};
}

DiskBackedQueue::~DiskBackedQueue() {
}

std::error_code DiskBackedQueue::Close() {
  if (file_) {
    auto ec = file_->Close();
    LOG_IF(WARNING, ec) << ec.message();

    std::string backing = absl::StrCat(absl::GetFlag(FLAGS_disk_backpressure_folder), id_);
    int errc = unlink(backing.c_str());
    LOG_IF(ERROR, errc != 0) << "Failed to unlink backing file: "
                             << std::error_code{errc, std::system_category()};
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

void DiskBackedQueue::MaybePunchHole() {
  // Punch holes over the aligned region we have fully read past so the OS can reclaim pages.
  // Both offset and length must be multiples of the filesystem block size: XFS returns EINVAL
  // otherwise, and ext4/tmpfs only zero partial blocks rather than freeing them.
  // We assume 4096-byte blocks (correct for virtually all deployments); a fully robust
  // implementation would query the actual block size via fstatfs(file_->GetFd(), &fsst) and
  // align to fsst.f_bsize instead.
  const size_t aligned_end = (next_read_offset_ / 4096) * 4096;
  if (aligned_end > punch_offset_) {
    int res = fallocate(file_->GetFd(), FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, punch_offset_,
                        aligned_end - punch_offset_);
    DCHECK_EQ(res, 0) << "fallocate punch failed: " << strerror(errno);
    punch_offset_ = aligned_end;
  }
}

void DiskBackedQueue::PushAsync(io::Bytes bytes, AsyncPushCallback cb) {
  const size_t offset = write_offset_;
  const size_t size = bytes.size();

  file_->WriteAsync(bytes, offset, [this, size, cb = std::move(cb)](int res) {
    if (res < 0) {
      std::error_code ec{-res, std::system_category()};
      VLOG(2) << "Failed to offload blob of size " << size << " to backing with error: " << ec;
      cb(ec);
      return;
    }

    write_offset_ += size;
    total_backing_bytes_ += size;
    VLOG(2) << "Offload connection " << this << " backpressure of " << size;
    cb({});
  });
}

void DiskBackedQueue::PopAsync(io::MutableBytes out, AsyncPopCallback cb) {
  const size_t to_read = std::min(total_backing_bytes_, out.size());
  const size_t offset = next_read_offset_;

  // Capture a subset of out for the actual read size
  io::MutableBytes read_buf = out.subspan(0, to_read);

  file_->ReadAsync(read_buf, offset, [this, to_read, offset, cb = std::move(cb)](int res) {
    if (res < 0) {
      std::error_code ec{-res, std::system_category()};
      LOG(ERROR) << "Could not load item at offset " << offset << " of size " << to_read
                 << " from disk with error: " << ec.value() << " " << ec.message();
      cb(nonstd::make_unexpected(ec));
      return;
    }

    size_t bytes_read = static_cast<size_t>(res);
    next_read_offset_ += bytes_read;
    total_backing_bytes_ -= bytes_read;

    VLOG(2) << "Loaded item with offset " << offset << " of size " << bytes_read
            << " for connection " << this;

    MaybePunchHole();

    cb(bytes_read);
  });
}

}  // namespace facade
