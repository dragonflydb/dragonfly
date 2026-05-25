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
#include <deque>
#include <string>
#include <utility>

#include "base/flags.h"
#include "facade/facade_types.h"
#include "util/fibers/uring_file.h"
#include "util/fibers/uring_proactor.h"

using facade::operator""_MB;
using facade::operator""_KB;

ABSL_FLAG(std::string, disk_backpressure_folder, "/tmp/",
          "Folder to store disk-backed connection backpressure");

ABSL_FLAG(size_t, disk_backpressure_file_max_bytes, 50_MB,
          "Maximum size of the backing file. When max size is reached, connection will "
          "stop offloading backpressure to disk and block on client read.");

ABSL_FLAG(size_t, disk_backpressure_hysteresis_trigger, 16_KB,
          "Low-water mark for the disk-backed queue hysteresis band. Once the high-water "
          "mark (disk_backpressure_hysteresis_arm) has been crossed, new socket reads are "
          "blocked when the queue shrinks below this threshold, ensuring a clean drain-to-"
          "memory transition without new writes racing the tail reads. Defaults to 2× "
          "kMaxChunkSize (16 KB).");

ABSL_FLAG(size_t, disk_backpressure_hysteresis_arm, 256_KB,
          "High-water mark for the disk-backed queue hysteresis band. The queue must "
          "accumulate at least this many bytes before the low-water drain phase can "
          "activate. Prevents a tiny transient queue from ever triggering the drain-to-"
          "memory block. Must be larger than disk_backpressure_hysteresis_trigger. "
          "Defaults to 256 KB.");

namespace facade {

struct DiskBackedQueue::Impl {
  std::unique_ptr<util::fb2::LinuxFile> file;
  size_t write_offset = 0;
  size_t total_backing_bytes = 0;  // bytes actually on disk, available to pop
  size_t queued_bytes = 0;         // bytes in write_queue not yet written to disk
  size_t next_read_offset = 0;
  // Tracks how far into the file holes have been punched (always 4096-aligned).
  size_t punch_offset = 0;
  size_t in_flight_callbacks = 0;
  bool write_in_flight = false;
  bool pop_in_flight = false;
  // Set by Cancel(). In-flight CQE callbacks skip accounting and chaining when true.
  bool cancelled = false;
  // Set when total bytes cross hysteresis_arm on the way up; cleared when the
  // queue fully empties. Together with hysteresis_trigger_ this forms the two-level
  // hysteresis band that governs draining mode.
  bool hysteresis_armed = false;
  size_t hysteresis_arm = 0;

  struct PendingWrite {
    Chunk chunk;
    AsyncPushCallback cb;
  };
  std::deque<PendingWrite> write_queue;

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

  void MaybeFlushQueue() {
    if (cancelled || write_in_flight || write_queue.empty())
      return;

    write_in_flight = true;

    PendingWrite pw = std::move(write_queue.front());
    write_queue.pop_front();

    const uint8_t* data_ptr = pw.chunk.data.data();
    const size_t size = pw.chunk.data.size();
    const size_t offset = write_offset;

    file->WriteAsync({data_ptr, size}, offset, [this, pw = std::move(pw), size](int res) mutable {
      --in_flight_callbacks;
      write_in_flight = false;

      std::error_code ec;
      if (res < 0) {
        ec = {-res, std::system_category()};
        LOG(ERROR) << "Failed to offload chunk of size " << size
                   << " to backing with error: " << ec;
        cancelled = true;
      } else if (!cancelled) {
        write_offset += size;
        total_backing_bytes += size;
        queued_bytes -= size;
        if (!hysteresis_armed && (total_backing_bytes + queued_bytes) >= hysteresis_arm)
          hysteresis_armed = true;
      }

      if (cancelled)
        ec = std::make_error_code(std::errc::operation_canceled);

      VLOG(2) << "Write CQE done: cancelled=" << cancelled << " write_in_flight=" << write_in_flight
              << " write_queue.size=" << write_queue.size() << " pop_in_flight=" << pop_in_flight;

      // Chain next write before invoking cb, so IsActive() reflects
      // the true state when the callback notifies the connection.
      // MaybeFlushQueue is a no-op when cancelled.
      if (!ec && !write_queue.empty())
        MaybeFlushQueue();

      pw.cb(ec);
    });
    ++in_flight_callbacks;
  }
};

DiskBackedQueue::DiskBackedQueue(uint32_t conn_id)
    : impl_(std::make_unique<Impl>()),
      max_backing_size_(absl::GetFlag(FLAGS_disk_backpressure_file_max_bytes)),
      hysteresis_trigger_(absl::GetFlag(FLAGS_disk_backpressure_hysteresis_trigger)),
      id_(conn_id) {
  impl_->hysteresis_arm = absl::GetFlag(FLAGS_disk_backpressure_hysteresis_arm);
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
    LOG_IF(ERROR, errc != 0) << "Failed to unlink backing file: "
                             << std::error_code{errc, std::system_category()};
    return ec;
  }

  return {};
}

bool DiskBackedQueue::Empty() const {
  return impl_->total_backing_bytes == 0;
}

void DiskBackedQueue::Cancel() {
  impl_->cancelled = true;
  impl_->queued_bytes = 0;
  impl_->total_backing_bytes = 0;
  // Drain items that were queued but never submitted to io_uring.
  // MaybeFlushQueue won't process them once cancelled is set, so we must
  // invoke their callbacks here to avoid leaving IsActive() permanently true.
  VLOG(2) << "DiskQueue Cancel: flushing write_queue.size=" << impl_->write_queue.size()
          << " write_in_flight=" << impl_->write_in_flight
          << " pop_in_flight=" << impl_->pop_in_flight;
  auto ec = std::make_error_code(std::errc::operation_canceled);
  for (auto& pw : impl_->write_queue)
    pw.cb(ec);
  impl_->write_queue.clear();
}

bool DiskBackedQueue::IsActive() const {
  return (!impl_->cancelled && impl_->total_backing_bytes > 0) || !impl_->write_queue.empty() ||
         impl_->write_in_flight || impl_->pop_in_flight;
}

bool DiskBackedQueue::IsDraining() const {
  if (!impl_->hysteresis_armed)
    return false;
  return (impl_->total_backing_bytes + impl_->queued_bytes) < hysteresis_trigger_;
}

bool DiskBackedQueue::IsPopInFlight() const {
  return impl_->pop_in_flight;
}

bool DiskBackedQueue::HasEnoughBackingSpaceFor(size_t bytes) const {
  return (bytes + impl_->total_backing_bytes + impl_->queued_bytes) < max_backing_size_;
}

bool DiskBackedQueue::CanPush(size_t bytes) const {
  if (impl_->cancelled)
    return false;
  if (IsDraining())
    return false;
  return HasEnoughBackingSpaceFor(bytes);
}

void DiskBackedQueue::PushAsync(Chunk chunk, AsyncPushCallback cb) {
  DCHECK(!chunk.data.empty());

  impl_->queued_bytes += chunk.data.size();
  impl_->write_queue.push_back({std::move(chunk), std::move(cb)});
  VLOG(3) << "PushAsync: queued_bytes=" << impl_->queued_bytes
          << " write_queue.size=" << impl_->write_queue.size()
          << " write_in_flight=" << impl_->write_in_flight << " armed=" << impl_->hysteresis_armed;
  impl_->MaybeFlushQueue();
}

void DiskBackedQueue::PopAsync(io::MutableBytes out, AsyncPopCallback cb) {
  DCHECK(!impl_->pop_in_flight);
  DCHECK(!impl_->cancelled);
  DCHECK_GT(impl_->total_backing_bytes, 0u);

  const size_t to_read = std::min(impl_->total_backing_bytes, out.size());
  const size_t offset = impl_->next_read_offset;
  ++impl_->in_flight_callbacks;
  impl_->pop_in_flight = true;

  io::MutableBytes read_buf = out.subspan(0, to_read);

  impl_->file->ReadAsync(read_buf, offset, [this, to_read, offset, cb = std::move(cb)](int res) {
    --impl_->in_flight_callbacks;
    impl_->pop_in_flight = false;

    if (impl_->cancelled) {
      cb(nonstd::make_unexpected(std::make_error_code(std::errc::operation_canceled)));
      return;
    }

    if (res < 0) {
      std::error_code ec{-res, std::system_category()};
      LOG(ERROR) << "Could not load item at offset " << offset << " of size " << to_read
                 << " from disk with error: " << ec.value() << " " << ec.message();
      impl_->cancelled = true;
      cb(nonstd::make_unexpected(ec));
      return;
    }

    size_t bytes_read = static_cast<size_t>(res);
    impl_->next_read_offset += bytes_read;
    impl_->total_backing_bytes -= bytes_read;

    // Reset hysteresis once the queue is fully drained (nothing on disk, nothing pending write).
    // This re-arms the high-water mark check so the next growth cycle starts fresh.
    if (impl_->total_backing_bytes == 0 && impl_->write_queue.empty() && !impl_->write_in_flight)
      impl_->hysteresis_armed = false;

    VLOG(2) << "PopAsync done: bytes_read=" << bytes_read
            << " total_backing=" << impl_->total_backing_bytes
            << " armed=" << impl_->hysteresis_armed << " cancelled=" << impl_->cancelled;

    impl_->MaybePunchHole();

    cb(bytes_read);
  });
}

}  // namespace facade

#else  // __linux__

namespace facade {

struct DiskBackedQueue::Impl {};

DiskBackedQueue::DiskBackedQueue(uint32_t conn_id)
    : impl_(std::make_unique<Impl>()), max_backing_size_(0), hysteresis_trigger_(0), id_(conn_id) {
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

bool DiskBackedQueue::IsActive() const {
  return false;
}

bool DiskBackedQueue::IsDraining() const {
  return false;
}

bool DiskBackedQueue::IsPopInFlight() const {
  return false;
}

bool DiskBackedQueue::CanPush(size_t) const {
  return false;
}

void DiskBackedQueue::Cancel() {
}

std::error_code DiskBackedQueue::Close() {
  return {};
}

void DiskBackedQueue::PushAsync(Chunk, AsyncPushCallback cb) {
  cb(std::make_error_code(std::errc::function_not_supported));
}

void DiskBackedQueue::PopAsync(io::MutableBytes, AsyncPopCallback cb) {
  cb(nonstd::make_unexpected(std::make_error_code(std::errc::function_not_supported)));
}

}  // namespace facade

#endif  // __linux__
