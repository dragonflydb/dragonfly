// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tiering/disk_storage.h"

#include "base/io_buf.h"
#include "base/logging.h"
#include "server/error.h"
#include "server/tiering/common.h"

namespace dfly::tiering {

std::error_code DiskStorage::Open(std::string_view path) {
  RETURN_ON_ERR(io_mgr_.Open(path));
  alloc_.AddStorage(0, io_mgr_.Span());
  return {};
}

void DiskStorage::Close() {
  io_mgr_.Shutdown();
}

void DiskStorage::Read(DiskSegment segment, ReadCb cb) {
  DCHECK_GT(segment.length, 0u);
  DCHECK_EQ(segment.offset % kPageSize, 0u);

  // TODO: use registered buffers (UringProactor::RegisterBuffers)
  // TODO: Make it error safe, don't leak if cb isn't called
  uint8_t* buf = new uint8_t[segment.length];
  auto io_cb = [cb, buf, segment](int res) {
    cb(std::string_view{reinterpret_cast<char*>(buf), segment.length});
    delete[] buf;  // because std::function needs to be copyable, unique_ptr can't be used
  };
  io_mgr_.ReadAsync(segment.offset, {buf, segment.length}, std::move(io_cb));
}

void DiskStorage::MarkAsFree(DiskSegment segment) {
  DCHECK_GT(segment.length, 0u);
  DCHECK_EQ(segment.offset % kPageSize, 0u);

  alloc_.Free(segment.offset, segment.length);
}

std::error_code DiskStorage::Stash(io::Bytes bytes, StashCb cb) {
  DCHECK_GT(bytes.length(), 0u);

  int64_t offset = alloc_.Malloc(bytes.size());

  // If we've run out of space, block and grow as much as needed
  if (offset < 0) {
    size_t start = io_mgr_.Span();
    size_t grow_size = -offset;
    RETURN_ON_ERR(io_mgr_.Grow(grow_size));

    alloc_.AddStorage(start, grow_size);
    offset = alloc_.Malloc(bytes.size());

    if (offset < 0)  // we can't fit it even after resizing
      return std::make_error_code(std::errc::file_too_large);
  }

  auto io_cb = [this, cb, offset, len = bytes.size()](int io_res) {
    if (io_res < 0) {
      MarkAsFree({size_t(offset), len});
      cb({});
    } else {
      cb({size_t(offset), len});
    }
  };

  return io_mgr_.WriteAsync(offset, io::View(bytes), std::move(io_cb));
}

DiskStorage::Stats DiskStorage::GetStats() const {
  return {alloc_.allocated_bytes()};
}

}  // namespace dfly::tiering
