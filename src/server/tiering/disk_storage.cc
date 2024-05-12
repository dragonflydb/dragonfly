// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tiering/disk_storage.h"

#include <system_error>

#include "base/flags.h"
#include "base/logging.h"
#include "io/io_buf.h"
#include "server/error.h"
#include "server/tiering/common.h"
#include "util/fibers/uring_proactor.h"

using namespace ::dfly::tiering::literals;

ABSL_FLAG(uint64_t, registered_buffer_size, 512_KB,
          "Size of registered buffer for IoUring fixed read/writes");

namespace dfly::tiering {

using namespace ::util::fb2;

namespace {

UringBuf AllocateTmpBuf(size_t size) {
  size = (size + kPageSize - 1) / kPageSize * kPageSize;
  VLOG(1) << "Fallback to temporary allocation: " << size;

  uint8_t* buf = new (std::align_val_t(kPageSize)) uint8_t[size];
  return UringBuf{{buf, size}, std::nullopt};
}

void DestroyTmpBuf(UringBuf buf) {
  DCHECK(!buf.buf_idx);
  ::operator delete[](buf.bytes.data(), std::align_val_t(kPageSize));
}

UringBuf PrepareBuf(size_t size) {
  DCHECK_EQ(ProactorBase::me()->GetKind(), ProactorBase::IOURING);
  auto* up = static_cast<UringProactor*>(ProactorBase::me());

  UringBuf buf;
  if (auto borrowed = up->RequestBuffer(size); borrowed)
    return *borrowed;
  else
    return AllocateTmpBuf(size);
}

void ReturnBuf(UringBuf buf) {
  DCHECK_EQ(ProactorBase::me()->GetKind(), ProactorBase::IOURING);
  auto* up = static_cast<UringProactor*>(ProactorBase::me());

  if (buf.buf_idx)
    up->ReturnBuffer(buf);
  else
    DestroyTmpBuf(buf);
}

}  // anonymous namespace

DiskStorage::DiskStorage(size_t max_size) : max_size_(max_size) {
}

std::error_code DiskStorage::Open(std::string_view path) {
  RETURN_ON_ERR(io_mgr_.Open(path));
  alloc_.AddStorage(0, io_mgr_.Span());

  DCHECK_EQ(ProactorBase::me()->GetKind(), ProactorBase::IOURING);
  auto* up = static_cast<UringProactor*>(ProactorBase::me());

  if (int io_res = up->RegisterBuffers(absl::GetFlag(FLAGS_registered_buffer_size)); io_res < 0)
    return std::error_code{-io_res, std::system_category()};

  return {};
}

void DiskStorage::Close() {
  io_mgr_.Shutdown();
}

void DiskStorage::Read(DiskSegment segment, ReadCb cb) {
  DCHECK_GT(segment.length, 0u);
  DCHECK_EQ(segment.offset % kPageSize, 0u);

  UringBuf buf = PrepareBuf(segment.length);
  auto io_cb = [cb = std::move(cb), buf, segment](int io_res) {
    if (io_res < 0)
      cb("", std::error_code{-io_res, std::system_category()});
    else
      cb(std::string_view{reinterpret_cast<char*>(buf.bytes.data()), segment.length}, {});
    ReturnBuf(buf);
  };
  io_mgr_.ReadAsync(segment.offset, buf, std::move(io_cb));
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

    if (alloc_.capacity() + grow_size >= max_size_)
      return std::make_error_code(std::errc::no_space_on_device);

    RETURN_ON_ERR(io_mgr_.Grow(grow_size));

    alloc_.AddStorage(start, grow_size);
    offset = alloc_.Malloc(bytes.size());

    if (offset < 0)  // we can't fit it even after resizing
      return std::make_error_code(std::errc::file_too_large);
  }

  UringBuf buf = PrepareBuf(bytes.size());
  memcpy(buf.bytes.data(), bytes.data(), bytes.length());

  auto io_cb = [this, cb, offset, buf, len = bytes.size()](int io_res) {
    if (io_res < 0) {
      MarkAsFree({size_t(offset), len});
      cb({}, std::error_code{-io_res, std::system_category()});
    } else {
      cb({size_t(offset), len}, {});
    }
    ReturnBuf(buf);
  };

  io_mgr_.WriteAsync(offset, buf, std::move(io_cb));
  return {};
}

DiskStorage::Stats DiskStorage::GetStats() const {
  return {alloc_.allocated_bytes(), alloc_.capacity()};
}

}  // namespace dfly::tiering
