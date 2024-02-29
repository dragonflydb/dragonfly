#include "server/tiering/disk_storage.h"

#include "base/logging.h"

namespace dfly::tiering {

void DiskStorage::Open(std::string_view path) {
  auto ec = io_mgr_.Open(std::string{path});
  CHECK(!ec);
  alloc_.AddStorage(0, io_mgr_.Span());
}

void DiskStorage::Shutdown() {
  io_mgr_.Shutdown();
}

std::string DiskStorage::Read(BlobLocator loc) {
  std::string out(loc.len, '\0');
  io_mgr_.Read(loc.offset, io::MutableBytes(reinterpret_cast<uint8_t*>(out.data()), out.size()));
  return out;
}

BlobLocator DiskStorage::Store(std::string_view blob) {
  int64_t offset = alloc_.Malloc(blob.size());
  while (offset < 0) {
    util::fb2::Done done;
    size_t start = io_mgr_.Span();
    auto cb = [start, grow_size = -offset, this, done](int io_res) mutable {
      if (io_res == 0)
        alloc_.AddStorage(start, grow_size);
      done.Notify();
    };

    std::error_code ec = io_mgr_.GrowAsync(-offset, std::move(cb));
    CHECK(!ec);
    done.Wait();

    offset = alloc_.Malloc(blob.size());
  }

  util::fb2::Done done;
  io_mgr_.WriteAsync(offset, blob, [done](int) mutable { done.Notify(); });
  done.Wait();

  VLOG(0) << "Disk store stored " << blob << " at " << offset << " " << blob.size();
  return {offset, blob.size()};
}

void DiskStorage::Delete(BlobLocator loc) {
  alloc_.Free(loc.offset, loc.len);
}

}  // namespace dfly::tiering