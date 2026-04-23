// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/container/inlined_vector.h>

#include <variant>

#include "base/function2.hpp"
#include "server/tiering/common.h"
#include "server/tiering/decoders.h"
#include "server/tiering/disk_storage.h"
#include "server/tiering/entry_map.h"
#include "util/fibers/future.h"

namespace dfly::tiering {

// Manages READ/DELETE/STASH operations on top of a DiskStorage.
// Implicitly combines reads with different offsets on the same 4kb page,
// safely schedules deletes after reads and allows cancelling pending stashes
class OpManager {
 public:
  struct Stats {
    DiskStorage::Stats disk_stats;

    size_t pending_read_cnt = 0;
    size_t pending_stash_cnt = 0;
  };

  using KeyRef = ::dfly::tiering::KeyRef;
  using ListNodeId = ::dfly::tiering::ListNodeId;

  using PendingId = ::dfly::tiering::PendingId;

  explicit OpManager(size_t max_size);
  virtual ~OpManager();

  // Open file with underlying disk storage, must be called before use
  std::error_code Open(std::string_view file);

  void Close();

  using ReadCallback =
      fu2::function_base<true /*owns*/, false /*moveable*/, fu2::capacity_fixed<40, 8>,
                         false /* non-throwing*/, false /* strong exceptions guarantees*/,
                         void(io::Result<Decoder*>)>;

  // Enqueue callback to be executed once value is read. Trigger read if none is pending yet for
  // this segment. Multiple entries can be obtained from a single segment, but every distinct id
  // will have it's own independent callback loop that can safely modify the underlying value
  void Enqueue(PendingId id, DiskSegment segment, const Decoder& decoder, ReadCallback cb,
               bool read_only = true);

  // Returns true if there is a pending modification for the given segment.
  bool HasModificationPending(DiskSegment segment) const;

  // Cancel entry with pending io
  void CancelPending(PendingId id);

  // Delete offloaded entry located at the segment.
  void DeleteOffloaded(DiskSegment segment);

  auto PrepareStash(size_t length) {
    return storage_.PrepareStash(length);
  }

  // Stash value to be offloaded. It is opaque to OpManager.
  void Stash(PendingId id, tiering::DiskSegment segment, util::fb2::RegisteredSlice buf);

  // PrepareStash + Stash via function
  std::error_code PrepareAndStash(
      PendingId id, size_t length,
      const std::function<size_t /*written*/ (io::MutableBytes)>& writer);

  Stats GetStats() const;

 protected:
  using OwnedEntryId = std::variant<uintptr_t, DbKeyId, ListNodeId>;

  // Notify that a stash succeeded and the entry was stored at the provided segment or failed with
  // given error
  virtual void NotifyStashed(const OwnedEntryId& id, const io::Result<DiskSegment>& segment) = 0;

  // Notify that an entry was successfully fetched. Includes whether entry was modified.
  // Returns true if value needs to be deleted from the storage.
  virtual bool NotifyFetched(const OwnedEntryId& id, DiskSegment segment, Decoder*) = 0;

  // Notify delete. Return true if the filled segment needs to be marked as free.
  virtual bool NotifyDelete(DiskSegment segment) = 0;

  // Describes pending read futures for a single entry
  struct EntryOps {
    EntryOps(OwnedEntryId id, DiskSegment segment, const Decoder& decoder, bool read_only);

    // unique identifier for the entry being read. Used to notify higher layers.
    OwnedEntryId id;

    // For multi-bin reads is a precise segment of the entry within a page.
    DiskSegment segment;

    // We may have multiple callbacks for the same entry.
    absl::InlinedVector<ReadCallback, 1> read_cbs;
    std::unique_ptr<Decoder> decoder;
    bool read_only;
    bool deleting = false;
  };

  // Describes an ongoing read operation for a fixed segment
  struct ReadOp {
    explicit ReadOp(DiskSegment segment) : segment(segment) {
    }

    // Get ops for id or create new
    EntryOps& ForSegment(DiskSegment segment, PendingId id, const Decoder& decoder, bool read_only);

    // Find if there are operations for the given segment, return nullptr otherwise
    EntryOps* Find(DiskSegment segment);
    const EntryOps* Find(DiskSegment segment) const;

    DiskSegment segment;  // spanning segment of whole read

    // enqueued operations for different keys for this segment.
    // Has size() > 1 only for small-bin pages with multiple items, otherwise size() == 1.
    absl::InlinedVector<EntryOps, 1> entry_ops;
  };

  // Prepare read operation for aligned segment or return pending if it exists.
  // Refernce is valid until any other read operations occur.
  ReadOp& PrepareRead(DiskSegment aligned_segment);

  // Called once read finished
  void ProcessRead(size_t offset, io::Result<std::string_view> value);

  // Called once Stash finished
  void ProcessStashed(const OwnedEntryId& id, unsigned version,
                      const io::Result<DiskSegment>& segment);

 private:
  static OwnedEntryId ToOwned(PendingId id);
  static std::string ToString(const OwnedEntryId& id);

  DiskStorage storage_;

  // Pending read operations are keyed by the offset of their aligned segment.
  // This prevents an ABA problem in scenarios like: read (pending) → delete → stash → read.
  // After the stash, the second read targets a different segment offset, so it won't
  // interfere with the first read's pending operation, even for the same PendingId.
  absl::flat_hash_map<size_t /* offset */, ReadOp> pending_reads_;

  size_t pending_stash_counter_ = 0;

  // todo: allow heterogeneous lookups with non owned id
  absl::flat_hash_map<OwnedEntryId, unsigned /* version */> pending_stash_ver_;
};

};  // namespace dfly::tiering
