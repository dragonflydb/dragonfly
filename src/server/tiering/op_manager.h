// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/container/inlined_vector.h>

#include <variant>

#include "server/tiering/common.h"
#include "server/tiering/disk_storage.h"
#include "server/tx_base.h"
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

  using KeyRef = std::pair<DbIndex, std::string_view>;

  // Two separate keyspaces are provided - one for strings, one for numeric identifiers.
  // Ids can be used to track auxiliary values that don't map to real keys (like a page index).
  // Specifically, we track page indexes when serializing small-bin pages with multiple items.
  using EntryId = std::variant<unsigned, KeyRef>;
  using OwnedEntryId = std::variant<unsigned, std::pair<DbIndex, std::string>>;

  // Callback for post-read completion. Returns whether the value was modified.
  // We use fu2 function to allow moveable semantics. The arguments are:
  // bool - true if the string is raw as it was extracted from the prime value.
  // string* - the string that may potentially be modified by the callbacks that subsribed to this
  //           read. The callback run in the same order as the order of invocation, guaranteeing
  //           consistent read after modifications.
  using ReadCallback =
      fu2::function_base<true /*owns*/, false /*moveable*/, fu2::capacity_fixed<40, 8>,
                         false /* non-throwing*/, false /* strong exceptions guarantees*/,
                         bool(bool, std::string*)>;

  explicit OpManager(size_t max_size);
  virtual ~OpManager();

  // Open file with underlying disk storage, must be called before use
  std::error_code Open(std::string_view file);

  void Close();

  // Enqueue callback to be executed once value is read. Trigger read if none is pending yet for
  // this segment. Multiple entries can be obtained from a single segment, but every distinct id
  // will have it's own independent callback loop that can safely modify the underlying value
  void Enqueue(EntryId id, DiskSegment segment, ReadCallback cb);

  // Delete entry with pending io
  void Delete(EntryId id);

  // Delete offloaded entry located at the segment.
  void DeleteOffloaded(DiskSegment segment);

  // Stash (value, footer) to be offloaded. Both arguments are opaque to OpManager.
  std::error_code Stash(EntryId id, std::string_view value, io::Bytes footer);

  Stats GetStats() const;

 protected:
  // Notify that a stash succeeded and the entry was stored at the provided segment or failed with
  // given error
  virtual void NotifyStashed(EntryId id, const io::Result<DiskSegment>& segment) = 0;

  // Notify that an entry was successfully fetched. Includes whether entry was modified.
  // Returns true if value needs to be deleted from the storage.
  virtual bool NotifyFetched(EntryId id, std::string_view value, DiskSegment segment,
                             bool modified) = 0;

  // Notify delete. Return true if the filled segment needs to be marked as free.
  virtual bool NotifyDelete(DiskSegment segment) = 0;

 protected:
  // Describes pending futures for a single entry
  struct EntryOps {
    EntryOps(OwnedEntryId id, DiskSegment segment) : id(std::move(id)), segment(segment) {
    }

    OwnedEntryId id;
    DiskSegment segment;
    absl::InlinedVector<ReadCallback, 1> callbacks;
    bool deleting = false;
  };

  // Describes an ongoing read operation for a fixed segment
  struct ReadOp {
    explicit ReadOp(DiskSegment segment) : segment(segment) {
    }

    // Get ops for id or create new
    EntryOps& ForSegment(DiskSegment segment, EntryId id);

    // Find if there are operations for the given segment, return nullptr otherwise
    EntryOps* Find(DiskSegment segment);

    DiskSegment segment;                       // spanning segment of whole read
    absl::InlinedVector<EntryOps, 1> key_ops;  // enqueued operations for different keys
  };

  // Prepare read operation for aligned segment or return pending if it exists.
  // Refernce is valid until any other read operations occur.
  ReadOp& PrepareRead(DiskSegment aligned_segment);

  // Called once read finished
  void ProcessRead(size_t offset, std::string_view value);

  // Called once Stash finished
  void ProcessStashed(EntryId id, unsigned version, const io::Result<DiskSegment>& segment);

 protected:
  DiskStorage storage_;

  absl::flat_hash_map<size_t /* offset */, ReadOp> pending_reads_;

  size_t pending_stash_counter_ = 0;
  // todo: allow heterogeneous lookups with non owned id
  absl::flat_hash_map<OwnedEntryId, unsigned /* version */> pending_stash_ver_;
};

};  // namespace dfly::tiering
