// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/container/inlined_vector.h>

#include <variant>

#include "server/tiering/common.h"
#include "server/tiering/disk_storage.h"
#include "util/fibers/future.h"

namespace dfly::tiering {

// Manages READ/DELETE/STASH operations on top of a DiskStorage.
// Implicitly combines reads with different offsets on the same 4kb page,
// safely schedules deletes after reads and allows cancelling pending stashes
class OpManager {
 public:
  // Two separate keyspaces are provided - one for strings, one for numeric identifiers.
  // Ids can be used to track auxiliary values that don't map to real keys (like packed pages).
  using EntryId = std::variant<unsigned, std::string_view>;
  using OwnedEntryId = std::variant<unsigned, std::string>;

  OpManager() = default;
  // Open file with underlying disk storage, must be called before use
  std::error_code Open(std::string_view file);

  void Close();

  // Schedule read for offloaded entry that will resolve the future
  util::fb2::Future<std::string> Read(EntryId id, DiskSegment segment);

  // Delete entry with pending io
  void Delete(EntryId id);

  // Delete offloaded entry
  void Delete(DiskSegment segment);

  // Stash value to be offloaded
  std::error_code Stash(EntryId id, std::string_view value);

 protected:
  // Report that a stash succeeded and the entry was stored at the provided segment
  virtual void ReportStashed(EntryId id, DiskSegment segment) = 0;

  // Report that an entry was successfully fetched
  virtual void ReportFetched(EntryId id, std::string_view value, DiskSegment segment) = 0;

 protected:
  // Describes pending futures for a single entry
  struct EntryOps {
    EntryOps(OwnedEntryId id, DiskSegment segment) : id{std::move(id)}, segment{segment} {
    }

    OwnedEntryId id;
    DiskSegment segment;
    absl::InlinedVector<util::fb2::Promise<std::string>, 1> futures;
  };

  // Describes an ongoing read operation for a fixed segment
  struct ReadOp {
    explicit ReadOp(DiskSegment segment) : segment(segment) {
    }

    // Get ops for id or create new
    EntryOps& ForId(EntryId id, DiskSegment segment);

    DiskSegment segment;                       // spanning segment of whole read
    absl::InlinedVector<EntryOps, 1> key_ops;  // enqueued operations for different keys
    bool delete_requested = false;             // whether to delete after reading the segment
  };

  // Prepare read operation for aligned segment or return pending if it exists.
  // Refernce is valid until any other read operations occur.
  ReadOp& PrepareRead(DiskSegment aligned_segment);

  // Called once read finished
  void ProcessRead(size_t offset, std::string_view value);

  // Called once Stash finished
  void ProcessStashed(EntryId id, unsigned version, DiskSegment segment);

 protected:
  DiskStorage storage_;

  absl::flat_hash_map<size_t /* offset */, ReadOp> pending_reads_;

  // todo: allow heterogeneous lookups with non owned id
  absl::flat_hash_map<OwnedEntryId, unsigned /* version */> pending_stash_ver_;
};

};  // namespace dfly::tiering
