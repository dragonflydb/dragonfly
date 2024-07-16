// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>

#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "server/tiering/disk_storage.h"
#include "server/tx_base.h"

namespace dfly::tiering {

// Small bins accumulate small values into larger bins that fill up 4kb pages.
// SIMPLEST VERSION for now.
class SmallBins {
 public:
  struct Stats {
    size_t stashed_bins_cnt = 0;
    size_t stashed_entries_cnt = 0;
    size_t current_bin_bytes = 0;
  };

  using BinId = unsigned;
  static const BinId kInvalidBin = std::numeric_limits<BinId>::max();

  struct BinInfo {
    DiskSegment segment;
    bool fragmented = false, empty = false;
  };

  // Bin filled with blob of serialized entries
  using FilledBin = std::pair<BinId, std::string>;

  // List of locations of values for corresponding keys of previously filled bin
  using KeySegmentList = std::vector<std::tuple<DbIndex, std::string /* key*/, DiskSegment>>;

  // List of item key db indices and hashes
  using KeyHashDbList = std::vector<std::tuple<DbIndex, uint64_t /* hash */, DiskSegment>>;

  // Returns true if the entry is pending inside SmallBins.
  bool IsPending(DbIndex dbid, std::string_view key) const {
    return current_bin_.count({dbid, std::string(key)}) > 0;
  }

  // Enqueue key/value pair for stash. Returns page to be stashed if it filled up.
  std::optional<FilledBin> Stash(DbIndex dbid, std::string_view key, std::string_view value,
                                 io::Bytes footer);

  // Report that a stash succeeeded. Returns list of stored keys with calculated value locations.
  KeySegmentList ReportStashed(BinId id, DiskSegment segment);

  // Report that a stash was aborted. Returns list of keys that the entry contained.
  std::vector<std::pair<DbIndex, std::string>> ReportStashAborted(BinId id);

  // Delete a key with pending io. Returns entry id if needs to be deleted.
  std::optional<BinId> Delete(DbIndex dbid, std::string_view key);

  // Delete a stored segment. Returns information about the current bin, which might indicate
  // the need for external actions like deleting empty segments or triggering defragmentation
  BinInfo Delete(DiskSegment segment);

  // Delete stashed bin. Returns list of recovered item key hashes and db indices.
  // Mainly used for defragmentation
  KeyHashDbList DeleteBin(DiskSegment segment, std::string_view value);

  Stats GetStats() const;

 private:
  // Flush current bin
  FilledBin FlushBin();

 private:
  struct StashInfo {
    uint8_t entries = 0;
    uint16_t bytes = 0;
  };
  static_assert(sizeof(StashInfo) == sizeof(unsigned));

  BinId last_bin_id_ = 0;

  unsigned current_bin_bytes_ = 0;
  absl::flat_hash_map<std::pair<DbIndex, std::string>, std::string> current_bin_;

  // Pending stashes, their keys and value sizes
  absl::flat_hash_map<unsigned /* id */,
                      absl::flat_hash_map<std::pair<DbIndex, std::string> /* key*/, DiskSegment>>
      pending_bins_;

  // Map of bins that were stashed and should be deleted when number of entries reaches 0
  absl::flat_hash_map<size_t /*offset*/, StashInfo> stashed_bins_;

  struct {
    size_t stashed_entries_cnt = 0;
  } stats_;
};

};  // namespace dfly::tiering
