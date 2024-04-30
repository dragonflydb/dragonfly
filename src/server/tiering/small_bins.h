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

  // Bin filled with blob of serialized entries
  using FilledBin = std::pair<BinId, std::string>;

  // List of locations of values for corresponding keys of previously filled bin
  using KeySegmentList = std::vector<std::pair<std::string /* key*/, DiskSegment>>;

  // Enqueue key/value pair for stash. Returns page to be stashed if it filled up.
  std::optional<FilledBin> Stash(std::string_view key, std::string_view value);

  // Report that a stash succeeeded. Returns list of stored keys with calculated value locations.
  KeySegmentList ReportStashed(BinId id, DiskSegment segment);

  // Report that a stash was aborted. Returns list of keys that the entry contained.
  std::vector<std::string /* key */> ReportStashAborted(BinId id);

  // Delete a key with pending io. Returns entry id if needs to be deleted.
  std::optional<BinId> Delete(std::string_view key);

  // Delete a stored segment. Returns page segment if it became emtpy and needs to be deleted.
  std::optional<DiskSegment> Delete(DiskSegment segment);

  Stats GetStats() const;

 private:
  // Flush current bin
  FilledBin FlushBin();

 private:
  BinId last_bin_id_ = 0;

  unsigned current_bin_bytes_ = 0;
  absl::flat_hash_map<std::string, std::string> current_bin_;

  // Pending stashes, their keys and value sizes
  absl::flat_hash_map<unsigned /* id */, absl::flat_hash_map<std::string /* key*/, DiskSegment>>
      pending_bins_;

  // Map of bins that were stashed and should be deleted when refcount reaches 0
  absl::flat_hash_map<size_t /*offset*/, unsigned /* refcount*/> stashed_bins_;

  struct {
    size_t total_stashed_entries = 0;
  } stats_;
};

};  // namespace dfly::tiering
