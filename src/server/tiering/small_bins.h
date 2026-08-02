// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>

#include <optional>
#include <string>
#include <vector>

#include "server/tiering/common.h"
#include "server/tiering/disk_storage.h"
#include "server/tiering/entry_map.h"

namespace dfly::tiering {

using DbIndex = uint16_t;

// Small bins accumulate small values into larger bins that fill up 4kb pages.
// SIMPLEST VERSION for now.
class SmallBins {
 public:
  struct Stats {
    size_t stashed_bins_cnt = 0;
    size_t stashed_entries_cnt = 0;
    size_t current_bin_bytes = 0;
    size_t current_entries_cnt = 0;
  };

  using BinId = unsigned;
  static const BinId kInvalidBin = std::numeric_limits<BinId>::max();

  struct BinInfo {
    DiskSegment segment;
    bool fragmented = false, empty = false;
  };

  // A pending value together with its per-key cooling-pool bypass flag. Kept per-key so a bin that
  // mixes write-path and background entries preserves each entry's disposition.
  struct BinValue {
    std::string value;
    bool bypass_cooling = false;
  };

  // Packaged bin ready to be serialized with SerializeBin()
  struct FilledBin {
    friend class SmallBins;
    BinId id;

   private:
    explicit FilledBin(BinId id) : id{id} {
    }

    unsigned bytes_ = 0;
    tiering::EntryMap<BinValue> entries_;
  };

  // List of locations of values for corresponding keys of previously filled bin, each with the
  // per-key cooling-pool bypass flag recorded at Stash() time.
  using KeySegmentList = std::vector<
      std::tuple<DbIndex, std::string /* key*/, bool /* bypass_cooling */, DiskSegment>>;

  // List of item key db indices and hashes
  using KeyHashDbList = std::vector<std::tuple<DbIndex, uint64_t /* hash */, DiskSegment>>;

  // Returns true if the entry is pending inside SmallBins.
  bool IsPending(DbIndex dbid, std::string_view key) const {
    return current_bin_.entries_.count(std::make_pair(dbid, key)) > 0;
  }

  // Enqueue key/value pair for stash. Returns page to be stashed if it filled up. bypass_cooling is
  // recorded per-key alongside the value and echoed back through ReportStashed().
  std::optional<FilledBin> Stash(DbIndex dbid, std::string_view key, std::string_view value,
                                 bool bypass_cooling);

  // Report that a stash succeeeded. Returns list of stored keys with calculated value locations.
  KeySegmentList ReportStashed(BinId id, DiskSegment segment);

  // Report that a stash was aborted. Returns list of keys that the entry contained.
  std::vector<std::pair<DbIndex, std::string>> ReportStashAborted(BinId id);

  // Delete a key with pending io. Returns entry id if needs to be deleted.
  std::optional<BinId> Delete(DbIndex dbid, std::string_view key);

  // Delete a stored segment. Returns information about the current bin, which might indicate
  // the need for external actions like deleting empty segments or triggering defragmentation
  BinInfo Delete(DiskSegment segment);

  // Returns true if the page exists and is fragmented
  bool IsFragmented(size_t offset);

  // Delete stashed bin. Returns list of recovered item key hashes and db indices.
  // Mainly used for defragmentation
  KeyHashDbList DeleteBin(DiskSegment segment, std::string_view value);

  // Serialize filled bin to destination buffer (4kb)
  size_t SerializeBin(FilledBin* bin, io::MutableBytes dest);

  Stats GetStats() const;

 private:
  struct StashInfo {
    uint8_t entries = 0;
    uint16_t bytes = 0;
  };
  static_assert(sizeof(StashInfo) == sizeof(unsigned));

  BinId last_bin_id_ = 0;
  FilledBin current_bin_{last_bin_id_};

  // In-page location of a pending value plus its per-key cooling-pool bypass flag.
  struct PendingLocation {
    DiskSegment segment;
    bool bypass_cooling = false;
  };

  // Pending stashes, their keys and in-page locations
  absl::flat_hash_map<unsigned /* id */, tiering::EntryMap<PendingLocation>> pending_bins_;

  // Map of bins that were stashed and should be deleted when number of entries reaches 0
  absl::flat_hash_map<size_t /*offset*/, StashInfo> stashed_bins_;

  struct {
    size_t stashed_entries_cnt = 0;
  } stats_;
};

};  // namespace dfly::tiering
