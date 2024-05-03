// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tiering/small_bins.h"

#include <algorithm>
#include <optional>
#include <utility>

#include "absl/base/internal/endian.h"
#include "base/logging.h"
#include "core/compact_object.h"
#include "server/tiering/common.h"
#include "server/tiering/disk_storage.h"
#include "server/tx_base.h"

namespace dfly::tiering {

std::optional<SmallBins::FilledBin> SmallBins::Stash(DbIndex dbid, std::string_view key,
                                                     std::string_view value) {
  DCHECK_LT(value.size(), 2_KB);

  // See FlushBin() for format details
  size_t value_bytes = 2 /* dbid */ + 8 /* hash */ + value.size();

  std::optional<FilledBin> filled_bin;
  if (2 /* num entries */ + current_bin_bytes_ + value_bytes >= kPageSize) {
    filled_bin = FlushBin();
  }

  current_bin_bytes_ += value_bytes;
  current_bin_.emplace(std::make_pair(dbid, key), value);
  return filled_bin;
}

SmallBins::FilledBin SmallBins::FlushBin() {
  std::string out;
  out.resize(current_bin_bytes_ + 2);

  BinId id = ++last_bin_id_;
  auto& pending_set = pending_bins_[id];

  char* data = out.data();

  // Store number of entries, 2 bytes
  absl::little_endian::Store16(data, current_bin_.size());
  data += sizeof(uint16_t);

  // Store all dbids and hashes, n * 10 bytes
  for (const auto& [key, _] : current_bin_) {
    absl::little_endian::Store16(data, key.first);
    data += sizeof(DbIndex);

    absl::little_endian::Store64(data, CompactObj::HashCode(key.second));
    data += sizeof(uint64_t);
  }

  // Store all values, n * x bytes
  for (const auto& [key, value] : current_bin_) {
    pending_set[key] = {size_t(data - out.data()), value.size()};

    memcpy(data, value.data(), value.size());
    data += value.size();
  }

  current_bin_bytes_ = 0;
  current_bin_.erase(current_bin_.begin(), current_bin_.end());

  return {id, std::move(out)};
}

SmallBins::KeySegmentList SmallBins::ReportStashed(BinId id, DiskSegment segment) {
  auto key_list = pending_bins_.extract(id);
  DCHECK_GT(key_list.mapped().size(), 0u);

  SmallBins::KeySegmentList list;
  for (auto& [key, sub_segment] : key_list.mapped()) {
    list.emplace_back(key.first, key.second,
                      DiskSegment{segment.offset + sub_segment.offset, sub_segment.length});
  }

  stats_.total_stashed_entries += list.size();
  return list;
}

std::vector<std::pair<DbIndex, std::string>> SmallBins::ReportStashAborted(BinId id) {
  std::vector<std::pair<DbIndex, std::string>> out;

  auto node = pending_bins_.extract(id);
  auto& entries = node.mapped();
  while (!entries.empty())
    out.emplace_back(std::move(entries.extract(entries.begin()).key()));

  return out;
}

std::optional<SmallBins::BinId> SmallBins::Delete(DbIndex dbid, std::string_view key) {
  std::pair<DbIndex, std::string> key_pair{dbid, key};

  if (current_bin_.erase(key_pair)) {
    return std::nullopt;
  }

  for (auto& [id, keys] : pending_bins_) {
    if (keys.erase(key_pair))
      return keys.empty() ? std::make_optional(id) : std::nullopt;
  }
  return std::nullopt;
}

std::optional<DiskSegment> SmallBins::Delete(DiskSegment segment) {
  segment = segment.FillPages();
  if (auto it = stashed_bins_.find(segment.offset); it != stashed_bins_.end()) {
    stats_.total_stashed_entries--;
    if (--it->second == 0) {
      stashed_bins_.erase(it);
      return segment;
    }
  }
  return std::nullopt;
}

SmallBins::Stats SmallBins::GetStats() const {
  return Stats{.stashed_bins_cnt = stashed_bins_.size(),
               .stashed_entries_cnt = stats_.total_stashed_entries,
               .current_bin_bytes = current_bin_bytes_};
}

}  // namespace dfly::tiering
