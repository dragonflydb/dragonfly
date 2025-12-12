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

namespace dfly::tiering {
using namespace std;

namespace {

// See FlushBin() for format details
size_t StashedValueSize(string_view value) {
  return 2 /* dbid */ + 8 /* hash */ + 2 /* strlen*/ + value.size();
}

}  // namespace

std::optional<SmallBins::FilledBin> SmallBins::Stash(DbIndex dbid, std::string_view key,
                                                     std::string_view value) {
  DCHECK_LT(value.size(), 2_KB);

  size_t value_bytes = StashedValueSize(value);

  std::optional<FilledBin> filled_bin;
  if (2 /* num entries */ + current_bin_.bytes + value_bytes >= kPageSize) {
    filled_bin = exchange(current_bin_, FilledBin{++last_bin_id_});
  }

  current_bin_.bytes += value_bytes;
  auto [it, inserted] = current_bin_.entries.emplace(std::make_pair(dbid, key), string(value));
  CHECK(inserted);

  return filled_bin;
}

size_t SmallBins::SerializeBin(FilledBin* bin, io::MutableBytes dest) {
  DCHECK_GT(bin->entries.size(), 0u);
  DCHECK_GE(dest.size(), 4_KB);

  auto& pending_set = pending_bins_[bin->id];
  uint8_t* data = dest.data();

  // Store number of entries, 2 bytes
  absl::little_endian::Store16(data, bin->entries.size());
  data += sizeof(uint16_t);

  // Store all dbids and hashes, n * 10 bytes
  for (const auto& [key, _] : bin->entries) {
    absl::little_endian::Store16(data, key.first);
    data += sizeof(DbIndex);

    absl::little_endian::Store64(data, CompactObj::HashCode(key.second));
    data += sizeof(uint64_t);
  }

  // Store all values with sizes, n * (2 + x) bytes
  for (const auto& [key, value] : bin->entries) {
    absl::little_endian::Store16(data, value.size());
    data += sizeof(uint16_t);

    pending_set[key] = {size_t(data - dest.data()), value.size()};
    memcpy(data, value.data(), value.size());
    data += value.size();
  }

  // Steal backing array from bin if relevant
  if (current_bin_.entries.empty()) {
    // erase doesn't shrink backing, so we can reuse the allocated capacity
    bin->entries.erase(bin->entries.begin(), bin->entries.end());
    current_bin_.entries = std::move(bin->entries);
  }

  return bin->bytes + 2;
}

SmallBins::KeySegmentList SmallBins::ReportStashed(BinId id, DiskSegment segment) {
  DVLOG(1) << "ReportStashed " << id;

  DCHECK(pending_bins_.contains(id));
  auto seg_map_node = pending_bins_.extract(id);
  const auto& seg_map = seg_map_node.mapped();
  DCHECK_GT(seg_map.size(), 0u) << id;

  uint16_t bytes = 0;
  SmallBins::KeySegmentList list;
  for (auto& [key, sub_segment] : seg_map) {
    bytes += sub_segment.length;

    DiskSegment real_segment{segment.offset + sub_segment.offset, sub_segment.length};
    list.emplace_back(key.first, key.second, real_segment);
  }

  stats_.stashed_entries_cnt += list.size();
  stashed_bins_[segment.offset] = {uint8_t(list.size()), bytes};
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
  auto& entries = current_bin_.entries;
  if (auto it = entries.find(make_pair(dbid, key)); it != entries.end()) {
    size_t stashed_size = StashedValueSize(it->second);
    DCHECK_GE(current_bin_.bytes, stashed_size);

    current_bin_.bytes -= stashed_size;
    entries.erase(it);
    return std::nullopt;
  }

  for (auto& [id, keys] : pending_bins_) {
    if (keys.erase(make_pair(dbid, key)))
      return keys.empty() ? std::make_optional(id) : std::nullopt;
  }
  return std::nullopt;
}

SmallBins::BinInfo SmallBins::Delete(DiskSegment segment) {
  auto full_segment = segment.ContainingPages();
  if (auto it = stashed_bins_.find(full_segment.offset); it != stashed_bins_.end()) {
    stats_.stashed_entries_cnt--;
    auto& bin = it->second;

    DCHECK_LE(segment.length, bin.bytes);
    bin.bytes -= segment.length;

    if (--bin.entries == 0) {
      DCHECK_EQ(bin.bytes, 0u);
      stashed_bins_.erase(it);
      return {full_segment, false /* fragmented */, true /* empty */};
    }

    if (bin.bytes < kPageSize / 2) {
      return {full_segment, true /* fragmented */, false /* empty */};
    }
  }

  return {segment};
}

SmallBins::Stats SmallBins::GetStats() const {
  return Stats{.stashed_bins_cnt = stashed_bins_.size(),
               .stashed_entries_cnt = stats_.stashed_entries_cnt,
               .current_bin_bytes = current_bin_.bytes,
               .current_entries_cnt = current_bin_.entries.size()};
}

SmallBins::KeyHashDbList SmallBins::DeleteBin(DiskSegment segment, std::string_view value) {
  DCHECK_EQ(value.size(), kPageSize);

  auto bin = stashed_bins_.extract(segment.offset);
  if (bin.empty())
    return {};

  stats_.stashed_entries_cnt -= bin.mapped().entries;

  const char* data = value.data();

  uint16_t entries = absl::little_endian::Load16(data);
  data += sizeof(uint16_t);

  KeyHashDbList out(entries);

  // Recover dbids and hashes
  for (size_t i = 0; i < entries; i++) {
    DbIndex dbid = absl::little_endian::Load16(data);
    data += sizeof(DbIndex);

    uint64_t hash = absl::little_endian::Load64(data);
    data += sizeof(hash);

    out[i] = {dbid, hash, {0, 0}};
  }

  // Recover segments
  for (size_t i = 0; i < entries; i++) {
    uint16_t length = absl::little_endian::Load16(data);
    data += sizeof(uint16_t);

    std::get<DiskSegment>(out[i]) = {segment.offset + (data - value.data()), length};
    data += length;
  }

  return out;
}

}  // namespace dfly::tiering
