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
#include "server/tiering/disk_storage.h"

namespace dfly::tiering {

std::optional<SmallBins::FilledBin> SmallBins::Stash(std::string_view key, std::string_view value) {
  DCHECK_LT(value.size(), 2_KB);

  // See FlushBin() for format details
  size_t value_bytes = 8 /* hash */ + value.size();

  std::optional<FilledBin> filled_bin;
  if (2 /* num entries */ + current_bin_bytes_ + value_bytes >= 4_KB) {
    filled_bin = FlushBin();
  }

  current_bin_bytes_ += value_bytes;
  current_bin_[key] = value;
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

  // Store all hashes, n * 8 bytes
  for (const auto& [key, _] : current_bin_) {
    absl::little_endian::Store64(data, CompactObj::HashCode(key));
    data += sizeof(uint64_t);
  }

  // Store all values, n * x bytes
  for (const auto& [key, value] : current_bin_) {
    pending_set[key] = {size_t(data - out.data()), value.size()};

    memcpy(data, value.data(), value.size());
    data += value.size();
  }

  current_bin_bytes_ = 0;  // num hashes
  current_bin_.erase(current_bin_.begin(), current_bin_.end());

  return {id, std::move(out)};
}

SmallBins::KeySegmentList SmallBins::ReportStashed(BinId id, DiskSegment segment) {
  auto key_list = pending_bins_.extract(id);
  return SmallBins::KeySegmentList{key_list.mapped().begin(), key_list.mapped().end()};
}

std::vector<std::string> SmallBins::ReportStashAborted(BinId id) {
  std::vector<std::string> out;

  auto node = pending_bins_.extract(id);
  auto& entries = node.mapped();
  while (!entries.empty())
    out.emplace_back(std::move(entries.extract(entries.begin()).key()));

  return out;
}

std::optional<SmallBins::BinId> SmallBins::Delete(std::string_view key) {
  for (auto& [id, keys] : pending_bins_) {
    if (keys.erase(key))
      return keys.empty() ? std::make_optional(id) : std::nullopt;
  }
  return std::nullopt;
}

std::optional<DiskSegment> SmallBins::Delete(DiskSegment segment) {
  segment = segment.FillPages();
  if (auto it = stashed_bins_.find(segment.offset);
      it != stashed_bins_.end() && --it->second == 0) {
    stashed_bins_.erase(it);
    return segment;
  }
  return std::nullopt;
}

}  // namespace dfly::tiering
