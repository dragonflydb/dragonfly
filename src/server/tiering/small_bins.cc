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
  size_t value_bytes = 8 /* hash */ + 2 /* length */ + value.size();

  std::optional<FilledBin> filled_bin;
  if (current_bin_bytes_ + value_bytes >= 4_KB) {
    filled_bin = FlushBin();
  }

  current_bin_bytes_ += value_bytes;
  current_bin_[key] = value;
  return filled_bin;
}

SmallBins::FilledBin SmallBins::FlushBin() {
  std::string out;
  out.resize(current_bin_bytes_ + 1);

  unsigned id = ++last_bin_id_;
  auto& pending_list = pending_bins_[id];

  char* data = out.data();
  while (!current_bin_.empty()) {
    auto node = current_bin_.extract(current_bin_.begin());

    // Each key/value pair is serialized as:
    auto& key = node.key();
    auto& value = node.mapped();

    // 1. 8 byte key hash
    absl::little_endian::Store64(data, CompactObj::HashCode(key));
    data += sizeof(uint64_t);

    // 2. 2 byte value length
    absl::little_endian::Store16(data, static_cast<uint16_t>(value.size()));
    data += sizeof(uint16_t);

    // 3. X bytes value
    memcpy(data, value.data(), value.size());
    data += value.size();

    pending_list.emplace_back(key, value.size());
  }

  current_bin_bytes_ = 0;
  DCHECK(current_bin_.empty());

  return {id, std::move(out)};
}

SmallBins::KeySegmentList SmallBins::ReportStashed(unsigned id, DiskSegment segment) {
  SmallBins::KeySegmentList out;
  auto key_list = pending_bins_.extract(id);

  size_t offset = 0;
  for (auto& [key, value_size] : key_list.mapped()) {
    offset += 8 /* hash */ + 2 /* length */;
    out.emplace_back(std::move(key), DiskSegment{segment.offset + offset, value_size});
    offset += value_size;
  }

  stashed_bins_[id] = out.size();
  return out;
}

std::vector<std::string> SmallBins::ReportStashAborted(unsigned id) {
  std::vector<std::string> out;

  auto node = pending_bins_.extract(id);
  for (auto& [key, _] : node.mapped())
    out.emplace_back(std::move(key));

  return out;
}

std::optional<unsigned> SmallBins::Delete(std::string_view key) {
  auto same_key = [key](const auto& p) { return p.first == key; };
  for (auto& [id, keys] : pending_bins_) {
    if (auto it = std::find_if(keys.begin(), keys.end(), same_key); it != keys.end()) {
      keys.erase(it);
      if (keys.empty()) {
        pending_bins_.erase(id);
        return id;
      }
      return std::nullopt;
    }
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
