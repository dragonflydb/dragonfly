// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "synonyms.h"

namespace dfly::search {

const absl::flat_hash_map<uint32_t, Synonyms::Group>& Synonyms::GetGroups() const {
  return groups_;
}

void Synonyms::UpdateGroup(uint32_t id, std::vector<std::string> terms) {
  auto& group = groups_[id];
  group.insert(terms.begin(), terms.end());
}

absl::flat_hash_set<uint32_t> Synonyms::GetGroupIds(const std::string& term) const {
  absl::flat_hash_set<uint32_t> result;
  for (const auto& [group_id, group] : groups_) {
    if (group.contains(term)) {
      result.insert(group_id);
    }
  }
  return result;
}

}  // namespace dfly::search
