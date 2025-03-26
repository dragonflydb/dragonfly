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

}  // namespace dfly::search
