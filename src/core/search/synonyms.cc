// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "synonyms.h"

#include <uni_algo/case.h>

namespace dfly::search {

const absl::flat_hash_map<std::string, Synonyms::Group>& Synonyms::GetGroups() const {
  return groups_;
}

const Synonyms::Group& Synonyms::UpdateGroup(std::string id,
                                             const std::vector<std::string_view>& terms) {
  auto& group = groups_[id];

  // Convert all terms to lowercase before adding them to the group
  for (const std::string_view& term : terms) {
    group.insert(una::cases::to_lowercase_utf8(term));
  }

  return group;
}

std::optional<std::string> Synonyms::GetGroupIDbyTerm(std::string term) const {
  term = una::cases::to_lowercase_utf8(term);
  for (const auto& [id, group] : groups_) {
    if (group.count(term)) {
      return id;
    }
  }

  return std::nullopt;
}

}  // namespace dfly::search
