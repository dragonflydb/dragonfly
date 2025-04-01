// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "synonyms.h"

#include <absl/strings/str_cat.h>
#include <uni_algo/case.h>

namespace dfly::search {

const absl::flat_hash_map<std::string, Synonyms::Group>& Synonyms::GetGroups() const {
  return groups_;
}

void Synonyms::UpdateGroup(const std::string_view& id, const std::vector<std::string_view>& terms) {
  auto& group = groups_[id];

  // Convert all terms to lowercase before adding them to the group
  for (const std::string_view& term : terms) {
    group.insert(una::cases::to_lowercase_utf8(term));
  }
}

std::optional<std::string> Synonyms::GetGroupToken(std::string term) const {
  term = una::cases::to_lowercase_utf8(term);
  for (const auto& [id, group] : groups_) {
    if (group.count(term)) {
      // Add space before group id to avoid matching the term itself
      return absl::StrCat(" ", id);
    }
  }

  return std::nullopt;
}

}  // namespace dfly::search
