// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

namespace dfly::search {

// Manages synonyms for search indices
class Synonyms {
 public:
  // Represents a group of synonymous terms
  using Group = absl::flat_hash_set<std::string>;

  // Get all synonym groups
  const absl::flat_hash_map<std::string, Group>& GetGroups() const;

  // Update or create a synonym group
  const Group& UpdateGroup(std::string id, const std::vector<std::string_view>& terms);

  // Get the group ID for a term
  std::optional<std::string> GetGroupIDbyTerm(std::string term) const;

 private:
  // Maps group ID to synonym group
  absl::flat_hash_map<std::string, Group> groups_;
};

}  // namespace dfly::search
