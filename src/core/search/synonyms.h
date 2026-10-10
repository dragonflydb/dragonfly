// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/types/span.h>

#include <string>
#include <string_view>
#include <vector>

namespace dfly::search {

// Class that manages synonym groups for search indices.
// Allows defining groups of related terms that should be considered equivalent during search.
// All terms are converted to lowercase for normalization.
//
// Group tokens returned by GetGroupTokens are the group identifiers with a space prefix. The
// space is intentionally added to avoid matching with the term itself during text tokenization
// and to distinguish the group identifier from regular terms during search. Group identifiers
// are case-sensitive, terms are not.
class Synonyms {
 public:
  // Represents a group of synonymous terms
  using Group = absl::flat_hash_set<std::string>;

  // Get all synonym groups
  const absl::flat_hash_map<std::string, Group>& GetGroups() const;

  // Update or create a synonym group
  void UpdateGroup(const std::string_view& id, const std::vector<std::string_view>& terms);

  // Group tokens of every group the term belongs to, sorted. The result depends only on the
  // current groups, never on insertion order or hash map layout, so a document is removed with
  // exactly the tokens it was indexed with.
  absl::Span<const std::string> GetGroupTokens(std::string_view term) const;

  // Every lowercase term with its sorted group tokens, ordered by term so that a prefix selects a
  // contiguous range.
  const absl::btree_map<std::string, std::vector<std::string>>& TermTokens() const {
    return term_tokens_;
  }

 private:
  // Maps group ID to synonym group
  absl::flat_hash_map<std::string, Group> groups_;
  // Maps lowercase term to its sorted group tokens
  absl::btree_map<std::string, std::vector<std::string>> term_tokens_;
};

}  // namespace dfly::search
