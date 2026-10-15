// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "synonyms.h"

#include <absl/strings/str_cat.h>
#include <uni_algo/case.h>

#include <algorithm>

namespace dfly::search {

const absl::flat_hash_map<std::string, Synonyms::Group>& Synonyms::GetGroups() const {
  return groups_;
}

namespace {

void InsertSorted(std::vector<std::string>* tokens, const std::string& token) {
  auto it = std::lower_bound(tokens->begin(), tokens->end(), token);
  if (it == tokens->end() || *it != token)
    tokens->insert(it, token);
}

}  // namespace

void Synonyms::UpdateGroup(const std::string_view& id, const std::vector<std::string_view>& terms) {
  auto& group = groups_[id];
  std::string token = absl::StrCat(" ", id);

  // Convert all terms to lowercase before adding them to the group
  for (const std::string_view& term : terms) {
    std::string lc_term = una::cases::to_lowercase_utf8(term);
    if (!group.insert(lc_term).second)
      continue;
    InsertSorted(&term_tokens_[std::move(lc_term)], token);
  }
}

absl::Span<const std::string> Synonyms::GetGroupTokens(std::string_view lowercase_term) const {
  auto it = term_tokens_.find(lowercase_term);
  return it == term_tokens_.end() ? absl::Span<const std::string>{} : it->second;
}

}  // namespace dfly::search
