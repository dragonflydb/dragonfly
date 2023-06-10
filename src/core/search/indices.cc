// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/indices.h"

#include <absl/container/flat_hash_set.h>
#include <absl/strings/ascii.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_split.h>

#include <algorithm>
#include <regex>

#include "base/logging.h"

namespace dfly::search {

using namespace std;

namespace {

// Get all words from text as matched by regex word boundaries
vector<string> GetWords(string_view text) {
  std::regex rx{"\\b.*?\\b", std::regex_constants::icase};
  std::cregex_iterator begin{text.data(), text.data() + text.size(), rx}, end{};

  absl::flat_hash_set<string> words;
  for (auto it = begin; it != end; ++it) {
    auto word = it->str();
    absl::AsciiStrToLower(&word);
    words.insert(move(word));
  }

  return vector<string>{make_move_iterator(words.begin()), make_move_iterator(words.end())};
}

};  // namespace

void NumericIndex::Add(DocId doc, string_view value) {
  int64_t num;
  if (absl::SimpleAtoi(value, &num))
    entries_.emplace(num, doc);
}

vector<DocId> NumericIndex::Range(int64_t l, int64_t r) const {
  auto it_l = entries_.lower_bound(l);
  auto it_r = entries_.lower_bound(r + 1);

  vector<DocId> out;
  for (auto it = it_l; it != it_r; ++it)
    out.push_back(it->second);

  sort(out.begin(), out.end());
  return out;
}

const vector<DocId>* BaseStringIndex::Matching(string_view str) const {
  auto it = entries_.find(absl::StripAsciiWhitespace(str));
  return (it != entries_.end()) ? &it->second : nullptr;
}

void TextIndex::Add(DocId doc, string_view value) {
  for (const auto& word : GetWords(value)) {
    auto& list = entries_[word];
    list.insert(upper_bound(list.begin(), list.end(), doc), doc);
  }
}

void TagIndex::Add(DocId doc, string_view value) {
  auto tags = absl::StrSplit(value, ',');
  for (string_view tag : tags) {
    auto& list = entries_[absl::StripAsciiWhitespace(tag)];
    list.insert(upper_bound(list.begin(), list.end(), doc), doc);
  }
}

}  // namespace dfly::search
