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
absl::flat_hash_set<string> Tokenize(string_view text) {
  std::regex rx{"\\b.*?\\b", std::regex_constants::icase};
  std::cregex_iterator begin{text.data(), text.data() + text.size(), rx}, end{};

  absl::flat_hash_set<string> words;
  for (auto it = begin; it != end; ++it) {
    auto word = it->str();
    absl::AsciiStrToLower(&word);
    words.insert(move(word));
  }
  return words;
}

// Split taglist, remove duplicates and convert all to lowercase
absl::flat_hash_set<string> NormalizeTags(string_view taglist) {
  string tmp;
  absl::flat_hash_set<string> tags;
  for (string_view tag : absl::StrSplit(taglist, ',')) {
    tmp = absl::StripAsciiWhitespace(tag);
    absl::AsciiStrToLower(&tmp);
    tags.insert(move(tmp));
  }
  return tags;
}

};  // namespace

void NumericIndex::Add(DocId id, DocumentAccessor* doc, string_view field) {
  int64_t num;
  if (absl::SimpleAtoi(doc->GetString(field), &num))
    entries_.emplace(num, id);
}

void NumericIndex::Remove(DocId id, DocumentAccessor* doc, string_view field) {
  int64_t num;
  if (absl::SimpleAtoi(doc->GetString(field), &num))
    entries_.erase({num, id});
}

vector<DocId> NumericIndex::Range(int64_t l, int64_t r) const {
  auto it_l = entries_.lower_bound({l, 0});
  auto it_r = entries_.lower_bound({r + 1, 0});

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

void TextIndex::Add(DocId id, DocumentAccessor* doc, string_view field) {
  for (const auto& word : Tokenize(doc->GetString(field))) {
    auto& list = entries_[word];
    list.insert(upper_bound(list.begin(), list.end(), id), id);
  }
}

void TextIndex::Remove(DocId id, DocumentAccessor* doc, string_view field) {
  for (const auto& word : Tokenize(doc->GetString(field))) {
    auto& list = entries_[word];
    auto it = lower_bound(list.begin(), list.end(), id);
    if (it != list.end() && *it == id)
      list.erase(it);
  }
}

void TagIndex::Add(DocId id, DocumentAccessor* doc, string_view field) {
  for (auto& tag : NormalizeTags(doc->GetString(field))) {
    auto& list = entries_[tag];
    list.insert(upper_bound(list.begin(), list.end(), id), id);
  }
}

void TagIndex::Remove(DocId id, DocumentAccessor* doc, string_view field) {
  for (auto& tag : NormalizeTags(doc->GetString(field))) {
    auto& list = entries_[tag];
    auto it = lower_bound(list.begin(), list.end(), id);
    if (it != list.end() && *it == id)
      list.erase(it);
  }
}

void VectorIndex::Add(DocId id, DocumentAccessor* doc, string_view field) {
  auto v = doc->GetVector(field);
  string vs;
  for (auto vv : v)
    vs = absl::StrCat(vs, vv);
  entries_[id] = doc->GetVector(field);
}

void VectorIndex::Remove(DocId id, DocumentAccessor* doc, string_view field) {
  entries_.erase(id);
}

FtVector VectorIndex::Get(DocId doc) const {
  auto it = entries_.find(doc);
  return it != entries_.end() ? it->second : FtVector{};
}

}  // namespace dfly::search
