// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/indices.h"

#include <absl/container/flat_hash_set.h>
#include <absl/strings/ascii.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_split.h>
#include <unicode/brkiter.h>
#include <unicode/unistr.h>

#include <algorithm>
#include <cctype>
#include <regex>

#include "base/logging.h"

namespace dfly::search {

using namespace std;

namespace {

bool IsAllAscii(string_view sv) {
  return all_of(sv.begin(), sv.end(), [](unsigned char c) { return isascii(c); });
}

// Get all words from text as matched by the ICU library
absl::flat_hash_set<std::string> ICUTokenizeWords(std::string_view text) {
  // Is text contains only ascii, skip working with ICU resources
  if (IsAllAscii(text)) {
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

  icu::UnicodeString uStr(text.data(), text.size(), "UTF-8");

  UErrorCode status = U_ZERO_ERROR;
  std::unique_ptr<icu::BreakIterator> wordIter{
      icu::BreakIterator::createWordInstance(icu::Locale::getDefault(), status)};

  if (U_FAILURE(status))
    return {};

  wordIter->setText(uStr);

  std::string tmpStdWord;
  absl::flat_hash_set<std::string> words;

  int32_t start = wordIter->first();
  for (int32_t end = wordIter->next(); end != icu::BreakIterator::DONE;
       start = end, end = wordIter->next()) {
    icu::UnicodeString word = uStr.tempSubStringBetween(start, end);
    // If the substring is not a space, convert it to lowercase and add to results
    if (!word.isBogus() && !word.trim().isEmpty()) {
      word.toLower();
      word.toUTF8String(tmpStdWord);
      words.emplace(move(tmpStdWord));
    }
  }

  return words;
}

// Convert string to lowercase with ICU library
std::string ICUToLowercase(string_view input) {
  icu::UnicodeString uStr = icu::UnicodeString::fromUTF8(input);
  uStr.toLower();
  std::string result;
  uStr.toUTF8String(result);
  return result;
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

const CompressedSortedSet* BaseStringIndex::Matching(string_view str) const {
  str = absl::StripAsciiWhitespace(str);

  string word;
  if (IsAllAscii(str))
    word = absl::AsciiStrToLower(str);
  else
    word = ICUToLowercase(str);

  auto it = entries_.find(word);
  return (it != entries_.end()) ? &it->second : nullptr;
}

void BaseStringIndex::Add(DocId id, DocumentAccessor* doc, string_view field) {
  for (const auto& word : Tokenize(doc->GetString(field)))
    entries_[word].Insert(id);
}

void BaseStringIndex::Remove(DocId id, DocumentAccessor* doc, string_view field) {
  for (const auto& word : Tokenize(doc->GetString(field)))
    entries_[word].Remove(id);
}

absl::flat_hash_set<std::string> TextIndex::Tokenize(std::string_view value) const {
  return ICUTokenizeWords(value);
}

absl::flat_hash_set<std::string> TagIndex::Tokenize(std::string_view value) const {
  return NormalizeTags(value);
}

VectorIndex::VectorIndex(size_t dim, VectorSimilarity sim) : dim_{dim}, sim_{sim}, entries_{} {
}

void VectorIndex::Add(DocId id, DocumentAccessor* doc, string_view field) {
  DCHECK_LE(id * dim_, entries_.size());
  if (id * dim_ == entries_.size())
    entries_.resize((id + 1) * dim_);

  // TODO: Let get vector write to buf itself
  auto [ptr, size] = doc->GetVector(field);

  if (size == dim_)
    memcpy(&entries_[id * dim_], ptr.get(), dim_ * sizeof(float));
}

void VectorIndex::Remove(DocId id, DocumentAccessor* doc, string_view field) {
  // noop
}

const float* VectorIndex::Get(DocId doc) const {
  return &entries_[doc * dim_];
}

std::pair<size_t /*dim*/, VectorSimilarity> VectorIndex::Info() const {
  return {dim_, sim_};
}

}  // namespace dfly::search
