// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/sort_indices.h"

#include <absl/container/flat_hash_set.h>
#include <absl/strings/ascii.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_split.h>

#include <algorithm>
#include <cctype>

#include "base/logging.h"

namespace dfly::search {

using namespace std;

namespace {}  // namespace

template <typename T>
void SimpleValueSortIndex<T>::Sort(std::vector<DocId>* entries, std::vector<ResultScore>* out,
                                   size_t limit, bool desc) const {
  auto cb = [this, desc](const auto& lhs, const auto& rhs) {
    return desc ? (values_[lhs] > values_[rhs]) : (values_[lhs] < values_[rhs]);
  };
  std::partial_sort(entries->begin(), entries->begin() + std::min(entries->size(), limit),
                    entries->end(), cb);

  out->clear();
  out->reserve(entries->size());
  for (auto id : *entries)
    out->push_back(values_[id]);
}

template <typename T>
void SimpleValueSortIndex<T>::Add(DocId id, DocumentAccessor* doc, std::string_view field) {
  if (id >= values_.size())
    values_.resize(id + 1);
  values_[id] = Get(id, doc, field);
}

template <typename T>
void SimpleValueSortIndex<T>::Remove(DocId id, DocumentAccessor* doc, std::string_view field) {
  values_[id] = T{};
}

template struct SimpleValueSortIndex<int64_t>;
template struct SimpleValueSortIndex<std::string>;

int64_t NumericSortIndex::Get(DocId id, DocumentAccessor* doc, std::string_view field) {
  int64_t v;
  if (!absl::SimpleAtoi(doc->GetString(field), &v))
    return 0;
  return v;
}

std::string StringSortIndex::Get(DocId id, DocumentAccessor* doc, std::string_view field) {
  return string{doc->GetString(field)};
}

}  // namespace dfly::search
