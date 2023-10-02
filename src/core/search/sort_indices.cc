// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/sort_indices.h"

#include <absl/container/flat_hash_set.h>
#include <absl/strings/ascii.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_split.h>

#include <algorithm>

namespace dfly::search {

using namespace std;

namespace {}  // namespace

template <typename T>
std::vector<ResultScore> SimpleValueSortIndex<T>::Sort(std::vector<DocId>* ids, size_t limit,
                                                       bool desc) const {
  auto cb = [this, desc](const auto& lhs, const auto& rhs) {
    return desc ? (values_[lhs] > values_[rhs]) : (values_[lhs] < values_[rhs]);
  };
  std::partial_sort(ids->begin(), ids->begin() + std::min(ids->size(), limit), ids->end(), cb);

  vector<ResultScore> out(min(ids->size(), limit));
  for (size_t i = 0; i < out.size(); i++)
    out[i] = values_[(*ids)[i]];
  return out;
}

template <typename T>
void SimpleValueSortIndex<T>::Add(DocId id, DocumentAccessor* doc, std::string_view field) {
  DCHECK(id <= values_.size());  // Doc ids grow at most by one
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
