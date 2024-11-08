// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/sort_indices.h"

#include <absl/container/flat_hash_set.h>
#include <absl/strings/ascii.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_split.h>

#include <algorithm>
#include <type_traits>

namespace dfly::search {

using namespace std;

namespace {}  // namespace

template <typename T>
SimpleValueSortIndex<T>::SimpleValueSortIndex(PMR_NS::memory_resource* mr) : values_{mr} {
}

template <typename T> SortableValue SimpleValueSortIndex<T>::Lookup(DocId doc) const {
  DCHECK_LT(doc, values_.size());
  if constexpr (is_same_v<T, PMR_NS::string>) {
    return std::string(values_[doc]);
  } else {
    return values_[doc];
  }
}

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
bool SimpleValueSortIndex<T>::IsValidFieldType(DocId id, DocumentAccessor* doc,
                                               std::string_view field) {
  auto strings_list = doc->GetStrings(field);
  return std::all_of(strings_list.begin(), strings_list.end(),
                     [&](const auto& str) { return Get(str); });
}

template <typename T>
void SimpleValueSortIndex<T>::Add(DocId id, DocumentAccessor* doc, std::string_view field) {
  DCHECK_LE(id, values_.size());  // Doc ids grow at most by one
  if (id >= values_.size())
    values_.resize(id + 1);

  auto strings_list = doc->GetStrings(field);
  if (!strings_list.empty()) {
    values_[id] = Get(strings_list.front()).value();  // TODO: handle multiple values
  }
}

template <typename T>
void SimpleValueSortIndex<T>::Remove(DocId id, DocumentAccessor* doc, std::string_view field) {
  DCHECK_LT(id, values_.size());
  values_[id] = T{};
}

template <typename T> PMR_NS::memory_resource* SimpleValueSortIndex<T>::GetMemRes() const {
  return values_.get_allocator().resource();
}

template struct SimpleValueSortIndex<double>;
template struct SimpleValueSortIndex<PMR_NS::string>;

std::optional<double> NumericSortIndex::Get(std::string_view field_value) {
  return ParseNumericField(field_value);
}

std::optional<PMR_NS::string> StringSortIndex::Get(std::string_view field_value) {
  return PMR_NS::string{field_value, GetMemRes()};
}

}  // namespace dfly::search
