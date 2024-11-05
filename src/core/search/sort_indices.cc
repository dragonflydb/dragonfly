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
bool SimpleValueSortIndex<T>::Matches(DocId id, DocumentAccessor* doc, std::string_view field) {
  return Get(id, doc, field).has_value();
}

template <typename T>
void SimpleValueSortIndex<T>::Add(DocId id, DocumentAccessor* doc, std::string_view field) {
  DCHECK_LE(id, values_.size());  // Doc ids grow at most by one
  if (id >= values_.size())
    values_.resize(id + 1);
  values_[id] = Get(id, doc, field).value();
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

std::optional<double> NumericSortIndex::Get(DocId id, DocumentAccessor* doc,
                                            std::string_view field) {
  auto str = doc->GetStrings(field);
  if (str.empty())
    return 0.0;

  auto value_as_double = ParseNumericField(str.front());
  if (!value_as_double) {
    LOG(WARNING) << "Failed to parse numeric value from field: " << field
                 << " value: " << str.front();
    return std::nullopt;
  }
  return value_as_double.value();
}

std::optional<PMR_NS::string> StringSortIndex::Get(DocId id, DocumentAccessor* doc,
                                                   std::string_view field) {
  auto str = doc->GetStrings(field);
  if (str.empty())
    return "";

  return PMR_NS::string{str.front(), GetMemRes()};
}

}  // namespace dfly::search
