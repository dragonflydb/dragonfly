// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/sort_indices.h"

#include <absl/container/flat_hash_set.h>
#include <absl/strings/ascii.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_split.h>

#include <algorithm>
#include <optional>
#include <type_traits>
#include <variant>

namespace dfly::search {

using namespace std;

namespace {
template <typename T>
using ScoreT = std::conditional_t<is_same_v<T, PMR_NS::string>, std::string, T>;
}  // namespace

template <typename T> bool SimpleValueSortIndex<T>::ParsedSortValue::HasValue() const {
  return !std::holds_alternative<std::monostate>(value);
}

template <typename T> bool SimpleValueSortIndex<T>::ParsedSortValue::IsNullValue() const {
  return std::holds_alternative<std::nullopt_t>(value);
}

template <typename T>
SimpleValueSortIndex<T>::SimpleValueSortIndex(PMR_NS::memory_resource* mr)
    : values_{mr}, occupied_(mr) {
}

template <typename T> SortableValue SimpleValueSortIndex<T>::Lookup(DocId doc) const {
  DCHECK_LT(doc, occupied_.size());
  if (!occupied_[doc])
    return std::monostate{};

  DCHECK_LT(doc, values_.size());
  return ScoreT<T>{values_[doc]};
}

template <typename T>
std::vector<SortableValue> SimpleValueSortIndex<T>::Sort(std::vector<DocId>* ids, size_t limit,
                                                         bool desc) const {
  auto cb = [this, desc](const auto& lhs, const auto& rhs) {
    // null values are at the end
    auto p1 = make_pair(!occupied_[lhs], cref(values_[lhs]));
    auto p2 = make_pair(!occupied_[rhs], cref(values_[rhs]));
    return desc ? (p1 > p2) : (p1 < p2);
  };
  std::partial_sort(ids->begin(), ids->begin() + std::min(ids->size(), limit), ids->end(), cb);

  // Turn PMR string into std::string
  vector<SortableValue> out(min(ids->size(), limit));
  for (size_t i = 0; i < out.size(); i++)
    out[i] = ScoreT<T>{values_[(*ids)[i]]};
  return out;
}

template <typename T>
bool SimpleValueSortIndex<T>::Add(DocId id, const DocumentAccessor& doc, std::string_view field) {
  auto field_value = Get(doc, field);
  if (!field_value.HasValue()) {
    return false;
  }

  if (id >= values_.size()) {
    values_.resize(id + 1);
    occupied_.resize(id + 1);
  }

  if (!field_value.IsNullValue()) {
    values_[id] = std::move(std::get<T>(field_value.value));
    occupied_[id] = true;
  }
  return true;
}

template <typename T>
void SimpleValueSortIndex<T>::Remove(DocId id, const DocumentAccessor& doc,
                                     std::string_view field) {
  DCHECK_LT(id, values_.size());
  DCHECK_EQ(values_.size(), occupied_.size());
  values_[id] = T{};
  occupied_[id] = false;
}

template <typename T>
std::vector<DocId> SimpleValueSortIndex<T>::GetAllDocsWithNonNullValues() const {
  std::vector<DocId> result;
  result.reserve(values_.size());

  for (DocId id = 0; id < values_.size(); ++id) {
    if (occupied_[id])
      result.push_back(id);
  }

  return result;
}

template <typename T> PMR_NS::memory_resource* SimpleValueSortIndex<T>::GetMemRes() const {
  return values_.get_allocator().resource();
}

template struct SimpleValueSortIndex<double>;
template struct SimpleValueSortIndex<PMR_NS::string>;

SimpleValueSortIndex<double>::ParsedSortValue NumericSortIndex::Get(const DocumentAccessor& doc,
                                                                    std::string_view field) {
  auto numbers_list = doc.GetNumbers(field);
  if (!numbers_list) {
    return {};
  }
  if (numbers_list->empty()) {
    return ParsedSortValue{std::nullopt};
  }
  return ParsedSortValue{numbers_list->front()};
}

SimpleValueSortIndex<PMR_NS::string>::ParsedSortValue StringSortIndex::Get(
    const DocumentAccessor& doc, std::string_view field) {
  auto strings_list = doc.GetTags(field);
  if (!strings_list) {
    return {};
  }
  if (strings_list->empty()) {
    return ParsedSortValue{std::nullopt};
  }
  return ParsedSortValue{PMR_NS::string{strings_list->front(), GetMemRes()}};
}

}  // namespace dfly::search
