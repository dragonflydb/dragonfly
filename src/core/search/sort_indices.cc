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

#include "core/search/search.h"

namespace dfly::search {

using namespace std;

namespace {}  // namespace

template <typename T> bool SimpleValueSortIndex<T>::ParsedSortValue::HasValue() const {
  return !std::holds_alternative<std::monostate>(value);
}

template <typename T> bool SimpleValueSortIndex<T>::ParsedSortValue::IsNullValue() const {
  return std::holds_alternative<std::nullopt_t>(value);
}

template <typename T>
SimpleValueSortIndex<T>::SimpleValueSortIndex(PMR_NS::memory_resource* mr)
    : values_{mr}, null_values_(mr) {
}

template <typename T> SortableValue SimpleValueSortIndex<T>::Lookup(DocId doc) const {
  if (null_values_.contains(doc)) {
    return std::monostate{};
  }

  DCHECK_LT(doc, values_.size());
  if constexpr (is_same_v<T, PMR_NS::string>) {
    return std::string(values_[doc]);
  } else {
    return values_[doc];
  }
}

template <typename T>
std::vector<SortableValue> SimpleValueSortIndex<T>::Sort(
    size_t shard_limit, SortOrder sort_order, SearchAlrgorithmResult* search_result) const {
  const size_t initial_size = search_result->ids.size();

  // TODO: remove this and use DocId or std::pair<DocId, ResultScore> instead in
  // SearchAlrgorithmResult
  std::vector<size_t> ids_to_sort(initial_size);
  for (size_t i = 0; i < initial_size; ++i) {
    ids_to_sort[i] = i;
  }

  auto compator = BuildAscDescComparator<size_t>(
      [&](const size_t& l, const size_t& r) {
        return values_[search_result->ids[l]] < values_[search_result->ids[r]];
      },
      [&](const size_t& l, const size_t& r) {
        return values_[search_result->ids[l]] > values_[search_result->ids[r]];
      },
      sort_order);

  size_t size = initial_size;
  if (shard_limit < size) {
    std::partial_sort(ids_to_sort.begin(), ids_to_sort.begin() + shard_limit, ids_to_sort.end(),
                      std::move(compator));
    size = shard_limit;
    ids_to_sort.resize(shard_limit);
  } else {
    std::sort(ids_to_sort.begin(), ids_to_sort.end(), std::move(compator));
  }

  search_result->RearrangeAccordingToIndexes(ids_to_sort);

  std::vector<SortableValue> out(size);
  for (size_t i = 0; i < size; i++) {
    const auto doc_id = search_result->ids[i];
    auto& value = values_[doc_id];
    if constexpr (!std::is_arithmetic_v<T>) {
      out[i] = std::string{value};
    } else {
      out[i] = static_cast<double>(value);
    }
  }
  return out;
}

template <typename T>
bool SimpleValueSortIndex<T>::Add(DocId id, const DocumentAccessor& doc, std::string_view field) {
  auto field_value = Get(doc, field);
  if (!field_value.HasValue()) {
    return false;
  }

  if (field_value.IsNullValue()) {
    null_values_.insert(id);
    return true;
  }

  if (id >= values_.size())
    values_.resize(id + 1);

  values_[id] = std::move(std::get<T>(field_value.value));
  return true;
}

template <typename T>
void SimpleValueSortIndex<T>::Remove(DocId id, const DocumentAccessor& doc,
                                     std::string_view field) {
  if (auto it = null_values_.find(id); it != null_values_.end()) {
    null_values_.erase(it);
    return;
  }

  DCHECK_LT(id, values_.size());
  values_[id] = T{};
}

template <typename T>
std::vector<DocId> SimpleValueSortIndex<T>::GetAllDocsWithNonNullValues() const {
  std::vector<DocId> result;
  result.reserve(values_.size());

  auto empty_value = T{};
  for (DocId id = 0; id < values_.size(); ++id) {
    if (values_[id] != empty_value) {
      result.push_back(id);
    }
  }

  // Result is already sorted by DocId
  // Also it has no duplicates
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
