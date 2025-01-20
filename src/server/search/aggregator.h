// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/types/span.h>

#include <string>
#include <variant>

#include "core/search/base.h"
#include "facade/reply_builder.h"
#include "io/io.h"

namespace dfly::aggregate {

struct Reducer;

using Value = ::dfly::search::SortableValue;

// DocValues sent through the pipeline
// TODO: Replace DocValues with compact linear search map instead of hash map
using DocValues = absl::flat_hash_map<std::string_view, Value>;

struct AggregationResult {
  // Values to be passed to the next step
  std::vector<DocValues> values;

  // Fields from values to be printed
  absl::flat_hash_set<std::string_view> fields_to_print;
};

struct SortParams {
  enum class SortOrder { ASC, DESC };

  constexpr static int64_t kSortAll = -1;

  bool SortAll() const {
    return max == kSortAll;
  }

  /* Fields to sort by. If multiple fields are provided, sorting works hierarchically:
     - First, the i-th field is compared.
     - If the i-th field values are equal, the (i + 1)-th field is compared, and so on. */
  absl::InlinedVector<std::pair<std::string, SortOrder>, 2> fields;
  /* Max number of elements to include in the sorted result.
     If set, only the first [max] elements are fully sorted using partial_sort. */
  int64_t max = kSortAll;
};

struct Aggregator {
  void DoGroup(absl::Span<const std::string> fields, absl::Span<const Reducer> reducers);
  void DoSort(const SortParams& sort_params);
  void DoLimit(size_t offset, size_t num);

  AggregationResult result;
};

using AggregationStep = std::function<void(Aggregator*)>;  // Group, Sort, etc.

// Iterator over Span<DocValues> that yields doc[field] or monostate if not present.
// Extra clumsy for STL compatibility!
struct ValueIterator {
  using iterator_category = std::forward_iterator_tag;
  using difference_type = std::ptrdiff_t;
  using value_type = const Value;
  using pointer = const Value*;
  using reference = const Value&;

  ValueIterator(std::string_view field, absl::Span<const DocValues> values)
      : field_{field}, values_{values} {
  }

  const Value& operator*() const;

  ValueIterator& operator++();

  bool operator==(const ValueIterator& other) const {
    return values_.size() == other.values_.size();
  }

  bool operator!=(const ValueIterator& other) const {
    return !operator==(other);
  }

  static ValueIterator end() {
    return ValueIterator{};
  }

 private:
  ValueIterator() = default;

  std::string_view field_;
  absl::Span<const DocValues> values_;
};

struct Reducer {
  using Func = Value (*)(ValueIterator);
  std::string source_field, result_field;
  Func func;
};

enum class ReducerFunc { COUNT, COUNT_DISTINCT, SUM, AVG, MAX, MIN };

// Find reducer function by uppercase name (COUNT, MAX, etc...), empty functor if not found
Reducer::Func FindReducerFunc(ReducerFunc name);

// Make `GROUPBY [fields...]`  with REDUCE step
AggregationStep MakeGroupStep(std::vector<std::string> fields, std::vector<Reducer> reducers);

// Make `SORTBY field [DESC]` step
AggregationStep MakeSortStep(SortParams sort_params);

// Make `LIMIT offset num` step
AggregationStep MakeLimitStep(size_t offset, size_t num);

// Process values with given steps
AggregationResult Process(std::vector<DocValues> values,
                          absl::Span<const std::string_view> fields_to_print,
                          absl::Span<const AggregationStep> steps);

}  // namespace dfly::aggregate
