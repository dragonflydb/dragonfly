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

using Value = ::dfly::search::SortableValue;
using DocValues = absl::flat_hash_map<std::string, Value>;  // documents sent through the pipeline

struct PipelineResult {
  // Values to be passed to the next step
  // TODO: Replace DocValues with compact linear search map instead of hash map
  std::vector<DocValues> values;

  // Fields from values to be printed
  absl::flat_hash_set<std::string> fields_to_print;
};

using PipelineStep = std::function<PipelineResult(PipelineResult)>;  // Group, Sort, etc.

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
PipelineStep MakeGroupStep(absl::Span<const std::string_view> fields,
                           std::vector<Reducer> reducers);

// Make `SORTBY field [DESC]` step
PipelineStep MakeSortStep(std::string_view field, bool descending = false);

// Make `LIMIT offset num` step
PipelineStep MakeLimitStep(size_t offset, size_t num);

// Process values with given steps
PipelineResult Process(std::vector<DocValues> values,
                       absl::Span<const std::string_view> fields_to_print,
                       absl::Span<const PipelineStep> steps);

}  // namespace dfly::aggregate
