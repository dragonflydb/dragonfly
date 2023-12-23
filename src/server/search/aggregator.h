#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/types/span.h>

#include <string>
#include <variant>

#include "facade/reply_builder.h"
#include "io/io.h"

namespace dfly::aggregate {

using Value = std::variant<std::monostate, double, std::string>;
using DocValues = absl::flat_hash_map<std::string, Value>;  // documents sent through the pipeline

using PipelineResult = io::Result<std::vector<DocValues>, facade::ErrorReply>;
using PipelineStep = std::function<PipelineResult(std::vector<DocValues>)>;  // Group, Sort, etc.

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

  const Value& operator*() const {
    auto it = values_.front().find(field_);
    return it == values_.front().end() ? kEmpty : it->second;
  }

  ValueIterator& operator++() {
    values_.remove_prefix(1);
    return *this;
  }

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

  static const Value kEmpty;
};

struct Reducer {
  using Func = std::function<Value(ValueIterator)>;
  std::string source_field, result_field;
  Func func;
};

// Find reducer function by uppercase name (COUNT, MAX, etc...), empty functor if not found
Reducer::Func FindReducerFunc(std::string_view name);

// Make `GROUPBY [fields...]`  with REDUCE step
PipelineStep MakeGroupStep(absl::Span<const std::string_view> fields,
                           std::vector<Reducer> reducers);

// Make `SORYBY field [DESC]` step
PipelineStep MakeSortStep(std::string field, bool descending = false);

// Make `LIMIT offset num` step
PipelineStep MakeLimitStep(size_t offset, size_t num);

// Process values with given steps
PipelineResult Process(std::vector<DocValues> values, absl::Span<PipelineStep> steps);

}  // namespace dfly::aggregate
