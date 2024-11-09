// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/container/inlined_vector.h>

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "base/pmr/memory_resource.h"
#include "core/string_map.h"

namespace dfly::search {

using DocId = uint32_t;

enum class VectorSimilarity { L2, COSINE };

using OwnedFtVector = std::pair<std::unique_ptr<float[]>, size_t /* dimension (size) */>;

// Query params represent named parameters for queries supplied via PARAMS.
struct QueryParams {
  std::string_view operator[](std::string_view name) const;
  std::string& operator[](std::string_view k);

  size_t Size() const {
    return params.size();
  }

 private:
  absl::flat_hash_map<std::string, std::string> params;
};

struct SortOption {
  std::string field;
  bool descending = false;
};

// Comparable string stored as char[]. Used to reduce size of std::variant with strings.
struct WrappedStrPtr {
  // Intentionally implicit and const std::string& for use in templates
  WrappedStrPtr(const PMR_NS::string& s);
  WrappedStrPtr(const std::string& s);
  bool operator<(const WrappedStrPtr& other) const;
  bool operator>=(const WrappedStrPtr& other) const;

  operator std::string_view() const;

 private:
  std::unique_ptr<char[]> ptr;
};

// Score produced either by KNN (float) or SORT (double / wrapped str)
using ResultScore = std::variant<std::monostate, float, double, WrappedStrPtr>;

// Values are either sortable as doubles or strings, or not sortable at all.
// Contrary to ResultScore it doesn't include KNN results and is not optimized for smaller struct
// size.
using SortableValue = std::variant<std::monostate, double, std::string>;

// Interface for accessing document values with different data structures underneath.
struct DocumentAccessor {
  // nullopt if field cannot be converted to type T
  template <typename T> using AccessResult = std::optional<T>;

  using VectorInfo = search::OwnedFtVector;
  using StringList = absl::InlinedVector<std::string_view, 1>;
  using NumsList = absl::InlinedVector<double, 1>;

  virtual ~DocumentAccessor() = default;

  virtual AccessResult<StringList> GetStrings(std::string_view active_field) const = 0;
  virtual AccessResult<VectorInfo> GetVector(std::string_view active_field) const = 0;
  virtual AccessResult<NumsList> GetNumbers(std::string_view active_field) const = 0;
};

// Base class for type-specific indices.
//
// Queries should be done directly on subclasses with their distinc
// query functions. All results for all index types should be sorted.
struct BaseIndex {
  virtual ~BaseIndex() = default;

  /* Returns true if the field type in the document matches the index and, therefore, the document
  can be added.
  TODO: Return the data needed for the Add() function to avoid retrieving the same data twice.*/
  virtual bool IsValidFieldType(DocumentAccessor* doc, std::string_view field) = 0;

  // Before adding the document make sure that Matches() returns true
  virtual void Add(DocId id, DocumentAccessor* doc, std::string_view field) = 0;
  virtual void Remove(DocId id, DocumentAccessor* doc, std::string_view field) = 0;
};

// Base class for type-specific sorting indices.
struct BaseSortIndex : BaseIndex {
  virtual SortableValue Lookup(DocId doc) const = 0;
  virtual std::vector<ResultScore> Sort(std::vector<DocId>* ids, size_t limit, bool desc) const = 0;
};

/* Used for converting field values to double. Returns false if the conversion fails */
std::optional<double> ParseNumericField(std::string_view value);

/* Temporary method to create an empty AccessResult<StringList> or AccessResult<NumsList>.
   The problem is that due to internal implementation details of absl::InlineVector, we are getting
   a -Wmaybe-uninitialized compiler warning. To suppress this false warning, we temporarily disable
   it around this block of code using GCC diagnostic directives. */
template <typename InlinedVector>
DocumentAccessor::AccessResult<InlinedVector> EmptyAccessResult() {
  // GCC 13.1 throws spurious warnings around this code.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
  return InlinedVector{};
#pragma GCC diagnostic pop
}

}  // namespace dfly::search
