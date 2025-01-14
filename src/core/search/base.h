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
  using VectorInfo = search::OwnedFtVector;
  using StringList = absl::InlinedVector<std::string_view, 1>;
  using NumsList = absl::InlinedVector<double, 1>;

  virtual ~DocumentAccessor() = default;

  /* Returns nullopt if the specified field is not a list of strings */
  virtual std::optional<StringList> GetStrings(std::string_view active_field) const = 0;

  /* Returns nullopt if the specified field is not a vector */
  virtual std::optional<VectorInfo> GetVector(std::string_view active_field) const = 0;

  /* Return nullopt if the specified field is not a list of doubles */
  virtual std::optional<NumsList> GetNumbers(std::string_view active_field) const = 0;

  /* Same as GetStrings, but also supports boolean values */
  virtual std::optional<StringList> GetTags(std::string_view active_field) const = 0;
};

// Base class for type-specific indices.
//
// Queries should be done directly on subclasses with their distinc
// query functions. All results for all index types should be sorted.
struct BaseIndex {
  virtual ~BaseIndex() = default;

  // Returns true if the document was added / indexed
  virtual bool Add(DocId id, const DocumentAccessor& doc, std::string_view field) = 0;
  virtual void Remove(DocId id, const DocumentAccessor& doc, std::string_view field) = 0;
};

// Base class for type-specific sorting indices.
struct BaseSortIndex : BaseIndex {
  virtual SortableValue Lookup(DocId doc) const = 0;
  virtual std::vector<ResultScore> Sort(std::vector<DocId>* ids, size_t limit, bool desc) const = 0;
};

/* Used for converting field values to double. Returns std::nullopt if the conversion fails */
std::optional<double> ParseNumericField(std::string_view value);

/* Temporary method to create an empty std::optional<InlinedVector> in DocumentAccessor::GetString
   and DocumentAccessor::GetNumbers methods. The problem is that due to internal implementation
   details of absl::InlineVector, we are getting a -Wmaybe-uninitialized compiler warning. To
   suppress this false warning, we temporarily disable it around this block of code using GCC
   diagnostic directives. */
template <typename InlinedVector> std::optional<InlinedVector> EmptyAccessResult() {
#if !defined(__clang__)
  // GCC 13.1 throws spurious warnings around this code.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

  return InlinedVector{};

#if !defined(__clang__)
#pragma GCC diagnostic pop
#endif
}

}  // namespace dfly::search
