// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "core/core_types.h"
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

struct WrappedStrPtr {
  WrappedStrPtr(const std::string& s);
  bool operator<(const WrappedStrPtr& other) const;
  bool operator>(const WrappedStrPtr& other) const;

 private:
  std::unique_ptr<char[]> ptr;
};

using ResultScore = std::variant<std::monostate, float, int64_t, WrappedStrPtr>;
// static_assert(sizeof(ResultScore) <= 2 * sizeof(int64_t));

// Interface for accessing document values with different data structures underneath.
struct DocumentAccessor {
  using VectorInfo = search::OwnedFtVector;

  virtual ~DocumentAccessor() = default;
  virtual std::string_view GetString(std::string_view active_field) const = 0;
  virtual VectorInfo GetVector(std::string_view active_field) const = 0;
};

// Base class for type-specific indices.
//
// Queries should be done directly on subclasses with their distinc
// query functions. All results for all index types should be sorted.
struct BaseIndex {
  virtual ~BaseIndex() = default;
  virtual void Add(DocId id, DocumentAccessor* doc, std::string_view field) = 0;
  virtual void Remove(DocId id, DocumentAccessor* doc, std::string_view field) = 0;
};

// Base class for type-specific sorting indices.
struct BaseSortIndex : BaseIndex {
  virtual void Sort(std::vector<DocId>* entries, std::vector<ResultScore>* out, size_t limit,
                    bool desc) const = 0;
};

}  // namespace dfly::search
