#pragma once

#include <algorithm>
#include <iostream>
#include <memory>
#include <ostream>
#include <regex>
#include <variant>
#include <vector>

#include "core/core_types.h"
#include "core/string_map.h"

namespace dfly::search {

using DocId = uint32_t;

using FtVector = std::vector<float>;

// Query params represent named parameters for queries supplied via PARAMS.
// Currently its only a placeholder to pass the vector to KNN.
struct QueryParams {
  FtVector knn_vec;
};

// Interface for accessing document values with different data structures underneath.
struct DocumentAccessor {
  virtual ~DocumentAccessor() = default;
  virtual std::string_view GetString(std::string_view active_field) const = 0;
  virtual FtVector GetVector(std::string_view active_field) const = 0;
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

}  // namespace dfly::search
