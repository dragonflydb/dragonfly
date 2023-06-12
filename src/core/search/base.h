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

// Base class for type-specific indices.
//
// Queries should be done directly on subclasses with their distinc
// query functions. All results for all index types should be sorted.
struct BaseIndex {
  virtual ~BaseIndex() = default;
  virtual void Add(DocId doc, std::string_view value) = 0;
  virtual void Remove(DocId doc, std::string_view value) = 0;
};

}  // namespace dfly::search
