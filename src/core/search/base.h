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

enum class VectorSimilarity { L2, CONSINE };

using OwnedFtVector = std::pair<std::unique_ptr<float[]>, size_t /*dims*/>;

// Query params represent named parameters for queries supplied via PARAMS.
struct QueryParams {
  size_t Size() const {
    return params.size();
  }

  std::string_view operator[](std::string_view name) const {
    if (auto it = params.find(name); it != params.end())
      return it->second;
    return "";
  }

  std::string& operator[](std::string_view k) {
    return params[k];
  }

 private:
  absl::flat_hash_map<std::string, std::string> params;
};

// Interface for accessing document values with different data structures underneath.
struct DocumentAccessor {
  using VectorInfo = std::pair<std::unique_ptr<float[]>, size_t /* dims */>;

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

}  // namespace dfly::search
