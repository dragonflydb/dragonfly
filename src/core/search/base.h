#pragma once

#include <absl/container/flat_hash_map.h>

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "core/core_types.h"
#include "core/string_map.h"

namespace dfly::search {

using DocId = uint32_t;

using FtVector = std::vector<float>;

// Query params represent named parameters for queries supplied via PARAMS.
struct QueryParams : private absl::flat_hash_map<std::string, std::string> {
  size_t Size() const {
    return size();
  }

  std::string_view operator[](std::string_view name) const {
    if (auto it = find(name); it != end())
      return it->second;
    return "";
  }

  decltype(auto) operator[](std::string_view k) {
    return static_cast<absl::flat_hash_map<std::string, std::string>*>(this)->operator[](k);
  }
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
