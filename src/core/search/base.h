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

// Interface for accessing document values with different data structures underneath.
struct DocumentAccessor {
  // Callback that's supplied with field values.
  using FieldConsumer = std::function<bool(std::string_view)>;
  virtual ~DocumentAccessor() = default;

  virtual bool Check(FieldConsumer f, std::string_view active_field) const = 0;
};

using DocId = uint32_t;

// Base class for type-specific indices.
//
// Queries should be done directly on subclasses with their distinc
// query functions. All results for all index types should be sorted.
struct BaseIndex {
  virtual ~BaseIndex() = default;
  virtual void Add(DocId doc, std::string_view value) = 0;
};

}  // namespace dfly::search
