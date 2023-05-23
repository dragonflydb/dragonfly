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

}  // namespace dfly::search
