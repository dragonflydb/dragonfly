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

// Represents input to a search tree.
struct SearchInput {
  // Callback that's supplied with field values.
  using FieldConsumer = std::function<bool(std::string_view)>;

  virtual ~SearchInput() = default;

  // Check if the given callback returns true on any of  the active fields.
  virtual bool Check(FieldConsumer) = 0;

  // Sets a single active field.
  virtual void SelectField(std::string_view field) = 0;

  // Removes current single active field, all fields are active.
  virtual void ClearField() = 0;
};

}  // namespace dfly::search
