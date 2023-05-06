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

// Interface for accessing hashset values with different data structures underneath.
struct HSetAccessor {
  // Callback that's supplied with field values.
  using FieldConsumer = std::function<bool(std::string_view)>;

  virtual bool Check(FieldConsumer f, std::string_view active_field) const = 0;
};

// Wrapper around hashset accessor and optional active field.
struct SearchInput {
  SearchInput(const HSetAccessor* hset, std::string_view active_field = {})
      : hset_{hset}, active_field_{active_field} {
  }

  SearchInput(const SearchInput& base, std::string_view active_field)
      : hset_{base.hset_}, active_field_{active_field} {
  }

  bool Check(HSetAccessor::FieldConsumer f) {
    return hset_->Check(move(f), active_field_);
  }

 private:
  const HSetAccessor* hset_;
  std::string_view active_field_;
};

}  // namespace dfly::search
