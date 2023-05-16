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

  virtual bool Check(FieldConsumer f, std::string_view active_field) const = 0;
};

// Wrapper around document accessor and optional active field.
struct SearchInput {
  SearchInput(const DocumentAccessor* doc, std::string_view active_field = {})
      : doc_{doc}, active_field_{active_field} {
  }

  SearchInput(const SearchInput& base, std::string_view active_field)
      : doc_{base.doc_}, active_field_{active_field} {
  }

  bool Check(DocumentAccessor::FieldConsumer f) {
    return doc_->Check(move(f), active_field_);
  }

 private:
  const DocumentAccessor* doc_;
  std::string_view active_field_;
};

}  // namespace dfly::search
