// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <functional>
#include <string>

#include "core/search/base.h"

namespace dfly::search {

struct AstNode;

// Interface for accessing document values with different data structures underneath.
struct DocumentAccessor {
  // Callback that's supplied with field values.
  using FieldConsumer = std::function<bool(std::string_view)>;
  virtual ~DocumentAccessor() = default;

  virtual bool Check(FieldConsumer f, std::string_view active_field) const = 0;
};

class SearchAlgorithm {
 public:
  SearchAlgorithm();
  ~SearchAlgorithm();

  // Init with query and return true if successful.
  bool Init(std::string_view query);

  // Interface will change
  bool Check(DocumentAccessor* accessor) const;

 private:
  std::unique_ptr<AstNode> query_;
};

}  // namespace dfly::search
