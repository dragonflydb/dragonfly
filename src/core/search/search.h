// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "core/search/base.h"

namespace dfly::search {

struct AstNode;

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
