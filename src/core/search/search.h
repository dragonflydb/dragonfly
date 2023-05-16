// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "core/search/base.h"

namespace dfly::search {

struct AstNode;

class SearchAlgorithm {
 public:
  SearchAlgorithm() = default;
  SearchAlgorithm(std::string_view query);

  bool Check(DocumentAccessor* accessor) const;

 private:
  std::unique_ptr<AstNode> query{};
};

}  // namespace dfly::search
