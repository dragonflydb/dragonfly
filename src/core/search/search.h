// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "core/search/ast_expr.h"

namespace dfly::search {

AstExpr ParseQuery(std::string_view query);

}  // namespace dfly::search
