// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/resp_expr.h"

#include "base/logging.h"

namespace facade {

void FillBackedArgs(const RespVec& src, cmn::BackedArguments* dest) {
  auto map = [](const RespExpr& expr) { return expr.GetView(); };
  auto range = base::it::Transform(map, base::it::Range(src.begin(), src.end()));

  dest->Assign(range.begin(), range.end(), src.size());
}

}  // namespace facade
