// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/resp_expr.h"

#include "base/logging.h"

namespace facade {

void RespExpr::VecToArgList(const Vec& src, CmdArgVec* dest) {
  dest->resize(src.size());
  for (size_t i = 0; i < src.size(); ++i) {
    DCHECK(src[i].type == RespExpr::STRING);

    (*dest)[i] = ToSV(src[i].GetBuf());
  }
}

}  // namespace facade
