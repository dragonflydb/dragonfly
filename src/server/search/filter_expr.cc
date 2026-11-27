// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/filter_expr.h"

namespace dfly::aggregate {

void FilterExprDeleter::operator()(FilterExprNode* node) const noexcept {
  if (!node)
    return;

  // Detach children before deleting their owner. The intrusive work list needs no allocation,
  // even when a failed parse is being unwound after an allocation failure.
  FilterExprNode* pending = node;
  node->teardown_next_ = nullptr;
  while (pending) {
    node = pending;
    pending = node->teardown_next_;
    node->Destroy(pending);
  }
}

}  // namespace dfly::aggregate
