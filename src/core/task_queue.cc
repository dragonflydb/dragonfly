// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/task_queue.h"

namespace dfly {

__thread unsigned TaskQueue::blocked_submitters_ = 0;

}  // namespace dfly
