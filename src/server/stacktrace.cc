// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "stacktrace.h"

#include "server/engine_shard_set.h"
#include "util/fibers/detail/fiber_interface.h"
#include "util/fibers/proactor_base.h"
#include "util/fibers/synchronization.h"

namespace dfly {

void PrintStackTraceOfAllFibers() {
  util::fb2::Mutex m;
  shard_set->pool()->AwaitFiberOnAll([&m](unsigned index, util::ProactorBase* base) {
    std::unique_lock lk(m);
    util::fb2::detail::FiberInterface::PrintAllFiberStackTraces();
  });
}

}  // namespace dfly
