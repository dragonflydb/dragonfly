// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

// An import header that centralizes all the imports from helio project regarding fibers

#ifdef USE_FB2

#include "util/fibers/fiber2.h"
#include "util/fibers/fiberqueue_threadpool.h"
#include "util/fibers/future.h"
#include "util/fibers/simple_channel.h"

namespace dfly {

using util::fb2::Barrier;
using util::fb2::BlockingCounter;
using util::fb2::CondVar;
using util::fb2::Done;
using util::fb2::EventCount;
using util::fb2::Fiber;
using util::fb2::FiberQueue;
using util::fb2::FiberQueueThreadPool;
using util::fb2::Future;
using util::fb2::Launch;
using util::fb2::Mutex;
using util::fb2::Promise;
using util::fb2::SimpleChannel;

}  // namespace dfly

namespace util {
using fb2::SharedMutex;
}

#else

#include "util/fiber_sched_algo.h"
#include "util/fibers/event_count.h"
#include "util/fibers/fiber.h"
#include "util/fibers/fiberqueue_threadpool.h"
#include "util/fibers/fibers_ext.h"
#include "util/fibers/simple_channel.h"

namespace dfly {

using util::fibers_ext::Barrier;
using util::fibers_ext::BlockingCounter;
using util::fibers_ext::Done;
using util::fibers_ext::EventCount;
using util::fibers_ext::Fiber;
using util::fibers_ext::FiberQueue;
using util::fibers_ext::FiberQueueThreadPool;
using util::fibers_ext::Future;
using util::fibers_ext::Launch;
using util::fibers_ext::Mutex;
using util::fibers_ext::Promise;
using util::fibers_ext::SimpleChannel;
using CondVar = ::boost::fibers::condition_variable;

}  // namespace dfly

#endif
