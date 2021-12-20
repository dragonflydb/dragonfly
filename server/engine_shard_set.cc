// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/engine_shard_set.h"

#include "base/logging.h"
#include "util/fiber_sched_algo.h"
#include "util/varz.h"

namespace dfly {

using namespace std;
using namespace util;
namespace fibers = ::boost::fibers;
namespace this_fiber = ::boost::this_fiber;

thread_local EngineShard* EngineShard::shard_ = nullptr;
constexpr size_t kQueueLen = 64;

EngineShard::EngineShard(util::ProactorBase* pb)
    : queue_(kQueueLen), db_slice_(pb->GetIndex(), this) {
  fiber_q_ = fibers::fiber([this, index = pb->GetIndex()] {
    this_fiber::properties<FiberProps>().set_name(absl::StrCat("shard_queue", index));
    queue_.Run();
  });

  periodic_task_ = pb->AddPeriodic(1, [] {
    auto* shard = EngineShard::tlocal();
    DCHECK(shard);
    // absl::GetCurrentTimeNanos() returns current time since the Unix Epoch.
    shard->db_slice().UpdateExpireClock(absl::GetCurrentTimeNanos() / 1000000);
  });
}

EngineShard::~EngineShard() {
  queue_.Shutdown();
  fiber_q_.join();
  if (periodic_task_) {
    ProactorBase::me()->CancelPeriodic(periodic_task_);
  }
}

void EngineShard::InitThreadLocal(ProactorBase* pb) {
  CHECK(shard_ == nullptr) << pb->GetIndex();
  shard_ = new EngineShard(pb);
}

void EngineShard::DestroyThreadLocal() {
  if (!shard_)
    return;

  uint32_t index = shard_->db_slice_.shard_id();
  delete shard_;
  shard_ = nullptr;

  VLOG(1) << "Shard reset " << index;
}

void EngineShardSet::Init(uint32_t sz) {
  CHECK_EQ(0u, size());

  shard_queue_.resize(sz);
}

void EngineShardSet::InitThreadLocal(ProactorBase* pb) {
  EngineShard::InitThreadLocal(pb);
  EngineShard* es = EngineShard::tlocal();
  shard_queue_[es->shard_id()] = es->GetQueue();
}

}  // namespace dfly
