// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/namespaces.h"

#include <ranges>

#include "base/flags.h"
#include "base/logging.h"
#include "server/blocking_controller.h"
#include "server/db_slice.h"
#include "server/engine_shard_set.h"

ABSL_DECLARE_FLAG(bool, cache_mode);
ABSL_DECLARE_FLAG(std::string, notify_keyspace_events);

namespace dfly {

using namespace std;

Namespace::Namespace() {
  shard_db_slices_.resize(shard_set->size());
  shard_blocking_controller_.resize(shard_set->size());
  shard_set->RunBriefInParallel([&](EngineShard* es) {
    CHECK(es != nullptr);
    ShardId sid = es->shard_id();
    shard_db_slices_[sid] = make_unique<DbSlice>(sid, absl::GetFlag(FLAGS_cache_mode), es, this);
  });
}

DbSlice& Namespace::GetCurrentDbSlice() {
  EngineShard* es = EngineShard::tlocal();
  CHECK(es != nullptr);
  return GetDbSlice(es->shard_id());
}

DbSlice& Namespace::GetDbSlice(ShardId sid) {
  CHECK_LT(sid, shard_db_slices_.size());
  return *shard_db_slices_[sid];
}

BlockingController* Namespace::GetOrAddBlockingController(EngineShard* shard) {
  if (!shard_blocking_controller_[shard->shard_id()]) {
    shard_blocking_controller_[shard->shard_id()] = make_unique<BlockingController>(shard, this);
  }

  return shard_blocking_controller_[shard->shard_id()].get();
}

BlockingController* Namespace::GetBlockingController(ShardId sid) {
  return shard_blocking_controller_[sid].get();
}

Namespaces::Namespaces() {
  {
    // Startup only: CONFIG SET is not reachable yet, the validated flag is safe to read.
    util::fb2::LockGuard guard(mu_);
    expired_events_recording_default_ = !absl::GetFlag(FLAGS_notify_keyspace_events).empty();
  }
  default_namespace_ = &GetOrInsert("");
}

Namespaces::~Namespaces() {
  Clear();
}

void Namespaces::Clear() {
  util::fb2::LockGuard guard(mu_);

  default_namespace_ = nullptr;

  if (namespaces_.empty()) {
    return;
  }

  shard_set->RunBriefInParallel([&](EngineShard* es) {
    CHECK(es != nullptr);
    LOG(ERROR) << "Namespaces::Clear: DbSlice::ShutdownThreadLocal shard=" << es->shard_id();
    DbSlice::ShutdownThreadLocal();

    // Marks each DbTable's prime/mcflag arena-destruct (no-op destructor), so the per-key
    // data they hold is left for the shard's mi_heap_destroy() to reclaim in bulk instead of
    // being walked/destructed one entry at a time. DbSlice/DbTable themselves are NOT
    // detached -- they're destructed for real right here (cheaply, now that prime/mcflag are
    // no-ops), explicitly, on this shard's own thread. That matters: DbTable::~DbTable()
    // asserts thread_index == ServerState::tlocal()->thread_index(), so it must run here,
    // inside this per-shard callback -- not later, implicitly, when namespaces_.clear() runs
    // on whatever thread called Namespaces::Clear().
    for (auto& val : ABSL_TS_UNCHECKED_READ(namespaces_) | views::values) {
      auto& db_slice = val.shard_db_slices_[es->shard_id()];
      LOG(ERROR) << "Namespaces::Clear: PrepareForSingleShotHeapDestroy shard=" << es->shard_id();
      db_slice->PrepareForSingleShotHeapDestroy();
      db_slice.reset();
    }
    LOG(ERROR) << "Namespaces::Clear: shard done, shard=" << es->shard_id();
  });

  LOG(ERROR) << "Namespaces::Clear: all shards done, clearing namespaces_ map";
  namespaces_.clear();
}

Namespace& Namespaces::GetDefaultNamespace() const {
  CHECK(default_namespace_ != nullptr);
  return *default_namespace_;
}

void Namespaces::SetExpiredEventsRecording(bool enable) {
  util::fb2::LockGuard guard(mu_);
  expired_events_recording_default_ = enable;
  // mu_ serializes this with namespace creation, which inherits the default.
  shard_set->pool()->AwaitFiberOnAll([&](unsigned, util::ProactorBase*) {
    EngineShard* shard = EngineShard::tlocal();
    if (shard) {
      for (auto& entry : ABSL_TS_UNCHECKED_READ(namespaces_)) {
        entry.second.GetDbSlice(shard->shard_id()).SetExpiredEventsRecording(enable);
      }
    }
  });
}

Namespace& Namespaces::GetOrInsert(std::string_view ns) {
  {
    // Try to look up under a shared lock
    dfly::SharedLock guard(mu_);
    auto it = namespaces_.find(ns);
    if (it != namespaces_.end()) {
      return it->second;
    }
  }

  {
    // Key was not found, so we create create it under unique lock
    util::fb2::LockGuard guard(mu_);
    auto it = namespaces_.find(ns);
    if (it != namespaces_.end()) {
      return it->second;
    }

    Namespace& new_ns = namespaces_[ns];
    // Not published yet (mu_ is held), so plain writes are safe.
    for (ShardId sid = 0; sid < shard_set->size(); ++sid) {
      new_ns.GetDbSlice(sid).SetExpiredEventsRecording(expired_events_recording_default_);
    }
    return new_ns;
  }
}

}  // namespace dfly
