// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/serializer_base.h"

#include <absl/strings/match.h>
#include <absl/strings/str_join.h>

#include "base/logging.h"
#include "redis/redis_aux.h"
#include "server/common_types.h"
#include "server/db_slice.h"
#include "server/engine_shard.h"
#include "server/execution_state.h"
#include "server/journal/journal.h"
#include "server/synchronization.h"
#include "server/table.h"
#include "server/tiered_storage.h"
#include "util/fibers/fibers.h"
#include "util/fibers/stacktrace.h"

namespace dfly {

void BucketDependencies::Increment(BucketIdentity bucket) {
  auto& counter = deps_[bucket];
  if (!counter)
    counter = std::make_shared<LocalLatch>();
  counter->lock();
}

void BucketDependencies::Decrement(BucketIdentity bucket) {
  auto it = deps_.find(bucket);
  CHECK(it != deps_.end());

  it->second->unlock();
  if (!it->second->IsBlocked())
    deps_.erase(it);
}

void BucketDependencies::Wait(BucketIdentity bucket) const {
  auto it = deps_.find(bucket);
  if (it == deps_.end())
    return;

  auto counter = it->second;  // copy value for address stability
  counter->Wait();
}

bool BucketDependencies::DEBUG_IsBusy(BucketIdentity bucket) const {
  return deps_.contains(bucket);
}

void DelayedEntryHandler::EnqueueOffloaded(BucketIdentity bucket, DbIndex db_index, PrimeKey pk,
                                           const PrimeValue& pv, time_t expire_time,
                                           uint32_t mc_flags) {
  DCHECK(pv.IsExternal());
  DCHECK(!pv.IsCool());
  DCHECK_EQ(pv.ObjType(), OBJ_STRING);

  auto key = pk.ToString();
  auto future = ReadTieredString(db_index, key, pv, EngineShard::tlocal()->tiered_storage());
  auto entry = std::make_unique<TieredDelayedEntry>(db_index, std::move(pk), std::move(future),
                                                    expire_time, mc_flags);

  deps_.Increment(bucket);
  delayed_entries_.emplace(bucket, std::move(entry));
}

void DelayedEntryHandler::ProcessDelayedEntries(bool force, BucketIdentity flush_bucket,
                                                ExecutionState* cntx) {
  const size_t kMaxDelayedEntries = 512;
  if (delayed_entries_.size() > kMaxDelayedEntries)
    force |= true;

  auto serialize_entry = [&](decltype(delayed_entries_)::iterator it) {
    auto& entry = it->second;
    auto value = entry->value.Get();

    if (!value.has_value()) {
      deps_.Decrement(it->first);
      cntx->ReportError(make_error_code(std::errc::io_error),
                        absl::StrCat("Failed to read tiered key: ", entry->key.ToString()));
      return;
    }

    PrimeValue pv{*value};
    SerializeFetchedEntry(*entry, pv);

    deps_.Decrement(it->first);
    delayed_entries_.erase(it++);
  };

  // Flush all entries of bucket
  if (flush_bucket) {
    auto range = delayed_entries_.equal_range(flush_bucket);
    for (auto it = range.first; it != range.second;) {
      serialize_entry(it++);
    }
  }

  // Serialize the delayed entries that are resolved, or all if force it true.
  for (auto it = delayed_entries_.begin(); it != delayed_entries_.end();) {
    if (!force && !it->second->value.IsResolved())
      it++;
    else
      serialize_entry(it++);
  }
}

SerializerBase::SerializerBase(DbSlice* slice, ExecutionState* cntx)
    : DelayedEntryHandler(static_cast<BucketDependencies&>(*this)),
      db_slice_(slice),
      base_cntx_(cntx) {
  DCHECK(base_cntx_);
}

SerializerBase::~SerializerBase() {
}

// Ordering invariant:
//   For any key K, the replica must receive K's baseline value strictly before any journal entry
//   that mutates K.
// RegisterChangeListener registers the DbSlice callback that routes mutations through
//   SerializerBase::OnChange. ConsumeJournalChange runs later on the
//   same fiber, so the baseline is serialized first. big_value_mu_ prevents this callback path
//   from interleaving with the traversal fiber's bucket serialization, which may preempt while
//   emitting large values.
void SerializerBase::RegisterChangeListener(bool replica) {
  db_array_ = db_slice_->databases();  // copy pointers to survive flush
  auto cb = [this](DbIndex dbid, const ChangeReq& req) {
    std::visit([&](auto it) { OnChangeBlocking(dbid, it); }, req);
  };
  snapshot_version_ = db_slice_->RegisterOnChange(replica, cb);
}

void SerializerBase::UnregisterChangeListener() {
  if (auto version = std::exchange(snapshot_version_, 0); version > 0)
    db_slice_->UnregisterOnChange(version);
}

bool SerializerBase::ProcessBucket(DbIndex db_index, PrimeTable::bucket_iterator it,
                                   bool on_update) {
  std::lock_guard guard(big_value_mu_);
  return ProcessBucketInternal(db_index, it, on_update);
}

bool SerializerBase::ProcessBucketInternal(DbIndex db_index, PrimeTable::bucket_iterator it,
                                           bool on_update) {
  DCHECK(big_value_mu_.is_locked());

  // Check if this bucket is stale
  if (it.GetVersion() >= snapshot_version_) {
    stats_.buckets_skipped++;

    // Force flush all delayed entries in the touched bucket
    if (EngineShard::tlocal()->tiered_storage() != nullptr && on_update)
      ProcessDelayedEntries(false, it.bucket_address(), base_cntx_);

    // Expected to be fully serialized due to big_value_mu_ guarding all paths
    // Otherwise, this needs to be changed to a wait
    DCHECK(!BucketDependencies::DEBUG_IsBusy(it.bucket_address()));
    return false;
  }

  // TODO: Flushing to earlier callbacks
  if (it.is_done()) {
    it.SetVersion(snapshot_version_);
    return false;
  }

  // For non updates (traversal flow), flush change to earlier snapshots and
  // acquire serialization latch.
  // We must make sure that earlier snapshots serialized this bucket before we update its
  // version below.
  std::optional<std::lock_guard<LocalLatch>> db_guard;
  if (!on_update) {
    db_slice_->FlushChangeToEarlierCallbacks(db_index, DbSlice::Iterator::FromPrime(it),
                                             snapshot_version_);
    db_guard.emplace(*db_slice_->GetLatch());
  }

  // We call it before SerializeBucketLocked because it dchecks on bucket version.
  it.SetVersion(snapshot_version_);
  BucketDependencies::Increment(it.bucket_address());

  stats_.keys_serialized += SerializeBucketLocked(db_index, it, on_update);
  stats_.buckets_serialized++;
  stats_.buckets_on_change += unsigned(on_update);

  BucketDependencies::Decrement(it.bucket_address());

  // Assert the version is equal to a snapshot version (might be a different concurrent one),
  // to prove no concurrent modifications are possible (they would've assigned a different version)
#if !defined(NDEBUG)
  DCHECK_GE(it.GetVersion(), snapshot_version_);
  auto current_snapshots = db_slice_->SnapshotVersions();
  DCHECK(std::ranges::find(current_snapshots, it.GetVersion()) != current_snapshots.end())
      << absl::StrJoin(current_snapshots, " ") << " does not contain " << it.GetVersion();
#endif

  if (EngineShard::tlocal()->tiered_storage() != nullptr)
    ProcessDelayedEntries(false, on_update ? it.bucket_address() : 0, base_cntx_);

  return true;
}

void SerializerBase::OnChangeBlocking(DbIndex db_index, PrimeTable::bucket_iterator it) {
  std::string_view active_name = util::fb2::detail::FiberActive()->name();
  if (!absl::StartsWith(active_name, "shard_queue") &&  //
      !absl::StartsWith(active_name, "l2_queue") &&     // pipelining
      !absl::StartsWith(active_name, "SliceSnapshot") &&
      active_name != "Dispatched"  // Comes from OnAllShards(... { migration->RunSync(); });
  ) {
    LOG(DFATAL) << "Unexpected fiber: " << active_name << " on " << util::fb2::GetStacktrace();
  }

  ProcessBucket(db_index, it, true);
}

void SerializerBase::OnChangeBlocking(DbIndex db_index, const PrimeTable::BucketSet& set) {
  // We must acquire the mutex ahead and process all buckets under the same lock.
  // This ensures that bucket processing and the table insertion that invoked this callback
  // will be operating on the same state as all writes are linarly ordered by this mutex.
  std::unique_lock lk{big_value_mu_};

  // We call Process even for up-to-date buckets to ensure all operations (delayed) are finished.
  for (auto it : set.buckets())
    ProcessBucketInternal(db_index, it, true);
}

}  // namespace dfly
