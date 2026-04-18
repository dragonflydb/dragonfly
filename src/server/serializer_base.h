// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>

#include <memory>
#include <vector>

#include "io/io.h"
#include "server/synchronization.h"
#include "server/table.h"
#include "util/fibers/future.h"
#include "util/fibers/synchronization.h"

namespace dfly {

class ExecutionState;
struct TestDriver;

// Opaque identity for a physical DashTable bucket — its memory address.
// Unique across all databases/segments for the lifetime of a serialization.
using BucketIdentity = uintptr_t;

// Track dependencies for buckets.
// Asynchronous processes during bucket serialization like big value streaming and tiered value
// loading must increase the dependency count for their bucket to hold off changes to it.
struct BucketDependencies {
  // Increase number of dependencies for bucket
  void Increment(BucketIdentity bucket);
  void Decrement(BucketIdentity bucket);

  // Wait for all bucket dependencies to resolve
  void Wait(BucketIdentity bucket) const;
  bool DEBUG_IsBusy(BucketIdentity) const;

 private:
  using SharedLatch = std::shared_ptr<LocalLatch>;
  absl::flat_hash_map<BucketIdentity, SharedLatch> deps_;
};

struct TieredDelayedEntry {
  DbIndex dbid;
  PrimeKey key;
  util::fb2::Future<io::Result<std::string>> value;
  time_t expire;
  uint32_t mc_flags;
};

// Tracks serialization progress of offloaded (delayed) entries.
struct DelayedEntryHandler {
  void EnqueueOffloaded(BucketIdentity bucket, DbIndex db_index, PrimeKey pk, const PrimeValue& pv,
                        time_t expire_time, uint32_t mc_flags);

  // Must be called periodically to progress on delayed entries. Calls SerializeFetchedEntry.
  // If force is false, only serializes entries whose futures are already resolved.
  // If flush_bucket is provided, flushes all entries belonging to this bucket.
  void ProcessDelayedEntries(bool force, BucketIdentity flush_bucket, ExecutionState* cntx);

  // Serialize delayed entry that was fetched with serializer specific implementation
  virtual void SerializeFetchedEntry(const TieredDelayedEntry& tde, const PrimeValue& pv) = 0;

 protected:
  explicit DelayedEntryHandler(BucketDependencies& deps) : deps_{deps} {
  }

 private:
  friend struct TestDriver;

  BucketDependencies& deps_;

  // Entries that are waiting for tiered storage reads to complete before they can be serialized.
  std::multimap<BucketIdentity, std::unique_ptr<TieredDelayedEntry>> delayed_entries_;
};

// Base class for operations relying on snapshotting and implementing SerializeBucket.
// Progress should be driven externally by calling ProcessBucket().
// Additionally, db_slice change listeners can be registered that invoke SerializeBucket
// before any modification are performed to ensure point-in-time isolation.
class SerializerBase : public BucketDependencies, public DelayedEntryHandler {
 public:
  struct Stats {
    uint64_t keys_serialized = 0;              // total number of keys serialized
    uint64_t buckets_serialized = 0;           // total number of buckets serialized
    uint64_t buckets_on_change = 0;            // buckets serialized by OnChangeBlocking flow
    uint64_t buckets_skipped = 0;              // already Covered when seen
    uint64_t change_during_serialization = 0;  // change hit an in-flight bucket
  };

  explicit SerializerBase(DbSlice* slice, ExecutionState* cntx);
  virtual ~SerializerBase();

  // Register db_slice change listener and save snapshot version.
  void RegisterChangeListener();

  // Unregisters the callback.  Safe to call if already unregistered.
  void UnregisterChangeListener();

  const Stats& GetStats() const {
    return stats_;
  }

 protected:
  // Process bucket if needed,
  // on_update is true if it's being called in the OnChangeBlocking flow,
  // and false if called by the traversal loop.
  bool ProcessBucket(DbIndex db_index, PrimeTable::bucket_iterator it, bool on_update);

  // Serialize a single bucket. Returns the number of entries serialized.
  // To be implemented by classses extending this base class.
  // Currently runs with big_value_mu_ held.
  virtual unsigned SerializeBucketLocked(DbIndex db_index, PrimeTable::bucket_iterator it,
                                         bool on_update) = 0;

  // Called when an existing bucket is about to be mutated. Calls ProcessBucket.
  void OnChangeBlocking(DbIndex db_index, PrimeTable::bucket_iterator it);

  // Called when a new key is about to be inserted. Calls ProcessBucket for the buckets.
  void OnChangeBlocking(DbIndex db_index, const PrimeTable::BucketSet& set);

  // --- Shared members (to be moved from subclasses in later PRs) ---

  DbSlice* const db_slice_;
  ExecutionState* const base_cntx_;

  DbTableArray db_array_;

  uint64_t snapshot_version_ = 0;
  ThreadLocalMutex big_value_mu_;
  Stats stats_;

 private:
  // Process single bucket and call SerializeBucket. Return true if processed, false if skipped
  bool ProcessBucketInternal(DbIndex db_index, PrimeTable::bucket_iterator it, bool on_update);

  uint64_t change_cb_id_ = 0;
};

}  // namespace dfly
