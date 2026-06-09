// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>

#include <memory>
#include <vector>

#include "io/io.h"
#include "server/db_slice.h"
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
  // Wait for no dependencies to exist
  void WaitEmpty() const;
  bool HasAny() const {
    return !deps_.empty();
  }

 private:
  using SharedLatch = std::shared_ptr<LocalLatch>;
  absl::flat_hash_map<BucketIdentity, SharedLatch> deps_;

  // Triggered when all dependencies are resolved
  mutable util::fb2::CondVarAny empty_q_;
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

// Serialization pipeline overview:
//
// ┌────────────────────┐     ┌───────────────────┐
// │  Traversal fiber   │     │ OnChange          │
// │ (IterateBucketsFb) │     │ (db_slice change) │
// └────────┬───────────┘     └────────┬──────────┘
//          │                          │
//          ▼                          ▼
// ┌───────────────────────────────────────────────┐
// │           SerializerBase::ProcessBucket       │  (serializer_base.cc)
// │  Stamps bucket version, manages dependencies  │
// └────────────────────┬──────────────────────────┘
//                      │
//                      ▼
// ┌───────────────────────────────────────────────┐
// │        SerializeBucketLocked (virtual)        │  (snapshot.cc / streamer.cc)
// │  Iterates bucket entries, serializes each one │
// └────────────────────┬──────────────────────────┘
//                      │
//          ┌───────────┴───────────┐
//          ▼                       ▼
// ┌─────────────────┐   ┌──────────────────────┐
// │  Inline entry   │   │  Tiered (offloaded)  │
// │  (SaveEntry /   │   │  EnqueueOffloaded →  │
// │   CmdSerializer)│   │  ProcessDelayedEntry │
// └────────┬────────┘   └──────────┬───────────┘
//          │                       │
//          ▼                       ▼
// ┌───────────────────────────────────────────────┐
// │     Output sink (consumer / socket write)     │
// └───────────────────────────────────────────────┘
//
// See also: docs/shard-serialization.md for locking & ordering details.

// Base class for operations relying on snapshotting and implementing SerializeBucket.
// Progress should be driven externally by calling ProcessBucket().
// Additionally, db_slice change listeners can be registered that invoke SerializeBucket
// before any modification are performed to ensure point-in-time isolation.
class SerializerBase : public BucketDependencies,
                       public DelayedEntryHandler,
                       public DbSlice::ChangeConsumerInterface {
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
  // Pass whether this is replication (true) or snapshotting (false)
  void RegisterChangeListener(bool replication);

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
  virtual unsigned SerializeBucketLocked(DbIndex db_index, PrimeTable::bucket_iterator it,
                                         bool on_update) = 0;

  // Serialize single entry with expire/flags
  virtual void SerializeEntryLocked(DbIndex db_index, const PrimeKey& pk, const PrimeValue& pv,
                                    time_t expire, uint32_t mc_flags) = 0;

  // Serialize entry while automatically handling delayed/cooled values, calls SerializeEntryLocked
  void SerializeEntry(BucketIdentity bucket, DbIndex db_index, const PrimeKey& pk,
                      const PrimeValue& pv);

  // Calls SerializeEntry internally under stream_mu_
  void SerializeFetchedEntry(const TieredDelayedEntry& tde, const PrimeValue& pv) override;

  // Called before a set of buckets is mutated.
  void OnChange(DbIndex db_index, const ChangeReq& req) override;

  bool IsAnyBucketBlocked() const override {
    return BucketDependencies::HasAny();
  }

  void WaitForNoBucketBlocked() const override;

  // --- Shared members ---

  DbSlice* const db_slice_;
  ExecutionState* const base_cntx_;

  DbTableArray db_array_;

  Stats stats_;

  // Guards output stream (serializer) to not be used from multiple fibers
  // as buffered changes can be flushed amid writing a value (logical stream)
  ThreadLocalMutex stream_mu_;
};

}  // namespace dfly
