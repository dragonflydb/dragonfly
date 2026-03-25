// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>

#include <vector>

#include "server/db_slice.h"
#include "server/execution_state.h"
#include "server/journal/types.h"
#include "server/synchronization.h"
#include "server/table.h"
#include "server/tiered_storage.h"

namespace dfly {

// Opaque identity for a physical DashTable bucket — its memory address.
// Unique across all databases/segments for the lifetime of a serialization.
using BucketIdentity = uintptr_t;

// Handles serialization of offloaded (delayed) entries
struct DelayedEntryHandler {
  using Key = std::pair<DbIndex, std::string>;  // Unique identifier of key

  void EnqueueOffloaded(DbIndex db_index, PrimeKey pk, const PrimeValue& pv, time_t expire_time,
                        uint32_t mc_flags);

  // Must be called periodically to progress on delayed entries. Calls SerializeFetchedEntry.
  // If force is false, only serializes entries whose futures are already resolved.
  // If tiered_keys is provided, only serializes entries whose keys are in the set.
  void ProcessDelayedEntries(bool force, std::vector<Key>* tiered_keys, ExecutionState* cntx);

  // Serialize delayed entry that was fetched with serializer specific implementation
  virtual void SerializeFetchedEntry(const TieredDelayedEntry& tde, const PrimeValue& pv) = 0;

 private:
  // Entries that are waiting for tiered storage reads to complete before they can be serialized.
  absl::flat_hash_map<Key, std::unique_ptr<TieredDelayedEntry>> delayed_entries_;
};

// SerializerBase owns the DbSlice change-listener registration and a per-bucket
// state machine that tracks each bucket through:
//
//   NotVisited  ->  Serializing  ->  (DelayedPending  ->)  Covered
//
// NotVisited and Covered are implicit (bucket absent from the map).
// Only transient states (Serializing, DelayedPending) are stored in the map.
//
// State tracking is purely observational in early PRs: it drives DCHECKs and
// stats but does not alter the serialization control flow.
class SerializerBase : public DelayedEntryHandler {
 public:
  struct Stats {
    uint64_t keys_serialized = 0;              // total number of keys serialized
    uint64_t buckets_serialized = 0;           // total number of buckets serialized
    uint64_t buckets_on_change = 0;            // buckets serialized by OnChange flow
    uint64_t buckets_skipped = 0;              // already Covered when seen
    uint64_t change_during_serialization = 0;  // change hit an in-flight bucket
  };

  explicit SerializerBase(DbSlice* slice);
  virtual ~SerializerBase();

  // Register db_slice change listener and save snapshot it
  void RegisterChangeListener();

  // Unregisters the callback.  Safe to call if already unregistered.
  void UnregisterChangeListener();

  const Stats& GetStats() const {
    return stats_;
  }

 protected:
  // Phase of an in-flight bucket (only stored while transient).
  enum class BucketPhase : uint8_t {
    kSerializing,     // bucket is being iterated by the main loop / OnChange
    kDelayedPending,  // all entries serialized but tiered reads still in-flight
  };

  // Transition bucket from DelayedPending -> Covered.
  void CompleteBucketDelayed(BucketIdentity bid);

  // Process single bucket and call SerializeBucket. Return true if processed, false if skipped
  bool ProcessBucket(DbIndex db_index, PrimeTable::bucket_iterator it, bool on_update);

  // Serialize a single bucket. Returns the number of entries serialized.
  // To be implemented by classses extending this base class.
  virtual unsigned SerializeBucket(DbIndex db_index, PrimeTable::bucket_iterator it,
                                   bool on_update) = 0;

  // Called when an existing bucket is about to be mutated. Calls ProcessBucket.
  void OnChange(DbIndex db_index, PrimeTable::bucket_iterator it);

  // Called when a new key is about to be inserted,
  // calls CVCUponInsert -> OnChange(bucket_iterator) for every touched bucket.
  void OnChange(DbIndex db_index, std::string_view key);

  // --- Shared members (to be moved from subclasses in later PRs) ---

  DbSlice* const db_slice_;
  DbTableArray db_array_;

  uint64_t snapshot_version_ = 0;
  ThreadLocalMutex big_value_mu_;
  Stats stats_;

 private:
  friend class SerializerBaseTest;
  SerializerBase() : db_slice_(nullptr) {
  }

  // Return identity if bucket should be processed.
  // Checks bucket validity, version and state
  std::optional<BucketIdentity> ShouldProcessBucket(PrimeTable::bucket_iterator);

  // Transition bucket from NotVisited -> Serializing.
  void MarkBucketSerializing(BucketIdentity bid);

  // Transition bucket from Serializing -> Covered (empty delayed) or
  // Serializing -> DelayedPending (non-empty delayed).
  void FinishBucketIteration(BucketIdentity bid, std::vector<TieredDelayedEntry> delayed);

  absl::flat_hash_map<BucketIdentity, BucketPhase> bucket_states_;
  uint64_t change_cb_id_ = 0;
};

}  // namespace dfly
