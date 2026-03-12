// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>

#include <vector>

#include "server/db_slice.h"
#include "server/synchronization.h"
#include "server/table.h"
#include "server/tiered_storage.h"

namespace dfly {

// Opaque identity for a physical DashTable bucket — its memory address.
// Unique across all databases/segments for the lifetime of a serialization.
using BucketIdentity = uintptr_t;

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
class SerializerBase {
 public:
  // Aggregated counters for observability.
  struct Stats {
    uint64_t buckets_loop = 0;       // main traversal loop
    uint64_t buckets_on_change = 0;  // OnChange callback fired
    uint64_t buckets_skipped = 0;    // already Covered when seen
    uint64_t keys_serialized = 0;
    uint64_t change_during_serialization = 0;  // change hit an in-flight bucket
  };

  explicit SerializerBase(DbSlice* slice);
  virtual ~SerializerBase();

  // Registers a ChangeCallback with DbSlice.  Returns the snapshot version
  // (version upper-bound for entries that must be saved).
  uint64_t RegisterChangeListener();

  // Unregisters the callback.  Safe to call if already unregistered.
  void UnregisterChangeListener();

  uint64_t snapshot_version() const {
    return snapshot_version_;
  }

  const Stats& GetStats() const {
    return stats_;
  }

 protected:
  // Phase of an in-flight bucket (only stored while transient).
  enum class BucketPhase : uint8_t {
    kSerializing,     // bucket is being iterated by the main loop / OnChange
    kDelayedPending,  // all entries serialized but tiered reads still in-flight
  };

  struct BucketState {
    BucketPhase phase;
    std::vector<TieredDelayedEntry> delayed;
  };

  // --- Bucket state machine ---

  // Transition bucket from NotVisited -> Serializing.
  // Must be called before DoSerializeBucket.  Caller is responsible for
  // stamping the bucket version to snapshot_version_ first.
  void MarkBucketSerializing(BucketIdentity bid);

  // Transition bucket from Serializing -> Covered (empty delayed) or
  // Serializing -> DelayedPending (non-empty delayed).
  // Takes ownership of the delayed entries.
  void FinishBucketIteration(BucketIdentity bid, std::vector<TieredDelayedEntry> delayed);

  // Transition bucket from DelayedPending -> Covered.
  void CompleteBucketDelayed(BucketIdentity bid);

  // --- Subclass serialization hook ---

  // Serialize a single bucket.  Returns the number of entries serialized.
  // Called while big_value_mu_ is held.
  virtual unsigned DoSerializeBucket(DbIndex db_index, PrimeTable::bucket_iterator it) = 0;

  // --- Change callbacks ---

  // Called when an existing bucket is about to be mutated.
  // Default: if unvisited, stamps version, MarkBucketSerializing, DoSerializeBucket,
  //          FinishBucketIteration.
  //          If in-flight, increments change_during_serialization (mutex barrier
  //          preserves the existing serialization behaviour).
  // Holds big_value_mu_ while running.
  virtual void OnChange(DbIndex db_index, PrimeTable::bucket_iterator it);

  // Called when a new key is about to be inserted.
  // Default: CVCUponInsert -> OnChange for every touched bucket.
  virtual void OnInsert(DbIndex db_index, std::string_view key);

  // --- Shared members (to be moved from subclasses in later PRs) ---

  DbSlice* db_slice_;
  DbTableArray db_array_;
  uint64_t snapshot_version_ = 0;
  ThreadLocalMutex big_value_mu_;
  Stats stats_;

 private:
  // Called by DbSlice when a change is detected.
  void HandleChangeReq(DbIndex db_index, const DbSlice::ChangeReq& req);

  absl::flat_hash_map<BucketIdentity, BucketState> bucket_states_;
  uint64_t change_cb_id_ = 0;

  // For unit-test only.
  size_t BucketStateCountForTesting() const {
    return bucket_states_.size();
  }
  friend class SerializerBaseTest;
};

}  // namespace dfly
