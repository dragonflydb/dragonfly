// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/serializer_base.h"

#include "base/logging.h"
#include "server/common_types.h"
#include "server/journal/journal.h"
#include "server/synchronization.h"

namespace dfly {

SerializerBase::SerializerBase(DbSlice* slice)
    : db_slice_(slice), db_array_(slice ? slice->databases() : DbTableArray{}) {
  DCHECK(db_slice_);
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
void SerializerBase::RegisterChangeListener() {
  db_array_ = db_slice_->databases();  // copy pointers to survive flush
  auto cb = [this](DbIndex dbid, const ChangeReq& req) {
    std::visit([&](auto it) { OnChange(dbid, it); }, req.change);
  };
  snapshot_version_ = db_slice_->RegisterOnChange(cb);
}

void SerializerBase::UnregisterChangeListener() {
  if (auto version = std::exchange(snapshot_version_, 0); version > 0)
    db_slice_->UnregisterOnChange(version);
}

void SerializerBase::MarkBucketSerializing(BucketIdentity bid) {
  DCHECK(!bucket_states_.contains(bid)) << "Bucket already in transient state";
  bucket_states_[bid] = {BucketPhase::kSerializing, {}};
}

void SerializerBase::FinishBucketIteration(BucketIdentity bid,
                                           std::vector<TieredDelayedEntry> delayed) {
  auto it = bucket_states_.find(bid);
  DCHECK(it != bucket_states_.end());
  DCHECK(it->second.phase == BucketPhase::kSerializing);

  if (delayed.empty()) {
    // Serializing -> Covered
    bucket_states_.erase(it);
    ++stats_.buckets_serialized;
  } else {
    // Serializing -> DelayedPending
    // TODO: Currently not used
    it->second.phase = BucketPhase::kDelayedPending;
    it->second.delayed = std::move(delayed);
  }
}

void SerializerBase::CompleteBucketDelayed(BucketIdentity bid) {
  auto it = bucket_states_.find(bid);
  DCHECK(it != bucket_states_.end() && it->second.phase == BucketPhase::kDelayedPending);
  bucket_states_.erase(it);
}

std::optional<BucketIdentity> SerializerBase::ShouldProcessBucket(PrimeTable::bucket_iterator it) {
  // Check if bucket is invalid or was already serialized
  if (it.is_done() || it.GetVersion() >= snapshot_version_) {
    ++stats_.buckets_skipped;
    return std::nullopt;
  }

  // Check if this bucker is currently being serialized
  if (bucket_states_.contains(it.bucket_address())) {
    ++stats_.change_during_serialization;
    return std::nullopt;
  }

  return it.bucket_address();
}

void SerializerBase::ProcessBucket(DbIndex db_index, PrimeTable::bucket_iterator it,
                                   bool on_update) {
  // Check if this bucket should be serialized
  std::optional<BucketIdentity> bid = ShouldProcessBucket(it);
  if (!bid)
    return;

  std::lock_guard guard(big_value_mu_);

  // For non updates, flush change to earlier callbacks and acquire serialization latch
  std::optional<std::lock_guard<LocalLatch>> db_guard;
  if (!on_update) {
    db_slice_->FlushChangeToEarlierCallbacks(db_index, DbSlice::Iterator::FromPrime(it),
                                             snapshot_version_);
    db_guard.emplace(*db_slice_->GetLatch());
  }

  it.SetVersion(snapshot_version_);
  MarkBucketSerializing(*bid);
  stats_.keys_serialized += SerializeBucket(db_index, it, true);
  FinishBucketIteration(*bid, {});
}

void SerializerBase::OnChange(DbIndex db_index, PrimeTable::bucket_iterator it) {
  ProcessBucket(db_index, it, true);
  ++stats_.buckets_on_change;
}

void SerializerBase::OnChange(DbIndex db_index, std::string_view key) {
  PrimeTable* table = db_slice_->GetTables(db_index);
  table->CVCUponInsert(snapshot_version_, key, [this, db_index](PrimeTable::bucket_iterator bit) {
    DCHECK_LT(bit.GetVersion(), snapshot_version_);
    OnChange(db_index, bit);
  });
}

}  // namespace dfly
