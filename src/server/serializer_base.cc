// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/serializer_base.h"

#include "base/logging.h"

namespace dfly {

SerializerBase::SerializerBase(DbSlice* slice)
    : db_slice_(slice), db_array_(slice ? slice->databases() : DbTableArray{}) {
}

SerializerBase::~SerializerBase() {
}

// Ordering invariant:
//   For any key K, the replica must receive K's baseline value strictly before any journal entry
//   that mutates K.
//
// RegisterChangeListener registers the DbSlice callback that routes mutations through
//   SerializerBase::OnChange. ConsumeJournalChange runs later on the
//   same fiber, so the baseline is serialized first. big_value_mu_ prevents this callback path
//   from interleaving with the traversal fiber's bucket serialization, which may preempt while
//   emitting large values.
uint64_t SerializerBase::RegisterChangeListener() {
  DCHECK(db_slice_);
  db_array_ = db_slice_->databases();
  auto cb = [this](DbIndex db_index, const DbSlice::ChangeReq& req) {
    HandleChangeReq(db_index, req);
  };
  snapshot_version_ = db_slice_->RegisterOnChange(std::move(cb));
  return snapshot_version_;
}

void SerializerBase::UnregisterChangeListener() {
  if (snapshot_version_ == 0)
    return;
  DCHECK(db_slice_);
  db_slice_->UnregisterOnChange(snapshot_version_);
  snapshot_version_ = 0;
}

void SerializerBase::MarkBucketSerializing(BucketIdentity bid) {
  DCHECK(!bucket_states_.contains(bid)) << "Bucket already in transient state";
  bucket_states_.emplace(bid, BucketState{BucketPhase::kSerializing, {}});
}

void SerializerBase::FinishBucketIteration(BucketIdentity bid,
                                           std::vector<TieredDelayedEntry> delayed) {
  auto it = bucket_states_.find(bid);
  DCHECK(it != bucket_states_.end() && it->second.phase == BucketPhase::kSerializing);

  if (delayed.empty()) {
    // Serializing -> Covered
    bucket_states_.erase(it);
  } else {
    // Serializing -> DelayedPending
    it->second.phase = BucketPhase::kDelayedPending;
    it->second.delayed = std::move(delayed);
  }
}

void SerializerBase::CompleteBucketDelayed(BucketIdentity bid) {
  auto it = bucket_states_.find(bid);
  DCHECK(it != bucket_states_.end() && it->second.phase == BucketPhase::kDelayedPending);
  bucket_states_.erase(it);
}

unsigned SerializerBase::DoSerializeBucketOnChange(DbIndex db_index,
                                                   PrimeTable::bucket_iterator it) {
  return DoSerializeBucket(db_index, it);
}

void SerializerBase::OnChange(DbIndex db_index, PrimeTable::bucket_iterator it) {
  std::lock_guard guard(big_value_mu_);

  if (it.is_done() || it.GetVersion() >= snapshot_version_) {
    ++stats_.buckets_skipped;
    return;
  }

  BucketIdentity bid = it.bucket_address();
  if (bucket_states_.contains(bid)) {
    ++stats_.change_during_serialization;
    return;
  }

  it.SetVersion(snapshot_version_);
  MarkBucketSerializing(bid);
  DoSerializeBucketOnChange(db_index, it);
  FinishBucketIteration(bid, {});
  ++stats_.buckets_on_change;
}

void SerializerBase::OnInsert(DbIndex db_index, std::string_view key) {
  DCHECK(db_slice_);
  PrimeTable* table = db_slice_->GetTables(db_index).first;
  table->CVCUponInsert(snapshot_version_, key, [this, db_index](PrimeTable::bucket_iterator bit) {
    DCHECK_LT(bit.GetVersion(), snapshot_version_);
    OnChange(db_index, bit);
  });
}

void SerializerBase::HandleChangeReq(DbIndex db_index, const DbSlice::ChangeReq& req) {
  if (auto update = req.update(); update) {
    OnChange(db_index, *update);
  } else {
    OnInsert(db_index, std::get<std::string_view>(req.change));
  }
}

}  // namespace dfly
