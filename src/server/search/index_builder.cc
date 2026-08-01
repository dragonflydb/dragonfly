// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/index_builder.h"

#include <absl/container/flat_hash_set.h>

#include <ranges>

#include "server/db_slice.h"
#include "server/engine_shard_set.h"
#include "server/search/doc_accessors.h"
#include "server/search/global_hnsw_index.h"
#include "server/tiered_storage.h"

namespace dfly::search {

namespace {

// How many deferred offloaded keys are collected before the traversal pauses to index them.
constexpr size_t kOffloadedBatchSize = 64;

}  // namespace

void IndexBuilder::Start(const OpArgs& op_args, bool is_restored,
                         std::function<void()> on_complete) {
  using namespace util::fb2;
  auto table = op_args.GetDbSlice().CopyDBTablePtr(op_args.db_cntx.db_index);
  DCHECK(table.get());

  is_restored_ = is_restored;

  auto cb = [this, table, db_cntx = op_args.db_cntx, on_complete = std::move(on_complete)] {
    CursorLoop(table.get(), db_cntx);
    VectorLoop(table.get(), db_cntx);

    // TODO: make it step by step + wire cancellation inside
    if (state_.IsRunning())
      index_->indices_->FinalizeInitialization();

    // Finish by clearing the fiber reference and calling on_complete as its last action
    {
      util::FiberAtomicGuard guard{};  // preserve cancellation
      fiber_.Detach();                 // builder is now safely deleteable
      if (!state_.IsCancelled())
        on_complete();
    }
  };

  fiber_ = Fiber{std::move(cb)};
}

void IndexBuilder::Cancel() {
  state_.Cancel();
  util::fb2::Fiber{std::move(fiber_)}.JoinIfNeeded();  // steal and wait for finish
}

util::fb2::Fiber IndexBuilder::Worker() {
  return std::move(fiber_);
}

void IndexBuilder::CursorLoop(dfly::DbTable* table, DbContext db_cntx) {
  auto cb = [this, db_cntx, scratch = std::string{}](PrimeTable::iterator it) mutable {
    PrimeValue& pv = it->second;
    std::string_view key = it->first.GetSlice(&scratch);

    if (!index_->Matches(key, pv.ObjType()))
      return;

    // An indexed value has to stay resident, so cancel its pending stash: otherwise the
    // small-bins flush would externalize this doc after it was indexed below.
    if (pv.HasStashPending()) {
      EngineShard::tlocal()->tiered_storage()->CancelStash(std::make_pair(db_cntx.db_index, key),
                                                           &pv);
    }

    // Its fields live on disk, and reading them would suspend this fiber in the middle of a
    // table traversal. Collect it and deal with it once the traversal is over.
    if (pv.IsExternal()) {
      offloaded_keys_.emplace_back(key);
      return;
    }

    IndexEntry(key, db_cntx, pv);
  };

  PrimeTable::Cursor cursor;
  do {
    cursor = table->prime.Traverse(cursor, cb);
    // Between traversal steps the cursor tolerates concurrent table modifications, so it is
    // safe to suspend here: draining in batches bounds the memory held by deferred keys.
    if (offloaded_keys_.size() >= kOffloadedBatchSize)
      IndexOffloaded(table, db_cntx);
    if (base::CycleClock::ToUsec(util::ThisFiber::GetRunningTimeCycles()) > 500)
      util::ThisFiber::Yield();
  } while (cursor && state_.IsRunning());

  IndexOffloaded(table, db_cntx);
  offloaded_keys_ = {};
}

void IndexBuilder::IndexOffloaded(dfly::DbTable* table, DbContext db_cntx) {
  TieredStorage* ts = EngineShard::tlocal()->tiered_storage();

  // Issue all reads before waiting on any: concurrent reads of one small-bins page share a
  // single disk fetch, and reads of independent pages overlap in the IO queue.
  std::vector<std::pair<std::string_view, util::fb2::Future<bool>>> pending;
  pending.reserve(offloaded_keys_.size());
  absl::flat_hash_set<std::string_view> seen;  // a cursor revisit may defer a key twice
  for (const std::string& key : offloaded_keys_) {
    if (!state_.IsRunning())
      break;
    if (!seen.insert(key).second)
      continue;
    auto it = table->prime.Find(key);
    if (!IsValid(it) || !index_->Matches(key, it->second.ObjType()))
      continue;  // deleted or recreated with another type while we were traversing
    pending.emplace_back(key, ts->MaterializeForIndexing(db_cntx.db_index, key, &it->second));
  }

  for (auto& [key, fut] : pending) {
    if (!state_.IsRunning())
      break;

    // Waiting suspends this fiber, so the entry has to be looked up again afterwards.
    if (!fut.Get()) {
      LOG(ERROR) << "Failed to read an offloaded value, not indexing key: " << key;
      // A restored build already has this key mapped; drop the mapping so no DocId points
      // at a document that is absent from the field indices.
      if (is_restored_) {
        if (auto doc_id = index_->key_index().Find(key); doc_id)
          index_->key_index_.Remove(*doc_id);
      }
      continue;
    }

    auto it = table->prime.Find(key);
    if (IsValid(it) && !it->second.IsExternal() && index_->Matches(key, it->second.ObjType()))
      IndexEntry(key, db_cntx, it->second);

    // Warming up cool values never suspends, so yield explicitly like the cursor loop does.
    if (base::CycleClock::ToUsec(util::ThisFiber::GetRunningTimeCycles()) > 500)
      util::ThisFiber::Yield();
  }

  offloaded_keys_.clear();  // keep the bounded capacity for the next batch
}

// TODO: make it a parameter of ShardDocIndex::AddDoc()
void IndexBuilder::IndexEntry(std::string_view key, const DbContext& db_cntx,
                              const PrimeValue& pv) {
  if (!is_restored_) {
    index_->AddDoc(key, db_cntx, pv);
    return;
  }

  // Use existing DocIds from the restored key_index_ to keep them aligned with
  // GlobalDocIds stored in the serialized HNSW graph. Only add to regular indices
  // (text/tag/numeric); vector indices are handled separately by VectorLoop.
  auto accessor = GetAccessor(db_cntx, pv);
  if (auto doc_id = index_->key_index().Find(key); doc_id) {
    if (!index_->indices_->Add(*doc_id, *accessor)) {
      LOG(WARNING) << "Failed to restore index entry for key: " << key
                   << ", removing from key index";
      index_->key_index_.Remove(*doc_id);
    }
  } else {
    // New document not in the restored key_index_ (added by journal events during
    // full sync before the index was created). Use AddNew to allocate a fresh DocId
    // that won't collide with serialized HNSW node ids from freed slots.
    DocId id = index_->key_index_.AddNew(key);
    if (!index_->indices_->Add(id, *accessor)) {
      index_->key_index_.Remove(id);
    }
  }
}

void IndexBuilder::VectorLoop(dfly::DbTable* table, DbContext db_cntx) {
  bool any_vector = std::ranges::any_of(index_->base_->schema.fields, [](const auto& item) {
    return item.second.IsIndexableHnswField();
  });
  if (!any_vector || !state_.IsRunning())
    return;

  // If any HNSW index was restored from RDB, use UpdateVectorData instead of Add.
  if (is_restored_) {
    // TODO: Add support for concurrent modifications
    OpArgs op_args{EngineShard::tlocal(), nullptr, db_cntx};
    index_->RestoreGlobalVectorIndices(index_->base_->name, op_args);
    return;
  }

  // Non-restored path: rebuilding HNSW from scratch. Clear the restoring flag and discard
  // any pending updates — the full table traversal below will pick up all current documents.
  index_->hnsw_state_ = ShardDocIndex::HnswState::kBuilding;
  index_->pending_vector_updates_.clear();

  auto cb = [this, db_cntx, scratch = std::string{}](PrimeTable::iterator it) mutable {
    PrimeValue& pv = it->second;
    std::string_view key = it->first.GetSlice(&scratch);

    if (auto local_id = index_->key_index().Find(key); local_id)
      index_->AddDocToGlobalVectorIndex(*local_id, db_cntx, &pv);
  };

  // NOTE(global-hnsw-mutex): HNSW (hnsw_alg) index uses a thread-blocking mutex making
  // AddDocToGlobalVectorIndex non-fiber-suspendable from a fiber point of view. This makes it safe
  // to perform add keys without locking them while sleeping of a mutex.
  PrimeTable::Cursor cursor;
  do {
    cursor = table->prime.Traverse(cursor, cb);
    if (base::CycleClock::ToUsec(util::ThisFiber::GetRunningTimeCycles()) > 500)
      util::ThisFiber::Yield();
  } while (cursor && state_.IsRunning());
}

}  // namespace dfly::search
