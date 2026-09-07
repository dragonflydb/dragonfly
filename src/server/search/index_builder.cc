// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/index_builder.h"

#include <algorithm>
#include <ranges>

#include "server/db_slice.h"
#include "server/engine_shard_set.h"
#include "server/search/doc_accessors.h"
#include "server/search/global_hnsw_index.h"
#include "server/tiered_storage.h"

namespace dfly::search {

namespace {

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

    const bool matches = index_->Matches(key, pv.ObjType());
    const bool must_be_resident = matches && index_->base_->type == DocIndex::HASH &&
                                  db_cntx.db_index == 0 && pv.ObjType() == OBJ_HASH;

    // Existing non-matching HASH values can stay external. Generic key-moving commands
    // materialize them before a new key can start matching an index prefix.
    if (must_be_resident && pv.HasStashPending()) {
      EngineShard::tlocal()->tiered_storage()->CancelStash(std::make_pair(db_cntx.db_index, key),
                                                           &pv);
    }

    // Reading from disk suspends, which is unsafe inside PrimeTable traversal. Defer the key and
    // re-lookup it after traversal yields a stable cursor.
    if (must_be_resident && pv.IsExternal()) {
      offloaded_keys_.push_back(
          {std::string(key), is_restored_ ? index_->key_index().Find(key) : std::nullopt});
      return;
    }

    if (matches)
      IndexEntry(key, db_cntx, pv);
  };

  PrimeTable::Cursor cursor;
  do {
    cursor = table->prime.Traverse(cursor, cb);
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

  // Issue a batch before waiting so reads from the same small-bin page are coalesced and reads
  // from different pages can overlap.
  struct PendingRead {
    std::string_view key;
    std::pair<size_t, size_t> segment;
    std::optional<DocId> restored_id;
    util::fb2::Future<bool> future;
  };
  std::vector<PendingRead> pending;
  pending.reserve(offloaded_keys_.size());

  auto cleanup_restored = [&](std::string_view key, std::optional<DocId> restored_id,
                              bool remove_mapping) {
    if (!restored_id)
      return;

    index_->RemoveFromAllHnswIndices(*restored_id);
    const auto current_id = index_->key_index().Find(key);
    const auto& indexed_docs = index_->indices_->GetAllDocs();
    if (remove_mapping && current_id == restored_id &&
        !std::binary_search(indexed_docs.begin(), indexed_docs.end(), *restored_id)) {
      index_->key_index_.Remove(*restored_id);
    }
  };

  for (const OffloadedKey& deferred : offloaded_keys_) {
    if (!state_.IsRunning())
      break;
    const std::string& key = deferred.key;

    auto it = table->prime.Find(key);
    if (!IsValid(it) || !index_->Matches(key, it->second.ObjType())) {
      cleanup_restored(key, deferred.restored_id, true);
      continue;
    }

    // The key may have been materialized by another fiber since CursorLoop deferred it.
    if (!it->second.IsExternal()) {
      if (deferred.restored_id && index_->key_index().Find(key) != deferred.restored_id)
        cleanup_restored(key, deferred.restored_id, false);
      IndexEntry(key, db_cntx, it->second);
      continue;
    }

    pending.push_back({key, it->second.GetExternalSlice(), deferred.restored_id,
                       ts->MaterializeForIndexing(db_cntx.db_index, key, &it->second)});
  }

  for (auto& read : pending) {
    if (!state_.IsRunning())
      break;

    // Waiting can move or replace the table entry, so never retain its old iterator/reference.
    read.future.Get();

    auto is_ready = [&](auto it) {
      return IsValid(it) && !it->second.IsExternal() &&
             index_->Matches(read.key, it->second.ObjType());
    };
    auto is_same_external_hash = [&](auto it) {
      return IsValid(it) && it->second.ObjType() == OBJ_HASH && it->second.IsExternal() &&
             it->second.GetExternalSlice() == read.segment;
    };

    auto it = table->prime.Find(read.key);
    if (!is_ready(it) && is_same_external_hash(it)) {
      // Retry once for a transient I/O failure. A persistent failure must not keep index
      // construction alive indefinitely.
      ts->MaterializeForIndexing(db_cntx.db_index, read.key, &it->second).Get();
      it = table->prime.Find(read.key);
    }

    const bool ready = is_ready(it);
    const auto current_id = is_restored_ ? index_->key_index().Find(read.key) : std::nullopt;

    // A restored graph still contains the node captured before this read. If the document went
    // away, could not be indexed, or now maps to a different id, remove that stale node. A
    // concurrently added replacement is buffered while the index is restoring and will be added
    // when pending vector updates are drained.
    if (read.restored_id && (!ready || current_id != read.restored_id))
      cleanup_restored(read.key, read.restored_id, !ready);

    if (ready) {
      IndexEntry(read.key, db_cntx, it->second);
    } else {
      LOG_IF(ERROR, is_same_external_hash(it))
          << "Failed to materialize an indexed value for key: " << read.key;
    }

    if (base::CycleClock::ToUsec(util::ThisFiber::GetRunningTimeCycles()) > 500)
      util::ThisFiber::Yield();
  }

  offloaded_keys_.clear();
}

// TODO: make restored handling a parameter of ShardDocIndex::AddDoc().
void IndexBuilder::IndexEntry(std::string_view key, const DbContext& db_cntx,
                              const PrimeValue& pv) {
  if (!is_restored_) {
    index_->AddDoc(key, db_cntx, pv);
    return;
  }

  // Reuse restored DocIds so they stay aligned with ids serialized in the HNSW graph. Vector
  // fields are populated later by VectorLoop.
  auto accessor = GetAccessor(db_cntx, pv);
  if (auto doc_id = index_->key_index().Find(key); doc_id) {
    const auto& indexed_docs = index_->indices_->GetAllDocs();
    if (std::binary_search(indexed_docs.begin(), indexed_docs.end(), *doc_id))
      return;

    if (!index_->indices_->Add(*doc_id, *accessor)) {
      LOG(WARNING) << "Failed to restore index entry for key: " << key
                   << ", removing from key index";
      index_->RemoveFromAllHnswIndices(*doc_id);
      index_->key_index_.Remove(*doc_id);
    }
  } else {
    DocId id = index_->key_index_.AddNew(key);
    if (!index_->indices_->Add(id, *accessor))
      index_->key_index_.Remove(id);
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
