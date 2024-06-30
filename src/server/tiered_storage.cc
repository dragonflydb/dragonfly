// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tiered_storage.h"

#include <mimalloc.h>

#include <cstddef>
#include <memory>
#include <optional>
#include <variant>

#include "absl/cleanup/cleanup.h"
#include "absl/flags/internal/flag.h"
#include "base/flags.h"
#include "base/logging.h"
#include "server/common.h"
#include "server/db_slice.h"
#include "server/engine_shard_set.h"
#include "server/snapshot.h"
#include "server/table.h"
#include "server/tiering/common.h"
#include "server/tiering/op_manager.h"
#include "server/tiering/small_bins.h"
#include "server/tx_base.h"

ABSL_FLAG(bool, tiered_storage_cache_fetched, true,
          "WIP: Load results of offloaded reads to memory");

ABSL_FLAG(unsigned, tiered_storage_write_depth, 50,
          "Maximum number of concurrent stash requests issued by background offload");

namespace dfly {

using namespace std;
using namespace util;

using namespace tiering::literals;

using KeyRef = tiering::OpManager::KeyRef;

namespace {

bool OccupiesWholePages(size_t size) {
  return size >= TieredStorage::kMinOccupancySize;
}

// Stashed bins no longer have bin ids, so this sentinel is used to differentiate from regular reads
constexpr auto kFragmentedBin = tiering::SmallBins::kInvalidBin - 1;

}  // anonymous namespace

class TieredStorage::ShardOpManager : public tiering::OpManager {
  friend class TieredStorage;

 public:
  ShardOpManager(TieredStorage* ts, DbSlice* db_slice, size_t max_size)
      : tiering::OpManager{max_size}, ts_{ts}, db_slice_{db_slice} {
    cache_fetched_ = absl::GetFlag(FLAGS_tiered_storage_cache_fetched);
  }

  // Called before overriding value with segment
  void RecordAdded(DbTableStats* stats, const PrimeValue& pv, tiering::DiskSegment segment) {
    stats->AddTypeMemoryUsage(pv.ObjType(), -pv.MallocUsed());
    stats->tiered_entries++;
    stats->tiered_used_bytes += segment.length;
  }

  // Called after setting new value in place of previous segment
  void RecordDeleted(DbTableStats* stats, const PrimeValue& pv, tiering::DiskSegment segment) {
    stats->AddTypeMemoryUsage(pv.ObjType(), pv.MallocUsed());
    stats->tiered_entries--;
    stats->tiered_used_bytes -= segment.length;
  }

  // Find entry by key in db_slice and store external segment in place of original value.
  // Update memory stats
  void SetExternal(OpManager::KeyRef key, tiering::DiskSegment segment) {
    if (auto pv = Find(key); pv) {
      RecordAdded(db_slice_->MutableStats(key.first), *pv, segment);

      pv->SetIoPending(false);
      pv->SetExternal(segment.offset, segment.length);

      stats_.total_stashes++;
    }
  }

  // Find bin by id and call SetExternal for all contained entries
  void SetExternal(tiering::SmallBins::BinId id, tiering::DiskSegment segment) {
    for (const auto& [sub_dbid, sub_key, sub_segment] : ts_->bins_->ReportStashed(id, segment))
      SetExternal({sub_dbid, sub_key}, sub_segment);
  }

  // Clear IO pending flag for entry
  void ClearIoPending(OpManager::KeyRef key) {
    if (auto pv = Find(key); pv) {
      pv->SetIoPending(false);
      stats_.total_cancels++;
    }
  }

  // Clear IO pending flag for all contained entries of bin
  void ClearIoPending(tiering::SmallBins::BinId id) {
    for (const auto& key : ts_->bins_->ReportStashAborted(id))
      ClearIoPending(key);
  }

  // Set value to be an in-memory type again, either empty or with a value. Update memory stats
  void SetInMemory(PrimeValue* pv, DbIndex dbid, string_view value, tiering::DiskSegment segment) {
    pv->Reset();
    if (!value.empty())
      pv->SetString(value);

    RecordDeleted(db_slice_->MutableStats(dbid), *pv, segment);

    (value.empty() ? stats_.total_deletes : stats_.total_fetches)++;
  }

  // Find entry by key and store it's up-to-date value in place of external segment.
  // Returns false if the value is outdated, true otherwise
  bool SetInMemory(OpManager::KeyRef key, string_view value, tiering::DiskSegment segment) {
    if (auto pv = Find(key); pv && pv->IsExternal() && segment == pv->GetExternalSlice()) {
      SetInMemory(pv, key.first, value, segment);
      return true;
    }
    return false;
  }

  // Load all values from bin by their hashes
  void Defragment(tiering::DiskSegment segment, string_view value) {
    // Note: Bin could've already been deleted, in that case DeleteBin returns an empty list
    for (auto [dbid, hash, sub_segment] : ts_->bins_->DeleteBin(segment, value)) {
      // Search for key with the same hash and value pointing to the same segment.
      // If it still exists, it must correspond to the value stored in this bin
      auto predicate = [sub_segment = sub_segment](const PrimeKey& key, const PrimeValue& probe) {
        return probe.IsExternal() && tiering::DiskSegment{probe.GetExternalSlice()} == sub_segment;
      };
      auto it = db_slice_->GetDBTable(dbid)->prime.FindFirst(hash, predicate);
      if (!IsValid(it))
        continue;

      stats_.total_defrags++;

      // Cut out relevant part of value and restore it to memory
      string_view sub_value = value.substr(sub_segment.offset - segment.offset, sub_segment.length);
      SetInMemory(&it->second, dbid, sub_value, sub_segment);
    }
  }

  void ReportStashed(EntryId id, tiering::DiskSegment segment, error_code ec) override {
    if (ec) {
      VLOG(1) << "Stash failed " << ec.message();
      visit([this](auto id) { ClearIoPending(id); }, id);
    } else {
      visit([this, segment](auto id) { SetExternal(id, segment); }, id);
    }
  }

  bool ReportFetched(EntryId id, string_view value, tiering::DiskSegment segment,
                     bool modified) override {
    if (id == EntryId{kFragmentedBin}) {  // Generally we read whole bins only for defrag
      Defragment(segment, value);
      return true;  // delete
    }

    if (!modified && !cache_fetched_)
      return false;

    if (SliceSnapshot::IsSnaphotInProgress())
      return false;

    SetInMemory(get<OpManager::KeyRef>(id), value, segment);
    return true;
  }

  bool ReportDelete(tiering::DiskSegment segment) override {
    if (OccupiesWholePages(segment.length))
      return true;

    auto bin = ts_->bins_->Delete(segment);
    if (bin.empty) {
      return true;
    }

    if (bin.fragmented) {
      // Trigger read to signal need for defragmentation. ReportFetched will handle it.
      VLOG(1) << "Enqueueing bin defragmentation for: x" << bin.segment.offset;
      Enqueue(kFragmentedBin, bin.segment, [](std::string*) { return false; });
    }

    return false;
  }

 private:
  friend class TieredStorage;

  PrimeValue* Find(OpManager::KeyRef key) {
    // TODO: Get DbContext for transaction for correct dbid and time
    // Bypass all update and stat mechanisms
    auto it = db_slice_->GetDBTable(key.first)->prime.Find(key.second);
    return IsValid(it) ? &it->second : nullptr;
  }

  bool cache_fetched_ = false;

  struct {
    size_t total_stashes = 0, total_fetches = 0, total_cancels = 0, total_deletes = 0;
    size_t total_defrags = 0;  // included in total_fetches
  } stats_;

  TieredStorage* ts_;
  DbSlice* db_slice_;
};

TieredStorage::TieredStorage(DbSlice* db_slice, size_t max_size)
    : op_manager_{make_unique<ShardOpManager>(this, db_slice, max_size)},
      bins_{make_unique<tiering::SmallBins>()} {
  write_depth_limit_ = absl::GetFlag(FLAGS_tiered_storage_write_depth);
}

TieredStorage::~TieredStorage() {
}

error_code TieredStorage::Open(string_view path) {
  return op_manager_->Open(absl::StrCat(path, ProactorBase::me()->GetPoolIndex()));
}

void TieredStorage::Close() {
  op_manager_->Close();
}

util::fb2::Future<string> TieredStorage::Read(DbIndex dbid, string_view key,
                                              const PrimeValue& value) {
  DCHECK(value.IsExternal());
  util::fb2::Future<string> future;
  auto cb = [future](string* value) mutable {
    future.Resolve(*value);
    return false;
  };
  op_manager_->Enqueue(KeyRef(dbid, key), value.GetExternalSlice(), std::move(cb));
  return future;
}

void TieredStorage::Read(DbIndex dbid, std::string_view key, const PrimeValue& value,
                         std::function<void(const std::string&)> readf) {
  DCHECK(value.IsExternal());
  auto cb = [readf = std::move(readf)](string* value) {
    readf(*value);
    return false;
  };
  op_manager_->Enqueue(KeyRef(dbid, key), value.GetExternalSlice(), std::move(cb));
}

template <typename T>
util::fb2::Future<T> TieredStorage::Modify(DbIndex dbid, std::string_view key,
                                           const PrimeValue& value,
                                           std::function<T(std::string*)> modf) {
  DCHECK(value.IsExternal());
  util::fb2::Future<T> future;
  auto cb = [future, modf = std::move(modf)](std::string* value) mutable {
    future.Resolve(modf(value));
    return true;
  };
  op_manager_->Enqueue(KeyRef(dbid, key), value.GetExternalSlice(), std::move(cb));
  return future;
}

template util::fb2::Future<size_t> TieredStorage::Modify(DbIndex dbid, std::string_view key,
                                                         const PrimeValue& value,
                                                         std::function<size_t(std::string*)> modf);

void TieredStorage::Stash(DbIndex dbid, string_view key, PrimeValue* value) {
  CHECK(!value->IsExternal() && !value->HasIoPending());

  // TODO: When we are low on memory we should introduce a back-pressure, to avoid OOMs
  // with a lot of underutilized disk space.
  if (op_manager_->GetStats().pending_stash_cnt >= write_depth_limit_) {
    ++stats_.stash_overflow_cnt;
    return;
  }

  string buf;
  string_view value_sv = value->GetSlice(&buf);
  value->SetIoPending(true);

  tiering::OpManager::EntryId id;
  error_code ec;
  if (OccupiesWholePages(value->Size())) {  // large enough for own page
    id = KeyRef(dbid, key);
    ec = op_manager_->Stash(id, value_sv);
  } else if (auto bin = bins_->Stash(dbid, key, value_sv); bin) {
    id = bin->first;
    ec = op_manager_->Stash(bin->first, bin->second);
  }

  if (ec) {
    LOG(ERROR) << "Stash failed immediately" << ec.message();
    visit([this](auto id) { op_manager_->ClearIoPending(id); }, id);
  }
}

void TieredStorage::Delete(DbIndex dbid, PrimeValue* value) {
  DCHECK(value->IsExternal());
  tiering::DiskSegment segment = value->GetExternalSlice();
  op_manager_->Delete(segment);
  op_manager_->SetInMemory(value, dbid, "", segment);
}

void TieredStorage::CancelStash(DbIndex dbid, std::string_view key, PrimeValue* value) {
  DCHECK(value->HasIoPending());
  if (OccupiesWholePages(value->Size())) {
    op_manager_->Delete(KeyRef(dbid, key));
  } else if (auto bin = bins_->Delete(dbid, key); bin) {
    op_manager_->Delete(*bin);
  }
  value->SetIoPending(false);
}

bool TieredStorage::ShouldStash(const PrimeValue& pv) const {
  return !pv.IsExternal() && pv.ObjType() == OBJ_STRING && pv.Size() >= kMinValueSize;
}

float TieredStorage::WriteDepthUsage() const {
  return 1.0f * op_manager_->GetStats().pending_stash_cnt / write_depth_limit_;
}

TieredStats TieredStorage::GetStats() const {
  TieredStats stats{};

  {  // ShardOpManager stats
    auto shard_stats = op_manager_->stats_;
    stats.total_fetches = shard_stats.total_fetches;
    stats.total_stashes = shard_stats.total_stashes;
    stats.total_cancels = shard_stats.total_cancels;
    stats.total_defrags = shard_stats.total_defrags;
  }

  {  // OpManager stats
    tiering::OpManager::Stats op_stats = op_manager_->GetStats();
    stats.pending_read_cnt = op_stats.pending_read_cnt;
    stats.pending_stash_cnt = op_stats.pending_stash_cnt;
    stats.allocated_bytes = op_stats.disk_stats.allocated_bytes;
    stats.capacity_bytes = op_stats.disk_stats.capacity_bytes;
    stats.total_heap_buf_allocs = op_stats.disk_stats.heap_buf_alloc_count;
    stats.total_registered_buf_allocs = op_stats.disk_stats.registered_buf_alloc_count;
  }

  {  // SmallBins stats
    tiering::SmallBins::Stats bins_stats = bins_->GetStats();
    stats.small_bins_cnt = bins_stats.stashed_bins_cnt;
    stats.small_bins_entries_cnt = bins_stats.stashed_entries_cnt;
    stats.small_bins_filling_bytes = bins_stats.current_bin_bytes;
  }

  {  // Own stats
    stats.total_stash_overflows = stats_.stash_overflow_cnt;
  }
  return stats;
}

void TieredStorage::RunOffloading(DbIndex dbid) {
  if (SliceSnapshot::IsSnaphotInProgress())
    return;

  PrimeTable& table = op_manager_->db_slice_->GetDBTable(dbid)->prime;
  int stash_limit = write_depth_limit_ - op_manager_->GetStats().pending_stash_cnt;
  if (stash_limit <= 0)
    return;

  std::string tmp;
  auto cb = [this, dbid, &tmp, &stash_limit](PrimeIterator it) {
    if (it->second.HasIoPending() || it->second.IsExternal())
      return;

    if (ShouldStash(it->second)) {
      Stash(dbid, it->first.GetSlice(&tmp), &it->second);
      stash_limit--;
    }
  };

  PrimeTable::Cursor start_cursor{};

  // Loop while we haven't traversed all entries or reached our stash io device limit.
  do {
    offloading_cursor_ = table.TraverseBySegmentOrder(offloading_cursor_, cb);
  } while (offloading_cursor_ != start_cursor && stash_limit > 0);
}

}  // namespace dfly
