// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tiered_storage.h"

#include <mimalloc.h>

#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <variant>

#include "absl/cleanup/cleanup.h"
#include "absl/flags/internal/flag.h"
#include "absl/functional/bind_front.h"
#include "absl/functional/overload.h"
#include "base/flag_utils.h"
#include "base/flags.h"
#include "base/logging.h"
#include "core/detail/listpack_wrap.h"
#include "core/qlist.h"
#include "server/db_slice.h"
#include "server/engine_shard_set.h"
#include "server/snapshot.h"
#include "server/table.h"
#include "server/tiering/common.h"
#include "server/tiering/op_manager.h"
#include "server/tiering/serialized_map.h"
#include "server/tiering/small_bins.h"

extern "C" {
#include "redis/listpack.h"
}

using namespace facade;

using AtLeast64 = base::ConstrainedNumericFlagValue<size_t, 64>;  // ABSL_FLAG breaks with commas
ABSL_FLAG(AtLeast64, tiered_min_value_size, 64,
          "Minimum size of values eligible for offloading. Must be at least 64");

ABSL_FLAG(bool, tiered_experimental_cooling, true,
          "If true, uses intermediate cooling layer "
          "when offloading values to storage");

ABSL_FLAG(unsigned, tiered_storage_write_depth, 200,
          "Maximum number of concurrent stash requests issued by background offload");

ABSL_FLAG(float, tiered_offload_threshold, 0.5,
          "Ratio of free memory (free/max memory) below which offloading starts");

ABSL_FLAG(float, tiered_upload_threshold, 0.1,
          "Ratio of free memory (free/max memory) below which uploading stops");

ABSL_FLAG(bool, tiered_experimental_hash_support, false, "Experimental hash datatype offloading");

ABSL_FLAG(bool, tiered_experimental_list_support, false, "Experimental list node offloading");

namespace dfly {

using namespace std;
using namespace util;
using tiering::FragmentRef;
using tiering::KeyRef;
using tiering::TieredCoolRecord;

namespace {

bool OccupiesWholePages(size_t size) {
  return size >= TieredStorage::kMinOccupancySize;
}

// Stashed bins no longer have bin ids, so this sentinel is used to differentiate from regular reads
constexpr auto kFragmentedBin = tiering::SmallBins::kInvalidBin - 1;

// Called after setting new value in place of previous segment
void RecordDeleted(const PrimeValue& pv, size_t tiered_len, DbTableStats* stats) {
  stats->AddTypeMemoryUsage(pv.ObjType(), pv.MallocUsed());
  stats->tiered_entries--;
  stats->tiered_used_bytes -= tiered_len;
}

tiering::DiskSegment FromCoolItem(const PrimeValue::CoolItem& item) {
  return {item.record->page_index * tiering::kPageSize + item.page_offset, item.serialized_size};
}

string SerializeToString(const TieredStorage::StashDescriptor& blobs) {
  size_t est_size = blobs.EstimatedSerializedSize();
  string s(est_size, 0);
  size_t written = blobs.Serialize({reinterpret_cast<uint8_t*>(s.data()), s.size()});
  s.resize(written);
  return s;
}

}  // anonymous namespace

size_t TieredStorage::StashDescriptor::EstimatedSerializedSize() const {
  return visit(
      absl::Overload{[](const array<string_view, 2>& a) { return a[0].size() + a[1].size(); },
                     [](uint8_t* ptr) {
                       detail::ListpackWrap lw{ptr};
                       return tiering::SerializedMap::EstimateSize(lw.UsedBytes(), lw.size());
                     }},
      blob);
};

size_t TieredStorage::StashDescriptor::Serialize(io::MutableBytes buffer) const {
  DCHECK_LE(EstimatedSerializedSize(), buffer.size());

  switch (rep) {
    case CompactObj::ExternalRep::STRING: {
      auto strs = std::get<std::array<std::string_view, 2>>(blob);
      memcpy(buffer.data(), strs[0].data(), strs[0].size());
      if (!strs[1].empty())
        memcpy(buffer.data() + strs[0].size(), strs[1].data(), strs[1].size());
      return strs[0].size() + strs[1].size();
    }
    case CompactObj::ExternalRep::SERIALIZED_MAP: {
      detail::ListpackWrap lw{static_cast<uint8_t*>(std::get<uint8_t*>(blob))};
      return tiering::SerializedMap::Serialize(
          lw, {reinterpret_cast<char*>(buffer.data()), buffer.length()});
    }
    case CompactObj::ExternalRep::LIST_NODE: {
      // LIST_NODE uses the string_view pair path (same as STRING).
      auto strs = std::get<std::array<std::string_view, 2>>(blob);
      memcpy(buffer.data(), strs[0].data(), strs[0].size());
      return strs[0].size();
    }
  };
  return 0;
}

class TieredStorage::ShardOpManager : public tiering::OpManager {
  friend class TieredStorage;

 public:
  ShardOpManager(TieredStorage* ts, DbSlice* db_slice, size_t max_size)
      : tiering::OpManager{max_size}, ts_{ts}, db_slice_{*db_slice} {
  }

  // Clear Stash pending flag for entry
  void ClearStashPending(OpManager::KeyRef key) {
    UnblockBackpressure(key, false);
    if (auto pv = Find(key.first, key.second); pv) {
      pv->SetStashPending(false);
      stats_.total_cancels++;
    }
  }

  // Clear stash pending flag for all contained entries of bin
  void ClearStashPending(tiering::SmallBins::BinId id) {
    for (const auto& key : ts_->bins_->ReportStashAborted(id))
      ClearStashPending(key);
  }

  // Clear stash pending flag for list node
  void ClearStashPending(tiering::ListNodeId id) {
    stats_.total_cancels++;
    QList::Node* node = reinterpret_cast<QList::Node*>(std::get<1>(id));
    node->io_pending = 0;
    // If stashing failed we need to decrease offloaded nodes count.
    QList* ql = reinterpret_cast<QList*>(std::get<2>(id));
    ql->DecreaseNumOffloadedNodes();
  }

  DbTableStats* GetDbTableStats(DbIndex dbid) {
    return db_slice_.MutableStats(dbid);
  }

  void DeleteOffloaded(DbIndex dbid, const tiering::DiskSegment& segment);

 private:
  PrimeValue* Find(DbIndex dbid, string_view key) {
    // TODO: Get DbContext for transaction for correct dbid and time
    // Bypass all update and stat mechanisms
    auto it = db_slice_.GetDBTable(dbid)->prime.Find(key);
    return IsValid(it) ? &it->second : nullptr;
  }

  // Load all values from bin by their hashes
  void Defragment(tiering::DiskSegment segment, string_view value);

  void NotifyStashed(const OwnedEntryId& id,
                     const io::Result<tiering::DiskSegment>& segment) override {
    if (!segment) {
      VLOG(1) << "Stash failed " << segment.error().message();
      visit([this](auto id) { ClearStashPending(id); }, id);
    } else {
      visit([this, segment](auto id) { SetExternal(id, *segment); }, id);
    }
  }

  bool NotifyFetched(const OwnedEntryId& id, tiering::DiskSegment segment,
                     tiering::Decoder* decoder) override;

  bool NotifyDelete(tiering::DiskSegment segment) override;

  // If we are low on memory, remove entries from the ColdQueue,
  // and promote their PrimeValues to be fully external.
  void RetireColdEntries(size_t additional_memory);

  // Set value to be an in-memory type again. Update memory stats.
  void Upload(DbIndex dbid, string_view value, PrimeValue* pv) {
    DCHECK(!value.empty());

    switch (pv->GetExternalRep()) {
      case CompactObj::ExternalRep::STRING:
        pv->Materialize(value, true);
        break;
      case CompactObj::ExternalRep::SERIALIZED_MAP: {
        tiering::SerializedMapDecoder decoder{};
        decoder.Initialize(value);
        decoder.Upload(pv);
        break;
      }
      case CompactObj::ExternalRep::LIST_NODE: {
        LOG(DFATAL) << "LIST_NODE should not be uploaded to PrimeValue";
        break;
      }
    };

    RecordDeleted(*pv, value.size(), GetDbTableStats(dbid));
  }

  // Find entry by key in db_slice and store external segment in place of original value.
  // Update memory stats
  void SetExternal(OpManager::KeyRef key, tiering::DiskSegment segment) {
    UnblockBackpressure(key, true);
    if (auto* pv = Find(key.first, key.second); pv) {
      auto* stats = GetDbTableStats(key.first);

      pv->SetStashPending(false);
      stats->tiered_entries++;
      stats->tiered_used_bytes += segment.length;
      stats_.total_stashes++;

      StashDescriptor blobs{FragmentRef{*pv}.GetSerializationDescr()};
      if (ts_->config_.experimental_cooling) {
        RetireColdEntries(pv->MallocUsed());
        ts_->CoolDown(key.first, key.second, segment, blobs.rep, pv);
      } else {
        stats->AddTypeMemoryUsage(pv->ObjType(), -pv->MallocUsed());
        pv->SetExternal(segment.offset, segment.length, blobs.rep);
      }
    } else {
      LOG(DFATAL) << "Should not reach here";
    }
  }

  // Find bin by id and call SetExternal for all contained entries
  void SetExternal(tiering::SmallBins::BinId id, tiering::DiskSegment segment) {
    for (const auto& [sub_dbid, sub_key, sub_segment] : ts_->bins_->ReportStashed(id, segment))
      SetExternal({sub_dbid, sub_key}, sub_segment);
  }

  // Finalize stash for a fragments identified by pointer
  void SetExternal(tiering::ListNodeId id, tiering::DiskSegment segment) {
    auto* stats = GetDbTableStats(std::get<0>(id));

    stats->tiered_entries++;
    stats->tiered_used_bytes += segment.length;
    stats_.total_stashes++;

    QList::Node* node = reinterpret_cast<QList::Node*>(std::get<1>(id));
    QList* ql = reinterpret_cast<QList*>(std::get<2>(id));

    node->io_pending = 0;

    // Adjust parent QList node malloc size.
    ql->AdjustMallocSize(-segment.length);
    node->SetExternal(segment.offset, segment.length);

    stats->AddTypeMemoryUsage(OBJ_LIST, -segment.length);
  }

  // If any backpressure (throttling) is active, notify that the operation finished
  void UnblockBackpressure(OpManager::KeyRef id, bool result) {
    if (auto node = ts_->stash_backpressure_.extract(id); !node.empty())
      node.mapped().Resolve(result);
  }

  struct {
    uint64_t total_stashes = 0, total_cancels = 0, total_fetches = 0;
    uint64_t total_defrags = 0;
    uint64_t total_uploads = 0;
  } stats_;

  TieredStorage* ts_;
  DbSlice& db_slice_;
};

void TieredStorage::ShardOpManager::Defragment(tiering::DiskSegment segment, string_view page) {
  // Note: Bin could've already been deleted, in that case DeleteBin returns an empty list
  for (auto [dbid, hash, item_segment] : ts_->bins_->DeleteBin(segment, page)) {
    // Search for key with the same hash and value pointing to the same segment.
    // If it still exists, it must correspond to the value stored in this bin
    auto predicate = [item_segment = item_segment](const PrimeKey& key, const PrimeValue& probe) {
      return probe.IsExternal() && tiering::DiskSegment{probe.GetExternalSlice()} == item_segment;
    };
    auto it = db_slice_.GetDBTable(dbid)->prime.FindFirst(hash, predicate);
    if (!IsValid(it))
      continue;

    // TODO: Handle upload and cooling via type dependent decoders

    stats_.total_defrags++;
    PrimeValue& pv = it->second;
    if (pv.IsCool()) {
      PrimeValue::CoolItem item = pv.GetCool();
      tiering::DiskSegment segment = FromCoolItem(item);

      // We remove it from both cool storage and the offline storage.
      pv = ts_->DeleteCool(item.record);
      auto* stats = GetDbTableStats(dbid);
      stats->tiered_entries--;
      stats->tiered_used_bytes -= segment.length;
    } else {
      // Cut out relevant part of value and restore it to memory
      string_view value = page.substr(item_segment.offset - segment.offset, item_segment.length);
      Upload(dbid, value, &pv);
    }
  }
}

bool TieredStorage::ShardOpManager::NotifyFetched(const OwnedEntryId& id,
                                                  tiering::DiskSegment segment,
                                                  tiering::Decoder* decoder) {
  ++stats_.total_fetches;

  if (const auto* key = std::get_if<tiering::ListNodeId>(&id); key) {
    ++stats_.total_uploads;
    QList* ql = reinterpret_cast<QList*>(std::get<2>(*key));
    ql->AdjustMallocSize(segment.length);
    DbTableStats* stats = GetDbTableStats(std::get<0>(*key));
    stats->AddTypeMemoryUsage(OBJ_LIST, segment.length);
    // We return false here, because we don't want to delete the value from storage yet.
    // It will be done in onload_cb callback.
    return false;
  }

  if (const auto* i = std::get_if<uintptr_t>(&id); i) {
    if (*i == kFragmentedBin) {  // Generally we read whole bins only for defrag
      auto* bdecoder = static_cast<tiering::BareDecoder*>(decoder);
      Defragment(segment, bdecoder->slice);
      return true;  // delete
    }
  }

  tiering::Decoder::UploadMetrics metrics = decoder->GetMetrics();

  // 1. When modified is true we MUST upload the value back to memory.
  // 2. On the other hand, if read is caused by snapshotting we do not want to fetch it.
  //    Currently, our heuristic is not very smart, because we stop uploading any reads during
  //    the snapshotting.
  // TODO: to revisit this when we rewrite it with more efficient snapshotting algorithm.
  bool should_upload = metrics.modified;
  should_upload |= (ts_->UploadBudget() > int64_t(metrics.estimated_mem_usage)) &&
                   !SliceSnapshot::IsSnaphotInProgress();

  if (!should_upload)
    return false;

  const auto& key = get<tiering::DbKeyId>(id);
  auto* pv = Find(key.first, key.second);
  if (pv && pv->IsExternal() && segment == pv->GetExternalSlice()) {
    if (metrics.modified || pv->WasTouched()) {
      ++stats_.total_uploads;
      decoder->Upload(pv);
      RecordDeleted(*pv, segment.length, GetDbTableStats(key.first));
      return true;
    }
    pv->SetTouched(true);
    return false;
  }

  LOG(DFATAL) << "Internal error, should not reach this";
  return false;
}

bool TieredStorage::ShardOpManager::NotifyDelete(tiering::DiskSegment segment) {
  DVLOG(2) << "NotifyDelete [" << segment.offset << "," << segment.length << "]";

  if (OccupiesWholePages(segment.length))
    return true;

  auto bin = ts_->bins_->Delete(segment);
  if (bin.empty) {
    return true;
  }

  if (bin.fragmented) {
    // Trigger read to signal need for defragmentation. NotifyFetched will handle it.
    DVLOG(2) << "Enqueueing bin defragmentation for: " << bin.segment.offset;
    Enqueue(
        kFragmentedBin, bin.segment, tiering::BareDecoder{}, [](auto res) {}, true);
  }

  return false;
}

void TieredStorage::ShardOpManager::RetireColdEntries(size_t additional_memory) {
  int64_t budget = ts_->UploadBudget() - additional_memory;
  if (budget > 0)
    return;

  size_t gained = ts_->ReclaimMemory(-budget);
  VLOG(1) << "Upload budget: " << budget << ", gained " << gained;

  // Update memory_budget directly since we know that gained bytes were released.
  // We will overwrite the budget correctly in the next Hearbeat.
  db_slice_.UpdateMemoryParams(gained + db_slice_.memory_budget(), db_slice_.bytes_per_object());
}

void TieredStorage::ShardOpManager::DeleteOffloaded(DbIndex dbid,
                                                    const tiering::DiskSegment& segment) {
  auto* stats = GetDbTableStats(dbid);
  OpManager::DeleteOffloaded(segment);
  stats->tiered_used_bytes -= segment.length;
  stats->tiered_entries--;
}

TieredStorage::TieredStorage(size_t max_size, DbSlice* db_slice)
    : op_manager_{make_unique<ShardOpManager>(this, db_slice, max_size)},
      bins_{make_unique<tiering::SmallBins>()} {
  UpdateFromFlags();
}

TieredStorage::~TieredStorage() {
}

error_code TieredStorage::Open(string_view base_path) {
  // dts - dragonfly tiered storage.
  string path = absl::StrCat(
      base_path, "-", absl::Dec(ProactorBase::me()->GetPoolIndex(), absl::kZeroPad4), ".dts");
  return op_manager_->Open(path);
}

void TieredStorage::Close() {
  is_closed_ = true;
  for (auto& [_, f] : stash_backpressure_)
    f.Resolve(false);
  op_manager_->Close();
}

bool TieredStorage::HasModificationPending(tiering::DiskSegment segment) const {
  return op_manager_->HasModificationPending(segment);
}

void TieredStorage::ReadInternal(tiering::ReadId id, const tiering::DiskSegment& segment,
                                 const tiering::Decoder& decoder,
                                 std::function<void(io::Result<tiering::Decoder*>)> cb,
                                 bool read_only) {
  // TODO: improve performance by avoiding one more function wrap
  op_manager_->Enqueue(std::visit([](auto&& value) -> tiering::PendingId { return value; }, id),
                       segment, decoder, std::move(cb), read_only);
}

void TieredStorage::StashPrimeValue(DbIndex dbid, string_view key, const StashDescriptor& blobs,
                                    BackPressureFuture* backpressure) {
  CHECK(!bins_->IsPending(dbid, key));  // Because has stash pending is false (ShouldStash checks)

  size_t est_size = blobs.EstimatedSerializedSize();
  DCHECK_GT(est_size, 0u);

  tiering::OpManager::PendingId id;
  error_code ec;

  if (OccupiesWholePages(est_size)) {  // large enough for own page
    id = KeyRef(dbid, key);
    auto serialize = absl::bind_front(&StashDescriptor::Serialize, &blobs);
    ec = op_manager_->PrepareAndStash(id, est_size, serialize);
  } else if (auto bin = bins_->Stash(dbid, key, SerializeToString(blobs)); bin) {
    id = bin->id;
    auto serialize = absl::bind_front(&tiering::SmallBins::SerializeBin, bins_.get(), &*bin);
    ec = op_manager_->PrepareAndStash(id, 4_KB, serialize);
  } else {
    return;  // added to bin, no operations pending
  }

  // Set stash pending to false on single value or whole bin
  if (ec) {
    // file_too_large if we reached the limits of the storage,
    // operation_would_block if we need to wait for a file to grow.
    bool to_log = ec != errc::file_too_large && ec != errc::operation_would_block &&
                  ec != errc::operation_in_progress;
    LOG_IF(ERROR, to_log) << "Stash failed: " << ec.message();
    visit([this](auto id) { op_manager_->ClearStashPending(id); }, id);
    return;
  }

  // If we are in the active offloading phase, throttle stashes by providing backpressure future
  if (backpressure && ShouldOffload()) {
    stats_.total_clients_throttled++;
    *backpressure = stash_backpressure_[{dbid, string{key}}];
  }
}

void TieredStorage::Delete(DbIndex dbid, FragmentRef fragment_ref) {
  DCHECK(!is_closed_);
  DCHECK(!fragment_ref.HasStashPending());
  ++stats_.total_deletes;

  tiering::DiskSegment segment = fragment_ref.GetExternalSlice();
  if (auto* cool = fragment_ref.GetCoolRecord(); cool) {
    auto hot = DeleteCool(cool);
    DCHECK_EQ(hot.ObjType(), OBJ_STRING);
  }
  fragment_ref.ClearOffloaded();
  op_manager_->DeleteOffloaded(dbid, segment);
}

void TieredStorage::CancelStash(tiering::PendingId id, tiering::FragmentRef fragment_ref) {
  DCHECK(fragment_ref.HasStashPending());
  DCHECK(std::holds_alternative<KeyRef>(id) || std::holds_alternative<tiering::ListNodeId>(id));
  if (auto* key = std::get_if<KeyRef>(&id)) {
    // If any previous write was happening, it has been cancelled
    if (auto node = stash_backpressure_.extract(*key); !node.empty())
      std::move(node.mapped()).Resolve(false);
    // TODO: Don't recompute size estimate, try-delete bin first
    StashDescriptor blobs{fragment_ref.GetSerializationDescr()};
    size_t size = blobs.EstimatedSerializedSize();
    if (OccupiesWholePages(size)) {
      op_manager_->CancelPending(id);
    } else if (auto bin = bins_->Delete(key->first, key->second); bin) {
      op_manager_->CancelPending(*bin);
    }
  } else if (auto* key = std::get_if<tiering::ListNodeId>(&id); key) {
    QList* ql = reinterpret_cast<QList*>(std::get<2>(*key));
    ql->DecreaseNumOffloadedNodes();
    op_manager_->CancelPending(id);
  }
  fragment_ref.SetStashPending(false);
}

TieredStats TieredStorage::GetStats() const {
  TieredStats stats{};

  {  // ShardOpManager stats
    auto shard_stats = op_manager_->stats_;
    stats.total_fetches = shard_stats.total_fetches;
    stats.total_stashes = shard_stats.total_stashes;
    stats.total_cancels = shard_stats.total_cancels;
    stats.total_defrags = shard_stats.total_defrags;
    stats.total_uploads = shard_stats.total_uploads;
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
    stats.small_bins_filling_entries_cnt = bins_stats.current_entries_cnt;
  }

  {  // Own stats
    stats.total_stash_overflows = stats_.stash_overflow_cnt;
    stats.cold_storage_bytes = stats_.cool_memory_used;
    stats.total_offloading_steps = stats_.offloading_steps;
    stats.total_offloading_stashes = stats_.offloading_stashes;
    stats.clients_throttled = stash_backpressure_.size();
    stats.total_clients_throttled = stats_.total_clients_throttled;
  }
  return stats;
}

float TieredStorage::WriteDepthUsage() const {
  return 1.0f * op_manager_->GetStats().pending_stash_cnt / config_.write_depth_limit;
}

void TieredStorage::UpdateFromFlags() {
  config_ = {
      .min_value_size = absl::GetFlag(FLAGS_tiered_min_value_size),
      .experimental_cooling = absl::GetFlag(FLAGS_tiered_experimental_cooling),
      .write_depth_limit = absl::GetFlag(FLAGS_tiered_storage_write_depth),
      .offload_threshold = absl::GetFlag(FLAGS_tiered_offload_threshold),
      .upload_threshold = absl::GetFlag(FLAGS_tiered_upload_threshold),
      .experimental_hash_offload = absl::GetFlag(FLAGS_tiered_experimental_hash_support),
      .experimental_list_offload = absl::GetFlag(FLAGS_tiered_experimental_list_support),
  };
}

std::vector<std::string> TieredStorage::GetMutableFlagNames() {
  return base::GetFlagNames(FLAGS_tiered_min_value_size, FLAGS_tiered_experimental_cooling,
                            FLAGS_tiered_storage_write_depth, FLAGS_tiered_offload_threshold,
                            FLAGS_tiered_upload_threshold, FLAGS_tiered_experimental_hash_support,
                            FLAGS_tiered_experimental_list_support);
}

bool TieredStorage::ShouldOffload() const {
  size_t free_memory = op_manager_->db_slice_.memory_budget();
  size_t per_shard = max_memory_limit.load(memory_order_relaxed) / shard_set->size();
  // Cool values are already offloadeded, so don't count them as used memory
  return (free_memory + CoolMemoryUsage()) < config_.offload_threshold * per_shard;
}

int64_t TieredStorage::UploadBudget() const {
  size_t free_memory = op_manager_->db_slice_.memory_budget();
  size_t per_shard = max_memory_limit.load(memory_order_relaxed) / shard_set->size();
  return int64_t(free_memory) - int64_t(config_.upload_threshold * per_shard);
}

void TieredStorage::RunOffloading(DbIndex dbid) {
  using namespace tiering::literals;
  if (SliceSnapshot::IsSnaphotInProgress())
    return;

  const auto start_cycles = base::CycleClock::Now();

  // Don't run offloading if there's only very little space left
  auto disk_stats = op_manager_->GetStats().disk_stats;
  if (disk_stats.allocated_bytes + 1_MB > disk_stats.max_file_size)
    return;

  string tmp;
  auto cb = [this, dbid, &tmp](PrimeIterator it) mutable {
    stats_.offloading_steps++;
    auto blobs = ShouldStash(it->second);
    if (blobs) {
      if (it->second.WasTouched()) {
        it->second.SetTouched(false);
      } else {
        stats_.offloading_stashes++;
        it->second.SetStashPending(true);
        StashPrimeValue(dbid, it->first.GetSlice(&tmp), *blobs, nullptr);
      }
    }
  };

  PrimeTable& table = op_manager_->db_slice_.GetDBTable(dbid)->prime;

  // Loop over entry with time and max stash budget.
  uint64_t cycles = 0;
  do {
    offloading_cursor_ = table.TraverseBySegmentOrder(offloading_cursor_, cb);

    if (op_manager_->GetStats().pending_stash_cnt >= config_.write_depth_limit)
      break;

    // TODO: yield as background fiber to perform more work on idle
    cycles = base::CycleClock::Now() - start_cycles;
    if (base::CycleClock::ToUsec(cycles) >= 100)
      break;
  } while (offloading_cursor_);
}

size_t TieredStorage::ReclaimMemory(size_t goal) {
  size_t gained = 0;
  do {
    size_t memory_before = stats_.cool_memory_used;
    TieredCoolRecord* record = PopCool();
    if (record == nullptr)  // nothing to pull anymore
      break;

    gained += memory_before - stats_.cool_memory_used;

    // Find the entry that points to the cool item and externalize it.
    auto predicate = [record](const PrimeKey& key, const PrimeValue& probe) {
      return probe.IsExternal() && probe.IsCool() && probe.GetCool().record == record;
    };

    PrimeIterator it = op_manager_->db_slice_.GetDBTable(record->db_index)
                           ->prime.FindFirst(record->key_hash, predicate);
    CHECK(IsValid(it));
    PrimeValue& pv = it->second;

    // Now the item is only in storage.
    tiering::DiskSegment segment = FromCoolItem(pv.GetCool());
    pv.Freeze(segment.offset, segment.length);

    auto* stats = op_manager_->GetDbTableStats(record->db_index);
    stats->AddTypeMemoryUsage(record->value.ObjType(), -record->value.MallocUsed());
    CompactObj::DeleteMR<TieredCoolRecord>(record);
  } while (gained < goal);

  return gained;
}

auto TieredStorage::ShouldStash(const tiering::FragmentRef& fragment_ref) const
    -> std::optional<StashDescriptor> {
  // Check value state
  if (fragment_ref.IsOffloaded() || fragment_ref.HasStashPending())
    return nullopt;

  // For now, hash offloading is conditional
  if (fragment_ref.ObjType() == OBJ_HASH && !config_.experimental_hash_offload)
    return nullopt;

  // For now, list node offloading is conditional
  if (fragment_ref.ObjType() == OBJ_LIST && !config_.experimental_list_offload)
    return nullopt;

  // Estimate value size
  StashDescriptor blobs{fragment_ref.GetSerializationDescr()};
  size_t estimated_size = blobs.EstimatedSerializedSize();
  if (estimated_size < config_.min_value_size)
    return nullopt;

  // Limit write depth. TODO: Provide backpressure?
  if (op_manager_->GetStats().pending_stash_cnt >= config_.write_depth_limit) {
    ++stats_.stash_overflow_cnt;
    return {};
  }

  const auto& disk_stats = op_manager_->GetStats().disk_stats;
  if (disk_stats.allocated_bytes + tiering::kPageSize + estimated_size < disk_stats.max_file_size) {
    return blobs;
  }
  return nullopt;
}

void TieredStorage::CoolDown(DbIndex db_ind, std::string_view str,
                             const tiering::DiskSegment& segment, CompactObj::ExternalRep rep,
                             PrimeValue* pv) {
  TieredCoolRecord* record = CompactObj::AllocateMR<TieredCoolRecord>();
  cool_queue_.push_front(*record);
  stats_.cool_memory_used += (sizeof(TieredCoolRecord) + pv->MallocUsed());

  record->key_hash = CompactObj::HashCode(str);
  record->db_index = db_ind;
  record->page_index = segment.offset / tiering::kPageSize;
  record->value = std::move(*pv);

  pv->SetCool(segment.offset, segment.length, rep, record);
}

PrimeValue TieredStorage::Warmup(DbIndex dbid, PrimeValue::CoolItem item) {
  tiering::DiskSegment segment = FromCoolItem(item);

  // We remove it from both cool storage and the offline storage.
  PrimeValue hot = DeleteCool(item.record);
  op_manager_->DeleteOffloaded(dbid, segment);
  return hot;
}

PrimeValue TieredStorage::DeleteCool(TieredCoolRecord* record) {
  auto it = CoolQueue::s_iterator_to(*record);
  cool_queue_.erase(it);

  PrimeValue hot{std::move(record->value)};
  stats_.cool_memory_used -= (sizeof(TieredCoolRecord) + hot.MallocUsed());
  CompactObj::DeleteMR<TieredCoolRecord>(record);
  return hot;
}

TieredCoolRecord* TieredStorage::PopCool() {
  if (cool_queue_.empty())
    return nullptr;

  TieredCoolRecord& res = cool_queue_.back();
  cool_queue_.pop_back();
  stats_.cool_memory_used -= (sizeof(TieredCoolRecord) + res.value.MallocUsed());
  return &res;
}

void StashPrimeValue(DbIndex dbid, std::string_view key, PrimeValue* pv, TieredStorage* ts,
                     BackPressureFuture* backpressure) {
  if (auto blobs = ts->ShouldStash(*pv); blobs) {
    pv->SetStashPending(true);
    ts->StashPrimeValue(dbid, key, *blobs, backpressure);
  }
}

void StashListNode(DbIndex dbid, QList::Node* node, QList* ql, TieredStorage* ts,
                   BackPressureFuture* backpressure) {
  if (auto blobs = ts->ShouldStash(*node); blobs) {
    node->io_pending = 1;
    tiering::ListNodeId id{dbid, node, ql};
    ts->StashPartialValue(id, *blobs, backpressure);
  }
}

void TieredStorage::StashPartialValue(tiering::PendingId id, const StashDescriptor& blobs,
                                      BackPressureFuture* backpressure) {
  size_t est_size = blobs.EstimatedSerializedSize();
  DCHECK_GT(est_size, 0u);

  auto serialize = absl::bind_front(&StashDescriptor::Serialize, &blobs);

  error_code ec = op_manager_->PrepareAndStash(id, est_size, serialize);
  if (ec) {
    bool to_log = ec != errc::file_too_large && ec != errc::operation_would_block &&
                  ec != errc::operation_in_progress;
    LOG_IF(ERROR, to_log) << "Node stash failed: " << ec.message();
    std::visit([this](const auto& value) { op_manager_->ClearStashPending(value); }, id);
  }
}

void ReadTiered(DbIndex dbid, std::string_view key, const PrimeValue& value,
                function<void(io::Result<string_view>)> readf, TieredStorage* ts) {
  auto cb = [readf = std::move(readf)](io::Result<tiering::StringDecoder*> res) mutable {
    readf(res.transform([](tiering::StringDecoder* d) { return d->GetView(); }));
  };
  ts->Read(KeyRef{dbid, key}, value.GetExternalSlice(), tiering::StringDecoder{value},
           std::move(cb));
}

void ReadTieredListNode(DbIndex dbid, QList::Node* node, QList* ql,
                        const tiering::DiskSegment& segment,
                        std::function<void(io::Result<std::string_view>)> readf,
                        TieredStorage* ts) {
  auto cb = [readf = std::move(readf)](io::Result<tiering::BareDecoder*> res) mutable {
    readf(res.transform([](tiering::BareDecoder* d) { return d->slice; }));
  };
  ts->Read(tiering::ListNodeId{dbid, node, ql}, segment, tiering::BareDecoder{}, std::move(cb));
}

template <typename T>
TieredStorage::TResult<T> ModifyTiered(DbIndex dbid, std::string_view key, const PrimeValue& value,
                                       std::function<T(std::string*)> modf, TieredStorage* ts) {
  DCHECK(value.IsExternal());
  DCHECK_EQ(value.ObjType(), OBJ_STRING);

  util::fb2::Future<io::Result<T>> future;

  auto cb = [future, modf = std::move(modf)](io::Result<tiering::StringDecoder*> res) mutable {
    future.Resolve(res.transform([&modf](auto* d) { return modf(d->Write()); }));
  };
  ts->Read(KeyRef{dbid, key}, value.GetExternalSlice(), tiering::StringDecoder{value},
           std::move(cb), false);

  return future;
}

// Instantiate for size_t only - used in string_family's OpExtend.
template TieredStorage::TResult<size_t> ModifyTiered(DbIndex dbid, std::string_view key,
                                                     const PrimeValue& value,
                                                     std::function<size_t(std::string*)> modf,
                                                     TieredStorage* ts);

}  // namespace dfly
