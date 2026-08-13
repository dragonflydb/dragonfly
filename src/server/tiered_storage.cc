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
#include "server/tiering/decoders.h"
#include "server/tiering/op_manager.h"
#include "server/tiering/small_bins.h"
#include "strings/human_readable.h"

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

ABSL_RETIRED_FLAG(unsigned, tiered_storage_write_depth, 200,
                  "Maximum number of concurrent stash requests issued by background offload. "
                  "Deprecated: prefer tiered_max_pending_stash_bytes.");

// 256kb is a realistic limit for moden NVMe drives: in-flight = avg latency * throughput/s
ABSL_FLAG(strings::MemoryBytesFlag, tiered_max_pending_stash_bytes, 256_KB,
          "Maximum bytes in-flight to disk before rejecting new stashes or applying client "
          "backpressure. Allows batching writes to saturate disk I/O even with few clients");

ABSL_FLAG(float, tiered_offload_threshold, 0.5,
          "Ratio of free memory (free/max memory) below which offloading starts");

ABSL_FLAG(float, tiered_upload_threshold, 0.1,
          "Ratio of free memory (free/max memory) below which uploading stops");

ABSL_FLAG(bool, tiered_experimental_hash_support, false, "Experimental hash datatype offloading");

ABSL_FLAG(bool, tiered_experimental_list_support, false, "Experimental list node offloading");

ABSL_FLAG(uint32, tiered_min_ttl_to_offload_ms, 5000,
          "Min remaining TTL in ms for a value to be eligible for offloading");

ABSL_FLAG(uint32, tiered_offload_scan_budget_us, 100,
          "Base cpu time-slice in microseconds granted to a single background offloading scan. "
          "Set to 0 to disable offloading scans");

ABSL_FLAG(uint32, tiered_defrag_scan_budget_us, 2,
          "Base cpu time-slice in microseconds granted to a single background defragmentation "
          "scan. Scales up to 3x this value with the amount of fragmentation found. Set to 0 to "
          "disable defragmentation scans");

ABSL_FLAG(uint32, tiered_max_pending_defrags, 50,
          "Maximum number of concurrent defragmentation read operations");

ABSL_FLAG(uint32, tiered_repack_scan_budget_us, 0,
          "Base cpu time-slice in microseconds granted to a single background bin-bucket repack "
          "scan, which regroups small offloaded values scattered across disk pages. Set to 0 "
          "(default) to disable repack scans");

ABSL_FLAG(uint32, tiered_repack_max_reads, 10,
          "Maximum number of concurrent page reads a single repack scan may issue");

ABSL_FLAG(uint32, tiered_repack_acceptable_waste, 2,
          "How many more disk pages than the theoretical minimum a bucket may spread its small "
          "values over before the repack scan regroups it");

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

// Called when a value returns to RAM and its disk segment is dropped: the bytes go back to the
// RAM ledger and leave the tiered counters.
void AccountTieredUpload(const FragmentRef& fragment_ref, size_t tiered_len, string_view key,
                         DbTable* db) {
  AccountObjectMemory(key, fragment_ref.ObjType(), fragment_ref.MallocUsed(), db);
  db->stats.tiered_entries--;
  db->stats.tiered_used_bytes -= tiered_len;
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
                       return lw.UsedBytes();
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
      detail::ListpackWrap lw{std::get<uint8_t*>(blob)};
      size_t bytes = lw.UsedBytes();
      memcpy(buffer.data(), lw.GetPointer(), bytes);
      return bytes;
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
    QList::Node* node = reinterpret_cast<QList::Node*>(std::get<2>(id));
    node->io_pending = 0;
    // If stashing failed we need to decrease offloaded nodes count.
    QList* ql = reinterpret_cast<QList*>(std::get<1>(id));
    ql->AdjustOffloadNodeCount(-1);
  }

  void CancelStash(tiering::KeyRef id, size_t size) {
    UnblockBackpressure(id, false);
    // TODO: Don't recompute size estimate, try-delete bin first
    if (OccupiesWholePages(size)) {
      CancelPending(id);
    } else if (auto bin = ts_->bins_->Delete(id.first, id.second); bin) {
      CancelPending(*bin);
    }
  }

  void CancelStash(tiering::ListNodeId id) {
    QList* ql = reinterpret_cast<QList*>(std::get<1>(id));
    ql->AdjustOffloadNodeCount(-1);
    CancelPending(id);
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

  bool NotifyDelete(tiering::DiskSegment segment, bool in_memory) override;

  void EnqueueForDefrag(tiering::DiskSegment segment);

  // If we are low on memory, remove entries from the ColdQueue,
  // and promote their PrimeValues to be fully external.
  void RetireColdEntries(size_t additional_memory);

  // Set value to be an in-memory type again. Update memory stats.
  void Upload(DbIndex dbid, string_view key, string_view value, PrimeValue* pv) {
    DCHECK(!value.empty());
    switch (pv->GetExternalRep()) {
      case CompactObj::ExternalRep::STRING: {
        pv->Materialize(value, true);
        break;
      }
      case CompactObj::ExternalRep::SERIALIZED_MAP: {
        tiering::ListpackMapDecoder decoder{};
        decoder.Initialize(value);
        decoder.Upload(pv);
        break;
      }
      case CompactObj::ExternalRep::LIST_NODE: {
        LOG(DFATAL) << "LIST_NODE should not be uploaded to PrimeValue";
        break;
      }
    };

    AccountTieredUpload(*pv, value.size(), key, db_slice_.GetDBTable(dbid));
  }

  // Find entry by key in db_slice and store external segment in place of original value.
  // Update memory stats
  void SetExternal(OpManager::KeyRef key, tiering::DiskSegment segment) {
    UnblockBackpressure(key, true);
    if (auto* pv = Find(key.first, key.second); pv) {
      DbTable* table = db_slice_.GetDBTable(key.first);

      pv->SetStashPending(false);
      table->stats.tiered_entries++;
      table->stats.tiered_used_bytes += segment.length;
      stats_.total_stashes++;

      StashDescriptor blobs{FragmentRef{*pv}.GetSerializationDescr()};
      // The value's bytes leave the RAM ledger; a cool copy is tracked by the cool cache only.
      AccountObjectMemory(key.second, pv->ObjType(), -int64_t(pv->MallocUsed()), table);
      if (ts_->config_.experimental_cooling) {
        RetireColdEntries(pv->MallocUsed());
        ts_->CoolDown(key.first, key.second, segment, blobs.rep, pv);
      } else {
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

    QList::Node* node = reinterpret_cast<QList::Node*>(std::get<2>(id));
    QList* ql = reinterpret_cast<QList*>(std::get<1>(id));

    node->io_pending = 0;

    // Adjust parent QList node malloc size / number of offloaded nodes.
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
    uint32_t pending_defrags = 0;
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
    string scratch;
    string_view item_key = it->first.GetSlice(&scratch);
    PrimeValue& pv = it->second;
    if (pv.IsCool()) {
      PrimeValue::CoolItem item = pv.GetCool();
      tiering::DiskSegment segment = FromCoolItem(item);

      // We remove it from both cool storage and the offline storage; the value becomes a
      // regular in-memory one, so it returns to the RAM ledger.
      pv = ts_->DeleteCool(item.record);
      AccountObjectMemory(item_key, pv.ObjType(), pv.MallocUsed(), db_slice_.GetDBTable(dbid));
      auto* stats = GetDbTableStats(dbid);
      stats->tiered_entries--;
      stats->tiered_used_bytes -= segment.length;
    } else {
      // Cut out relevant part of value and restore it to memory
      string_view value = page.substr(item_segment.offset - segment.offset, item_segment.length);
      Upload(dbid, item_key, value, &pv);
    }
  }
}

bool TieredStorage::ShardOpManager::NotifyFetched(const OwnedEntryId& id,
                                                  tiering::DiskSegment segment,
                                                  tiering::Decoder* decoder) {
  ++stats_.total_fetches;

  if (const auto* i = std::get_if<uintptr_t>(&id); i) {
    if (*i == kFragmentedBin) {  // Generally we read whole bins only for defrag
      auto* bdecoder = static_cast<tiering::BareDecoder*>(decoder);
      Defragment(segment, bdecoder->slice);
      return true;  // delete
    }
  }

  tiering::Decoder::UploadMetrics metrics = decoder->GetMetrics();

  // We must upload the value if it was modified
  bool should_upload = metrics.modified;

  // Snapshotting casuses reads that are not from clients, so ignore request to upload
  // List tiering uploads on it own rules from the ends
  const bool upload_disabled =
      SliceSnapshot::IsSnaphotInProgress() && !std::holds_alternative<tiering::ListNodeId>(id);

  // Give way for upload by reducing cooled queue if needed
  constexpr size_t kUploadReclaimMargin = 1_MB;
  int64_t needed = int64_t(metrics.estimated_mem_usage);
  if (ts_->UploadBudget() <= needed)
    RetireColdEntries(needed + kUploadReclaimMargin);

  should_upload |= !upload_disabled && ts_->UploadBudget() > needed;

  if (!should_upload)
    return false;

  if (const auto* key = std::get_if<tiering::ListNodeId>(&id); key) {
    DbIndex db_id = std::get<0>(*key);
    QList::Node* node = reinterpret_cast<QList::Node*>(std::get<2>(*key));
    ++stats_.total_uploads;
    decoder->Upload(node);
    // TODO: per-slot accounting is skipped because node ids carry no slot/key information.
    auto* stats = GetDbTableStats(db_id);
    stats->AddTypeMemoryUsage(OBJ_LIST, node->sz);
    stats->tiered_entries--;
    stats->tiered_used_bytes -= segment.length;
    return true;
  }

  if (const auto* key = std::get_if<tiering::DbKeyId>(&id); key) {
    auto* pv = Find(key->first, key->second);
    if (pv && pv->IsExternal() && segment == pv->GetExternalSlice()) {
      if (metrics.modified || pv->WasTouched()) {
        ++stats_.total_uploads;
        decoder->Upload(pv);
        AccountTieredUpload(*pv, segment.length, key->second, db_slice_.GetDBTable(key->first));
        return true;
      }
      pv->SetTouched(true);
      return false;
    }
  }

  LOG(DFATAL) << "Internal error, should not reach this";
  return false;
}

bool TieredStorage::ShardOpManager::NotifyDelete(tiering::DiskSegment segment, bool in_memory) {
  DVLOG(2) << "NotifyDelete [" << segment.offset << "," << segment.length << "]";

  if (OccupiesWholePages(segment.length))
    return true;

  auto bin = ts_->bins_->Delete(segment);
  if (bin.empty) {
    return true;
  }

  // If we have memory, upload the page for defrag. It will be reshuffled and offloaded more packed.
  // Otherwise background scans of fragmented bins will discover them
  if (bin.fragmented && ts_->UploadBudget() > 0) {
    // Limit number of IO operations if we need to read from disk (in_memory is false)
    if (in_memory || stats_.pending_defrags < ts_->config_.max_pending_defrags) {
      EnqueueForDefrag(bin.segment);
    }
  }

  return false;
}

void TieredStorage::ShardOpManager::EnqueueForDefrag(tiering::DiskSegment segment) {
  // Trigger read to signal need for defragmentation. NotifyFetched will handle it on success.
  DVLOG(2) << "Enqueueing bin defragmentation for: " << segment.offset;
  stats_.pending_defrags++;
  Enqueue(
      kFragmentedBin, segment, tiering::BareDecoder{},
      [this](io::Result<tiering::Decoder*> res) { stats_.pending_defrags--; }, true);
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

void TieredStorage::CancelLoad(tiering::DiskSegment segment) {
  op_manager_->CancelPendingLoad(segment);
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

  // Throttle if we're low on memory and reached the offloading limit
  if (backpressure && ShouldOffload() && WriteDepthUsage() >= 1.0f) {
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
    // With experimental hash support a cool record may hold a hash, not only a string.
    DeleteCool(cool);
  }
  fragment_ref.ClearOffloaded();
  op_manager_->DeleteOffloaded(dbid, segment);
}

void TieredStorage::CancelStash(tiering::PendingId id, tiering::FragmentRef fragment_ref) {
  DCHECK(fragment_ref.HasStashPending());
  DCHECK(std::holds_alternative<KeyRef>(id) || std::holds_alternative<tiering::ListNodeId>(id));
  std::visit(absl::Overload{[&fragment_ref, this](KeyRef id) {
                              StashDescriptor blobs{fragment_ref.GetSerializationDescr()};
                              op_manager_->CancelStash(id, blobs.EstimatedSerializedSize());
                            },
                            [this](tiering::ListNodeId id) { op_manager_->CancelStash(id); },
                            // Make variant exhaustive, but we should never call with this type.
                            [](uintptr_t) { LOG(DFATAL) << "Invalid id type for CancelStash"; }},
             id);
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
    stats.pending_stash_bytes = op_stats.disk_stats.pending_stash_bytes;
    stats.total_heap_buf_allocs = op_stats.disk_stats.heap_buf_alloc_count;
    stats.total_registered_buf_allocs = op_stats.disk_stats.registered_buf_alloc_count;
  }

  {  // SmallBins stats
    tiering::SmallBins::Stats bins_stats = bins_->GetStats();
    stats.small_bins_cnt = bins_stats.stashed_bins_cnt;
    stats.small_bins_entries_cnt = bins_stats.stashed_entries_cnt;
    stats.small_bins_entries_bytes = bins_stats.stashed_entries_bytes;
    stats.small_bins_filling_bytes = bins_stats.current_bin_bytes;
    stats.small_bins_filling_entries_cnt = bins_stats.current_entries_cnt;
  }

  {  // Own stats
    stats.total_deletes = stats_.total_deletes;
    stats.total_stash_overflows = stats_.stash_overflow_cnt;
    stats.cold_storage_bytes = stats_.cool_memory_used;
    stats.total_offloading_usec = stats_.offloading_usec;
    stats.total_defrag_usec = stats_.defrag_usec;
    stats.total_repack_usec = stats_.repack_usec;
    stats.total_offloading_stashes = stats_.offloading_stashes;
    stats.total_repacks = stats_.total_repacks;
    stats.estimated_bin_bucket_fragmentation = stats_.estimated_bin_bucket_fragmentation;
    stats.clients_throttled = stash_backpressure_.size();
    stats.total_clients_throttled = stats_.total_clients_throttled;
  }
  return stats;
}

float TieredStorage::WriteDepthUsage() const {
  auto disk_stats = op_manager_->GetStats().disk_stats;
  return 1.0f * float(disk_stats.pending_stash_bytes) / float(config_.max_pending_stash_bytes);
}

void TieredStorage::UpdateFromFlags() {
  config_ = {
      .min_value_size = absl::GetFlag(FLAGS_tiered_min_value_size),
      .experimental_cooling = absl::GetFlag(FLAGS_tiered_experimental_cooling),
      .max_pending_stash_bytes = absl::GetFlag(FLAGS_tiered_max_pending_stash_bytes),
      .offload_threshold = absl::GetFlag(FLAGS_tiered_offload_threshold),
      .upload_threshold = absl::GetFlag(FLAGS_tiered_upload_threshold),
      .experimental_hash_offload = absl::GetFlag(FLAGS_tiered_experimental_hash_support),
      .experimental_list_offload = absl::GetFlag(FLAGS_tiered_experimental_list_support),
      .min_ttl_to_offload_ms = absl::GetFlag(FLAGS_tiered_min_ttl_to_offload_ms),
      .offload_scan_budget_us = absl::GetFlag(FLAGS_tiered_offload_scan_budget_us),
      .defrag_scan_budget_us = absl::GetFlag(FLAGS_tiered_defrag_scan_budget_us),
      .max_pending_defrags = absl::GetFlag(FLAGS_tiered_max_pending_defrags),
      .repack_scan_budget_us = absl::GetFlag(FLAGS_tiered_repack_scan_budget_us),
      .repack_max_reads = absl::GetFlag(FLAGS_tiered_repack_max_reads),
      .repack_acceptable_waste = absl::GetFlag(FLAGS_tiered_repack_acceptable_waste),
  };

  LOG_IF(WARNING, config_.upload_threshold > config_.offload_threshold)
      << "tiered_upload_threshold should be less than tiered_offload_threshold to maximize cache "
         "and defragmentation effectiveness";
}

std::vector<std::string> TieredStorage::GetMutableFlagNames() {
  return base::GetFlagNames(FLAGS_tiered_min_value_size, FLAGS_tiered_experimental_cooling,
                            FLAGS_tiered_max_pending_stash_bytes, FLAGS_tiered_offload_threshold,
                            FLAGS_tiered_upload_threshold, FLAGS_tiered_experimental_hash_support,
                            FLAGS_tiered_experimental_list_support,
                            FLAGS_tiered_min_ttl_to_offload_ms, FLAGS_tiered_offload_scan_budget_us,
                            FLAGS_tiered_defrag_scan_budget_us, FLAGS_tiered_max_pending_defrags,
                            FLAGS_tiered_repack_scan_budget_us, FLAGS_tiered_repack_max_reads,
                            FLAGS_tiered_repack_acceptable_waste);
}

bool TieredStorage::ShouldOffload() const {
  // Cool values can be dropped, so count as free as well
  int64_t actual_free = op_manager_->db_slice_.memory_budget() + int64_t(CoolMemoryUsage());
  int64_t target_free = double(config_.offload_threshold) *
                        max_memory_limit.load(memory_order_relaxed) / shard_set->size();
  return actual_free < target_free;
}

int64_t TieredStorage::UploadBudget() const {
  int64_t free_memory = op_manager_->db_slice_.memory_budget();
  int64_t per_shard = max_memory_limit.load(memory_order_relaxed) / shard_set->size();
  return free_memory - double(config_.upload_threshold) * per_shard;
}

void TieredStorage::RunOffloading(DbIndex dbid) {
  using namespace tiering::literals;
  if (SliceSnapshot::IsSnaphotInProgress())
    return;

  // Takes up a small bounded amount of time and is best done before offloading (to be picked up)
  RunDefragScan();

  // Don't run offloading if there's only very little space left
  auto disk_stats = op_manager_->GetStats().disk_stats;
  if (disk_stats.allocated_bytes + 1_MB > disk_stats.max_file_size)
    return;

  if (config_.offload_scan_budget_us == 0)
    return;

  string tmp;
  auto cb = [this, dbid, &tmp](PrimeIterator it) mutable {
    auto blobs = ShouldStash(it->second, StashContext{.key_expire_ms = it->first.GetExpireTime()});
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

  const auto start_cycles = base::CycleClock::Now();

  // Loop over entry with time and max stash budget.
  uint64_t cycles = 0;
  do {
    // We hit backpressure limit, so stop
    if (op_manager_->GetStats().disk_stats.pending_stash_bytes >= config_.max_pending_stash_bytes)
      break;

    offloading_cursor_ = table.TraverseBySegmentOrder(offloading_cursor_, cb);

    // TODO: yield as background fiber to perform more work on idle
    // Limit allowed cpu-timeslice
    cycles = base::CycleClock::Now() - start_cycles;
    if (base::CycleClock::ToUsec(cycles) >= config_.offload_scan_budget_us)
      break;
  } while (offloading_cursor_);

  stats_.offloading_usec += base::CycleClock::ToUsec(cycles);
}

void TieredStorage::RunDefragScan() {
  // Scale the cpu time-slice with the backlog found by the previous scan, up to 3x the base
  // budget: sleepy on a stale table, more aggressive while there's fragmentation to clear.
  const uint64_t time_budget_us =
      config_.defrag_scan_budget_us * std::min<uint64_t>(1 + last_defrag_scan_hits_, 3);
  if (time_budget_us == 0)
    return;

  const auto start_cycles = base::CycleClock::Now();

  unsigned hits = 0;
  auto cb = [this, &hits](size_t offset) {
    tiering::DiskSegment segment{offset, tiering::kPageSize};
    if (!op_manager_->HasReadPending(kFragmentedBin, segment)) {
      op_manager_->EnqueueForDefrag({offset, tiering::kPageSize});
      ++hits;
    }
  };

  uint64_t cycles = 0;
  do {
    if (UploadBudget() <= 0)
      break;

    if (op_manager_->stats_.pending_defrags >= config_.max_pending_defrags)
      break;

    defrag_cursor_ = bins_->TraverseFragmented(defrag_cursor_, cb);

    // TODO: yield as background fiber to perform more work on idle
    // Limit allowed cpu-timeslice
    cycles = base::CycleClock::Now() - start_cycles;
    if (base::CycleClock::ToUsec(cycles) >= time_budget_us)
      break;
  } while (defrag_cursor_);

  stats_.defrag_usec += base::CycleClock::ToUsec(cycles);
  last_defrag_scan_hits_ = hits;
}

void TieredStorage::RunRepackScan(DbIndex dbid) {
  // Only run when explicitly enabled and there are small values stashed on disk to regroup.
  if (config_.repack_scan_budget_us == 0 || bins_->GetStats().stashed_bins_cnt == 0)
    return;

  if (SliceSnapshot::IsSnaphotInProgress())
    return;

  PrimeTable& table = op_manager_->db_slice_.GetDBTable(dbid)->prime;
  const auto start_cycles = base::CycleClock::Now();

  size_t issued_reads = 0;

  // With negative upload budget we only recalculate the fragmentation estimate and issue no reads.
  if (UploadBudget() < 0)
    issued_reads = config_.repack_max_reads + 1;

  auto cb = [&](PrimeTable::bucket_iterator it) {
    size_t total_size = 0;
    std::vector<size_t> pages;

    auto it2 = it;
    for (it2.AdvanceIfNotOccupied(); !it2.is_done(); ++it2) {
      PrimeValue& pv = it2->second;
      if (!pv.IsExternal() || pv.IsCool())
        continue;
      tiering::DiskSegment segment{pv.GetExternalSlice()};
      if (OccupiesWholePages(segment.length))
        continue;
      total_size += segment.length;
      size_t page = segment.ContainingPages().offset;
      if (std::find(pages.begin(), pages.end(), page) == pages.end())
        pages.push_back(page);
    }

    if (pages.empty())
      return;

    repack_state_.cycle_buckets++;
    repack_state_.cycle_pages += pages.size();

    // Skip buckets already within the acceptable page-waste bound above the theoretical minimum.
    size_t min_required = total_size / tiering::kPageSize + 1;
    if (pages.size() <= min_required + config_.repack_acceptable_waste)
      return;

    if (issued_reads > config_.repack_max_reads)
      return;

    stats_.total_repacks++;

    std::string scratch;
    for (it.AdvanceIfNotOccupied(); !it.is_done(); ++it) {
      PrimeValue& pv = it->second;
      if (!pv.IsExternal() || pv.IsCool())
        continue;
      tiering::DiskSegment segment{pv.GetExternalSlice()};
      if (OccupiesWholePages(segment.length))
        continue;
      if (op_manager_->HasModificationPending(segment))
        continue;

      pv.SetTouched(true);
      string_view key = it->first.GetSlice(&scratch);
      Read(
          tiering::KeyRef{dbid, key}, segment, tiering::StringDecoder{pv},
          [](io::Result<tiering::StringDecoder*>) {}, /*read_only=*/true);
    }
    issued_reads += pages.size();
  };

  // TraverseBuckets advances the cursor monotonically each call (visiting a bounded number of
  // buckets and returning an end-cursor once the whole table is covered), so the clock budget
  // alone bounds the scan.
  uint64_t cycles = 0;
  do {
    repack_state_.cursor = table.TraverseBuckets(repack_state_.cursor, cb);
    cycles = base::CycleClock::Now() - start_cycles;
    if (base::CycleClock::ToUsec(cycles) >= config_.repack_scan_budget_us)
      break;
  } while (repack_state_.cursor);

  stats_.repack_usec += base::CycleClock::ToUsec(cycles);

  if (!repack_state_.cursor) {  // completed a full pass over the table
    if (repack_state_.cycle_buckets > 0) {
      stats_.estimated_bin_bucket_fragmentation =
          float(repack_state_.cycle_pages) / float(repack_state_.cycle_buckets);
    }
    repack_state_.cycle_buckets = 0;
    repack_state_.cycle_pages = 0;
  }
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

    // Now the item is only in storage. Its bytes already left the RAM ledger at cool-down.
    tiering::DiskSegment segment = FromCoolItem(pv.GetCool());
    pv.Freeze(segment.offset, segment.length);

    CompactObj::DeleteMR<TieredCoolRecord>(record);
  } while (gained < goal);

  return gained;
}

auto TieredStorage::ShouldStash(const tiering::FragmentRef& fragment_ref,
                                const StashContext& stash_ctx) const
    -> std::optional<StashDescriptor> {
  // Check value state
  if (fragment_ref.IsOffloaded() || fragment_ref.HasStashPending())
    return nullopt;

  const bool should_offload = ShouldOffload();

  if (!should_offload && stash_ctx.key_expire_ms > 0 && config_.min_ttl_to_offload_ms > 0) {
    if (stash_ctx.key_expire_ms <= GetCurrentTimeMs() + config_.min_ttl_to_offload_ms) {
      return nullopt;
    }
  }

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

  // If the fragment is list node we offload only if it occupies whole page.
  if (fragment_ref.ObjType() == OBJ_LIST && !OccupiesWholePages(estimated_size))
    return nullopt;

  // Track if we oversature disk (backpressure fails to stop clients, possibly many new).
  const auto& disk_stats = op_manager_->GetStats().disk_stats;
  if (disk_stats.pending_stash_bytes >= 2 * config_.max_pending_stash_bytes) {
    ++stats_.stash_overflow_cnt;
    // Discard the write if we don't require offloading to not oversaturate the disk
    if (!should_offload)
      return std::nullopt;
  }

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

PrimeValue TieredStorage::Warmup(DbIndex dbid, std::string_view key, PrimeValue::CoolItem item) {
  tiering::DiskSegment segment = FromCoolItem(item);

  // We remove it from both cool storage and the offline storage. The value returns to RAM,
  // so it returns to the RAM ledger as well.
  PrimeValue hot = DeleteCool(item.record);
  AccountObjectMemory(key, hot.ObjType(), hot.MallocUsed(),
                      op_manager_->db_slice_.GetDBTable(dbid));
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

void StashPrimeValue(DbIndex dbid, std::string_view key, const PrimeKey& pk, PrimeValue* pv,
                     TieredStorage* ts, BackPressureFuture* backpressure) {
  if (auto blobs =
          ts->ShouldStash(*pv, TieredStorage::StashContext{.key_expire_ms = pk.GetExpireTime()});
      blobs) {
    pv->SetStashPending(true);
    ts->StashPrimeValue(dbid, key, *blobs, backpressure);
  }
}

bool StashListNode(DbIndex dbid, QList* ql, QList::Node* node, TieredStorage* ts,
                   BackPressureFuture* backpressure) {
  if (auto blobs = ts->ShouldStash(*node, {}); blobs) {
    // Increment before stashing; decremented on failure in `ClearStashPending`
    ql->AdjustOffloadNodeCount(1);
    node->io_pending = 1;
    ts->StashPartialValue(tiering::ListNodeId{dbid, ql, node}, *blobs, backpressure);
    return true;
  }
  return false;
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

TieredStorage::TResult<bool> ReadTieredListNode(DbIndex dbid, QList* ql, QList::Node* node,
                                                const tiering::DiskSegment& segment,
                                                TieredStorage* ts) {
  TieredStorage::TResult<bool> fut;
  auto read_cb = [fut](const io::Result<tiering::ListNodeDecoder*>& res) mutable {
    fut.Resolve(res.transform([](tiering::ListNodeDecoder* d) { return true; }));
  };
  ts->Read(tiering::ListNodeId{dbid, ql, node}, segment, tiering::ListNodeDecoder{ql},
           std::move(read_cb));
  return fut;
}

void PrefetchTieredListNode(DbIndex dbid, QList* ql, QList::Node* node, TieredStorage* ts) {
  DCHECK(node->offloaded);
  DCHECK(!node->io_pending);
  node->io_pending = 1;
  auto read_cb = [node](const io::Result<tiering::ListNodeDecoder*>& res) {
    node->io_pending = 0;
    if (!res) {
      LOG(WARNING) << "Failed to prefetch list node from tiered storage: " << res.error().message();
    }
  };
  ts->Read(tiering::ListNodeId{dbid, ql, node}, node->GetExternalSlice(),
           tiering::ListNodeDecoder{ql}, std::move(read_cb));
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
