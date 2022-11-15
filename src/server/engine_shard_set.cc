// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/engine_shard_set.h"

extern "C" {
#include "redis/object.h"
#include "redis/zmalloc.h"
}

#include <mimalloc-types.h>

#include "base/flags.h"
#include "base/logging.h"
#include "base/proc_util.h"
#include "server/blocking_controller.h"
#include "server/server_state.h"
#include "server/tiered_storage.h"
#include "server/transaction.h"
#include "strings/human_readable.h"
#include "util/fiber_sched_algo.h"
#include "util/varz.h"

using namespace std;

ABSL_FLAG(string, backing_prefix, "", "");

ABSL_FLAG(uint32_t, hz, 100,
          "Base frequency at which the server performs other background tasks. "
          "Warning: not advised to decrease in production.");

ABSL_FLAG(bool, cache_mode, false,
          "If true, the backend behaves like a cache, "
          "by evicting entries when getting close to maxmemory limit");

ABSL_FLAG(uint64_t, mem_defrag_threshold, 20,
          "Threshold level to run memory fragmentation between total available memory and "
          "currently commited memory");

ABSL_FLAG(float, commit_use_threshold, 1.3,
          "The threshold to apply memory fragmentation between commited and used memory");

namespace dfly {

using namespace util;
namespace this_fiber = ::boost::this_fiber;
namespace fibers = ::boost::fibers;
using absl::GetFlag;

namespace {

constexpr DbIndex DEFAULT_DB_INDEX = 0;

inline auto MatchType(int redis_type, std::uint8_t op_type) -> bool {
  return op_type == redis_type;
}

struct MemoryDefragParams {
  mi_heap_t* heap_ptr = nullptr;
  DbSlice* slice = nullptr;
  uint64_t cursor = 0;
  uint8_t object_type = OBJ_STRING;  // use with map for type

  MemoryDefragParams(mi_heap_t* hp, DbSlice* s) : heap_ptr{hp}, slice{s} {
  }
};

vector<EngineShardSet::CachedStats> cached_stats;  // initialized in EngineShardSet::Init

// few issues here:
// 1. Do we need to lock? (I think we do, since this is a task that I'm not sure is running
// exclusively). - No we don't need a lock
// 2. If we do need to lock, we can use the code similar to scan - ScanCb function. This require the
// DB index, which we don't have! - we would be using index 0
// 3. We need to find a way to map between the memory that we are moving to the new location that we
// want to copy to
//    we have a function that can tell use about memory usage for the given pointer (which in this
//    case can be either the key or the value!), so are we going to move the value? (I think so),
//    the key? (not sure), both?
//  No we don't need, when we are doing new allocation, it would ensure that the allocation will
//  happen to the first available space, this would result in moving all the memory to the "front"
//  of the list.
// 4, This leave the biggest issue, we need to find a new location to move to, how? - No we don't
// see above To summarized:
//  IF I understand this correctly:
//    for each database id on this shared: (this is only 0)
//      lock each key - don't
//      check memory utilization for the address  of the key/value -> only for the value
//      move the key/value to new location  -> just do allocation (for the value), move data to new
//      memory location and free old one.
//   The for each is running by the task itself, and the cursor is the for index - again for now
//   only index 0!

auto ReallocateValueImpl(PrimeValue& current_value) -> bool {
  // we know that this is a string, we checked that before
  auto val = current_value.ToString();
  // we can now reset the old object as we are going to create a new one
  current_value.Reset();
  // This should allocate a new value
  current_value.SetString(val);
  return current_value.MallocUsed() > 0;
}

auto ReallocateValue(PrimeIterator it, void* from, const MemoryDefragParams& params, DbIndex index)
    -> bool {
  // allocate a new memory for the value, and then free the memory that used for the old one
  CHECK(it->second.ObjType() == OBJ_STRING);  // only supporting for string right now!
  CHECK(it->second.IsExternal());

  params.slice->PreUpdate(index, it);
  auto key = it->first.ToString();
  auto state = ReallocateValueImpl(it->second);
  params.slice->PostUpdate(index, it, key);
  return state;
}

auto MemoryReallocate(PrimeIterator it, const MemoryDefragParams& params, DbIndex index) -> bool {
  DCHECK(!IsValid(it));

  if (MatchType(it->second.ObjType(), params.object_type)) {
    auto ptr = it->second.RObjPtr();
    return ReallocateValue(it, ptr, params, index);
  }
  return false;
}

auto DefragMemory(const MemoryDefragParams& params, DbIndex index) -> std::tuple<bool, uint64_t> {
  constexpr size_t CURSOR_COUNT_RELATION = 50;

  auto shard_id = params.slice->shard_id();
  DCHECK(params.slice->IsDbValid(index));

  unsigned cnt = 0;
  std::size_t max = params.slice->DbSize(index) / CURSOR_COUNT_RELATION;
  VLOG(1) << "PrimeTable shared id/index " << params.slice->shard_id() << "/" << index << " has "
          << params.slice->DbSize(index) << " elements in it";

  PrimeTable::Cursor cur = params.cursor;
  auto [prime_table, expire_table] = params.slice->GetTables(index);
  bool state = max > 0;

  while (cnt < max) {
    cur = prime_table->Traverse(
        cur, [&](PrimeIterator it) { state = MemoryReallocate(it, params, index); });
    if (!cur) {
      break;
    }
  }

  if (cnt > 0) {
    VLOG(1) << shard_id << ": ran the memory re-ordering task for " << cnt << " times - cursor "
            << cur.value();
  }

  return {state, cur.value()};
}

inline auto MemUsageChanged(uint64_t current, uint64_t now) -> bool {
  static const float commit_use_threshold = GetFlag(FLAGS_commit_use_threshold);
  // we only care whether the memory usage is going up
  // otherwise we don't really case
  return (current * commit_use_threshold) < float(now);
}

auto IsMemDefragRequired(uint16_t id, uint64_t* current_commit, uint64_t* current_use, int count)
    -> bool {
  static const uint64_t mem_size = max_memory_limit;
  static const uint64_t threshold_mem = mem_size / GetFlag(FLAGS_mem_defrag_threshold);
  static const float commit_use_threshold = GetFlag(FLAGS_commit_use_threshold);

  uint64_t commited = GetMallocCurrentCommitted();
  uint64_t mem_in_use = used_mem_current.load(memory_order_relaxed);

  // we want to make sure that we are not running this to many times - i.e.
  // if there was no change to the memory, don't run this
  if (MemUsageChanged(*current_commit, commited) || MemUsageChanged(*current_use, mem_in_use)) {
    if (threshold_mem < commited && mem_in_use != 0 &&
        (float(mem_in_use * commit_use_threshold) < commited)) {
      // we have way more commited then actual usage
      VLOG(1) << id << ": need to update memory - commited memory "
              << strings::HumanReadableNumBytes(commited) << ", the use memory is "
              << strings::HumanReadableNumBytes(mem_in_use);
      *current_commit = commited;
      *current_use = mem_in_use;
      return true;
    }
  }
  return false;
}

}  // namespace

constexpr size_t kQueueLen = 64;

thread_local EngineShard* EngineShard::shard_ = nullptr;
EngineShardSet* shard_set = nullptr;
uint64_t TEST_current_time_ms = 0;

EngineShard::Stats& EngineShard::Stats::operator+=(const EngineShard::Stats& o) {
  ooo_runs += o.ooo_runs;
  quick_runs += o.quick_runs;

  return *this;
}

void EngineShard::StartDefragTask(util::ProactorBase* pb) {
  MemoryDefragParams params(mi_resource_.heap(), &db_slice_);

  uint64_t commited = GetMallocCurrentCommitted();
  uint64_t mem_in_use = used_mem_current.load(memory_order_relaxed);

  defrag_task_ = pb->AddOnIdleTask(
      [task_params = std::move(params), commited, mem_in_use, count = 0]() mutable {
        auto shard_id = task_params.slice->shard_id();
        auto required_state = IsMemDefragRequired(shard_id, &commited, &mem_in_use, ++count);
        if (required_state) {
          auto [res, cur] = DefragMemory(task_params,
                                         DEFAULT_DB_INDEX);  // we are only checking for DB == 0
          task_params.cursor = cur;
          if (res) {
            return util::ProactorBase::kOnIdleMaxLevel;  // we need to run more of this
          }
        } else {
          task_params.cursor = 0;  // set it back, we're not running any more
        }
        // by default we just want to not get in the way..
        return 0u;
      });
}

EngineShard::EngineShard(util::ProactorBase* pb, bool update_db_time, mi_heap_t* heap)
    : queue_(kQueueLen), txq_([](const Transaction* t) { return t->txid(); }), mi_resource_(heap),
      db_slice_(pb->GetIndex(), GetFlag(FLAGS_cache_mode), this) {
  fiber_q_ = fibers::fiber([this, index = pb->GetIndex()] {
    this_fiber::properties<FiberProps>().set_name(absl::StrCat("shard_queue", index));
    queue_.Run();
  });

  if (update_db_time) {
    uint32_t clock_cycle_ms = 1000 / std::max<uint32_t>(1, GetFlag(FLAGS_hz));
    if (clock_cycle_ms == 0)
      clock_cycle_ms = 1;

    periodic_task_ = pb->AddPeriodic(clock_cycle_ms, [this] { Heartbeat(); });
  }

  tmp_str1 = sdsempty();

  db_slice_.UpdateExpireBase(absl::GetCurrentTimeNanos() / 1000000, 0);

  StartDefragTask(pb);
}

EngineShard::~EngineShard() {
  sdsfree(tmp_str1);
}

void EngineShard::Shutdown() {
  queue_.Shutdown();
  fiber_q_.join();

  if (tiered_storage_) {
    tiered_storage_->Shutdown();
  }

  if (periodic_task_) {
    ProactorBase::me()->CancelPeriodic(periodic_task_);
  }

  ProactorBase::me()->RemoveOnIdleTask(defrag_task_);
}

void EngineShard::InitThreadLocal(ProactorBase* pb, bool update_db_time) {
  CHECK(shard_ == nullptr) << pb->GetIndex();

  mi_heap_t* data_heap = ServerState::tlocal()->data_heap();
  void* ptr = mi_heap_malloc_aligned(data_heap, sizeof(EngineShard), alignof(EngineShard));
  shard_ = new (ptr) EngineShard(pb, update_db_time, data_heap);

  CompactObj::InitThreadLocal(shard_->memory_resource());
  SmallString::InitThreadLocal(data_heap);

  string backing_prefix = GetFlag(FLAGS_backing_prefix);
  if (!backing_prefix.empty()) {
    string fn =
        absl::StrCat(backing_prefix, "-", absl::Dec(pb->GetIndex(), absl::kZeroPad4), ".ssd");

    shard_->tiered_storage_.reset(new TieredStorage(&shard_->db_slice_));
    error_code ec = shard_->tiered_storage_->Open(fn);
    CHECK(!ec) << ec.message();  // TODO
  }
}

void EngineShard::DestroyThreadLocal() {
  if (!shard_)
    return;

  uint32_t index = shard_->db_slice_.shard_id();
  mi_heap_t* tlh = shard_->mi_resource_.heap();

  shard_->Shutdown();

  shard_->~EngineShard();
  mi_free(shard_);
  shard_ = nullptr;
  CompactObj::InitThreadLocal(nullptr);
  mi_heap_delete(tlh);
  VLOG(1) << "Shard reset " << index;
}

// Is called by Transaction::ExecuteAsync in order to run transaction tasks.
// Only runs in its own thread.
void EngineShard::PollExecution(const char* context, Transaction* trans) {
  VLOG(2) << "PollExecution " << context << " " << (trans ? trans->DebugId() : "") << " "
          << txq_.size() << " " << continuation_trans_;

  ShardId sid = shard_id();

  uint16_t trans_mask = trans ? trans->GetLocalMask(sid) : 0;
  if (trans_mask & Transaction::AWAKED_Q) {
    DCHECK(continuation_trans_ == nullptr)
        << continuation_trans_->DebugId() << " when polling " << trans->DebugId();

    CHECK_EQ(committed_txid_, trans->notify_txid());
    bool keep = trans->RunInShard(this);
    if (keep)
      return;
  }

  if (continuation_trans_) {
    if (trans == continuation_trans_)
      trans = nullptr;

    if (continuation_trans_->IsArmedInShard(sid)) {
      bool to_keep = continuation_trans_->RunInShard(this);
      DVLOG(1) << "RunContTrans: " << continuation_trans_->DebugId() << " keep: " << to_keep;
      if (!to_keep) {
        continuation_trans_ = nullptr;
      }
    }
  }

  Transaction* head = nullptr;
  string dbg_id;

  if (continuation_trans_ == nullptr) {
    while (!txq_.Empty()) {
      // we must check every iteration so that if the current transaction awakens
      // another transaction, the loop won't proceed further and will break, because we must run
      // the notified transaction before all other transactions in the queue can proceed.
      bool has_awaked_trans = blocking_controller_ && blocking_controller_->HasAwakedTransaction();
      if (has_awaked_trans)
        break;

      auto val = txq_.Front();
      head = absl::get<Transaction*>(val);

      // The fact that Tx is in the queue, already means that coordinator fiber will not progress,
      // hence here it's enough to test for run_count and check local_mask.
      bool is_armed = head->IsArmedInShard(sid);
      VLOG(2) << "Considering head " << head->DebugId() << " isarmed: " << is_armed;

      if (!is_armed)
        break;

      // It could be that head is processed and unblocks multi-hop transaction .
      // The transaction will schedule again and will arm another callback.
      // Then we will reach invalid state by running trans after this loop,
      // which is not what we want.
      // This function should not process 2 different callbacks for the same transaction.
      // Hence we make sure to reset trans if it has been processed via tx-queue.
      if (head == trans)
        trans = nullptr;
      TxId txid = head->txid();

      // committed_txid_ is strictly increasing when processed via TxQueue.
      DCHECK_LT(committed_txid_, txid);

      // We update committed_txid_ before calling RunInShard() to avoid cases where
      // a transaction stalls the execution with IO while another fiber queries this shard for
      // committed_txid_ (for example during the scheduling).
      committed_txid_ = txid;
      if (VLOG_IS_ON(2)) {
        dbg_id = head->DebugId();
      }

      bool keep = head->RunInShard(this);

      // We should not access head from this point since RunInShard callback decrements refcount.
      DLOG_IF(INFO, !dbg_id.empty()) << "RunHead " << dbg_id << ", keep " << keep;

      if (keep) {
        continuation_trans_ = head;
        break;
      }
    }       // while(!txq_.Empty())
  } else {  // if (continuation_trans_ == nullptr)
    DVLOG(1) << "Skipped TxQueue " << continuation_trans_;
  }

  // we need to run trans if it's OOO or when trans is blocked in this shard and should
  // be treated here as noop.
  bool should_run = trans_mask & (Transaction::OUT_OF_ORDER | Transaction::SUSPENDED_Q);

  // It may be that there are other transactions that touch those keys but they necessary ordered
  // after trans in the queue, hence it's safe to run trans out of order.
  if (trans && should_run) {
    DCHECK(trans != head);
    DCHECK(!trans->IsMulti());  // multi, global transactions can not be OOO.
    DCHECK(trans_mask & Transaction::ARMED);

    dbg_id.clear();

    if (VLOG_IS_ON(1)) {
      dbg_id = trans->DebugId();
    }
    ++stats_.ooo_runs;

    bool keep = trans->RunInShard(this);
    DLOG_IF(INFO, !dbg_id.empty()) << "Eager run " << sid << ", " << dbg_id << ", keep " << keep;
  }
}

void EngineShard::ShutdownMulti(Transaction* multi) {
  if (continuation_trans_ == multi) {
    continuation_trans_ = nullptr;
  }
}

#if 0
// There are several cases that contain proof of convergence for this shard:
// 1. txq_ empty - it means that anything that is goonna be scheduled will already be scheduled
//    with txid > notifyid.
// 2. committed_txid_ > notifyid - similarly, this shard can not affect the result with timestamp
//    notifyid.
// 3. committed_txid_ == notifyid, then if a transaction in progress (continuation_trans_ != NULL)
//    the this transaction can still affect the result, hence we require continuation_trans_ is null
//    which will point to converged result @notifyid. However, we never awake a transaction
//    when there is a multi-hop transaction in progress to avoid false positives.
//    Therefore, continuation_trans_ must always be null when calling this function.
// 4. Finally with committed_txid_ < notifyid.
//    we can check if the next in line (HeadScore) is after notifyid in that case we can also
//    conclude regarding the result convergence for this shard.
//
bool EngineShard::HasResultConverged(TxId notifyid) const {
  CHECK(continuation_trans_ == nullptr);

  if (committed_txid_ >= notifyid)
    return true;

  // This could happen if a single lpush (not in transaction) woke multi-shard blpop.
  DVLOG(1) << "HasResultConverged: cmtxid - " << committed_txid_ << " vs " << notifyid;

  // We must check for txq head - it's not an optimization - we need it for correctness.
  // If a multi-transaction has been scheduled and it does not have any presence in
  // this shard (no actual keys) and we won't check for it HasResultConverged will
  // return false. The blocked transaction will wait for this shard to progress and
  // will also block other shards from progressing (where it has been notified).
  // If this multi-transaction has presence in those shards, it won't progress there as well.
  // Therefore, we will get a deadlock. By checking txid of the head we will avoid this situation:
  // if the head.txid is after notifyid then this shard obviously converged.
  // if the head.txid <= notifyid that transaction will be able to progress in other shards.
  // and we must wait for it to finish.
  return txq_.Empty() || txq_.HeadScore() > notifyid;
}
#endif

void EngineShard::Heartbeat() {
  CacheStats();
  constexpr double kTtlDeleteLimit = 200;
  constexpr double kRedLimitFactor = 0.1;

  uint32_t traversed = GetMovingSum6(TTL_TRAVERSE);
  uint32_t deleted = GetMovingSum6(TTL_DELETE);
  unsigned ttl_delete_target = 5;

  if (deleted > 10) {
    // deleted should be <= traversed.
    // hence we map our delete/traversed ratio into a range [0, kTtlDeleteLimit).
    // The higher t
    ttl_delete_target = kTtlDeleteLimit * double(deleted) / (double(traversed) + 10);
  }

  ssize_t redline = (max_memory_limit * kRedLimitFactor) / shard_set->size();
  DbContext db_cntx;
  db_cntx.time_now_ms = GetCurrentTimeMs();

  for (unsigned i = 0; i < db_slice_.db_array_size(); ++i) {
    if (!db_slice_.IsDbValid(i))
      continue;

    db_cntx.db_index = i;
    auto [pt, expt] = db_slice_.GetTables(i);
    if (expt->size() > pt->size() / 4) {
      DbSlice::DeleteExpiredStats stats = db_slice_.DeleteExpiredStep(db_cntx, ttl_delete_target);

      counter_[TTL_TRAVERSE].IncBy(stats.traversed);
      counter_[TTL_DELETE].IncBy(stats.deleted);
    }

    // if our budget is below the limit
    if (db_slice_.memory_budget() < redline) {
      db_slice_.FreeMemWithEvictionStep(i, redline - db_slice_.memory_budget());
    }
  }
}

void EngineShard::CacheStats() {
  // mi_heap_visit_blocks(tlh, false /* visit all blocks*/, visit_cb, &sum);
  mi_stats_merge();

  // Used memory for this shard.
  size_t used_mem = UsedMemory();
  cached_stats[db_slice_.shard_id()].used_memory.store(used_mem, memory_order_relaxed);
  ssize_t free_mem = max_memory_limit - used_mem_current.load(memory_order_relaxed);

  size_t entries = 0;
  size_t table_memory = 0;
  for (size_t i = 0; i < db_slice_.db_array_size(); ++i) {
    DbTable* table = db_slice_.GetDBTable(i);
    if (table) {
      entries += table->prime.size();
      table_memory += (table->prime.mem_usage() + table->expire.mem_usage());
    }
  }
  size_t obj_memory = table_memory <= used_mem ? used_mem - table_memory : 0;

  size_t bytes_per_obj = entries > 0 ? obj_memory / entries : 0;
  db_slice_.SetCachedParams(free_mem / shard_set->size(), bytes_per_obj);
}

size_t EngineShard::UsedMemory() const {
  return mi_resource_.used() + zmalloc_used_memory_tl + SmallString::UsedThreadLocal();
}

void EngineShard::AddBlocked(Transaction* trans) {
  if (!blocking_controller_) {
    blocking_controller_.reset(new BlockingController(this));
  }
  blocking_controller_->AddWatched(trans);
}

void EngineShard::TEST_EnableHeartbeat() {
  auto* pb = ProactorBase::me();
  periodic_task_ = pb->AddPeriodic(1, [this] { Heartbeat(); });
}

/**


  _____                _               ____   _                      _  ____         _
 | ____| _ __    __ _ (_) _ __    ___ / ___| | |__    __ _  _ __  __| |/ ___|   ___ | |_
 |  _|  | '_ \  / _` || || '_ \  / _ \\___ \ | '_ \  / _` || '__|/ _` |\___ \  / _ \| __|
 | |___ | | | || (_| || || | | ||  __/ ___) || | | || (_| || |  | (_| | ___) ||  __/| |_
 |_____||_| |_| \__, ||_||_| |_| \___||____/ |_| |_| \__,_||_|   \__,_||____/  \___| \__|
                |___/

 */

void EngineShardSet::Init(uint32_t sz, bool update_db_time) {
  CHECK_EQ(0u, size());
  cached_stats.resize(sz);
  shard_queue_.resize(sz);

  pp_->AwaitFiberOnAll([&](uint32_t index, ProactorBase* pb) {
    if (index < shard_queue_.size()) {
      InitThreadLocal(pb, update_db_time);
    }
  });
}

void EngineShardSet::Shutdown() {
  RunBlockingInParallel([](EngineShard*) { EngineShard::DestroyThreadLocal(); });
}

void EngineShardSet::InitThreadLocal(ProactorBase* pb, bool update_db_time) {
  EngineShard::InitThreadLocal(pb, update_db_time);
  EngineShard* es = EngineShard::tlocal();
  shard_queue_[es->shard_id()] = es->GetFiberQueue();
}

const vector<EngineShardSet::CachedStats>& EngineShardSet::GetCachedStats() {
  return cached_stats;
}

void EngineShardSet::TEST_EnableHeartBeat() {
  RunBriefInParallel([](EngineShard* shard) { shard->TEST_EnableHeartbeat(); });
}

void EngineShardSet::TEST_EnableCacheMode() {
  RunBriefInParallel([](EngineShard* shard) { shard->db_slice().TEST_EnableCacheMode(); });
}

}  // namespace dfly
