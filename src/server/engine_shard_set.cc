// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/engine_shard_set.h"

extern "C" {
#include "redis/object.h"
#include "redis/zmalloc.h"
}

#include "base/logging.h"
#include "server/blocking_controller.h"
#include "server/tiered_storage.h"
#include "server/transaction.h"
#include "util/fiber_sched_algo.h"
#include "util/varz.h"

DEFINE_string(backing_prefix, "", "");

namespace dfly {

using namespace std;
using namespace util;
namespace this_fiber = ::boost::this_fiber;
namespace fibers = ::boost::fibers;

namespace {

vector<EngineShardSet::CachedStats> cached_stats;  // initialized in EngineShardSet::Init

}  // namespace

thread_local EngineShard* EngineShard::shard_ = nullptr;
constexpr size_t kQueueLen = 64;

EngineShard::Stats& EngineShard::Stats::operator+=(const EngineShard::Stats& o) {
  ooo_runs += o.ooo_runs;
  quick_runs += o.quick_runs;

  return *this;
}

EngineShard::EngineShard(util::ProactorBase* pb, bool update_db_time, mi_heap_t* heap)
    : queue_(kQueueLen), txq_([](const Transaction* t) { return t->txid(); }), mi_resource_(heap),
      db_slice_(pb->GetIndex(), this) {
  fiber_q_ = fibers::fiber([this, index = pb->GetIndex()] {
    this_fiber::properties<FiberProps>().set_name(absl::StrCat("shard_queue", index));
    queue_.Run();
  });

  if (update_db_time) {
    constexpr uint32_t kClockCycleMs = 1;

    periodic_task_ = pb->AddPeriodic(kClockCycleMs, [this] { Heartbeat(); });
  }

  tmp_str1 = sdsempty();
  tmp_str2 = sdsempty();
  db_slice_.UpdateExpireBase(absl::GetCurrentTimeNanos() / 1000000, 0);
}

EngineShard::~EngineShard() {
  sdsfree(tmp_str1);
  sdsfree(tmp_str2);
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
}

void EngineShard::InitThreadLocal(ProactorBase* pb, bool update_db_time) {
  CHECK(shard_ == nullptr) << pb->GetIndex();
  CHECK(mi_heap_get_backing() == mi_heap_get_default());

  mi_heap_t* tlh = mi_heap_new();
  init_zmalloc_threadlocal(tlh);

  void* ptr = mi_heap_malloc_aligned(tlh, sizeof(EngineShard), alignof(EngineShard));
  shard_ = new (ptr) EngineShard(pb, update_db_time, tlh);

  CompactObj::InitThreadLocal(shard_->memory_resource());
  SmallString::InitThreadLocal(tlh);

  if (!FLAGS_backing_prefix.empty()) {
    string fn =
        absl::StrCat(FLAGS_backing_prefix, "-", absl::Dec(pb->GetIndex(), absl::kZeroPad4), ".ssd");

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
  DVLOG(1) << "PollExecution " << context << " " << (trans ? trans->DebugId() : "");
  ShardId sid = shard_id();

  uint16_t trans_mask = trans ? trans->GetLocalMask(sid) : 0;
  if (trans_mask & Transaction::AWAKED_Q) {
    DCHECK(continuation_trans_ == nullptr);

    CHECK_EQ(committed_txid_, trans->notify_txid()) << "TBD";
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

  bool has_awaked_trans = blocking_controller_ && blocking_controller_->HasAwakedTransaction();
  Transaction* head = nullptr;
  string dbg_id;

  if (continuation_trans_ == nullptr && !has_awaked_trans) {
    while (!txq_.Empty()) {
      auto val = txq_.Front();
      head = absl::get<Transaction*>(val);

      // The fact that Tx is in the queue, already means that coordinator fiber will not progress,
      // hence here it's enough to test for run_count and check local_mask.
      bool is_armed = head->IsArmedInShard(sid);
      DVLOG(2) << "Considering head " << head->DebugId() << " isarmed: " << is_armed;

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
  } else {  // if (continuation_trans_ == nullptr && !has_awaked_trans)
    DVLOG(1) << "Skipped TxQueue " << continuation_trans_ << " " << has_awaked_trans;
  }

  // For SUSPENDED_Q - if transaction has not been notified, it will still be
  // in the watch queue. We need to unlock an Execute by running a noop.
  if (trans_mask & Transaction::SUSPENDED_Q) {
    // This case happens when some other shard notified the transaction and now it
    // runs FindFirst on all shards.
    // TxId notify_txid = trans->notify_txid();
    // DCHECK(HasResultConverged(notify_txid));
    trans->RunNoop(this);
    return;
  }

  // If trans is out of order, i.e. locks keys that previous transactions have not locked.
  // It may be that there are other transactions that touch those keys but they necessary ordered
  // after trans in the queue, hence it's safe to run trans out of order.
  if (trans && (trans_mask & Transaction::OUT_OF_ORDER)) {
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

    // Should be enforced via Schedule(). TODO: to remove the check once the code is mature.
    CHECK(!keep) << "multi-hop transactions can not be OOO.";
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
  // absl::GetCurrentTimeNanos() returns current time since the Unix Epoch.
  db_slice().UpdateExpireClock(absl::GetCurrentTimeNanos() / 1000000);

  if (task_iters_++ % 8 == 0) {
    CacheStats();

    for (unsigned i = 0; i < db_slice_.db_array_size(); ++i) {
      if (db_slice_.IsDbValid(i)) {
        auto [pt, expt] = db_slice_.GetTables(i);
        if (expt->size() > pt->size() / 4) {
          auto [trav, del] = db_slice_.DeleteExpired(i);

          counter_[TTL_TRAVERSE].IncBy(trav);
          counter_[TTL_DELETE].IncBy(del);
        }
      }
    }
  }
}

void EngineShard::CacheStats() {
#if 0
  mi_heap_t* tlh = mi_resource_.heap();
  struct Sum {
    size_t used = 0;
    size_t comitted = 0;
  } sum;

  auto visit_cb = [](const mi_heap_t* heap, const mi_heap_area_t* area, void* block,
                     size_t block_size, void* arg) -> bool {
    DCHECK(!block);
    Sum* sum = (Sum*)arg;

    // mimalloc mistakenly exports used in blocks instead of bytes.
    sum->used += block_size * area->used;
    sum->comitted += area->committed;

    DVLOG(1) << "block_size " << block_size << "/" << area->block_size << ", reserved "
             << area->reserved << " comitted " << area->committed << " used: " << area->used;
    return true;  // continue iteration
  };
#endif
  // mi_heap_visit_blocks(tlh, false /* visit all blocks*/, visit_cb, &sum);
  mi_stats_merge();

  size_t used_mem = UsedMemory();
  cached_stats[db_slice_.shard_id()].used_memory.store(used_mem, memory_order_relaxed);
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

/**


  _____                _               ____   _                      _  ____         _
 | ____| _ __    __ _ (_) _ __    ___ / ___| | |__    __ _  _ __  __| |/ ___|   ___ | |_
 |  _|  | '_ \  / _` || || '_ \  / _ \\___ \ | '_ \  / _` || '__|/ _` |\___ \  / _ \| __|
 | |___ | | | || (_| || || | | ||  __/ ___) || | | || (_| || |  | (_| | ___) ||  __/| |_
 |_____||_| |_| \__, ||_||_| |_| \___||____/ |_| |_| \__,_||_|   \__,_||____/  \___| \__|
                |___/

 */

void EngineShardSet::Init(uint32_t sz) {
  CHECK_EQ(0u, size());
  cached_stats.resize(sz);
  shard_queue_.resize(sz);
}

void EngineShardSet::InitThreadLocal(ProactorBase* pb, bool update_db_time) {
  EngineShard::InitThreadLocal(pb, update_db_time);
  EngineShard* es = EngineShard::tlocal();
  shard_queue_[es->shard_id()] = es->GetFiberQueue();
}

const vector<EngineShardSet::CachedStats>& EngineShardSet::GetCachedStats() {
  return cached_stats;
}

}  // namespace dfly
