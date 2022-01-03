// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/engine_shard_set.h"

#include "base/logging.h"
#include "server/transaction.h"
#include "util/fiber_sched_algo.h"
#include "util/varz.h"

namespace dfly {

using namespace std;
using namespace util;
namespace this_fiber = ::boost::this_fiber;
namespace fibers = ::boost::fibers;

thread_local EngineShard* EngineShard::shard_ = nullptr;
constexpr size_t kQueueLen = 64;

EngineShard::EngineShard(util::ProactorBase* pb, bool update_db_time)
    : queue_(kQueueLen), txq_([](const Transaction* t) { return t->txid(); }),
      db_slice_(pb->GetIndex(), this) {
  fiber_q_ = fibers::fiber([this, index = pb->GetIndex()] {
    this_fiber::properties<FiberProps>().set_name(absl::StrCat("shard_queue", index));
    queue_.Run();
  });

  if (update_db_time) {
    periodic_task_ = pb->AddPeriodic(1, [] {
      auto* shard = EngineShard::tlocal();
      DCHECK(shard);
      // absl::GetCurrentTimeNanos() returns current time since the Unix Epoch.
      shard->db_slice().UpdateExpireClock(absl::GetCurrentTimeNanos() / 1000000);
    });
  }

  tmp_str = sdsempty();
}

EngineShard::~EngineShard() {
  queue_.Shutdown();
  fiber_q_.join();
  sdsfree(tmp_str);
  if (periodic_task_) {
    ProactorBase::me()->CancelPeriodic(periodic_task_);
  }
}

void EngineShard::InitThreadLocal(ProactorBase* pb, bool update_db_time) {
  CHECK(shard_ == nullptr) << pb->GetIndex();
  shard_ = new EngineShard(pb, update_db_time);
}

void EngineShard::DestroyThreadLocal() {
  if (!shard_)
    return;

  uint32_t index = shard_->db_slice_.shard_id();
  delete shard_;
  shard_ = nullptr;

  VLOG(1) << "Shard reset " << index;
}

// Is called by Transaction::ExecuteAsync in order to run transaction tasks.
// Only runs in its own thread.
void EngineShard::PollExecution(Transaction* trans) {
  DVLOG(1) << "PollExecution " << (trans ? trans->DebugId() : "");
  ShardId sid = shard_id();

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

    if (continuation_trans_) {
      // Once we start executing transaction we do not continue until it's finished.
      // This preserves atomicity property of multi-hop transactions.
      return;
    }
  }

  DCHECK(!continuation_trans_);

  Transaction* head = nullptr;
  string dbg_id;
  while (!txq_.Empty()) {
    auto val = txq_.Front();
    head = absl::get<Transaction*>(val);

    // The fact that Tx is in the queue, already means that coordinator fiber will not progress,
    // hence here it's enough to test for run_count and check local_mask.
    bool is_armed = head->IsArmedInShard(sid);
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

    DCHECK_LT(committed_txid_, txid);

    // We update committed_txid_ before calling RunInShard() to avoid cases where
    // a transaction stalls the execution with IO while another fiber queries this shard for
    // committed_txid_ (for example during the scheduling).
    committed_txid_ = txid;
    if (VLOG_IS_ON(2)) {
      dbg_id = head->DebugId();
    }
    bool keep = head->RunInShard(this);
    DCHECK(head == absl::get<Transaction*>(txq_.Front()));

    // We should not access head from this point since RunInShard callback decrements refcount.
    DLOG_IF(INFO, !dbg_id.empty()) << "RunHead " << dbg_id << ", keep " << keep;
    txq_.PopFront();

    if (keep) {
      continuation_trans_ = head;
      break;
    }
  }

  if (!trans)
    return;

  uint16_t local_mask = trans->GetLocalMask(sid);

  // If trans is out of order, i.e. locks keys that previous transactions have not locked.
  // It may be that there are other transactions that touch those keys but they necessary ordered
  // after trans in the queue, hence it's safe to run trans out of order.
  if (local_mask & Transaction::OUT_OF_ORDER) {
    DCHECK(trans != head);

    dbg_id.clear();

    uint32_t pos = trans->TxQueuePos(sid);
    if (VLOG_IS_ON(1)) {
      dbg_id = trans->DebugId();
    }

    bool keep = trans->RunInShard(this);  // resets TxQueuePos, this is why we get it before.
    DLOG_IF(INFO, !dbg_id.empty()) << "Eager run " << sid << ", " << dbg_id << ", keep " << keep;

    // Should be enforced via Schedule(). TODO: to remove the check once the code is mature.
    CHECK(!keep) << "multi-hop transactions can not be OOO.";
    txq_.Remove(pos);
  }
}

void EngineShardSet::Init(uint32_t sz) {
  CHECK_EQ(0u, size());

  shard_queue_.resize(sz);
}

void EngineShardSet::InitThreadLocal(ProactorBase* pb, bool update_db_time) {
  EngineShard::InitThreadLocal(pb, update_db_time);
  EngineShard* es = EngineShard::tlocal();
  shard_queue_[es->shard_id()] = es->GetFiberQueue();
}

}  // namespace dfly
