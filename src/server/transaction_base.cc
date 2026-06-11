// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/transaction_base.h"

#include <absl/strings/str_cat.h>

#include <boost/smart_ptr/intrusive_ptr.hpp>

#include "base/flags.h"
#include "base/logging.h"
#include "server/command_registry.h"
#include "server/engine_shard_set.h"
#include "server/namespaces.h"
#include "server/server_state.h"

ABSL_DECLARE_FLAG(uint32_t, tx_queue_warning_len);

namespace dfly {

using namespace std;

atomic_uint64_t op_seq{1};

TransactionBase::TransactionBase(const CommandId* cid) : cid_{cid} {
  InitTxTime();
}

TransactionBase::~TransactionBase() {
}

void TransactionBase::InitTxTime() {
  time_now_ms_ = GetCurrentTimeMs();
}

string_view TransactionBase::Name() const {
  return cid_ ? cid_->name() : "null-command";
}

IntentLock::Mode TransactionBase::LockMode() const {
  return cid_->IsReadOnly() ? IntentLock::SHARED : IntentLock::EXCLUSIVE;
}

ShardId TransactionBase::GetUniqueShard() const {
  DCHECK_EQ(GetUniqueShardCnt(), 1U);
  return unique_shard_id_;
}

DbSlice& TransactionBase::GetDbSlice(ShardId shard_id) const {
  CHECK(namespace_ != nullptr);
  return namespace_->GetDbSlice(shard_id);
}

bool TransactionBase::CanRunInlined() const {
  auto* ss = ServerState::tlocal();
  auto* es = EngineShard::tlocal();

  if (unique_shard_cnt_ == 1 && unique_shard_id_ == ss->thread_index() &&
      ss->AllowInlineScheduling() && !IsGlobal() && es->running_tx() == nullptr) {
    ss->stats.tx_inline_runs++;
    return true;
  }
  return false;
}

void TransactionBase::FinishHop() {
  boost::intrusive_ptr<TransactionBase> guard(this);
  run_barrier_.Dec();
}

void AnalyzeTxQueue(const EngineShard* shard, const TxQueue* txq) {
  unsigned q_limit = absl::GetFlag(FLAGS_tx_queue_warning_len);
  if (txq->size() > q_limit) {
    static thread_local time_t last_log_time = 0;
    time_t now = time(nullptr);
    if (now >= last_log_time + 10) {
      last_log_time = now;
      EngineShard::TxQueueInfo info = shard->AnalyzeTxQueue();
      string msg = absl::StrCat("TxQueue is too long. ", info.Format());
      absl::StrAppend(&msg, "poll_executions:", shard->stats().poll_execution_total);

      const TransactionBase* cont_tx = shard->GetContTx();
      if (cont_tx) {
        absl::StrAppend(&msg, " continuation_tx: ", cont_tx->DebugId(shard->shard_id()), " ",
                        cont_tx->DEBUG_IsArmedInShard(shard->shard_id()) ? " armed" : "");
      }

      LOG(WARNING) << msg;
    }
  }
}

void RecordTxScheduleStats(const TransactionBase* tx) {
  auto* ss = ServerState::tlocal();
  ++(tx->IsGlobal() ? ss->stats.tx_global_cnt : ss->stats.tx_normal_cnt);
  ++ss->stats.tx_width_freq_arr[tx->GetUniqueShardCnt() - 1];
}

}  // namespace dfly
