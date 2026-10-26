// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/aof_replay.h"

#include <algorithm>

#include "base/logging.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/journal/aof_chain_reader.h"
#include "server/journal/journal_applier.h"
#include "server/journal/serializer.h"
#include "server/journal/tx_executor.h"
#include "server/transaction.h"

namespace dfly {

using namespace std;

namespace {

// Replays one shard's chain through the applier, then repairs it. Runs on the shard's thread.
error_code ReplayChain(string_view dir, ShardId sid, Service* service,
                       shared_ptr<MultiShardExecution> mse, AofResume* resume, TxId* max_txid) {
  AofChainReader chain(string(dir), sid, shard_set->size());

  // kNoSegments is a normal first start.
  if (error_code ec = chain.Open(); ec)
    return ec;

  ExecutionState cntx;
  JournalReader reader{&chain, /*dbid=*/0};
  TransactionReader tx_reader;
  JournalApplier applier(service, std::move(mse));
  TransactionData tx;

  // Records read, compared with LastLsn() below.
  uint64_t records = 0;
  while (tx_reader.NextTxData(&reader, &cntx, &tx)) {
    // Before the PING skip: a PING has an LSN too.
    ++records;
    if (tx.opcode == journal::Op::PING)
      continue;
    *max_txid = max(*max_txid, tx.txid);
    if (!applier.Apply(std::move(tx), &cntx)) {
      LOG(ERROR) << "Failed to apply an AOF record of shard " << sid;
      return make_error_code(errc::state_not_recoverable);
    }
  }

  // JournalReader reports running out of input as an error, so the loop always ends with one. It is
  // the normal end only if the chain reached the end of its log; otherwise a read or parse failed.
  if (!chain.AtEnd())
    return cntx.GetError();

  // The records parsed must match the block headers. A record cut at the end is in neither count.
  // TODO: start from the cut's LSN, not 1.
  if (records != chain.LastLsn()) {
    LOG(ERROR) << "AOF shard " << sid << ": read " << records << " records but the log ends at lsn "
               << chain.LastLsn();
    return make_error_code(errc::state_not_recoverable);
  }

  // TODO: resume after a record cut at the end. Only a crash leaves one: a record split across
  // blocks whose first part was written and the rest was not; rotations, shutdowns and heartbeats
  // never split a record across segments. Repair() keeps that block, since rewriting it could tear
  // the complete records it holds. Instead, the resumed segment's header must mark the record
  // abandoned, and replay must drop its bytes at that boundary. Otherwise the next replay sees a
  // new record where it expects the continuation, and stops there.
  if (chain.EndsMidRecord()) {
    LOG(ERROR) << "AOF shard " << sid << ": the log ends inside a record after lsn "
               << chain.LastLsn() << ", resuming after it is not supported yet";
    return make_error_code(errc::state_not_recoverable);
  }

  RETURN_ON_ERR(chain.Repair());
  *resume = {chain.NextSeq(), chain.LastLsn() + 1};
  return {};
}
}  // namespace

error_code ReplayAof(string_view dir, Service* service, vector<AofResume>* resume) {
  uint32_t shard_count = shard_set->size();
  auto mse = make_shared<MultiShardExecution>(shard_count);
  vector<error_code> errors(shard_count);
  vector<TxId> max_txids(shard_count, 0);
  resume->assign(shard_count, {});

  shard_set->RunBlockingInParallel([&](EngineShard* shard) {
    ShardId sid = shard->shard_id();
    errors[sid] = ReplayChain(dir, sid, service, mse, &(*resume)[sid], &max_txids[sid]);

    // A chain that ended, cleanly or torn, or has no segments never reaches another barrier: stop
    // counting it, so longer chains can run a global command it lacks.
    // TODO: that global command then exists only in the longer chains, and resuming the logs as
    // they are breaks a later restart. For example, with shards A and B:
    //   Run 1: FLUSHALL G1 reaches B's log but not A's (A's log just ends before it). Crash.
    //   Restart 1: A ends and leaves; B runs G1 alone. Both logs resume.
    //   Run 2: new writes, then FLUSHALL G2 is logged on both shards. Crash.
    //   Restart 2: A's first global command is G2, B's is G1. Their txids differ, so the round
    //   is cancelled (kOutOfOrder) and startup fails until the files are fixed by hand. Without
    //   G2, A would end and leave, and B would run G1 alone after run 2's writes, erasing them.
    // Fix: checkpoint in LOADING after restart 1, see "Resuming the log" in docs/aof.md.
    if (!errors[sid] || errors[sid] == AofReadError::kNoSegments) {
      mse->RemoveFlow();
    } else {
      LOG(ERROR) << "AOF replay failed on shard " << sid << ": " << errors[sid].message();

      // The load fails; release the shards waiting at a barrier.
      mse->CancelAllBlockingEntities();
    }
  });

  size_t empty = ranges::count(errors, AofReadError::kNoSegments);

  // TODO: the manifest will tell a first start from lost segments.
  if (empty == shard_count)
    return {};

  // Includes kNoSegments when only some chains are empty.
  for (const error_code& ec : errors) {
    if (ec)
      return ec;
  }

  // MultiShardExecution checks that shards meet the same global command by comparing txids.
  // Txids must not repeat across restarts, or shards that diverged at a crash pass that check: in
  // the example above, G2 could get G1's txid, and restart 2 would run them as one command.
  // Multi-shard tail atomicity, planned in docs/aof.md, also needs txids unique across restarts.
  AdvanceTxIdPast(ranges::max(max_txids));
  return {};
}

}  // namespace dfly
