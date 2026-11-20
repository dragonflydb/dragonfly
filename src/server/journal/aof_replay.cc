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
#include "util/fibers/fibers.h"
#include "util/proactor_pool.h"

namespace dfly {

using namespace std;

namespace {

// Replays chain sid of a log written by source_count shards, from its cut, through the applier.
// Commands are dispatched normally, so each key lands on the shard that owns it now. With resume,
// also repairs the chain so its shard's log continues in it.
error_code ReplayChain(string_view dir, ShardId sid, uint32_t source_count, AofManifest::Cut cut,
                       Service* service, shared_ptr<MultiShardExecution> mse, AofResume* resume,
                       TxId* max_txid) {
  AofChainReader chain(string(dir), sid, source_count);
  RETURN_ON_ERR(chain.Open(cut.seq));

  ExecutionState cntx;
  JournalReader reader{&chain, /*dbid=*/0};
  TransactionReader tx_reader;
  JournalApplier applier(service, std::move(mse));
  TransactionData tx;

  // Records read, compared with the block headers below.
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

  // The records parsed must match the block headers, counted from the cut. A chain that does not
  // start at the cut fails it too. A record cut at the end is in neither count.
  uint64_t logged = chain.LastLsn() ? chain.LastLsn() + 1 - cut.lsn : 0;
  if (records != logged) {
    LOG(ERROR) << "AOF shard " << sid << ": read " << records << " records, the block headers say "
               << logged << " from the cut at lsn " << cut.lsn;
    return make_error_code(errc::state_not_recoverable);
  }

  // A chain that is not resumed needs no repair: a checkpoint replaces it.
  if (!resume)
    return {};

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
  // An empty chain resumes at its cut.
  *resume = {chain.NextSeq(), max(chain.LastLsn() + 1, cut.lsn), chain.LogSize()};
  return {};
}
}  // namespace

error_code ReplayAof(string_view dir, const AofManifest& manifest, Service* service,
                     vector<AofResume>* resume) {
  // The log's shard count: one chain per source shard, whatever the shard count is now.
  uint32_t source_count = manifest.cuts.size();
  bool same_shards = source_count == shard_set->size();
  auto mse = make_shared<MultiShardExecution>(source_count);
  vector<error_code> errors(source_count);
  vector<TxId> max_txids(source_count, 0);
  resume->clear();
  if (same_shards)
    resume->resize(source_count);

  // One fiber per chain, chain i on proactor i % pool size, as replica flows: with the same shard
  // count, that is shard i's thread.
  auto replay = [&](ShardId sid) {
    errors[sid] = ReplayChain(dir, sid, source_count, manifest.cuts[sid], service, mse,
                              same_shards ? &(*resume)[sid] : nullptr, &max_txids[sid]);

    // A chain that ended, cleanly or torn, never reaches another barrier: stop counting it, so
    // longer chains can run a global command it lacks.
    // TODO: that global command then exists only in the longer chains, and resuming the logs as
    // they are breaks a later restart. For example, with shards A and B:
    //   Run 1: FLUSHALL G1 reaches B's log but not A's (A's log just ends before it). Crash.
    //   Restart 1: A ends and leaves; B runs G1 alone. Both logs resume.
    //   Run 2: new writes, then FLUSHALL G2 is logged on both shards. Crash.
    //   Restart 2: A's first global command is G2, B's is G1. Their txids differ, so the round
    //   is cancelled (kOutOfOrder) and startup fails until the files are fixed by hand. Without
    //   G2, A would end and leave, and B would run G1 alone after run 2's writes, erasing them.
    // Fix: checkpoint in LOADING after restart 1, see "Resuming the log" in docs/aof.md.
    if (!errors[sid]) {
      mse->RemoveFlow();
    } else {
      LOG(ERROR) << "AOF replay failed on shard " << sid << ": " << errors[sid].message();

      // The load fails; release the shards waiting at a barrier.
      mse->CancelAllBlockingEntities();
    }
  };
  util::ProactorPool* pool = shard_set->pool();
  vector<util::fb2::Fiber> fibers;
  for (ShardId sid = 0; sid < source_count; ++sid) {
    fibers.push_back(
        pool->at(sid % pool->size())->LaunchFiber("aof_replay", [&replay, sid] { replay(sid); }));
  }
  for (util::fb2::Fiber& fiber : fibers)
    fiber.Join();

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
