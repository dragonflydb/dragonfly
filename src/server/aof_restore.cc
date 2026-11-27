// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/aof_restore.h"

#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/strip.h>
#include <unistd.h>

#include <algorithm>
#include <filesystem>

#include "base/logging.h"
#include "io/file_util.h"
#include "server/db_slice.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/journal/aof_chain_reader.h"
#include "server/journal/aof_segment_writer.h"
#include "server/journal/aof_streamer.h"
#include "server/journal/journal_applier.h"
#include "server/journal/serializer.h"
#include "server/journal/tx_executor.h"
#include "server/namespaces.h"
#include "server/server_state.h"
#include "server/transaction.h"
#include "util/fibers/proactor_base.h"

namespace dfly {

using namespace std;

namespace {

#ifdef __linux__
// For fatal startup errors: exit() would run destructors while server fibers still run.
[[noreturn]] void ErrorAndExit(string_view reason) {
  LOG(ERROR) << reason;
  base::FlushLogs();
  std::_Exit(EXIT_FAILURE);
}

// Starts every shard's AofStreamer where its log resumes. False if any failed.
bool StartStreamers(string_view dir, const vector<AofResume>& resume) {
  atomic_bool failed = false;
  shard_set->RunBlockingInParallel([&](EngineShard* shard) {
    ShardId sid = shard->shard_id();
    auto streamer = make_unique<AofStreamer>(string(dir), sid, shard_set->size());
    const AofResume& r = resume[sid];
    if (error_code ec = streamer->Start(r.seq, r.next_lsn, r.log_size); ec) {
      LOG(ERROR) << "Failed to start AOF on shard " << sid << ": " << ec.message();
      failed = true;
      return;
    }
    shard->set_aof_streamer(std::move(streamer));
  });
  return !failed;
}

// Turns expiry and eviction on or off on every shard.
void AllowExpiryOnAllShards(bool allowed) {
  shard_set->RunBriefInParallel([allowed](EngineShard* shard) {
    namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id()).SetExpireAllowed(allowed);
  });
}

// Loads the base named by the manifest, or exits. False if a shutdown stopped it.
bool LoadAofBase(string_view dir, const string& base_path, const AofLoadFn& load) {
  string path = absl::StrCat(dir, "/", base_path);

  // The user may have deleted or moved a dump that is the base.
  if (access(path.c_str(), F_OK) != 0)
    ErrorAndExit(absl::StrCat("the AOF base ", path, " is missing"));
  if (GenericError ec = load(path); ec) {
    // A shutdown rejects the load's commands: it stops the restore, it is no failure.
    if (ServerState::tlocal()->gstate() == GlobalState::SHUTTING_DOWN)
      return false;
    ErrorAndExit(absl::StrCat("failed to load the AOF base ", path, ": ", ec.Format()));
  }
  return true;
}

// Starts the streamers paused, in new segments past the existing ones, so every file stays until a
// new base commits.
void StartPausedStreamers(string_view dir, const AofManifest& manifest) {
  io::Result<vector<string>> files = ListAofFiles(dir);
  if (!files)
    ErrorAndExit(absl::StrCat("failed to list AOF files: ", files.error().message()));
  vector<AofResume> resume(shard_set->size());
  for (ShardId sid = 0; sid < resume.size() && sid < manifest.cuts.size(); ++sid)
    resume[sid].seq = manifest.cuts[sid].seq;
  for (const string& path : *files) {
    string_view name = string_view(path).substr(path.rfind('/') + 1);
    if (!absl::ConsumeSuffix(&name, ".tmp"))
      absl::ConsumeSuffix(&name, ".discarded");
    uint32_t sid;
    uint64_t seq;
    if (AofSegmentWriter::ParseSegmentName(name, &sid, &seq) && sid < resume.size())
      resume[sid].seq = max(resume[sid].seq, seq + 1);
  }
  if (!StartStreamers(dir, resume))
    ErrorAndExit("failed to open AOF segments");
  shard_set->RunBriefInParallel([](EngineShard* shard) { shard->aof_streamer()->Pause(); });
}

void ResumeRebasing(string_view dir, const AofManifest& manifest) {
  LOG(WARNING) << "AOF manifest is rebasing: a full sync was interrupted. Starting empty; the old "
                  "AOF files stay until a new base commits";
  StartPausedStreamers(dir, manifest);
}
#endif

}  // namespace

#ifdef __linux__
// Bytes of the DFS file set whose summary is base_path: <prefix>summary.dfs and every
// <prefix>NNNN.dfs, listed like FileSnapshotStorage::ListShardFiles(), since an adopted dump may
// come from a server with more shards. Exact names, so other dumps sharing the prefix don't count.
size_t AofBaseSize(string_view dir, string_view base_path) {
  string_view prefix = base_path;
  absl::ConsumeSuffix(&prefix, "summary.dfs");
  size_t size = 0;
  error_code ec;
  uintmax_t summary_size = filesystem::file_size(filesystem::path(dir) / base_path, ec);
  if (!ec)
    size += summary_size;

  io::Result<io::StatShortVec> files = io::StatFiles(absl::StrCat(dir, "/", prefix, "????.dfs"));
  if (!files)
    return size;
  for (const io::StatShort& file : *files) {
    string_view name = string_view(file.name).substr(file.name.rfind('/') + 1);
    if (absl::ConsumePrefix(&name, prefix) && absl::ConsumeSuffix(&name, ".dfs") &&
        ranges::all_of(name, absl::ascii_isdigit)) {
      size += file.size;
    }
  }
  return size;
}

#else
size_t AofBaseSize(string_view dir, string_view base_path) {
  return 0;
}
#endif

void AofRestore::Start(util::fb2::ProactorBase* pb, string dir, AofStartPlan plan, Service* service,
                       AofLoadFn load, DoneCb on_done) {
  dir_ = std::move(dir);
  plan_ = std::move(plan);
  service_ = service;
  load_ = std::move(load);
  on_done_ = std::move(on_done);
  restore_done_.Start(1);
  restore_fb_ = pb->LaunchFiber("aof_load", [this] {
    on_done_(Run());
    restore_done_.Dec();
  });
}

bool AofRestore::Running() const {
  return !restore_done_.IsCompleted();
}

void AofRestore::Cancel() {
  cancel_.store(true, memory_order_relaxed);
  restore_done_.Wait();
}

void AofRestore::Join() {
  restore_fb_.JoinIfNeeded();
}

optional<AofBase> AofRestore::Run() {
#ifdef __linux__
  optional<AofBase> base;
  switch (plan_.kind) {
    case AofStartPlan::Kind::kBootstrap:
      if (optional<string> path = Bootstrap(); path)
        base = AofBase{.file = std::move(*path)};
      break;
    case AofStartPlan::Kind::kRebasing:
      ResumeRebasing(dir_, plan_.manifest);
      base = AofBase{.rebasing = true};
      break;
    case AofStartPlan::Kind::kResume:
      base = Resume();
      break;
  }
  if (!base) {
    LOG(INFO) << "AOF restore stopped by shutdown";
    return nullopt;
  }
  if (!base->file.empty())
    base->size = AofBaseSize(dir_, base->file);
  return base;
#else
  return nullopt;
#endif
}

#ifdef __linux__
// First start: adopts the --dbfilename dump, if any, as the base instead of taking a snapshot.
// Returns the base, empty if none, or nullopt if a shutdown stopped it: the manifest stays
// bootstrapping, so the next start redoes it.
optional<string> AofRestore::Bootstrap() {
  AofManifest manifest;

  // Nothing is logged before the load finishes, so every shard's cut is the journal's first lsn.
  manifest.cuts.assign(shard_set->size(), {0, 1});
  if (error_code ec = WriteAofManifest(dir_, manifest); ec)
    ErrorAndExit(absl::StrCat("failed to write the AOF manifest: ", ec.message()));

  if (!plan_.adopt_path.empty()) {
    // TODO: record the dump's identity, in case it is replaced before the next start.
    manifest.base_path = filesystem::path(plan_.adopt_path).filename().string();
    if (!LoadAofBase(dir_, manifest.base_path, load_))
      return nullopt;
  }

  // A shutdown that came after the load: no streamers; the next start redoes the bootstrap.
  if (ServerState::tlocal()->gstate() == GlobalState::SHUTTING_DOWN)
    return nullopt;
  if (!StartStreamers(dir_, vector<AofResume>(shard_set->size())))
    ErrorAndExit("failed to open AOF segments");

  // Committed once the cut segments exist, so an active manifest always has them.
  manifest.state = AofManifest::State::kActive;
  if (error_code ec = WriteAofManifest(dir_, manifest); ec)
    ErrorAndExit(absl::StrCat("failed to write the AOF manifest: ", ec.message()));
  return manifest.base_path;
}

// Loads the manifest's base, replays the log on top and resumes it. nullopt if a shutdown stopped
// it: nothing was appended, so the next start replays again.
optional<AofBase> AofRestore::Resume() {
  const AofManifest& manifest = plan_.manifest;
  if (error_code ec = CollectAofGarbage(dir_, manifest); ec)
    ErrorAndExit(absl::StrCat("failed to remove stale AOF files: ", ec.message()));

  // No expiry or eviction from the base load until the streamers run: a key may pass its deadline
  // before a later record, such as PERSIST, changes it, and commands allowed while LOADING (SCAN)
  // would expire it. Resumed once the streamers run, so every expiry is logged.
  AllowExpiryOnAllShards(false);

  // TODO(#8410): RdbLoader still drops keys whose TTL passed (ShouldDiscardKey), so a key the log
  // later PERSISTs is lost. The base must load as it was, as Valkey does for AOF preambles.
  if (!manifest.base_path.empty() && !LoadAofBase(dir_, manifest.base_path, load_))
    return nullopt;

  // A shutdown that came after the base loaded: nothing to replay or append.
  if (ServerState::tlocal()->gstate() == GlobalState::SHUTTING_DOWN)
    return nullopt;
  vector<AofResume> resume;
  if (error_code ec = Replay(&resume); ec) {
    if (ServerState::tlocal()->gstate() == GlobalState::SHUTTING_DOWN)
      return nullopt;
    if (ec != errc::operation_canceled)
      ErrorAndExit(absl::StrCat("AOF replay failed: ", ec.message()));
    Abandon();
    return AofBase{.rebasing = true};
  }

  // A replay that ended without noticing a shutdown, e.g. an empty log.
  if (ServerState::tlocal()->gstate() == GlobalState::SHUTTING_DOWN)
    return nullopt;
  if (!StartStreamers(dir_, resume))
    ErrorAndExit("failed to open AOF segments");

  AllowExpiryOnAllShards(true);
  return AofBase{.file = manifest.base_path};
}

void AofRestore::Abandon() {
  LOG(INFO) << "AOF restore cancelled by REPLICAOF: a full sync replaces the data";
  AofManifest rebasing = plan_.manifest;
  rebasing.state = AofManifest::State::kRebasing;
  if (error_code ec = WriteAofManifest(dir_, rebasing); ec)
    ErrorAndExit(absl::StrCat("failed to mark the AOF manifest rebasing: ", ec.message()));
  StartPausedStreamers(dir_, plan_.manifest);
  AllowExpiryOnAllShards(true);
}

// Replays every shard's chain from its cut in parallel, global commands meeting at the barrier.
error_code AofRestore::Replay(vector<AofResume>* resume) {
  const AofManifest& manifest = plan_.manifest;
  uint32_t shard_count = shard_set->size();

  // TODO: replay into a different shard count.
  if (manifest.cuts.size() != shard_count)
    return AofError::kShardCountChanged;
  auto mse = make_shared<MultiShardExecution>(shard_count);
  vector<error_code> errors(shard_count);
  vector<TxId> max_txids(shard_count, 0);
  resume->assign(shard_count, {});

  shard_set->RunBlockingInParallel([&](EngineShard* shard) {
    ShardId sid = shard->shard_id();
    errors[sid] = ReplayChain(sid, mse, &(*resume)[sid], &max_txids[sid]);

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
      if (errors[sid] != errc::operation_canceled &&
          ServerState::tlocal()->gstate() != GlobalState::SHUTTING_DOWN)
        LOG(ERROR) << "AOF replay failed on shard " << sid << ": " << errors[sid].message();

      // The load fails; release the shards waiting at a barrier.
      mse->CancelAllBlockingEntities();
    }
  });

  // Chains waiting at a barrier fail once a cancelled one releases them.
  if (cancel_.load(memory_order_relaxed))
    return make_error_code(errc::operation_canceled);
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

// Replays one shard's chain from its cut through the applier, then repairs it. Runs on the shard's
// thread.
error_code AofRestore::ReplayChain(ShardId sid, shared_ptr<MultiShardExecution> mse,
                                   AofResume* resume, TxId* max_txid) {
  const AofManifest::Cut& cut = plan_.manifest.cuts[sid];
  AofChainReader chain(dir_, sid, shard_set->size());
  RETURN_ON_ERR(chain.Open(cut.seq));

  ExecutionState cntx;
  JournalReader reader{&chain, /*dbid=*/0};
  TransactionReader tx_reader;
  JournalApplier applier(service_, std::move(mse));
  TransactionData tx;

  // Records read, compared with the block headers below.
  uint64_t records = 0;
  while (tx_reader.NextTxData(&reader, &cntx, &tx)) {
    if (cancel_.load(memory_order_relaxed))
      return make_error_code(errc::operation_canceled);

    // Before the PING skip: a PING has an LSN too.
    ++records;
    if (tx.opcode == journal::Op::PING)
      continue;
    *max_txid = max(*max_txid, tx.txid);
    if (!applier.Apply(std::move(tx), &cntx)) {
      // A shutdown rejects every command; the caller stops the restore without a failure.
      if (ServerState::tlocal()->gstate() != GlobalState::SHUTTING_DOWN)
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
#endif

}  // namespace dfly
