// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/cluster/cluster_shard_migration.h"

#include <absl/flags/flag.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>

#include "base/logging.h"
#include "server/error.h"
#include "server/journal/serializer.h"
#include "server/journal/tx_executor.h"

ABSL_DECLARE_FLAG(int, source_connect_timeout_ms);

namespace dfly {

using namespace std;
using namespace facade;
using namespace util;
using absl::GetFlag;

ClusterShardMigration::ClusterShardMigration(ServerContext server_context, uint32_t shard_id,
                                             uint32_t sync_id, Service* service,
                                             std::shared_ptr<MultiShardExecution> cmse)
    : ProtocolClient(server_context),
      source_shard_id_(shard_id),
      sync_id_(sync_id),
      service_(*service),
      multi_shard_exe_(cmse) {
  executor_ = std::make_unique<JournalExecutor>(service);
}

ClusterShardMigration::~ClusterShardMigration() {
  JoinFlow();
}

std::error_code ClusterShardMigration::StartSyncFlow(Context* cntx) {
  RETURN_ON_ERR(ConnectAndAuth(absl::GetFlag(FLAGS_source_connect_timeout_ms) * 1ms, &cntx_));

  leftover_buf_.emplace(128);
  ResetParser(/*server_mode=*/false);

  std::string cmd = absl::StrCat("DFLYMIGRATE FLOW ", sync_id_, " ", source_shard_id_);
  VLOG(1) << "cmd: " << cmd;

  RETURN_ON_ERR(SendCommand(cmd));

  auto read_resp = ReadRespReply(&*leftover_buf_);
  if (!read_resp.has_value()) {
    return read_resp.error();
  }

  PC_RETURN_ON_BAD_RESPONSE(CheckRespIsSimpleReply("OK"));

  leftover_buf_->ConsumeInput(read_resp->left_in_buffer);

  sync_fb_ =
      fb2::Fiber("shard_migration_full_sync", &ClusterShardMigration::FullSyncShardFb, this, cntx);

  return {};
}

bool ClusterShardMigration::TransactionData::AddEntry(journal::ParsedEntry&& entry) {
  ++journal_rec_count;

  switch (entry.opcode) {
    case journal::Op::PING:
      is_ping = true;
      return true;
    case journal::Op::EXPIRED:
    case journal::Op::COMMAND:
      commands.push_back(std::move(entry.cmd));
      [[fallthrough]];
    case journal::Op::EXEC:
      shard_cnt = entry.shard_cnt;
      dbid = entry.dbid;
      txid = entry.txid;
      return true;
    case journal::Op::MULTI_COMMAND:
      commands.push_back(std::move(entry.cmd));
      dbid = entry.dbid;
      return false;
    default:
      DCHECK(false) << "Unsupported opcode";
  }
  return false;
}

ClusterShardMigration::TransactionData ClusterShardMigration::TransactionData::FromSingle(
    journal::ParsedEntry&& entry) {
  TransactionData data;
  bool res = data.AddEntry(std::move(entry));
  DCHECK(res);
  return data;
}

auto ClusterShardMigration::TransactionReader::NextTxData(JournalReader* reader, Context* cntx)
    -> optional<TransactionData> {
  io::Result<journal::ParsedEntry> res;
  while (true) {
    if (res = reader->ReadEntry(); !res) {
      cntx->ReportError(res.error());
      return std::nullopt;
    }

    // Check if journal command can be executed right away.
    // Expiration checks lock on master, so it never conflicts with running multi transactions.
    if (res->opcode == journal::Op::EXPIRED || res->opcode == journal::Op::COMMAND ||
        res->opcode == journal::Op::PING)
      return TransactionData::FromSingle(std::move(res.value()));

    // Otherwise, continue building multi command.
    DCHECK(res->opcode == journal::Op::MULTI_COMMAND || res->opcode == journal::Op::EXEC);
    DCHECK(res->txid > 0);

    auto txid = res->txid;
    auto& txdata = current_[txid];
    if (txdata.AddEntry(std::move(res.value()))) {
      auto out = std::move(txdata);
      current_.erase(txid);
      return out;
    }
  }

  return std::nullopt;
}

void ClusterShardMigration::FullSyncShardFb(Context* cntx) {
  DCHECK(leftover_buf_);
  io::PrefixSource ps{leftover_buf_->InputBuffer(), Sock()};

  // uint8_t ok_buf[4];
  // ps.ReadAtLeast(io::MutableBytes{ok_buf, 4}, 4);

  // if (string_view(reinterpret_cast<char*>(ok_buf), 4) != "SYNC") {
  //   VLOG(1) << "FullSyncShardFb incorrect data transfer";
  //   cntx->ReportError(std::make_error_code(errc::protocol_error),
  //                     "Incorrect FullSync data, only for tets");
  // }

  // VLOG(1) << "FullSyncShardFb finished after reading 4 bytes";

  JournalReader reader{&ps, 0};
  TransactionReader tx_reader{};

  while (!cntx->IsCancelled()) {
    waker_.await([&]() {
      static constexpr size_t kYieldAfterItemsInQueue = 50;
      return ((trans_data_queue_.size() < kYieldAfterItemsInQueue) || cntx->IsCancelled());
    });
    if (cntx->IsCancelled())
      break;

    auto tx_data = tx_reader.NextTxData(&reader, cntx);
    if (!tx_data)
      break;

    TouchIoTime();

    if (!tx_data->is_ping) {
      ExecuteTxWithNoShardSync(std::move(*tx_data), cntx);
    } else {
      // force_ping_ = true;
      // journal_rec_executed_.fetch_add(1, std::memory_order_relaxed);
    }

    waker_.notify();
  }
}

bool ClusterShardMigration::TransactionData::IsGlobalCmd() const {
  if (commands.size() > 1) {
    return false;
  }

  auto& command = commands.front();
  if (command.cmd_args.empty()) {
    return false;
  }

  auto& args = command.cmd_args;
  if (absl::EqualsIgnoreCase(ToSV(args[0]), "FLUSHDB"sv) ||
      absl::EqualsIgnoreCase(ToSV(args[0]), "FLUSHALL"sv) ||
      (absl::EqualsIgnoreCase(ToSV(args[0]), "DFLYCLUSTER"sv) &&
       absl::EqualsIgnoreCase(ToSV(args[1]), "FLUSHSLOTS"sv))) {
    return true;
  }

  return false;
}

void ClusterShardMigration::ExecuteTxWithNoShardSync(TransactionData&& tx_data, Context* cntx) {
  if (cntx->IsCancelled()) {
    return;
  }

  bool was_insert = false;
  if (tx_data.IsGlobalCmd()) {
    was_insert = multi_shard_exe_->InsertTxToSharedMap(tx_data.txid, tx_data.shard_cnt);
  }

  ExecuteTx(std::move(tx_data), was_insert, cntx);
}

void ClusterShardMigration::ExecuteTx(TransactionData&& tx_data, bool inserted_by_me,
                                      Context* cntx) {
  if (cntx->IsCancelled()) {
    return;
  }
  if (tx_data.shard_cnt <= 1 || !tx_data.IsGlobalCmd()) {
    VLOG(2) << "Execute cmd without sync between shards. txid: " << tx_data.txid;
    executor_->Execute(tx_data.dbid, absl::MakeSpan(tx_data.commands));
    // journal_rec_executed_.fetch_add(tx_data.journal_rec_count, std::memory_order_relaxed);
    return;
  }

  VLOG(2) << "Execute txid: " << tx_data.txid;
  std::unique_lock lk(multi_shard_exe_->map_mu);
  auto it = multi_shard_exe_->tx_sync_execution.find(tx_data.txid);
  DCHECK(it != multi_shard_exe_->tx_sync_execution.end());
  auto& multi_shard_data = it->second;
  lk.unlock();

  VLOG(2) << "Execute txid: " << tx_data.txid << " waiting for data in all shards";
  // Wait until shards flows got transaction data and inserted to map.
  // This step enforces that replica will execute multi shard commands that finished on master
  // and replica recieved all the commands from all shards.
  multi_shard_data.block.Wait();
  // Check if we woke up due to cancellation.
  if (cntx_.IsCancelled())
    return;
  VLOG(2) << "Execute txid: " << tx_data.txid << " block wait finished";

  if (tx_data.IsGlobalCmd()) {
    VLOG(2) << "Execute txid: " << tx_data.txid << " global command execution";
    // Wait until all shards flows get to execution step of this transaction.
    multi_shard_data.barrier.Wait();
    // Check if we woke up due to cancellation.
    if (cntx_.IsCancelled())
      return;
    // Global command will be executed only from one flow fiber. This ensure corectness of data in
    // replica.
    if (inserted_by_me) {
      executor_->Execute(tx_data.dbid, absl::MakeSpan(tx_data.commands));
    }
    // Wait until exection is done, to make sure we done execute next commands while the global is
    // executed.
    multi_shard_data.barrier.Wait();
    // Check if we woke up due to cancellation.
    if (cntx_.IsCancelled())
      return;
  } else {  // Non global command will be executed by each flow fiber
    VLOG(2) << "Execute txid: " << tx_data.txid << " executing shard transaction commands";
    executor_->Execute(tx_data.dbid, absl::MakeSpan(tx_data.commands));
  }
  // journal_rec_executed_.fetch_add(tx_data.journal_rec_count, std::memory_order_relaxed);

  // Erase from map can be done only after all flow fibers executed the transaction commands.
  // The last fiber which will decrease the counter to 0 will be the one to erase the data from
  // map
  auto val = multi_shard_data.counter.fetch_sub(1, std::memory_order_relaxed);
  VLOG(2) << "txid: " << tx_data.txid << " counter: " << val;
  if (val == 1) {
    std::lock_guard lg{multi_shard_exe_->map_mu};
    multi_shard_exe_->tx_sync_execution.erase(tx_data.txid);
  }
}

void ClusterShardMigration::Cancel() {
  CloseSocket();
}

void ClusterShardMigration::JoinFlow() {
  sync_fb_.JoinIfNeeded();
}

}  // namespace dfly
