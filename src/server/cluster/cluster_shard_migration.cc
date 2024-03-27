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

ClusterShardMigration::ClusterShardMigration(ServerContext server_context, uint32_t local_sync_id,
                                             uint32_t shard_id, uint32_t sync_id, Service* service)
    : ProtocolClient(server_context), source_shard_id_(shard_id), sync_id_(sync_id) {
  executor_ = std::make_unique<JournalExecutor>(service);
  executor_->connection_context()->slot_migration_id = local_sync_id;
}

ClusterShardMigration::~ClusterShardMigration() {
}

void ClusterShardMigration::Start(Context* cntx, io::Source* source) {
  JournalReader reader{source, 0};
  TransactionReader tx_reader{false};

  while (!cntx->IsCancelled()) {
    if (cntx->IsCancelled())
      break;

    auto tx_data = tx_reader.NextTxData(&reader, cntx);
    if (!tx_data) {
      VLOG(1) << "No tx data";
      break;
    }

    // TouchIoTime();

    if (tx_data->opcode == journal::Op::FIN) {
      VLOG(2) << "Flow " << source_shard_id_ << " is finalized";
      is_finalized_ = true;
      break;
    } else if (tx_data->opcode == journal::Op::PING) {
      // TODO check about ping logic
    } else {
      ExecuteTxWithNoShardSync(std::move(*tx_data), cntx);
    }
  }
}

void ClusterShardMigration::ExecuteTxWithNoShardSync(TransactionData&& tx_data, Context* cntx) {
  if (cntx->IsCancelled()) {
    return;
  }
  CHECK(tx_data.shard_cnt <= 1);  // we don't support sync for multishard execution
  if (!tx_data.IsGlobalCmd()) {
    VLOG(3) << "Execute cmd without sync between shards. txid: " << tx_data.txid;
    executor_->Execute(tx_data.dbid, absl::MakeSpan(tx_data.commands));
  } else {
    // TODO check which global commands should be supported
    CHECK(false) << "We don't support command: " << ToSV(tx_data.commands.front().cmd_args[0])
                 << "in cluster migration process.";
  }
}

void ClusterShardMigration::Cancel() {
  CloseSocket();
}

}  // namespace dfly
