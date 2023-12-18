// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/cluster/cluster_shard_migration.h"

#include <absl/flags/flag.h>
#include <absl/strings/str_cat.h>

#include "base/logging.h"
#include "server/error.h"

ABSL_DECLARE_FLAG(int, source_connect_timeout_ms);

namespace dfly {

using namespace std;
using namespace facade;
using absl::GetFlag;

ClusterShardMigration::ClusterShardMigration(ServerContext server_context, uint32_t shard_id)
    : ProtocolClient(server_context), source_shard_id_(shard_id) {
}

ClusterShardMigration::~ClusterShardMigration() {
}

std::error_code ClusterShardMigration::StartSyncFlow(Context* cntx) {
  using nonstd::make_unexpected;

  RETURN_ON_ERR(ConnectAndAuth(absl::GetFlag(FLAGS_source_connect_timeout_ms) * 1ms, &cntx_));

  VLOG(1) << "Sending on flow " << source_shard_id_;

  std::string cmd = absl::StrCat("DFLYMIGRATE FLOW ", source_shard_id_);

  ResetParser(/*server_mode=*/false);
  leftover_buf_.emplace(128);
  RETURN_ON_ERR(SendCommand(cmd));
  auto read_resp = ReadRespReply(&*leftover_buf_);
  if (!read_resp.has_value()) {
    return read_resp.error();
  }

  PC_RETURN_ON_BAD_RESPONSE(CheckRespFirstTypes({RespExpr::STRING}));

  return {};
}

void ClusterShardMigration::Cancel() {
  CloseSocket();
}

}  // namespace dfly
