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
using namespace util;
using absl::GetFlag;

ClusterShardMigration::ClusterShardMigration(ServerContext server_context, uint32_t shard_id,
                                             uint32_t sync_id)
    : ProtocolClient(server_context), source_shard_id_(shard_id), sync_id_(sync_id) {
}

ClusterShardMigration::~ClusterShardMigration() {
  JoinFlow();
}

std::error_code ClusterShardMigration::StartSyncFlow(Context* cntx) {
  using nonstd::make_unexpected;

  RETURN_ON_ERR(ConnectAndAuth(absl::GetFlag(FLAGS_source_connect_timeout_ms) * 1ms, &cntx_));

  VLOG(1) << "Sending on flow " << source_shard_id_;

  std::string cmd = absl::StrCat("DFLYMIGRATE FLOW ", sync_id_, source_shard_id_);

  ResetParser(/*server_mode=*/false);
  leftover_buf_.emplace(128);
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

void ClusterShardMigration::FullSyncShardFb(Context* cntx) {
  DCHECK(leftover_buf_);
  io::PrefixSource ps{leftover_buf_->InputBuffer(), Sock()};

  uint8_t ok_buf[2];
  ps.ReadAtLeast(io::MutableBytes{ok_buf, 2}, 2);
  if (string_view(reinterpret_cast<char*>(ok_buf), 2) != "OK") {
    cntx->ReportError(std::make_error_code(errc::protocol_error),
                      "Incorrect FullSync data, only for tets");
  }

  VLOG(1) << "FullSyncShardFb finished after reading 2 bytes";
}

void ClusterShardMigration::Cancel() {
  CloseSocket();
}

void ClusterShardMigration::JoinFlow() {
  sync_fb_.JoinIfNeeded();
}

}  // namespace dfly
