// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/cluster/cluster_slot_migration.h"

#include <absl/flags/flag.h>

#include "base/logging.h"
#include "server/error.h"
#include "server/main_service.h"

ABSL_FLAG(int, source_connect_timeout_ms, 20000,
          "Timeout for establishing connection to a source node");

ABSL_DECLARE_FLAG(int32_t, port);

namespace dfly {

using namespace std;
using namespace facade;
using absl::GetFlag;

ClusterSlotMigration::ClusterSlotMigration(string host_ip, uint16_t port,
                                           std::vector<ClusterConfig::SlotRange> slots)
    : ProtocolClient(move(host_ip), port), slots_(std::move(slots)) {
}

ClusterSlotMigration::~ClusterSlotMigration() {
}

error_code ClusterSlotMigration::Start(ConnectionContext* cntx) {
  VLOG(1) << "Starting slot migration";

  auto check_connection_error = [this, &cntx](error_code ec, const char* msg) -> error_code {
    if (ec) {
      cntx->SendError(absl::StrCat(msg, ec.message()));
    }
    return ec;
  };

  VLOG(1) << "Resolving host DNS";
  error_code ec = ResolveHostDns();
  RETURN_ON_ERR(check_connection_error(ec, "could not resolve host dns"));

  VLOG(1) << "Connecting to source";
  ec = ConnectAndAuth(absl::GetFlag(FLAGS_source_connect_timeout_ms) * 1ms, &cntx_);
  RETURN_ON_ERR(check_connection_error(ec, "couldn't connect to source"));

  VLOG(1) << "Greeting";
  ec = Greet();
  RETURN_ON_ERR(check_connection_error(ec, "couldn't greet source "));

  state_ = ClusterSlotMigration::C_CONNECTING;

  return {};
}

error_code ClusterSlotMigration::Greet() {
  ResetParser(false);
  VLOG(1) << "greeting message handling";
  RETURN_ON_ERR(SendCommandAndReadResponse("PING"));
  PC_RETURN_ON_BAD_RESPONSE(CheckRespIsSimpleReply("PONG"));

  auto port = absl::GetFlag(FLAGS_port);
  auto cmd = absl::StrCat("DFLYMIGRATE CONF ", port);
  for (const auto& s : slots_) {
    absl::StrAppend(&cmd, " ", s.start, " ", s.end);
  }
  VLOG(1) << "Migration command: " << cmd;
  RETURN_ON_ERR(SendCommandAndReadResponse(cmd));
  // Response is: num_shards
  if (!CheckRespFirstTypes({RespExpr::INT64}))
    return make_error_code(errc::bad_message);

  souce_shards_num_ = get<int64_t>(LastResponseArgs()[0].u);

  return error_code{};
}

ClusterSlotMigration::Info ClusterSlotMigration::GetInfo() const {
  const auto& ctx = server();
  return {ctx.host, ctx.port, state_};
}

}  // namespace dfly
