// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/cluster/cluster_slot_migration.h"

#include <absl/flags/flag.h>

#include "base/logging.h"
#include "server/error.h"
#include "server/main_service.h"

ABSL_FLAG(int, source_connect_timeout_ms, 20000,
          "Timeout for establishing connection to a replication master");

namespace dfly {

using namespace std;
using absl::GetFlag;

ClusterSlotMigration::ClusterSlotMigration(string host, uint16_t port)
    : ProtocolClient(std::move(host), port) {
}

ClusterSlotMigration::~ClusterSlotMigration() {
}

error_code ClusterSlotMigration::Start(ConnectionContext* cntx) {
  VLOG(1) << "Starting slot migration";

  auto check_connection_error = [this, &cntx](error_code ec, const char* msg) -> error_code {
    if (ec) {
      (*cntx)->SendError(absl::StrCat(msg, ec.message()));
      cntx_.Cancel();
    }
    return ec;
  };

  VLOG(1) << "Resolving source host DNS";
  error_code ec = ResolveMasterDns();
  RETURN_ON_ERR(check_connection_error(ec, "could not resolve master dns"));

  VLOG(1) << "Connecting to source";
  ec = ConnectAndAuth(absl::GetFlag(FLAGS_source_connect_timeout_ms) * 1ms, &cntx_);
  RETURN_ON_ERR(check_connection_error(ec, "couldn't connect to source"));

  VLOG(1) << "Greeting";
  ec = Greet();
  RETURN_ON_ERR(check_connection_error(ec, "could not greet master "));

  (*cntx)->SendOk();

  return {};
}

error_code ClusterSlotMigration::Greet() {
  ResetParser(false);
  VLOG(1) << "greeting message handling";
  RETURN_ON_ERR(SendCommandAndReadResponse("PING"));
  PC_RETURN_ON_BAD_RESPONSE(CheckRespIsSimpleReply("PONG"));

  return error_code{};
}

}  // namespace dfly
