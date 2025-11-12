// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/coordinator.h"

#include "base/logging.h"
#include "facade/redis_parser.h"
#include "facade/socket_utils.h"
#include "server/cluster/cluster_config.h"

using namespace std;
using namespace facade;

namespace dfly::cluster {

class Coordinator::CrossShardClient : private ProtocolClient {
 public:
  CrossShardClient(std::string host, uint16_t port) : ProtocolClient(std::move(host), port) {
  }

  using ProtocolClient::CloseSocket;
  ~CrossShardClient() {
    CloseSocket();
  }

  void Init() {
    VLOG(1) << "Resolving host DNS to " << server().Description();
    if (error_code ec = ResolveHostDns(); ec) {
      LOG(WARNING) << "Could not resolve host DNS to " << server().Description() << ": "
                   << ec.message();
      exec_st_.ReportError(GenericError(ec, "Could not resolve host dns."));
      return;
    }
    VLOG(1) << "Start coordinator connection to " << server().Description();
    auto timeout = 3000ms;  // TODO add flag;
    if (auto ec = ConnectAndAuth(timeout, &exec_st_); ec) {
      LOG(WARNING) << "Couldn't connect to " << server().Description() << ": " << ec.message()
                   << ", socket state: " << GetSocketInfo(Sock()->native_handle());
      exec_st_.ReportError(GenericError(ec, "Couldn't connect to source."));
      return;
    }

    ResetParser(RedisParser::Mode::CLIENT);
  }

  void Cancel() {
    ShutdownSocket();
  }

  void SendCommand(std::string_view cmd) {
    if (auto ec = SendCommandAndReadResponse(cmd); ec) {
      LOG(WARNING) << "Coordinator could not send command to : " << ec.message()
                   << ", socket state: " << GetSocketInfo(Sock()->native_handle());
      exec_st_.ReportError(GenericError(ec, "Could not send command."));
    }
    // add response processing
  }
};

Coordinator& Coordinator::Current() {
  static Coordinator instance;
  return instance;
}

void Coordinator::DispatchAll(std::string_view command) {
  auto cluster_config = ClusterConfig::Current();
  if (!cluster_config) {
    VLOG(2) << "No cluster config found for coordinator plan creation.";
    return;
  }
  VLOG(2) << "Dispatching command to all shards: " << command;
  auto shards_config = cluster_config->GetConfig();

  std::vector<std::unique_ptr<CrossShardClient>> clients;
  for (const auto& shard : shards_config) {
    if (shard.master.id == cluster_config->MyId()) {
      continue;
    }
    clients.emplace_back(std::make_unique<CrossShardClient>(shard.master.ip, shard.master.port));
    clients.back()->Init();
    clients.back()->SendCommand(std::string(command));
  }
}

}  // namespace dfly::cluster
