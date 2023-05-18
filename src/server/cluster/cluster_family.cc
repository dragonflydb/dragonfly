// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/cluster_family.h"

#include <jsoncons/json.hpp>
#include <mutex>
#include <string>

#include "base/flags.h"
#include "base/logging.h"
#include "core/json_object.h"
#include "facade/dragonfly_connection.h"
#include "facade/error.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/dflycmd.h"
#include "server/error.h"
#include "server/replica.h"
#include "server/server_family.h"
#include "server/server_state.h"

ABSL_FLAG(std::string, cluster_mode, "",
          "Cluster mode supported."
          "default: \"\"");
ABSL_FLAG(std::string, cluster_announce_ip, "", "ip that cluster commands announce to the client");

ABSL_DECLARE_FLAG(uint32_t, port);

namespace dfly {
namespace {

using namespace std;
using namespace facade;
using CI = CommandId;

void BuildClusterSlotNetworkInfo(ConnectionContext* cntx, std::string_view host, uint32_t port,
                                 std::string_view id) {
  constexpr unsigned int kNetworkInfoSize = 3;

  (*cntx)->StartArray(kNetworkInfoSize);
  (*cntx)->SendBulkString(host);
  (*cntx)->SendLong(port);
  (*cntx)->SendBulkString(id);
}

}  // namespace

ClusterFamily::ClusterFamily(ServerFamily* server_family) : server_family_(server_family) {
  CHECK_NOTNULL(server_family_);
  string cluster_mode = absl::GetFlag(FLAGS_cluster_mode);

  if (cluster_mode == "emulated") {
    is_emulated_cluster_ = true;
  } else if (cluster_mode == "yes") {
    cluster_config_ = std::make_unique<ClusterConfig>(server_family_->master_id());
  } else if (!cluster_mode.empty()) {
    LOG(ERROR) << "invalid cluster_mode. Exiting...";
    exit(1);
  }
}

bool ClusterFamily::IsEnabledOrEmulated() const {
  return is_emulated_cluster_ || ClusterConfig::IsClusterEnabled();
}

string ClusterFamily::BuildClusterNodeReply(ConnectionContext* cntx) const {
  ServerState& etl = *ServerState::tlocal();
  auto epoch_master_time = std::time(nullptr) * 1000;
  if (etl.is_master) {
    std::string cluster_announce_ip = absl::GetFlag(FLAGS_cluster_announce_ip);
    std::string preferred_endpoint =
        cluster_announce_ip.empty() ? cntx->owner()->LocalBindAddress() : cluster_announce_ip;
    auto vec = server_family_->GetDflyCmd()->GetReplicasRoleInfo();
    auto my_port = absl::GetFlag(FLAGS_port);
    const char* connect_state = vec.empty() ? "disconnected" : "connected";
    std::string msg = absl::StrCat(server_family_->master_id(), " ", preferred_endpoint, ":",
                                   my_port, "@", my_port, " myself,master - 0 ", epoch_master_time,
                                   " 1 ", connect_state, " 0-16383\r\n");
    if (!vec.empty()) {  // info about the replica
      const auto& info = vec[0];
      absl::StrAppend(&msg, etl.remote_client_id_, " ", info.address, ":", info.listening_port, "@",
                      info.listening_port, " slave 0 ", server_family_->master_id(), " 1 ",
                      connect_state, "\r\n");
    }
    return msg;
  } else {
    Replica::Info info = server_family_->GetReplicaInfo();
    auto my_ip = cntx->owner()->LocalBindAddress();
    auto my_port = absl::GetFlag(FLAGS_port);
    const char* connect_state = info.master_link_established ? "connected" : "disconnected";
    std::string msg = absl::StrCat(server_family_->master_id(), " ", my_ip, ":", my_port, "@",
                                   my_port, " myself,slave ", server_family_->master_id(), " 0 ",
                                   epoch_master_time, " 1 ", connect_state, "\r\n");
    absl::StrAppend(&msg, server_family_->GetReplicaMasterId(), " ", info.host, ":", info.port, "@",
                    info.port, " master - 0 ", epoch_master_time, " 1 ", connect_state,
                    " 0-16383\r\n");
    return msg;
  }
}

void ClusterFamily::ClusterHelp(ConnectionContext* cntx) {
  string_view help_arr[] = {
      "CLUSTER <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
      "SLOTS",
      "   Return information about slots range mappings. Each range is made of:",
      "   start, end, master and replicas IP addresses, ports and ids.",
      "NODES",
      "   Return cluster configuration seen by node. Output format:",
      "   <id> <ip:port> <flags> <master> <pings> <pongs> <epoch> <link> <slot> ...",
      "INFO",
      "  Return information about the cluster",
      "HELP",
      "    Prints this help.",
  };
  return (*cntx)->SendSimpleStrArr(help_arr);
}

void ClusterFamily::ClusterSlots(ConnectionContext* cntx) {
  // For more details https://redis.io/commands/cluster-slots/
  constexpr unsigned int kClustersShardingCount = 1;
  constexpr unsigned int kNoReplicaInfoSize = 3;
  constexpr unsigned int kWithReplicaInfoSize = 4;

  /* Format: 1) 1) start slot
   *            2) end slot
   *            3) 1) master IP
   *               2) master port
   *               3) node ID
   *            4) 1) replica IP (optional)
   *               2) replica port
   *               3) node ID
   *           ... note that in this case, only 1 slot
   */
  ServerState& etl = *ServerState::tlocal();
  // we have 3 cases here
  // 1. This is a stand alone, in this case we only sending local information
  // 2. We are the master, and we have replica, in this case send us as master
  // 3. We are replica to a master, sends the information about us as replica
  (*cntx)->StartArray(kClustersShardingCount);
  if (etl.is_master) {
    std::string cluster_announce_ip = absl::GetFlag(FLAGS_cluster_announce_ip);
    std::string preferred_endpoint =
        cluster_announce_ip.empty() ? cntx->owner()->LocalBindAddress() : cluster_announce_ip;
    auto vec = server_family_->GetDflyCmd()->GetReplicasRoleInfo();
    unsigned int info_len = vec.empty() ? kNoReplicaInfoSize : kWithReplicaInfoSize;
    (*cntx)->StartArray(info_len);
    (*cntx)->SendLong(0);                           // start sharding range
    (*cntx)->SendLong(ClusterConfig::kMaxSlotNum);  // end sharding range
    BuildClusterSlotNetworkInfo(cntx, preferred_endpoint, absl::GetFlag(FLAGS_port),
                                server_family_->master_id());
    if (!vec.empty()) {  // info about the replica
      const auto& info = vec[0];
      BuildClusterSlotNetworkInfo(cntx, info.address, info.listening_port, etl.remote_client_id_);
    }
  } else {
    Replica::Info info = server_family_->GetReplicaInfo();
    (*cntx)->StartArray(kWithReplicaInfoSize);
    (*cntx)->SendLong(0);                           // start sharding range
    (*cntx)->SendLong(ClusterConfig::kMaxSlotNum);  // end sharding range
    BuildClusterSlotNetworkInfo(cntx, info.host, info.port, server_family_->GetReplicaMasterId());
    BuildClusterSlotNetworkInfo(cntx, cntx->owner()->LocalBindAddress(), absl::GetFlag(FLAGS_port),
                                server_family_->master_id());
  }
}

void ClusterFamily::ClusterNodes(ConnectionContext* cntx) {
  // Support for NODES commands can help in case we are working in cluster mode
  // In this case, we can save information about the cluster
  // In case this is the master, it can save the information about the replica from this command
  std::string msg = BuildClusterNodeReply(cntx);
  (*cntx)->SendBulkString(msg);
}

void ClusterFamily::ClusterInfo(ConnectionContext* cntx) {
  std::string msg;
  auto append = [&msg](absl::AlphaNum a1, absl::AlphaNum a2) {
    absl::StrAppend(&msg, a1, ":", a2, "\r\n");
  };
  // info command just return some stats about this instance
  int known_nodes = 1;
  long epoch = 1;
  ServerState& etl = *ServerState::tlocal();
  if (etl.is_master) {
    auto vec = server_family_->GetDflyCmd()->GetReplicasRoleInfo();
    if (!vec.empty()) {
      known_nodes = 2;
    }
  } else {
    if (server_family_->HasReplica()) {
      known_nodes = 2;
      epoch = server_family_->GetReplicaInfo().master_last_io_sec;
    }
  }
  int cluster_size = known_nodes - 1;
  append("cluster_state", "ok");
  append("cluster_slots_assigned", ClusterConfig::kMaxSlotNum);
  append("cluster_slots_ok", ClusterConfig::kMaxSlotNum);
  append("cluster_slots_pfail", 0);
  append("cluster_slots_fail", 0);
  append("cluster_known_nodes", known_nodes);
  append("cluster_size", cluster_size);
  append("cluster_current_epoch", epoch);
  append("cluster_my_epoch", 1);
  append("cluster_stats_messages_ping_sent", 1);
  append("cluster_stats_messages_pong_sent", 1);
  append("cluster_stats_messages_sent", 1);
  append("cluster_stats_messages_ping_received", 1);
  append("cluster_stats_messages_pong_received", 1);
  append("cluster_stats_messages_meet_received", 0);
  append("cluster_stats_messages_received", 1);
  (*cntx)->SendBulkString(msg);
}

void ClusterFamily::Cluster(CmdArgList args, ConnectionContext* cntx) {
  // In emulated cluster mode, all slots are mapped to the same host, and number of cluster
  // instances is thus 1.

  ToUpper(&args[0]);
  string_view sub_cmd = ArgS(args, 0);

  if (!is_emulated_cluster_ && !ClusterConfig::IsClusterEnabled()) {
    return (*cntx)->SendError(
        "CLUSTER commands requires --cluster_mode=emulated or --cluster_mode=yes");
  }

  if (sub_cmd == "HELP") {
    return ClusterHelp(cntx);
  } else if (sub_cmd == "SLOTS") {
    return ClusterSlots(cntx);
  } else if (sub_cmd == "NODES") {
    return ClusterNodes(cntx);
  } else if (sub_cmd == "INFO") {
    return ClusterInfo(cntx);
  } else {
    return (*cntx)->SendError(facade::UnknownSubCmd(sub_cmd, "CLUSTER"), facade::kSyntaxErrType);
  }
}

void ClusterFamily::ReadOnly(CmdArgList args, ConnectionContext* cntx) {
  if (!is_emulated_cluster_) {
    return (*cntx)->SendError("READONLY command requires --cluster_mode=emulated");
  }
  (*cntx)->SendOk();
}

void ClusterFamily::ReadWrite(CmdArgList args, ConnectionContext* cntx) {
  if (!is_emulated_cluster_) {
    return (*cntx)->SendError("READWRITE command requires --cluster_mode=emulated");
  }
  (*cntx)->SendOk();
}

void ClusterFamily::DflyCluster(CmdArgList args, ConnectionContext* cntx) {
  if (!ClusterConfig::IsClusterEnabled()) {
    return (*cntx)->SendError("DFLYCLUSTER commands requires --cluster_mode=yes");
  }
  CHECK_NE(cluster_config_, nullptr);

  // TODO check admin port
  ToUpper(&args[0]);
  string_view sub_cmd = ArgS(args, 0);
  if (sub_cmd == "GETSLOTINFO") {
    return DflyClusterGetSlotInfo(args, cntx);
  } else if (sub_cmd == "CONFIG") {
    return DflyClusterConfig(args, cntx);
  }

  return (*cntx)->SendError(UnknownSubCmd(sub_cmd, "DFLYCLUSTER"), kSyntaxErrType);
}

void ClusterFamily::DflyClusterConfig(CmdArgList args, ConnectionContext* cntx) {
  SinkReplyBuilder* rb = cntx->reply_builder();

  if (args.size() != 2) {
    return rb->SendError(WrongNumArgsError("DFLYCLUSTER CONFIG"));
  }

  string_view json_str = ArgS(args, 1);
  optional<JsonType> json = JsonFromString(json_str);
  if (!json.has_value()) {
    LOG(WARNING) << "Can't parse JSON for ClusterConfig " << json_str;
    return rb->SendError("Invalid JSON cluster config", kSyntaxErrType);
  }

  if (!cluster_config_->SetConfig(json.value())) {
    return rb->SendError("Invalid cluster configuration.");
  }

  return rb->SendOk();
}

void ClusterFamily::DflyClusterGetSlotInfo(CmdArgList args, ConnectionContext* cntx) {
  if (args.size() == 2) {
    return (*cntx)->SendError(facade::WrongNumArgsError("DFLYCLUSTER GETSLOTINFO"), kSyntaxErrType);
  }
  ToUpper(&args[1]);
  string_view slots_str = ArgS(args, 1);
  if (slots_str != "SLOTS") {
    return (*cntx)->SendError(kSyntaxErr, kSyntaxErrType);
  }

  vector<std::pair<SlotId, SlotStats>> slots_stats;
  for (size_t i = 2; i < args.size(); ++i) {
    string_view slot_str = ArgS(args, i);
    uint32_t sid;
    if (!absl::SimpleAtoi(slot_str, &sid)) {
      return (*cntx)->SendError(kInvalidIntErr);
    }
    if (sid > ClusterConfig::kMaxSlotNum) {
      return (*cntx)->SendError("Invalid slot id");
    }
    slots_stats.emplace_back(sid, SlotStats{});
  }

  Mutex mu;

  auto cb = [&](auto*) {
    EngineShard* shard = EngineShard::tlocal();
    if (shard == nullptr)
      return;

    lock_guard lk(mu);
    for (auto& [slot, data] : slots_stats) {
      data += shard->db_slice().GetSlotStats(slot);
    }
  };

  shard_set->pool()->AwaitFiberOnAll(std::move(cb));

  (*cntx)->StartArray(slots_stats.size());

  for (const auto& slot_data : slots_stats) {
    (*cntx)->StartArray(3);
    (*cntx)->SendBulkString(absl::StrCat(slot_data.first));
    (*cntx)->SendBulkString("key_count");
    (*cntx)->SendBulkString(absl::StrCat(slot_data.second.key_count));
  }
}

using EngineFunc = void (ClusterFamily::*)(CmdArgList args, ConnectionContext* cntx);

inline CommandId::Handler HandlerFunc(ClusterFamily* se, EngineFunc f) {
  return [=](CmdArgList args, ConnectionContext* cntx) { return (se->*f)(args, cntx); };
}

#define HFUNC(x) SetHandler(HandlerFunc(this, &ClusterFamily::x))

void ClusterFamily::Register(CommandRegistry* registry) {
  *registry << CI{"CLUSTER", CO::READONLY, 2, 0, 0, 0}.HFUNC(Cluster)
            << CI{"DFLYCLUSTER", CO::ADMIN | CO::GLOBAL_TRANS | CO::HIDDEN, -2, 0, 0, 0}.HFUNC(
                   DflyCluster)
            << CI{"READONLY", CO::READONLY, 1, 0, 0, 0}.HFUNC(ReadOnly)
            << CI{"READWRITE", CO::READONLY, 1, 0, 0, 0}.HFUNC(ReadWrite);
}

}  // namespace dfly
