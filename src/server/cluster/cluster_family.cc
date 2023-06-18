// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/cluster_family.h"

#include <jsoncons/json.hpp>
#include <memory>
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
#include "server/main_service.h"
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
using ClusterShard = ClusterConfig::ClusterShard;
using ClusterShards = ClusterConfig::ClusterShards;
using Node = ClusterConfig::Node;
using SlotRange = ClusterConfig::SlotRange;

constexpr string_view kClusterDisabled =
    "Cluster is disabled. Enabled via passing --cluster_mode=emulated|yes";
constexpr string_view kDflyClusterCmdPort = "DflyCluster command allowed only under admin port";

thread_local shared_ptr<ClusterConfig> tl_cluster_config;

}  // namespace

ClusterFamily::ClusterFamily(ServerFamily* server_family) : server_family_(server_family) {
  CHECK_NOTNULL(server_family_);
  string cluster_mode = absl::GetFlag(FLAGS_cluster_mode);

  if (cluster_mode == "emulated") {
    is_emulated_cluster_ = true;
  } else if (cluster_mode == "yes") {
    ClusterConfig::EnableCluster();
  } else if (!cluster_mode.empty()) {
    LOG(ERROR) << "invalid cluster_mode. Exiting...";
    exit(1);
  }
}

ClusterConfig* ClusterFamily::cluster_config() {
  return tl_cluster_config.get();
}

bool ClusterFamily::IsEnabledOrEmulated() const {
  return is_emulated_cluster_ || ClusterConfig::IsClusterEnabled();
}

ClusterShard ClusterFamily::GetEmulatedShardInfo(ConnectionContext* cntx) const {
  ClusterShard info{
      .slot_ranges = {{.start = 0, .end = ClusterConfig::kMaxSlotNum}},
      .master = {},
      .replicas = {},
  };

  optional<Replica::Info> replication_info = server_family_->GetReplicaInfo();
  ServerState& etl = *ServerState::tlocal();
  if (!replication_info.has_value()) {
    DCHECK(etl.is_master);
    std::string cluster_announce_ip = absl::GetFlag(FLAGS_cluster_announce_ip);
    std::string preferred_endpoint =
        cluster_announce_ip.empty() ? cntx->owner()->LocalBindAddress() : cluster_announce_ip;

    info.master = {.id = server_family_->master_id(),
                   .ip = preferred_endpoint,
                   .port = static_cast<uint16_t>(absl::GetFlag(FLAGS_port))};

    for (const auto& replica : server_family_->GetDflyCmd()->GetReplicasRoleInfo()) {
      info.replicas.push_back({.id = etl.remote_client_id_,
                               .ip = replica.address,
                               .port = static_cast<uint16_t>(replica.listening_port)});
    }
  } else {
    info.master = {
        .id = etl.remote_client_id_, .ip = replication_info->host, .port = replication_info->port};
    info.replicas.push_back({.id = server_family_->master_id(),
                             .ip = cntx->owner()->LocalBindAddress(),
                             .port = static_cast<uint16_t>(absl::GetFlag(FLAGS_port))});
  }

  return info;
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

namespace {
void ClusterShardsImpl(const ClusterShards& config, ConnectionContext* cntx) {
  // For more details https://redis.io/commands/cluster-shards/
  constexpr unsigned int kEntrySize = 4;

  auto WriteNode = [&](const Node& node, string_view role) {
    constexpr unsigned int kNodeSize = 14;
    (*cntx)->StartArray(kNodeSize);
    (*cntx)->SendBulkString("id");
    (*cntx)->SendBulkString(node.id);
    (*cntx)->SendBulkString("endpoint");
    (*cntx)->SendBulkString(node.ip);
    (*cntx)->SendBulkString("ip");
    (*cntx)->SendBulkString(node.ip);
    (*cntx)->SendBulkString("port");
    (*cntx)->SendLong(node.port);
    (*cntx)->SendBulkString("role");
    (*cntx)->SendBulkString(role);
    (*cntx)->SendBulkString("replication-offset");
    (*cntx)->SendLong(0);
    (*cntx)->SendBulkString("health");
    (*cntx)->SendBulkString("online");
  };

  (*cntx)->StartArray(config.size());
  for (const auto& shard : config) {
    (*cntx)->StartArray(kEntrySize);
    (*cntx)->SendBulkString("slots");

    (*cntx)->StartArray(shard.slot_ranges.size() * 2);
    for (const auto& slot_range : shard.slot_ranges) {
      (*cntx)->SendLong(slot_range.start);
      (*cntx)->SendLong(slot_range.end);
    }

    (*cntx)->SendBulkString("nodes");
    (*cntx)->StartArray(1 + shard.replicas.size());
    WriteNode(shard.master, "master");
    for (const auto& replica : shard.replicas) {
      WriteNode(replica, "replica");
    }
  }
}
}  // namespace

void ClusterFamily::ClusterShards(ConnectionContext* cntx) {
  if (is_emulated_cluster_) {
    return ClusterShardsImpl({GetEmulatedShardInfo(cntx)}, cntx);
  } else if (tl_cluster_config != nullptr) {
    return ClusterShardsImpl(tl_cluster_config->GetConfig(), cntx);
  } else {
    return (*cntx)->SendError(kClusterNotConfigured);
  }
}

namespace {
void ClusterSlotsImpl(const ClusterShards& config, ConnectionContext* cntx) {
  // For more details https://redis.io/commands/cluster-slots/
  auto WriteNode = [&](const Node& node) {
    constexpr unsigned int kNodeSize = 3;
    (*cntx)->StartArray(kNodeSize);
    (*cntx)->SendBulkString(node.ip);
    (*cntx)->SendLong(node.port);
    (*cntx)->SendBulkString(node.id);
  };

  unsigned int slot_ranges = 0;
  for (const auto& shard : config) {
    slot_ranges += shard.slot_ranges.size();
  }

  (*cntx)->StartArray(slot_ranges);
  for (const auto& shard : config) {
    for (const auto& slot_range : shard.slot_ranges) {
      const unsigned int array_size =
          /* slot-start, slot-end */ 2 + /* master */ 1 + /* replicas */ shard.replicas.size();
      (*cntx)->StartArray(array_size);
      (*cntx)->SendLong(slot_range.start);
      (*cntx)->SendLong(slot_range.end);
      WriteNode(shard.master);
      for (const auto& replica : shard.replicas) {
        WriteNode(replica);
      }
    }
  }
}
}  // namespace

void ClusterFamily::ClusterSlots(ConnectionContext* cntx) {
  if (is_emulated_cluster_) {
    return ClusterSlotsImpl({GetEmulatedShardInfo(cntx)}, cntx);
  } else if (tl_cluster_config != nullptr) {
    return ClusterSlotsImpl(tl_cluster_config->GetConfig(), cntx);
  } else {
    return (*cntx)->SendError(kClusterNotConfigured);
  }
}

namespace {
void ClusterNodesImpl(const ClusterShards& config, string_view my_id, ConnectionContext* cntx) {
  // For more details https://redis.io/commands/cluster-nodes/

  string result;

  auto WriteNode = [&](const Node& node, string_view role, string_view master_id,
                       const vector<SlotRange>& ranges) {
    absl::StrAppend(&result, node.id, " ");

    absl::StrAppend(&result, node.ip, ":", node.port, "@", node.port, " ");

    if (my_id == node.id) {
      absl::StrAppend(&result, "myself,");
    }
    absl::StrAppend(&result, role, " ");

    absl::StrAppend(&result, master_id, " ");

    absl::StrAppend(&result, "0 0 0 connected");

    for (const auto& range : ranges) {
      absl::StrAppend(&result, " ", range.start);
      if (range.start != range.end) {
        absl::StrAppend(&result, "-", range.end);
      }
    }

    absl::StrAppend(&result, "\r\n");
  };

  for (const auto& shard : config) {
    WriteNode(shard.master, "master", "-", shard.slot_ranges);
    for (const auto& replica : shard.replicas) {
      // Only the master prints ranges, so we send an empty set for replicas.
      WriteNode(replica, "slave", shard.master.id, {});
    }
  }

  return (*cntx)->SendBulkString(result);
}
}  // namespace

void ClusterFamily::ClusterNodes(ConnectionContext* cntx) {
  if (is_emulated_cluster_) {
    return ClusterNodesImpl({GetEmulatedShardInfo(cntx)}, server_family_->master_id(), cntx);
  } else if (tl_cluster_config != nullptr) {
    return ClusterNodesImpl(tl_cluster_config->GetConfig(), server_family_->master_id(), cntx);
  } else {
    return (*cntx)->SendError(kClusterNotConfigured);
  }
}

namespace {
void ClusterInfoImpl(const ClusterShards& config, ConnectionContext* cntx) {
  std::string msg;
  auto append = [&msg](absl::AlphaNum a1, absl::AlphaNum a2) {
    absl::StrAppend(&msg, a1, ":", a2, "\r\n");
  };

  // Initialize response variables to emulated mode.
  string_view state = "ok"sv;
  SlotId slots_assigned = ClusterConfig::kMaxSlotNum + 1;
  size_t known_nodes = 1;
  long epoch = 1;
  size_t cluster_size = 1;

  if (config.empty()) {
    state = "fail"sv;
    slots_assigned = 0;
    cluster_size = 0;
    known_nodes = 0;
  } else {
    known_nodes = 0;
    cluster_size = 0;
    for (const auto& shard_config : config) {
      known_nodes += 1;  // For master
      known_nodes += shard_config.replicas.size();

      if (!shard_config.slot_ranges.empty()) {
        ++cluster_size;
      }
    }
  }

  append("cluster_state", state);
  append("cluster_slots_assigned", slots_assigned);
  append("cluster_slots_ok", slots_assigned);  // We do not support other failed nodes.
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
}  // namespace

void ClusterFamily::ClusterInfo(ConnectionContext* cntx) {
  if (is_emulated_cluster_) {
    return ClusterInfoImpl({GetEmulatedShardInfo(cntx)}, cntx);
  } else if (tl_cluster_config != nullptr) {
    return ClusterInfoImpl(tl_cluster_config->GetConfig(), cntx);
  } else {
    return ClusterInfoImpl({}, cntx);
  }
}

void ClusterFamily::Cluster(CmdArgList args, ConnectionContext* cntx) {
  // In emulated cluster mode, all slots are mapped to the same host, and number of cluster
  // instances is thus 1.

  ToUpper(&args[0]);
  string_view sub_cmd = ArgS(args, 0);

  if (!is_emulated_cluster_ && !ClusterConfig::IsClusterEnabled()) {
    return (*cntx)->SendError(kClusterDisabled);
  }

  if (sub_cmd == "HELP") {
    return ClusterHelp(cntx);
  } else if (sub_cmd == "SHARDS") {
    return ClusterShards(cntx);
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
    return (*cntx)->SendError(kClusterDisabled);
  }
  (*cntx)->SendOk();
}

void ClusterFamily::ReadWrite(CmdArgList args, ConnectionContext* cntx) {
  if (!is_emulated_cluster_) {
    return (*cntx)->SendError(kClusterDisabled);
  }
  (*cntx)->SendOk();
}

void ClusterFamily::DflyCluster(CmdArgList args, ConnectionContext* cntx) {
  if (!is_emulated_cluster_ && !ClusterConfig::IsClusterEnabled()) {
    return (*cntx)->SendError(kClusterDisabled);
  }

  if (!cntx->owner()->IsAdmin()) {
    return (*cntx)->SendError(kDflyClusterCmdPort);
  }

  ToUpper(&args[0]);
  string_view sub_cmd = ArgS(args, 0);
  if (sub_cmd == "GETSLOTINFO") {
    return DflyClusterGetSlotInfo(args, cntx);
  } else if (sub_cmd == "CONFIG") {
    return DflyClusterConfig(args, cntx);
  } else if (sub_cmd == "MYID") {
    return DflyClusterMyId(args, cntx);
  }

  return (*cntx)->SendError(UnknownSubCmd(sub_cmd, "DFLYCLUSTER"), kSyntaxErrType);
}

void ClusterFamily::DflyClusterMyId(CmdArgList args, ConnectionContext* cntx) {
  if (args.size() != 1) {
    return (*cntx)->SendError(WrongNumArgsError("DFLYCLUSTER MYID"));
  }
  (*cntx)->SendBulkString(server_family_->master_id());
}

namespace {
SlotSet GetDeletedSlots(bool is_first_config, const SlotSet& before, const SlotSet& after) {
  SlotSet result;
  for (SlotId id = 0; id <= ClusterConfig::kMaxSlotNum; ++id) {
    if ((before.contains(id) || is_first_config) && !after.contains(id)) {
      result.insert(id);
    }
  }
  return result;
}

// Guards set configuration, so that we won't handle 2 in parallel.
Mutex set_config_mu;
}  // namespace

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

  shared_ptr<ClusterConfig> new_config =
      ClusterConfig::CreateFromConfig(server_family_->master_id(), json.value());
  if (new_config == nullptr) {
    LOG(WARNING) << "Can't set cluster config";
    return rb->SendError("Invalid cluster configuration.");
  }

  lock_guard gu(set_config_mu);

  bool is_first_config = true;
  SlotSet before;
  if (tl_cluster_config != nullptr) {
    is_first_config = false;
    before = tl_cluster_config->GetOwnedSlots();
  }

  auto cb = [&](util::ProactorBase* pb) { tl_cluster_config = new_config; };
  server_family_->service().proactor_pool().AwaitFiberOnAll(std::move(cb));

  DCHECK(tl_cluster_config != nullptr);

  SlotSet after = tl_cluster_config->GetOwnedSlots();

  // Delete old slots data.
  SlotSet deleted_slot_ids = GetDeletedSlots(is_first_config, before, after);
  if (!deleted_slot_ids.empty()) {
    auto cb = [&](auto*) {
      EngineShard* shard = EngineShard::tlocal();
      if (shard == nullptr)
        return;

      shard->db_slice().FlushSlots(deleted_slot_ids);
    };
    shard_set->pool()->AwaitFiberOnAll(std::move(cb));
  }

  return rb->SendOk();
}

void ClusterFamily::DflyClusterGetSlotInfo(CmdArgList args, ConnectionContext* cntx) {
  if (args.size() <= 2) {
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
    (*cntx)->StartArray(7);
    (*cntx)->SendLong(slot_data.first);
    (*cntx)->SendBulkString("key_count");
    (*cntx)->SendLong(static_cast<long>(slot_data.second.key_count));
    (*cntx)->SendBulkString("total_reads");
    (*cntx)->SendLong(static_cast<long>(slot_data.second.total_reads));
    (*cntx)->SendBulkString("total_writes");
    (*cntx)->SendLong(static_cast<long>(slot_data.second.total_writes));
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
