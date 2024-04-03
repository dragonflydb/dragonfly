// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/cluster_family.h"

#include <memory>
#include <mutex>
#include <string>

#include "absl/cleanup/cleanup.h"
#include "base/flags.h"
#include "base/logging.h"
#include "facade/cmd_arg_parser.h"
#include "facade/dragonfly_connection.h"
#include "facade/error.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/dflycmd.h"
#include "server/error.h"
#include "server/journal/journal.h"
#include "server/main_service.h"
#include "server/server_family.h"
#include "server/server_state.h"

ABSL_FLAG(std::string, cluster_announce_ip, "", "ip that cluster commands announce to the client");
ABSL_FLAG(std::string, cluster_node_id, "",
          "ID within a cluster, used for slot assignment. MUST be unique. If empty, uses master "
          "replication ID (random string)");

ABSL_DECLARE_FLAG(int32_t, port);

namespace dfly {
namespace {

using namespace std;
using namespace facade;
using namespace util;

using CI = CommandId;

constexpr char kIdNotFound[] = "syncid not found";

constexpr string_view kClusterDisabled =
    "Cluster is disabled. Enabled via passing --cluster_mode=emulated|yes";

thread_local shared_ptr<ClusterConfig> tl_cluster_config;

}  // namespace

ClusterFamily::ClusterFamily(ServerFamily* server_family) : server_family_(server_family) {
  CHECK_NOTNULL(server_family_);

  ClusterConfig::Initialize();

  id_ = absl::GetFlag(FLAGS_cluster_node_id);
  if (id_.empty()) {
    id_ = server_family_->master_replid();
  } else if (ClusterConfig::IsEmulated()) {
    LOG(ERROR) << "Setting --cluster_node_id in emulated mode is unsupported";
    exit(1);
  }
}

ClusterConfig* ClusterFamily::cluster_config() {
  return tl_cluster_config.get();
}

ClusterShardInfo ClusterFamily::GetEmulatedShardInfo(ConnectionContext* cntx) const {
  ClusterShardInfo info{.slot_ranges = {{.start = 0, .end = ClusterConfig::kMaxSlotNum}},
                        .master = {},
                        .replicas = {},
                        .migrations = {}};

  optional<Replica::Info> replication_info = server_family_->GetReplicaInfo();
  ServerState& etl = *ServerState::tlocal();
  if (!replication_info.has_value()) {
    DCHECK(etl.is_master);
    std::string cluster_announce_ip = absl::GetFlag(FLAGS_cluster_announce_ip);
    std::string preferred_endpoint =
        cluster_announce_ip.empty() ? cntx->conn()->LocalBindAddress() : cluster_announce_ip;

    info.master = {.id = id_,
                   .ip = preferred_endpoint,
                   .port = static_cast<uint16_t>(absl::GetFlag(FLAGS_port))};

    for (const auto& replica : server_family_->GetDflyCmd()->GetReplicasRoleInfo()) {
      info.replicas.push_back({.id = replica.id,
                               .ip = replica.address,
                               .port = static_cast<uint16_t>(replica.listening_port)});
    }
  } else {
    // TODO: We currently don't save the master's ID in the replica
    info.master = {.id = "", .ip = replication_info->host, .port = replication_info->port};
    info.replicas.push_back({.id = id_,
                             .ip = cntx->conn()->LocalBindAddress(),
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
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  return rb->SendSimpleStrArr(help_arr);
}

namespace {
void ClusterShardsImpl(const ClusterShardInfos& config, ConnectionContext* cntx) {
  // For more details https://redis.io/commands/cluster-shards/
  constexpr unsigned int kEntrySize = 4;
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());

  auto WriteNode = [&](const ClusterNodeInfo& node, string_view role) {
    constexpr unsigned int kNodeSize = 14;
    rb->StartArray(kNodeSize);
    rb->SendBulkString("id");
    rb->SendBulkString(node.id);
    rb->SendBulkString("endpoint");
    rb->SendBulkString(node.ip);
    rb->SendBulkString("ip");
    rb->SendBulkString(node.ip);
    rb->SendBulkString("port");
    rb->SendLong(node.port);
    rb->SendBulkString("role");
    rb->SendBulkString(role);
    rb->SendBulkString("replication-offset");
    rb->SendLong(0);
    rb->SendBulkString("health");
    rb->SendBulkString("online");
  };

  rb->StartArray(config.size());
  for (const auto& shard : config) {
    rb->StartArray(kEntrySize);
    rb->SendBulkString("slots");

    rb->StartArray(shard.slot_ranges.size() * 2);
    for (const auto& slot_range : shard.slot_ranges) {
      rb->SendLong(slot_range.start);
      rb->SendLong(slot_range.end);
    }

    rb->SendBulkString("nodes");
    rb->StartArray(1 + shard.replicas.size());
    WriteNode(shard.master, "master");
    for (const auto& replica : shard.replicas) {
      WriteNode(replica, "replica");
    }
  }
}
}  // namespace

void ClusterFamily::ClusterShards(ConnectionContext* cntx) {
  if (ClusterConfig::IsEmulated()) {
    return ClusterShardsImpl({GetEmulatedShardInfo(cntx)}, cntx);
  } else if (tl_cluster_config != nullptr) {
    return ClusterShardsImpl(tl_cluster_config->GetConfig(), cntx);
  } else {
    return cntx->SendError(kClusterNotConfigured);
  }
}

namespace {
void ClusterSlotsImpl(const ClusterShardInfos& config, ConnectionContext* cntx) {
  // For more details https://redis.io/commands/cluster-slots/
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  auto WriteNode = [&](const ClusterNodeInfo& node) {
    constexpr unsigned int kNodeSize = 3;
    rb->StartArray(kNodeSize);
    rb->SendBulkString(node.ip);
    rb->SendLong(node.port);
    rb->SendBulkString(node.id);
  };

  unsigned int slot_ranges = 0;
  for (const auto& shard : config) {
    slot_ranges += shard.slot_ranges.size();
  }

  rb->StartArray(slot_ranges);
  for (const auto& shard : config) {
    for (const auto& slot_range : shard.slot_ranges) {
      const unsigned int array_size =
          /* slot-start, slot-end */ 2 + /* master */ 1 + /* replicas */ shard.replicas.size();
      rb->StartArray(array_size);
      rb->SendLong(slot_range.start);
      rb->SendLong(slot_range.end);
      WriteNode(shard.master);
      for (const auto& replica : shard.replicas) {
        WriteNode(replica);
      }
    }
  }
}
}  // namespace

void ClusterFamily::ClusterSlots(ConnectionContext* cntx) {
  if (ClusterConfig::IsEmulated()) {
    return ClusterSlotsImpl({GetEmulatedShardInfo(cntx)}, cntx);
  } else if (tl_cluster_config != nullptr) {
    return ClusterSlotsImpl(tl_cluster_config->GetConfig(), cntx);
  } else {
    return cntx->SendError(kClusterNotConfigured);
  }
}

namespace {
void ClusterNodesImpl(const ClusterShardInfos& config, string_view my_id, ConnectionContext* cntx) {
  // For more details https://redis.io/commands/cluster-nodes/

  string result;

  auto WriteNode = [&](const ClusterNodeInfo& node, string_view role, string_view master_id,
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

    // Separate lines with only \n, not \r\n, see #2726
    absl::StrAppend(&result, "\n");
  };

  for (const auto& shard : config) {
    WriteNode(shard.master, "master", "-", shard.slot_ranges);
    for (const auto& replica : shard.replicas) {
      // Only the master prints ranges, so we send an empty set for replicas.
      WriteNode(replica, "slave", shard.master.id, {});
    }
  }

  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  return rb->SendBulkString(result);
}
}  // namespace

void ClusterFamily::ClusterNodes(ConnectionContext* cntx) {
  if (ClusterConfig::IsEmulated()) {
    return ClusterNodesImpl({GetEmulatedShardInfo(cntx)}, id_, cntx);
  } else if (tl_cluster_config != nullptr) {
    return ClusterNodesImpl(tl_cluster_config->GetConfig(), id_, cntx);
  } else {
    return cntx->SendError(kClusterNotConfigured);
  }
}

namespace {
void ClusterInfoImpl(const ClusterShardInfos& config, ConnectionContext* cntx) {
  std::string msg;
  auto append = [&msg](absl::AlphaNum a1, absl::AlphaNum a2) {
    // Separate lines with \r\n, not \n, see #2726
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
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  rb->SendBulkString(msg);
}
}  // namespace

void ClusterFamily::ClusterInfo(ConnectionContext* cntx) {
  if (ClusterConfig::IsEmulated()) {
    return ClusterInfoImpl({GetEmulatedShardInfo(cntx)}, cntx);
  } else if (tl_cluster_config != nullptr) {
    return ClusterInfoImpl(tl_cluster_config->GetConfig(), cntx);
  } else {
    return ClusterInfoImpl({}, cntx);
  }
}

void ClusterFamily::KeySlot(CmdArgList args, ConnectionContext* cntx) {
  if (args.size() != 2) {
    return cntx->SendError(WrongNumArgsError("CLUSTER KEYSLOT"));
  }

  SlotId id = ClusterConfig::KeySlot(ArgS(args, 1));
  return cntx->SendLong(id);
}

void ClusterFamily::Cluster(CmdArgList args, ConnectionContext* cntx) {
  // In emulated cluster mode, all slots are mapped to the same host, and number of cluster
  // instances is thus 1.

  ToUpper(&args[0]);
  string_view sub_cmd = ArgS(args, 0);

  if (!ClusterConfig::IsEnabledOrEmulated()) {
    return cntx->SendError(kClusterDisabled);
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
  } else if (sub_cmd == "KEYSLOT") {
    return KeySlot(args, cntx);
  } else {
    return cntx->SendError(facade::UnknownSubCmd(sub_cmd, "CLUSTER"), facade::kSyntaxErrType);
  }
}

void ClusterFamily::ReadOnly(CmdArgList args, ConnectionContext* cntx) {
  if (!ClusterConfig::IsEmulated()) {
    return cntx->SendError(kClusterDisabled);
  }
  cntx->SendOk();
}

void ClusterFamily::ReadWrite(CmdArgList args, ConnectionContext* cntx) {
  if (!ClusterConfig::IsEmulated()) {
    return cntx->SendError(kClusterDisabled);
  }
  cntx->SendOk();
}

void ClusterFamily::DflyCluster(CmdArgList args, ConnectionContext* cntx) {
  if (!ClusterConfig::IsEnabledOrEmulated()) {
    return cntx->SendError(kClusterDisabled);
  }

  ToUpper(&args[0]);
  string_view sub_cmd = ArgS(args, 0);
  args.remove_prefix(1);  // remove subcommand name
  if (sub_cmd == "GETSLOTINFO") {
    return DflyClusterGetSlotInfo(args, cntx);
  } else if (sub_cmd == "CONFIG") {
    return DflyClusterConfig(args, cntx);
  } else if (sub_cmd == "MYID") {
    return DflyClusterMyId(args, cntx);
  } else if (sub_cmd == "FLUSHSLOTS") {
    return DflyClusterFlushSlots(args, cntx);
  } else if (sub_cmd == "SLOT-MIGRATION-STATUS") {
    return DflyClusterSlotMigrationStatus(args, cntx);
  }

  return cntx->SendError(UnknownSubCmd(sub_cmd, "DFLYCLUSTER"), kSyntaxErrType);
}

void ClusterFamily::DflyClusterMyId(CmdArgList args, ConnectionContext* cntx) {
  if (!args.empty()) {
    return cntx->SendError(WrongNumArgsError("DFLYCLUSTER MYID"));
  }
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  rb->SendBulkString(id_);
}

namespace {
// Guards set configuration, so that we won't handle 2 in parallel.
util::fb2::Mutex set_config_mu;

void DeleteSlots(const SlotRanges& slots_ranges) {
  if (slots_ranges.empty()) {
    return;
  }

  auto cb = [&](auto*) {
    EngineShard* shard = EngineShard::tlocal();
    if (shard == nullptr)
      return;

    shard->db_slice().FlushSlots(slots_ranges);
  };
  shard_set->pool()->AwaitFiberOnAll(std::move(cb));
}

void WriteFlushSlotsToJournal(const SlotRanges& slot_ranges) {
  if (slot_ranges.empty()) {
    return;
  }

  // Build args
  vector<string> args;
  args.reserve(slot_ranges.size() + 1);
  args.push_back("FLUSHSLOTS");
  for (SlotRange range : slot_ranges) {
    args.push_back(absl::StrCat(range.start));
    args.push_back(absl::StrCat(range.end));
  }

  // Build view
  vector<string_view> args_view(args.size());
  for (size_t i = 0; i < args.size(); ++i) {
    args_view[i] = args[i];
  }

  auto cb = [&](auto*) {
    EngineShard* shard = EngineShard::tlocal();
    if (shard == nullptr) {
      return;
    }

    auto journal = EngineShard::tlocal()->journal();
    if (journal == nullptr) {
      return;
    }

    // Send journal entry
    // TODO: Break slot migration upon FLUSHSLOTS
    journal->RecordEntry(/* txid= */ 0, journal::Op::COMMAND, /* dbid= */ 0,
                         /* shard_cnt= */ shard_set->size(), nullopt,
                         make_pair("DFLYCLUSTER", args_view), false);
  };
  shard_set->pool()->AwaitFiberOnAll(std::move(cb));
}
}  // namespace

void ClusterFamily::DflyClusterConfig(CmdArgList args, ConnectionContext* cntx) {
  if (args.size() != 1) {
    return cntx->SendError(WrongNumArgsError("DFLYCLUSTER CONFIG"));
  }

  string_view json_str = ArgS(args, 0);
  shared_ptr<ClusterConfig> new_config = ClusterConfig::CreateFromConfig(id_, json_str);
  if (new_config == nullptr) {
    LOG(WARNING) << "Can't set cluster config";
    return cntx->SendError("Invalid cluster configuration.");
  }

  lock_guard gu(set_config_mu);

  auto prev_config = tl_cluster_config;
  SlotSet before = prev_config ? prev_config->GetOwnedSlots() : SlotSet(true);

  // Ignore blocked commands because we filter them with CancelBlockingOnThread
  DispatchTracker tracker{server_family_->GetListeners(), cntx->conn(), false /* ignore paused */,
                          true /* ignore blocked */};

  auto blocking_filter = [&new_config](ArgSlice keys) {
    bool moved = any_of(keys.begin(), keys.end(), [&](auto k) { return !new_config->IsMySlot(k); });
    return moved ? OpStatus::KEY_MOVED : OpStatus::OK;
  };

  auto cb = [this, &tracker, &new_config, blocking_filter](util::ProactorBase* pb) {
    server_family_->CancelBlockingOnThread(blocking_filter);
    tl_cluster_config = new_config;
    tracker.TrackOnThread();
  };

  server_family_->service().proactor_pool().AwaitFiberOnAll(std::move(cb));
  DCHECK(tl_cluster_config != nullptr);

  if (!StartSlotMigrations(new_config->GetNewOutgoingMigrations(prev_config), cntx)) {
    return cntx->SendError("Can't start the migration");
  }
  RemoveFinishedMigrations();

  if (!tracker.Wait(absl::Seconds(1))) {
    LOG(WARNING) << "Cluster config change timed out";
  }

  SlotSet after = tl_cluster_config->GetOwnedSlots();
  if (ServerState::tlocal()->is_master) {
    auto deleted_slots = (before.GetRemovedSlots(after)).ToSlotRanges();
    DeleteSlots(deleted_slots);
    WriteFlushSlotsToJournal(deleted_slots);
  }

  return cntx->SendOk();
}

void ClusterFamily::DflyClusterGetSlotInfo(CmdArgList args, ConnectionContext* cntx) {
  CmdArgParser parser(args);
  parser.ExpectTag("SLOTS");
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());

  vector<std::pair<SlotId, SlotStats>> slots_stats;
  do {
    auto sid = parser.Next<uint32_t>();
    if (sid > ClusterConfig::kMaxSlotNum)
      return rb->SendError("Invalid slot id");
    slots_stats.emplace_back(sid, SlotStats{});
  } while (parser.HasNext());

  if (auto err = parser.Error(); err)
    return rb->SendError(err->MakeReply());

  fb2::Mutex mu;

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

  rb->StartArray(slots_stats.size());

  for (const auto& slot_data : slots_stats) {
    rb->StartArray(9);
    rb->SendLong(slot_data.first);
    rb->SendBulkString("key_count");
    rb->SendLong(static_cast<long>(slot_data.second.key_count));
    rb->SendBulkString("total_reads");
    rb->SendLong(static_cast<long>(slot_data.second.total_reads));
    rb->SendBulkString("total_writes");
    rb->SendLong(static_cast<long>(slot_data.second.total_writes));
    rb->SendBulkString("memory_bytes");
    rb->SendLong(static_cast<long>(slot_data.second.memory_bytes));
  }
}

void ClusterFamily::DflyClusterFlushSlots(CmdArgList args, ConnectionContext* cntx) {
  SlotRanges slot_ranges;

  CmdArgParser parser(args);
  do {
    auto [slot_start, slot_end] = parser.Next<SlotId, SlotId>();
    slot_ranges.emplace_back(SlotRange{slot_start, slot_end});
  } while (parser.HasNext());

  if (auto err = parser.Error(); err)
    return cntx->SendError(err->MakeReply());

  DeleteSlots(slot_ranges);

  return cntx->SendOk();
}

bool ClusterFamily::StartSlotMigrations(std::vector<MigrationInfo> migrations,
                                        ConnectionContext* cntx) {
  // Add validating and error processing
  for (auto m : migrations) {
    auto outgoing_migration =
        CreateOutgoingMigration(std::move(m.ip), m.port, std::move(m.slot_ranges));
    outgoing_migration->Start(cntx);
  }
  return true;
}

static std::string_view state_to_str(MigrationState state) {
  switch (state) {
    case MigrationState::C_NO_STATE:
      return "NO_STATE"sv;
    case MigrationState::C_CONNECTING:
      return "CONNECTING"sv;
    case MigrationState::C_SYNC:
      return "SYNC"sv;
    case MigrationState::C_FINISHED:
      return "FINISHED"sv;
    case MigrationState::C_MAX_INVALID:
      break;
  }
  DCHECK(false) << "Unknown State value " << static_cast<underlying_type_t<MigrationState>>(state);
  return "UNDEFINED_STATE"sv;
}

void ClusterFamily::DflyClusterSlotMigrationStatus(CmdArgList args, ConnectionContext* cntx) {
  CmdArgParser parser(args);
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());

  if (parser.HasNext()) {
    auto [host_ip, port] = parser.Next<std::string_view, uint16_t>();
    if (auto err = parser.Error(); err)
      return rb->SendError(err->MakeReply());

    lock_guard lk(migration_mu_);
    // find incoming slot migration
    for (const auto& m : incoming_migrations_jobs_) {
      const auto& info = m->GetInfo();
      if (info.host == host_ip && info.port == port)
        return rb->SendSimpleString(state_to_str(m->GetState()));
    }
    // find outgoing slot migration
    for (const auto& [_, info] : outgoing_migration_jobs_) {
      if (info->GetHostIp() == host_ip && info->GetPort() == port)
        return rb->SendSimpleString(state_to_str(info->GetState()));
    }
  } else if (auto arr_size = incoming_migrations_jobs_.size() + outgoing_migration_jobs_.size();
             arr_size != 0) {
    rb->StartArray(arr_size);
    const auto& send_answer = [rb](std::string_view direction, std::string_view host, uint16_t port,
                                   auto state) {
      auto str = absl::StrCat(direction, " ", host, ":", port, " ", state_to_str(state));
      rb->SendSimpleString(str);
    };
    lock_guard lk(migration_mu_);
    for (const auto& m : incoming_migrations_jobs_) {
      const auto& info = m->GetInfo();
      send_answer("in", info.host, info.port, m->GetState());
    }
    for (const auto& [_, info] : outgoing_migration_jobs_) {
      send_answer("out", info->GetHostIp(), info->GetPort(), info->GetState());
    }
    return;
  }
  return rb->SendSimpleString(state_to_str(MigrationState::C_NO_STATE));
}

void ClusterFamily::DflyMigrate(CmdArgList args, ConnectionContext* cntx) {
  ToUpper(&args[0]);
  string_view sub_cmd = ArgS(args, 0);
  args.remove_prefix(1);
  if (sub_cmd == "INIT") {
    InitMigration(args, cntx);
  } else if (sub_cmd == "FLOW") {
    DflyMigrateFlow(args, cntx);
  } else if (sub_cmd == "ACK") {
    DflyMigrateAck(args, cntx);
  } else {
    cntx->SendError(facade::UnknownSubCmd(sub_cmd, "DFLYMIGRATE"), facade::kSyntaxErrType);
  }
}

ClusterSlotMigration* ClusterFamily::CreateIncomingMigration(std::string host_ip, uint16_t port,
                                                             SlotRanges slots,
                                                             uint32_t shards_num) {
  lock_guard lk(migration_mu_);
  for (const auto& mj : incoming_migrations_jobs_) {
    if (auto info = mj->GetInfo(); info.host == host_ip && info.port == port) {
      return nullptr;
    }
  }
  return incoming_migrations_jobs_
      .emplace_back(make_shared<ClusterSlotMigration>(
          std::string(host_ip), port, &server_family_->service(), std::move(slots), shards_num))
      .get();
}

std::shared_ptr<ClusterSlotMigration> ClusterFamily::GetIncomingMigration(std::string host_ip,
                                                                          uint16_t port) {
  lock_guard lk(migration_mu_);
  for (const auto& mj : incoming_migrations_jobs_) {
    if (auto info = mj->GetInfo(); info.host == host_ip && info.port == port) {
      return mj;
    }
  }
  return nullptr;
}

void ClusterFamily::RemoveFinishedMigrations() {
  lock_guard lk(migration_mu_);
  auto removed_items_it =
      std::remove_if(incoming_migrations_jobs_.begin(), incoming_migrations_jobs_.end(),
                     [](const auto& m) { return m->GetState() == MigrationState::C_FINISHED; });
  incoming_migrations_jobs_.erase(removed_items_it, incoming_migrations_jobs_.end());

  for (auto it = outgoing_migration_jobs_.begin(); it != outgoing_migration_jobs_.end();) {
    if (it->second->GetState() == MigrationState::C_FINISHED) {
      VLOG(1) << "erase finished migration " << it->first;
      it = outgoing_migration_jobs_.erase(it);
    } else {
      ++it;
    }
  }
}

void ClusterFamily::InitMigration(CmdArgList args, ConnectionContext* cntx) {
  VLOG(1) << "Create incoming migration, args: " << args;
  CmdArgParser parser{args};
  auto [port, flows_num] = parser.Next<uint32_t, uint32_t>();

  SlotRanges slots;
  do {
    auto [slot_start, slot_end] = parser.Next<SlotId, SlotId>();
    slots.emplace_back(SlotRange{slot_start, slot_end});
  } while (parser.HasNext());

  if (auto err = parser.Error(); err)
    return cntx->SendError(err->MakeReply());

  VLOG(1) << "Init migration " << cntx->conn()->RemoteEndpointAddress() << ":" << port;

  CreateIncomingMigration(cntx->conn()->RemoteEndpointAddress(), port, std::move(slots), flows_num);

  return cntx->SendOk();
}

std::shared_ptr<OutgoingMigration> ClusterFamily::CreateOutgoingMigration(std::string host,
                                                                          uint16_t port,
                                                                          SlotRanges slots) {
  std::lock_guard lk(migration_mu_);
  auto sync_id = next_sync_id_++;
  auto err_handler = [](const GenericError& err) {
    LOG(INFO) << "Slot migration error: " << err.Format();

    // Todo add error processing, stop migration process
    // fb2::Fiber("stop_Migration", &ClusterFamily::StopMigration, this, sync_id).Detach();
  };
  auto migration = make_shared<OutgoingMigration>(host, port, std::move(slots), this, err_handler,
                                                  server_family_);
  auto [it, inserted] = outgoing_migration_jobs_.emplace(sync_id, migration);
  CHECK(inserted);
  return migration;
}

void ClusterFamily::DflyMigrateFlow(CmdArgList args, ConnectionContext* cntx) {
  CmdArgParser parser{args};
  auto [port, shard_id] = parser.Next<uint32_t, uint32_t>();

  if (auto err = parser.Error(); err) {
    return cntx->SendError(err->MakeReply());
  }

  auto host_ip = cntx->conn()->RemoteEndpointAddress();

  VLOG(1) << "Create flow " << host_ip << ":" << port << " shard_id: " << shard_id;

  cntx->conn()->SetName(absl::StrCat("migration_flow_", host_ip, ":", port));

  auto migration = GetIncomingMigration(cntx->conn()->RemoteEndpointAddress(), port);
  if (!migration)
    return cntx->SendError(kIdNotFound);

  DCHECK(cntx->sync_dispatch);
  // we do this to be ignored by the dispatch tracker
  // TODO provide a more clear approach
  cntx->sync_dispatch = false;

  cntx->SendOk();

  migration->StartFlow(shard_id, cntx->conn()->socket());
}

void ClusterFamily::UpdateConfig(const std::vector<SlotRange>& slots, bool enable) {
  lock_guard gu(set_config_mu);

  auto new_config = tl_cluster_config->CloneWithChanges(slots, enable);

  shard_set->pool()->AwaitFiberOnAll(
      [&new_config](util::ProactorBase* pb) { tl_cluster_config = new_config; });
  DCHECK(tl_cluster_config != nullptr);
}

void ClusterFamily::DflyMigrateAck(CmdArgList args, ConnectionContext* cntx) {
  CmdArgParser parser{args};
  auto port = parser.Next<uint16_t>();

  if (auto err = parser.Error(); err) {
    return cntx->SendError(err->MakeReply());
  }

  auto host_ip = cntx->conn()->RemoteEndpointAddress();
  auto migration = GetIncomingMigration(host_ip, port);
  if (!migration)
    return cntx->SendError(kIdNotFound);

  migration->Join();

  if (migration->GetState() != MigrationState::C_FINISHED) {
    return cntx->SendError("Migration process is not in C_FINISHED state");
  }

  UpdateConfig(migration->GetSlots(), true);

  cntx->SendOk();
}

shared_ptr<OutgoingMigration> ClusterFamily::GetOutgoingMigration(uint32_t sync_id) {
  unique_lock lk(migration_mu_);
  auto sync_it = outgoing_migration_jobs_.find(sync_id);
  return sync_it != outgoing_migration_jobs_.end() ? sync_it->second : nullptr;
}

using EngineFunc = void (ClusterFamily::*)(CmdArgList args, ConnectionContext* cntx);

inline CommandId::Handler HandlerFunc(ClusterFamily* se, EngineFunc f) {
  return [=](CmdArgList args, ConnectionContext* cntx) { return (se->*f)(args, cntx); };
}

#define HFUNC(x) SetHandler(HandlerFunc(this, &ClusterFamily::x))

namespace acl {
constexpr uint32_t kCluster = SLOW;
// Reconsider to maybe more sensible defaults
constexpr uint32_t kDflyCluster = ADMIN | SLOW;
constexpr uint32_t kReadOnly = FAST | CONNECTION;
constexpr uint32_t kReadWrite = FAST | CONNECTION;
constexpr uint32_t kDflyMigrate = ADMIN | SLOW | DANGEROUS;
}  // namespace acl

void ClusterFamily::Register(CommandRegistry* registry) {
  registry->StartFamily();
  *registry << CI{"CLUSTER", CO::READONLY, -2, 0, 0, acl::kCluster}.HFUNC(Cluster)
            << CI{"DFLYCLUSTER",    CO::ADMIN | CO::GLOBAL_TRANS | CO::HIDDEN, -2, 0, 0,
                  acl::kDflyCluster}
                   .HFUNC(DflyCluster)
            << CI{"READONLY", CO::READONLY, 1, 0, 0, acl::kReadOnly}.HFUNC(ReadOnly)
            << CI{"READWRITE", CO::READONLY, 1, 0, 0, acl::kReadWrite}.HFUNC(ReadWrite)
            << CI{"DFLYMIGRATE", CO::ADMIN | CO::HIDDEN, -1, 0, 0, acl::kDflyMigrate}.HFUNC(
                   DflyMigrate);
}

}  // namespace dfly
