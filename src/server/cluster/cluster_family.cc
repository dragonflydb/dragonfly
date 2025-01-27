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
#include "server/namespaces.h"
#include "server/server_family.h"
#include "server/server_state.h"
#include "util/fibers/synchronization.h"

ABSL_FLAG(std::string, cluster_announce_ip, "",
          "IP address that Dragonfly announces to cluster clients");

ABSL_FLAG(std::string, cluster_node_id, "",
          "ID within a cluster, used for slot assignment. MUST be unique. If empty, uses master "
          "replication ID (random string)");

ABSL_DECLARE_FLAG(int32_t, port);
ABSL_DECLARE_FLAG(uint16_t, announce_port);
ABSL_DECLARE_FLAG(bool, managed_service_info);

namespace dfly {
namespace acl {
constexpr uint32_t kCluster = SLOW;
// Reconsider to maybe more sensible defaults
constexpr uint32_t kDflyCluster = ADMIN | SLOW;
constexpr uint32_t kReadOnly = FAST | CONNECTION;
constexpr uint32_t kReadWrite = FAST | CONNECTION;
constexpr uint32_t kDflyMigrate = ADMIN | SLOW | DANGEROUS;
}  // namespace acl
}  // namespace dfly

namespace dfly::cluster {
namespace {

using namespace std;
using namespace facade;
using namespace util;
using Payload = journal::Entry::Payload;
using CI = CommandId;

constexpr char kIdNotFound[] = "syncid not found";

constexpr string_view kClusterDisabled =
    "Cluster is disabled. Enabled via passing --cluster_mode=emulated|yes";

thread_local shared_ptr<ClusterConfig> tl_cluster_config;

ClusterShardInfos GetConfigForStats(ConnectionContext* cntx) {
  CHECK(!IsClusterEmulated());
  CHECK(tl_cluster_config != nullptr);

  auto config = tl_cluster_config->GetConfig();
  if (cntx->conn()->IsPrivileged() || !absl::GetFlag(FLAGS_managed_service_info)) {
    return config;
  }

  // We can't mutate `config` so we copy it over
  std::vector<ClusterShardInfo> infos;
  infos.reserve(config.size());

  for (auto& node : config) {
    infos.push_back(node);
    infos.rbegin()->replicas.clear();
  }

  return ClusterShardInfos{std::move(infos)};
}

}  // namespace

ClusterFamily::ClusterFamily(ServerFamily* server_family) : server_family_(server_family) {
  CHECK_NOTNULL(server_family_);

  InitializeCluster();

  id_ = absl::GetFlag(FLAGS_cluster_node_id);
  if (id_.empty()) {
    id_ = server_family_->master_replid();
  } else if (IsClusterEmulated()) {
    LOG(ERROR) << "Setting --cluster_node_id in emulated mode is unsupported";
    exit(1);
  }
}

ClusterConfig* ClusterFamily::cluster_config() {
  return tl_cluster_config.get();
}

void ClusterFamily::Shutdown() {
  shard_set->pool()->at(0)->Await([this]() ABSL_LOCKS_EXCLUDED(set_config_mu) {
    PreparedToRemoveOutgoingMigrations outgoing_migrations;  // should be removed without mutex lock
    {
      util::fb2::LockGuard lk(set_config_mu);
      if (!tl_cluster_config)
        return;

      auto empty_config = tl_cluster_config->CloneWithoutMigrations();
      outgoing_migrations = TakeOutOutgoingMigrations(empty_config, tl_cluster_config);
      RemoveIncomingMigrations(empty_config->GetFinishedIncomingMigrations(tl_cluster_config));

      util::fb2::LockGuard migration_lk(migration_mu_);
      DCHECK(outgoing_migration_jobs_.empty());
      DCHECK(incoming_migrations_jobs_.empty());
    }
  });
}

std::optional<ClusterShardInfos> ClusterFamily::GetShardInfos(ConnectionContext* cntx) const {
  if (IsClusterEmulated()) {
    return {GetEmulatedShardInfo(cntx)};
  }

  if (tl_cluster_config != nullptr) {
    return GetConfigForStats(cntx);
  }
  return nullopt;
}

ClusterShardInfo ClusterFamily::GetEmulatedShardInfo(ConnectionContext* cntx) const {
  ClusterShardInfo info{.slot_ranges = SlotRanges({{.start = 0, .end = kMaxSlotNum}}),
                        .master = {},
                        .replicas = {},
                        .migrations = {}};

  optional<Replica::Summary> replication_info = server_family_->GetReplicaSummary();
  ServerState& etl = *ServerState::tlocal();
  if (!replication_info.has_value()) {
    DCHECK(etl.is_master);
    std::string cluster_announce_ip = absl::GetFlag(FLAGS_cluster_announce_ip);
    std::string preferred_endpoint =
        cluster_announce_ip.empty() ? cntx->conn()->LocalBindAddress() : cluster_announce_ip;
    uint16_t cluster_announce_port = absl::GetFlag(FLAGS_announce_port);
    uint16_t preferred_port = cluster_announce_port == 0
                                  ? static_cast<uint16_t>(absl::GetFlag(FLAGS_port))
                                  : cluster_announce_port;

    info.master = {.id = id_, .ip = preferred_endpoint, .port = preferred_port};

    if (cntx->conn()->IsPrivileged() || !absl::GetFlag(FLAGS_managed_service_info)) {
      for (const auto& replica : server_family_->GetDflyCmd()->GetReplicasRoleInfo()) {
        info.replicas.push_back({.id = replica.id,
                                 .ip = replica.address,
                                 .port = static_cast<uint16_t>(replica.listening_port)});
      }
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

void ClusterFamily::ClusterHelp(SinkReplyBuilder* builder) {
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
  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  return rb->SendSimpleStrArr(help_arr);
}

namespace {
void ClusterShardsImpl(const ClusterShardInfos& config, SinkReplyBuilder* builder) {
  // For more details https://redis.io/commands/cluster-shards/
  constexpr unsigned int kEntrySize = 4;
  auto* rb = static_cast<RedisReplyBuilder*>(builder);

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

    rb->StartArray(shard.slot_ranges.Size() * 2);
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

void ClusterFamily::ClusterShards(SinkReplyBuilder* builder, ConnectionContext* cntx) {
  auto shard_infos = GetShardInfos(cntx);
  if (shard_infos) {
    return ClusterShardsImpl(*shard_infos, builder);
  }
  return builder->SendError(kClusterNotConfigured);
}

namespace {
void ClusterSlotsImpl(const ClusterShardInfos& config, SinkReplyBuilder* builder) {
  // For more details https://redis.io/commands/cluster-slots/
  auto* rb = static_cast<RedisReplyBuilder*>(builder);

  auto WriteNode = [&](const ClusterNodeInfo& node) {
    constexpr unsigned int kNodeSize = 3;
    rb->StartArray(kNodeSize);
    rb->SendBulkString(node.ip);
    rb->SendLong(node.port);
    rb->SendBulkString(node.id);
  };

  unsigned int slot_ranges = 0;
  for (const auto& shard : config) {
    slot_ranges += shard.slot_ranges.Size();
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

void ClusterFamily::ClusterSlots(SinkReplyBuilder* builder, ConnectionContext* cntx) {
  auto shard_infos = GetShardInfos(cntx);
  if (shard_infos) {
    return ClusterSlotsImpl(*shard_infos, builder);
  }
  return builder->SendError(kClusterNotConfigured);
}

namespace {
void ClusterNodesImpl(const ClusterShardInfos& config, string_view my_id,
                      SinkReplyBuilder* builder) {
  // For more details https://redis.io/commands/cluster-nodes/

  string result;

  auto WriteNode = [&](const ClusterNodeInfo& node, string_view role, string_view master_id,
                       const SlotRanges& ranges) {
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

  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  return rb->SendBulkString(result);
}
}  // namespace

void ClusterFamily::ClusterNodes(SinkReplyBuilder* builder, ConnectionContext* cntx) {
  auto shard_infos = GetShardInfos(cntx);
  if (shard_infos) {
    return ClusterNodesImpl(*shard_infos, id_, builder);
  }
  return builder->SendError(kClusterNotConfigured);
}

namespace {
void ClusterInfoImpl(const ClusterShardInfos& config, SinkReplyBuilder* builder) {
  std::string msg;
  auto append = [&msg](absl::AlphaNum a1, absl::AlphaNum a2) {
    // Separate lines with \r\n, not \n, see #2726
    absl::StrAppend(&msg, a1, ":", a2, "\r\n");
  };

  // Initialize response variables to emulated mode.
  string_view state = "ok"sv;
  SlotId slots_assigned = kMaxSlotNum + 1;
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

      if (!shard_config.slot_ranges.Empty()) {
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
  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  rb->SendBulkString(msg);
}
}  // namespace

void ClusterFamily::ClusterInfo(SinkReplyBuilder* builder, ConnectionContext* cntx) {
  auto shard_infos = GetShardInfos(cntx);
  return ClusterInfoImpl(shard_infos.value_or(ClusterShardInfos{}), builder);
}

void ClusterFamily::KeySlot(CmdArgList args, SinkReplyBuilder* builder) {
  if (args.size() != 2) {
    return builder->SendError(WrongNumArgsError("CLUSTER KEYSLOT"));
  }

  SlotId id = dfly::KeySlot(ArgS(args, 1));
  return builder->SendLong(id);
}

void ClusterFamily::Cluster(CmdArgList args, const CommandContext& cmd_cntx) {
  // In emulated cluster mode, all slots are mapped to the same host, and number of cluster
  // instances is thus 1.
  string sub_cmd = absl::AsciiStrToUpper(ArgS(args, 0));

  auto* builder = cmd_cntx.rb;
  if (!IsClusterEnabledOrEmulated()) {
    return builder->SendError(kClusterDisabled);
  }

  if (sub_cmd == "KEYSLOT") {
    return KeySlot(args, builder);
  }

  if (args.size() > 1) {
    return builder->SendError(WrongNumArgsError(absl::StrCat("CLUSTER ", sub_cmd)));
  }

  if (sub_cmd == "HELP") {
    return ClusterHelp(builder);
  } else if (sub_cmd == "MYID") {
    return ClusterMyId(builder);
  } else if (sub_cmd == "SHARDS") {
    return ClusterShards(builder, cmd_cntx.conn_cntx);
  } else if (sub_cmd == "SLOTS") {
    return ClusterSlots(builder, cmd_cntx.conn_cntx);
  } else if (sub_cmd == "NODES") {
    return ClusterNodes(builder, cmd_cntx.conn_cntx);
  } else if (sub_cmd == "INFO") {
    return ClusterInfo(builder, cmd_cntx.conn_cntx);
  } else {
    return builder->SendError(facade::UnknownSubCmd(sub_cmd, "CLUSTER"), facade::kSyntaxErrType);
  }
}

void ClusterFamily::ReadOnly(CmdArgList args, const CommandContext& cmd_cntx) {
  if (!IsClusterEmulated()) {
    return cmd_cntx.rb->SendError(kClusterDisabled);
  }
  cmd_cntx.rb->SendOk();
}

void ClusterFamily::ReadWrite(CmdArgList args, const CommandContext& cmd_cntx) {
  if (!IsClusterEmulated()) {
    return cmd_cntx.rb->SendError(kClusterDisabled);
  }
  cmd_cntx.rb->SendOk();
}

void ClusterFamily::DflyCluster(CmdArgList args, const CommandContext& cmd_cntx) {
  auto* builder = cmd_cntx.rb;
  auto* cntx = cmd_cntx.conn_cntx;
  if (!(IsClusterEnabled() || (IsClusterEmulated() && cntx->journal_emulated))) {
    return builder->SendError("Cluster is disabled. Use --cluster_mode=yes to enable.");
  }

  if (cntx->conn()) {
    VLOG(2) << "Got DFLYCLUSTER command (" << cntx->conn()->GetClientId() << "): " << args;
  } else {
    VLOG(2) << "Got DFLYCLUSTER command (NO_CLIENT_ID): " << args;
  }

  string sub_cmd = absl::AsciiStrToUpper(ArgS(args, 0));
  args.remove_prefix(1);  // remove subcommand name
  if (sub_cmd == "GETSLOTINFO") {
    return DflyClusterGetSlotInfo(args, builder);
  } else if (sub_cmd == "CONFIG") {
    return DflyClusterConfig(args, builder, cntx);
  } else if (sub_cmd == "FLUSHSLOTS") {
    return DflyClusterFlushSlots(args, builder);
  } else if (sub_cmd == "SLOT-MIGRATION-STATUS") {
    return DflySlotMigrationStatus(args, builder);
  }

  return builder->SendError(UnknownSubCmd(sub_cmd, "DFLYCLUSTER"), kSyntaxErrType);
}

void ClusterFamily::ClusterMyId(SinkReplyBuilder* builder) {
  builder->SendSimpleString(id_);
}

namespace {

void DeleteSlots(const SlotRanges& slots_ranges) {
  if (slots_ranges.Empty()) {
    return;
  }

  auto cb = [&](auto*) {
    EngineShard* shard = EngineShard::tlocal();
    if (shard == nullptr)
      return;

    namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id()).FlushSlots(slots_ranges);
  };
  shard_set->pool()->AwaitFiberOnAll(std::move(cb));
}

void WriteFlushSlotsToJournal(const SlotRanges& slot_ranges) {
  if (slot_ranges.Empty()) {
    return;
  }

  // Build args
  vector<string> args;
  args.reserve(slot_ranges.Size() + 1);
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
                         Payload("DFLYCLUSTER", args_view));
  };
  shard_set->pool()->AwaitFiberOnAll(std::move(cb));
}
}  // namespace

void ClusterFamily::DflyClusterConfig(CmdArgList args, SinkReplyBuilder* builder,
                                      ConnectionContext* cntx) {
  if (args.size() != 1) {
    return builder->SendError(WrongNumArgsError("DFLYCLUSTER CONFIG"));
  }

  string_view json_str = ArgS(args, 0);
  shared_ptr<ClusterConfig> new_config = ClusterConfig::CreateFromConfig(id_, json_str);
  if (new_config == nullptr) {
    LOG(WARNING) << "Can't set cluster config";
    return builder->SendError("Invalid cluster configuration.");
  } else if (tl_cluster_config && tl_cluster_config->GetConfig() == new_config->GetConfig()) {
    return builder->SendOk();
  }

  PreparedToRemoveOutgoingMigrations outgoing_migrations;  // should be removed without mutex lock

  {
    VLOG(1) << "Setting new cluster config: " << json_str;
    util::fb2::LockGuard gu(set_config_mu);

    outgoing_migrations = TakeOutOutgoingMigrations(new_config, tl_cluster_config);
    RemoveIncomingMigrations(new_config->GetFinishedIncomingMigrations(tl_cluster_config));

    SlotRanges enable_slots, disable_slots;

    {
      util::fb2::LockGuard lk(migration_mu_);
      // If migration state is changed simultaneously, the changes to config will be applied after
      // set_config_mu is unlocked and even if we apply the same changes 2 times it's not a problem
      for (const auto& m : incoming_migrations_jobs_) {
        if (m->GetState() == MigrationState::C_FINISHED) {
          enable_slots.Merge(m->GetSlots());
        }
      }
      for (const auto& m : outgoing_migration_jobs_) {
        if (m->GetState() == MigrationState::C_FINISHED) {
          disable_slots.Merge(m->GetSlots());
        }
      }
    }

    new_config = new_config->CloneWithChanges(enable_slots, disable_slots);

    StartSlotMigrations(new_config->GetNewOutgoingMigrations(tl_cluster_config));

    SlotSet before = tl_cluster_config ? tl_cluster_config->GetOwnedSlots() : SlotSet(true);

    // Ignore blocked commands because we filter them with CancelBlockingOnThread
    DispatchTracker tracker{server_family_->GetNonPriviligedListeners(), cntx->conn(),
                            true /* ignore paused */, true /* ignore blocked */};

    auto blocking_filter = [&new_config](ArgSlice keys) {
      bool moved =
          any_of(keys.begin(), keys.end(), [&](auto k) { return !new_config->IsMySlot(k); });
      return moved ? OpStatus::KEY_MOVED : OpStatus::OK;
    };

    auto cb = [this, &tracker, &new_config, blocking_filter](util::ProactorBase*) {
      server_family_->CancelBlockingOnThread(blocking_filter);
      tl_cluster_config = new_config;
      tracker.TrackOnThread();
    };

    server_family_->service().proactor_pool().AwaitFiberOnAll(std::move(cb));
    DCHECK(tl_cluster_config != nullptr);

    if (!tracker.Wait(absl::Seconds(1))) {
      LOG(WARNING) << "Cluster config change timed for: " << MyID();
    }

    SlotSet after = tl_cluster_config->GetOwnedSlots();
    if (ServerState::tlocal()->is_master) {
      auto deleted_slots = (before.GetRemovedSlots(after)).ToSlotRanges();
      deleted_slots.Merge(outgoing_migrations.slot_ranges);
      DeleteSlots(deleted_slots);
      LOG_IF(INFO, !deleted_slots.Empty())
          << "Flushing newly unowned slots: " << deleted_slots.ToString();
      WriteFlushSlotsToJournal(deleted_slots);
    }
  }

  return builder->SendOk();
}

void ClusterFamily::DflyClusterGetSlotInfo(CmdArgList args, SinkReplyBuilder* builder) {
  CmdArgParser parser(args);
  parser.ExpectTag("SLOTS");
  auto* rb = static_cast<RedisReplyBuilder*>(builder);

  vector<std::pair<SlotId, SlotStats>> slots_stats;
  do {
    auto sid = parser.Next<uint32_t>();
    if (sid > kMaxSlotNum)
      return builder->SendError("Invalid slot id");
    slots_stats.emplace_back(sid, SlotStats{});
  } while (parser.HasNext());

  if (auto err = parser.Error(); err)
    return builder->SendError(err->MakeReply());

  fb2::Mutex mu;

  auto cb = [&](auto*) ABSL_LOCKS_EXCLUDED(mu) {
    EngineShard* shard = EngineShard::tlocal();
    if (shard == nullptr)
      return;

    util::fb2::LockGuard lk(mu);
    for (auto& [slot, data] : slots_stats) {
      data += namespaces->GetDefaultNamespace().GetDbSlice(shard->shard_id()).GetSlotStats(slot);
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

void ClusterFamily::DflyClusterFlushSlots(CmdArgList args, SinkReplyBuilder* builder) {
  std::vector<SlotRange> slot_ranges;

  CmdArgParser parser(args);
  do {
    auto [slot_start, slot_end] = parser.Next<SlotId, SlotId>();
    slot_ranges.emplace_back(SlotRange{slot_start, slot_end});
  } while (parser.HasNext());

  if (auto err = parser.Error(); err)
    return builder->SendError(err->MakeReply());

  DeleteSlots(SlotRanges(std::move(slot_ranges)));

  return builder->SendOk();
}

void ClusterFamily::StartSlotMigrations(std::vector<MigrationInfo> migrations) {
  // Add validating and error processing
  for (auto& m : migrations) {
    auto outgoing_migration = CreateOutgoingMigration(std::move(m));
    outgoing_migration->Start();
  }
}

static string_view StateToStr(MigrationState state) {
  switch (state) {
    case MigrationState::C_CONNECTING:
      return "CONNECTING"sv;
    case MigrationState::C_SYNC:
      return "SYNC"sv;
    case MigrationState::C_ERROR:
      return "ERROR"sv;
    case MigrationState::C_FINISHED:
      return "FINISHED"sv;
  }
  DCHECK(false) << "Unknown State value " << static_cast<underlying_type_t<MigrationState>>(state);
  return "UNDEFINED_STATE"sv;
}

void ClusterFamily::DflySlotMigrationStatus(CmdArgList args, SinkReplyBuilder* builder) {
  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  CmdArgParser parser(args);

  util::fb2::LockGuard lk(migration_mu_);

  string_view node_id;
  if (parser.HasNext()) {
    node_id = parser.Next<std::string_view>();
    if (auto err = parser.Error(); err) {
      return builder->SendError(err->MakeReply());
    }
  }

  struct Reply {
    string_view direction;
    string node_id;
    string_view state;
    size_t keys_number;
    string error;
  };
  vector<Reply> reply;
  reply.reserve(incoming_migrations_jobs_.size() + outgoing_migration_jobs_.size());

  auto append_answer = [&reply](string_view direction, string node_id, string_view filter,
                                MigrationState state, size_t keys_number, string error) {
    if (filter.empty() || filter == node_id) {
      error = error.empty() ? "0" : error;
      reply.emplace_back(
          Reply{direction, std::move(node_id), StateToStr(state), keys_number, std::move(error)});
    }
  };

  for (const auto& m : incoming_migrations_jobs_) {
    // TODO add error status
    append_answer("in", m->GetSourceID(), node_id, m->GetState(), m->GetKeyCount(),
                  m->GetErrorStr());
  }
  for (const auto& m : outgoing_migration_jobs_) {
    append_answer("out", m->GetMigrationInfo().node_info.id, node_id, m->GetState(),
                  m->GetKeyCount(), m->GetErrorStr());
  }

  rb->StartArray(reply.size());
  for (const auto& r : reply) {
    rb->StartArray(5);
    rb->SendBulkString(r.direction);
    rb->SendBulkString(r.node_id);
    rb->SendBulkString(r.state);
    rb->SendLong(r.keys_number);
    rb->SendBulkString(r.error);
  }
}

void ClusterFamily::DflyMigrate(CmdArgList args, const CommandContext& cmd_cntx) {
  string sub_cmd = absl::AsciiStrToUpper(ArgS(args, 0));

  args.remove_prefix(1);
  auto* builder = cmd_cntx.rb;
  if (sub_cmd == "INIT") {
    InitMigration(args, builder);
  } else if (sub_cmd == "FLOW") {
    DflyMigrateFlow(args, builder, cmd_cntx.conn_cntx);
  } else if (sub_cmd == "ACK") {
    DflyMigrateAck(args, builder);
  } else {
    builder->SendError(facade::UnknownSubCmd(sub_cmd, "DFLYMIGRATE"), facade::kSyntaxErrType);
  }
}

std::shared_ptr<IncomingSlotMigration> ClusterFamily::GetIncomingMigration(
    std::string_view source_id) {
  util::fb2::LockGuard lk(migration_mu_);
  for (const auto& mj : incoming_migrations_jobs_) {
    if (mj->GetSourceID() == source_id) {
      return mj;
    }
  }
  return nullptr;
}

ClusterFamily::PreparedToRemoveOutgoingMigrations::~PreparedToRemoveOutgoingMigrations() = default;

[[nodiscard]] ClusterFamily::PreparedToRemoveOutgoingMigrations
ClusterFamily::TakeOutOutgoingMigrations(shared_ptr<ClusterConfig> new_config,
                                         shared_ptr<ClusterConfig> old_config) {
  auto migrations = new_config->GetFinishedOutgoingMigrations(old_config);
  util::fb2::LockGuard lk(migration_mu_);
  SlotRanges removed_slots;
  PreparedToRemoveOutgoingMigrations res;
  for (const auto& m : migrations) {
    auto it = std::find_if(outgoing_migration_jobs_.begin(), outgoing_migration_jobs_.end(),
                           [&m](const auto& om) {
                             // we can have only one migration per target-source pair
                             return m.node_info.id == om->GetMigrationInfo().node_info.id;
                           });
    DCHECK(it != outgoing_migration_jobs_.end());
    DCHECK(it->get() != nullptr);
    OutgoingMigration& migration = *it->get();
    const auto& slots = migration.GetSlots();
    removed_slots.Merge(slots);
    LOG(INFO) << "Outgoing migration cancelled: slots " << slots.ToString() << " to "
              << migration.GetHostIp() << ":" << migration.GetPort();
    migration.Finish();
    res.migrations.push_back(std::move(*it));
    outgoing_migration_jobs_.erase(it);
  }

  // Flush non-owned migrations
  SlotSet migration_slots(removed_slots);
  res.slot_ranges = migration_slots.GetRemovedSlots(new_config->GetOwnedSlots()).ToSlotRanges();

  // Flushing of removed slots is done outside this function.
  return res;
}

namespace {

// returns removed incoming migration
bool RemoveIncomingMigrationImpl(std::vector<std::shared_ptr<IncomingSlotMigration>>& jobs,
                                 string_view source_id) {
  auto it = std::find_if(jobs.begin(), jobs.end(), [source_id](const auto& im) {
    // we can have only one migration per target-source pair
    return source_id == im->GetSourceID();
  });
  if (it == jobs.end()) {
    return false;
  }
  DCHECK(it->get() != nullptr);
  std::shared_ptr<IncomingSlotMigration> migration = *it;

  // Flush non-owned migrations
  SlotSet migration_slots(migration->GetSlots());
  SlotSet removed = migration_slots.GetRemovedSlots(tl_cluster_config->GetOwnedSlots());

  migration->Stop();
  // all migration fibers has migration shared_ptr so the object can be removed later
  jobs.erase(it);

  // TODO make it outside in one run with other slots that should be flushed
  if (!removed.Empty()) {
    auto removed_ranges = removed.ToSlotRanges();
    LOG_IF(WARNING, migration->GetState() == MigrationState::C_FINISHED)
        << "Flushing slots of removed FINISHED migration " << migration->GetSourceID()
        << ", slots: " << removed_ranges.ToString();
    DeleteSlots(removed_ranges);
  }

  return true;
}
}  // namespace

void ClusterFamily::RemoveIncomingMigrations(const std::vector<MigrationInfo>& migrations) {
  util::fb2::LockGuard lk(migration_mu_);
  for (const auto& m : migrations) {
    RemoveIncomingMigrationImpl(incoming_migrations_jobs_, m.node_info.id);
    VLOG(1) << "Migration was canceled from: " << m.node_info.id;
  }
}

void ClusterFamily::InitMigration(CmdArgList args, SinkReplyBuilder* builder) {
  VLOG(1) << "Create incoming migration, args: " << args;
  CmdArgParser parser{args};

  auto [source_id, flows_num] = parser.Next<string_view, uint32_t>();

  std::vector<SlotRange> slots;
  do {
    auto [slot_start, slot_end] = parser.Next<SlotId, SlotId>();
    slots.emplace_back(SlotRange{slot_start, slot_end});
  } while (parser.HasNext());

  if (auto err = parser.Error(); err)
    return builder->SendError(err->MakeReply());

  SlotRanges slot_ranges(std::move(slots));

  const auto& incoming_migrations = cluster_config()->GetIncomingMigrations();
  bool found = any_of(incoming_migrations.begin(), incoming_migrations.end(),
                      [&source_id, &slot_ranges](const MigrationInfo& info) {
                        return info.node_info.id == source_id && info.slot_ranges == slot_ranges;
                      });
  if (!found) {
    VLOG(1) << "Unrecognized incoming migration from " << source_id;
    return builder->SendSimpleString(OutgoingMigration::kUnknownMigration);
  }

  VLOG(1) << "Init migration " << source_id;

  util::fb2::LockGuard lk(migration_mu_);
  auto was_removed = RemoveIncomingMigrationImpl(incoming_migrations_jobs_, source_id);
  LOG_IF(WARNING, was_removed) << "Reinit issued for migration from:" << source_id;

  incoming_migrations_jobs_.emplace_back(make_shared<IncomingSlotMigration>(
      string(source_id), &server_family_->service(), std::move(slot_ranges), flows_num));

  return builder->SendOk();
}

std::shared_ptr<OutgoingMigration> ClusterFamily::CreateOutgoingMigration(MigrationInfo info) {
  util::fb2::LockGuard lk(migration_mu_);
  auto migration = make_shared<OutgoingMigration>(std::move(info), this, server_family_);
  outgoing_migration_jobs_.emplace_back(migration);
  return migration;
}

void ClusterFamily::DflyMigrateFlow(CmdArgList args, SinkReplyBuilder* builder,
                                    ConnectionContext* cntx) {
  CmdArgParser parser{args};
  auto [source_id, shard_id] = parser.Next<std::string_view, uint32_t>();

  if (auto err = parser.Error(); err) {
    return builder->SendError(err->MakeReply());
  }

  auto host_ip = cntx->conn()->RemoteEndpointAddress();

  VLOG(1) << "Create flow " << source_id << " shard_id: " << shard_id;

  cntx->conn()->SetName(absl::StrCat("migration_flow_", source_id));

  auto migration = GetIncomingMigration(source_id);
  if (!migration) {
    return builder->SendError(kIdNotFound);
  }

  DCHECK(cntx->sync_dispatch);
  // we do this to be ignored by the dispatch tracker
  // TODO provide a more clear approach
  cntx->sync_dispatch = false;

  builder->SendOk();

  migration->StartFlow(shard_id, cntx->conn()->socket());
}

void ClusterFamily::ApplyMigrationSlotRangeToConfig(std::string_view node_id,
                                                    const SlotRanges& slots, bool is_incoming) {
  VLOG(1) << "Update config for slots ranges: " << slots.ToString() << " for " << MyID() << " : "
          << node_id;
  util::fb2::LockGuard gu(set_config_mu);
  util::fb2::LockGuard lk(migration_mu_);

  bool is_migration_valid = false;
  if (is_incoming) {
    for (const auto& mj : incoming_migrations_jobs_) {
      if (mj->GetSourceID() == node_id && slots == mj->GetSlots()) {
        is_migration_valid = true;
        break;
      }
    }
  } else {
    for (const auto& mj : outgoing_migration_jobs_) {
      if (mj->GetMigrationInfo().node_info.id == node_id &&
          mj->GetMigrationInfo().slot_ranges == slots) {
        is_migration_valid = true;
        break;
      }
    }
  }
  if (!is_migration_valid) {
    LOG(WARNING) << "Config wasn't updated for slots ranges: " << slots.ToString() << " for "
                 << MyID() << " : " << node_id;
    return;
  }

  auto new_config = is_incoming ? tl_cluster_config->CloneWithChanges(slots, {})
                                : tl_cluster_config->CloneWithChanges({}, slots);

  // we don't need to use DispatchTracker here because for IncomingMingration we don't have
  // connectionas that should be tracked and for Outgoing migration we do it under Pause
  server_family_->service().proactor_pool().AwaitFiberOnAll(
      [&new_config](util::ProactorBase*) { tl_cluster_config = new_config; });
  DCHECK(tl_cluster_config != nullptr);
  VLOG(1) << "Config is updated for slots ranges: " << slots.ToString() << " for " << MyID()
          << " : " << node_id;
}

void ClusterFamily::DflyMigrateAck(CmdArgList args, SinkReplyBuilder* builder) {
  CmdArgParser parser{args};
  auto [source_id, attempt] = parser.Next<std::string_view, long>();

  if (auto err = parser.Error(); err) {
    return builder->SendError(err->MakeReply());
  }

  VLOG(1) << "DFLYMIGRATE ACK" << args;
  auto in_migrations = tl_cluster_config->GetIncomingMigrations();
  auto m_it =
      std::find_if(in_migrations.begin(), in_migrations.end(),
                   [source_id = source_id](const auto& m) { return m.node_info.id == source_id; });
  if (m_it == in_migrations.end()) {
    LOG(WARNING) << "migration isn't in config";
    return builder->SendError(OutgoingMigration::kUnknownMigration);
  }

  auto migration = GetIncomingMigration(source_id);
  if (!migration)
    return builder->SendError(kIdNotFound);

  if (!migration->Join(attempt)) {
    return builder->SendError("Join timeout happened");
  }

  ApplyMigrationSlotRangeToConfig(migration->GetSourceID(), migration->GetSlots(), true);

  return builder->SendLong(attempt);
}

void ClusterFamily::PauseAllIncomingMigrations(bool pause) {
  util::fb2::LockGuard lk(migration_mu_);
  LOG_IF(ERROR, incoming_migrations_jobs_.empty()) << "No incoming migrations!";
  for (auto& im : incoming_migrations_jobs_) {
    im->Pause(pause);
  }
}

using EngineFunc = void (ClusterFamily::*)(CmdArgList args, const CommandContext& cmd_cntx);

inline CommandId::Handler3 HandlerFunc(ClusterFamily* se, EngineFunc f) {
  return [=](CmdArgList args, const CommandContext& cmd_cntx) { return (se->*f)(args, cmd_cntx); };
}

#define HFUNC(x) SetHandler(HandlerFunc(this, &ClusterFamily::x))

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

}  // namespace dfly::cluster
