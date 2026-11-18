// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/replication_family.h"

#include <absl/strings/ascii.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <chrono>

#include "base/flags.h"
#include "base/logging.h"
#include "facade/cmd_arg_parser.h"
#include "facade/dragonfly_connection.h"
#include "facade/error.h"
#include "facade/reply_builder.h"
#include "facade/string_socket.h"
#include "server/acl/acl_commands_def.h"
#include "server/cluster/cluster_family.h"
#include "server/cluster_support.h"
#include "server/command_registry.h"
#include "server/common.h"
#include "server/conn_context.h"
#include "server/dflycmd.h"
#include "server/engine_shard_set.h"
#include "server/journal/journal.h"
#include "server/server_state.h"
#include "server/version.h"

ABSL_DECLARE_FLAG(bool, aof);
ABSL_DECLARE_FLAG(bool, experimental_cascaded_partial_sync);
ABSL_DECLARE_FLAG(bool, info_replication_valkey_compatible);

namespace dfly {

using namespace std;
using namespace facade;
using namespace util;
using absl::GetFlag;

namespace rng = std::ranges;

namespace {

struct ReplicaOfArgs {
  string host;
  uint16_t port;
  std::optional<cluster::SlotRange> slot_range;
  static nonstd::expected<ReplicaOfArgs, ErrorReply> FromCmdArgs(facade::ParsedArgs args);
  bool IsReplicaOfNoOne() const {
    return port == 0;
  }
  friend std::ostream& operator<<(std::ostream& os, const ReplicaOfArgs& args) {
    if (args.IsReplicaOfNoOne()) {
      return os << "NO ONE";
    }
    os << args.host << ":" << args.port;
    if (args.slot_range.has_value()) {
      os << " SLOTS [" << args.slot_range.value().start << "-" << args.slot_range.value().end
         << "]";
    }
    return os;
  }
};

nonstd::expected<ReplicaOfArgs, ErrorReply> ReplicaOfArgs::FromCmdArgs(facade::ParsedArgs args) {
  ReplicaOfArgs replicaof_args;
  CmdArgParser parser(args);

  if (parser.Check("NO")) {
    parser.ExpectTag("ONE");
    replicaof_args.port = 0;
  } else {
    replicaof_args.host = parser.Next<string>();
    replicaof_args.port = parser.Next<Positive<uint16_t>>("port is out of range");
    if (auto err = parser.TakeError(); err) {
      return nonstd::make_unexpected(ErrorReply("port is out of range"));
    }
    if (parser.HasNext()) {
      auto [slot_start, slot_end] = parser.Next<SlotId, SlotId>();
      replicaof_args.slot_range = cluster::SlotRange{slot_start, slot_end};
      if (auto err = parser.TakeError(); err || !replicaof_args.slot_range->IsValid()) {
        return nonstd::make_unexpected(ErrorReply("Invalid slot range"));
      }
    }
  }

  if (auto err = parser.TakeError(); err) {
    return nonstd::make_unexpected(err.MakeReply());
  }
  return replicaof_args;
}

// Returns {acked, running} for the supplied replica snapshot, reading ACKs on their owning shards.
pair<uint32_t, uint32_t> CountAckedReplicas(
    const vector<shared_ptr<DflyCmd::ReplicaInfo>>& replicas, const vector<LSN>& target_lsns,
    atomic<bool>* role_changed) ABSL_NO_THREAD_SAFETY_ANALYSIS {
  size_t n = replicas.size();
  vector<vector<bool>> shard_acked(target_lsns.size(), vector<bool>(n, true));

  shard_set->RunBriefInParallel([&](EngineShard* shard) ABSL_NO_THREAD_SAFETY_ANALYSIS {
    if (!ServerState::IsMaster()) {
      role_changed->store(true, std::memory_order_relaxed);
      return;
    }
    uint32_t sid = shard->shard_id();
    if (!shard->journal() || target_lsns[sid] == 0)
      return;

    for (size_t i = 0; i < n; i++) {
      shard_acked[sid][i] = replicas[i]->GetFlow(sid).last_acked_lsn >= target_lsns[sid];
    }
  });

  uint32_t acked = 0, running = 0;
  for (size_t i = 0; i < n; i++) {
    if (!replicas[i]->GetExecState().IsRunning())
      continue;
    ++running;
    if (rng::all_of(shard_acked, [i](const auto& shard) { return shard[i]; }))
      ++acked;
  }
  return {acked, running};
}

}  // namespace

ReplicationFamily::ReplicationFamily(ReplicaManager* replica_manager, DflyCmd* dfly_cmd)
    : replica_manager_(replica_manager), dfly_cmd_(dfly_cmd) {
}

void ReplicationFamily::AddReplicaOf(CmdArgParser parser, CommandContext* cmd_cntx) {
  if (!replica_manager_->IsReplica()) {
    return cmd_cntx->SendError(
        "Calling ADDREPLICAOFF allowed only after server is already a replica");
  }

  auto replicaof_args = ReplicaOfArgs::FromCmdArgs(parser.UnparsedArgs());
  if (!replicaof_args.has_value()) {
    return cmd_cntx->SendError(replicaof_args.error());
  }
  if (replicaof_args->IsReplicaOfNoOne()) {
    return cmd_cntx->SendError("ADDREPLICAOF does not support no one");
  }
  LOG(INFO) << "Add Replica " << *replicaof_args;

  GenericError ec = replica_manager_->AddClusterReplica(replicaof_args->host, replicaof_args->port,
                                                        replicaof_args->slot_range);
  if (ec) {
    return cmd_cntx->SendError(ec.Format());
  }
  cmd_cntx->rb()->SendOk();
}

void ReplicationFamily::ReplicaOf(CmdArgParser parser, CommandContext* cmd_cntx) {
  ReplicaOfInternal(parser.UnparsedArgs(), cmd_cntx, ReplicaManager::kReturnOnError);
}

void ReplicationFamily::Replicate(string_view host, string_view port) {
  StringVec replicaof_params{string(host), string(port)};

  CmdArgVec args_vec;
  for (auto& s : replicaof_params) {
    args_vec.emplace_back(MutableSlice{s.data(), s.size()});
  }
  CmdArgList args_list = absl::MakeSpan(args_vec);
  facade::StringSocket sock;
  sock.set_null(true);
  facade::RedisReplyBuilder rb(&sock);
  CommandContext cmd_cntx{&rb, nullptr};
  ReplicaOfInternal(args_list, &cmd_cntx, ReplicaManager::kContinueReplication);
}

void ReplicationFamily::ReplicaOfInternal(facade::ParsedArgs args, CommandContext* cmd_cntx,
                                          ReplicaManager::ActionOnConnectionFail on_error) {
  auto replicaof_args = ReplicaOfArgs::FromCmdArgs(args);
  if (!replicaof_args.has_value()) {
    return cmd_cntx->SendError(replicaof_args.error());
  }

  LOG(INFO) << "Initiate replication with: " << *replicaof_args;
  // This is a "weak" check. For example, if the node is already a replica,
  // it could be the case that one of the flows disconnects. The MainReplicationFiber
  // will then loop and if it can't partial sync it will enter LOADING state because of
  // full sync. Note that the fiber is not aware of the replica manager mutex so even
  // if that mutex is locked below before any state check we can't really enforce
  // that the old replication fiber won't try to full sync and update the state to LOADING.
  // What is more here is that we always call `replica->Stop()`. So even if we end up in the
  // scenario described, the semantics are well defined. First, cancel the old replica and
  // move on with the new one. Cancelation will be slower and ReplicaOf() will
  // induce higher latency -- but that's ok because it's an highly improbable flow with
  // well defined semantics.
  ServerState* ss = ServerState::tlocal();

  if (ServerState::IsMaster() && ss->gstate() == GlobalState::LOADING) {
    return cmd_cntx->SendError(kLoadingErr);
  }

  // replicaof no one
  if (replicaof_args->IsReplicaOfNoOne()) {
    replica_manager_->PromoteToMaster();
    return cmd_cntx->rb()->SendOk();
  }

  // A full sync loads data bypassing the journal, so the AOF would miss it.
  if (GetFlag(FLAGS_aof))
    return cmd_cntx->SendError("REPLICAOF is not supported with --aof");

  GenericError ec = replica_manager_->StartReplication(replicaof_args->host, replicaof_args->port,
                                                       replicaof_args->slot_range, on_error);
  if (ec) {
    return cmd_cntx->SendError(ec.Format());
  }

  cmd_cntx->rb()->SendOk();
}

// REPLTAKEOVER <seconds> [SAVE]
// SAVE is used only by tests.
void ReplicationFamily::ReplTakeOver(facade::CmdArgParser parser, CommandContext* cmd_cntx) {
  VLOG(1) << "ReplTakeOver start";

  int timeout_sec = parser.Next<int>();
  bool save_flag = static_cast<bool>(parser.Check("SAVE"));

  auto* builder = cmd_cntx->rb();
  if (parser.HasNext())
    return cmd_cntx->SendError(absl::StrCat("Unsupported option:", string_view(parser.Next())));

  RETURN_ON_PARSE_ERROR(parser, cmd_cntx);

  // We allow zero timeouts for tests.
  if (timeout_sec < 0) {
    return cmd_cntx->SendError("timeout is negative");
  }

  GenericError ec = replica_manager_->TakeOver(timeout_sec, save_flag);
  if (ec) {
    return cmd_cntx->SendError(ec.Format());
  }
  return builder->SendOk();
}

void ReplicationFamily::ReplConf(CmdArgParser parser, CommandContext* cmd_cntx) {
  facade::ParsedArgs args = parser.UnparsedArgs();
  auto* builder = cmd_cntx->rb();

  // Replicating a replica (cascaded replication) is only supported behind the experimental flag.
  if (!absl::GetFlag(FLAGS_experimental_cascaded_partial_sync)) {
    if (replica_manager_->IsReplica()) {
      return cmd_cntx->SendError("Replicating a replica is unsupported");
    }
  }

  auto err_cb = [&]() mutable {
    LOG(ERROR) << "Error in receiving command, num args: " << args.size();
    cmd_cntx->SendError(kSyntaxErr);
  };

  if (args.size() % 2 == 1)
    return err_cb();

  ConnectionContext* cntx = cmd_cntx->server_conn_cntx();
  for (unsigned i = 0; i < args.size(); i += 2) {
    DCHECK_LT(i + 1, args.size());

    string cmd = absl::AsciiStrToUpper(args[i]);
    std::string_view arg = args[i + 1];
    if (cmd == "CAPA") {
      if (arg == "dragonfly" && args.size() == 2 && i == 0) {
        auto [sid, flow_count] = dfly_cmd_->CreateSyncSession(&cntx->conn_state);
        cntx->conn()->SetName(absl::StrCat("repl_ctrl_", sid));

        string sync_id = absl::StrCat("SYNC", sid);
        cntx->conn_state.replication_info.repl_session_id = sid;

        cntx->replica_conn = true;

        // The response for 'capa dragonfly' is <master_repl_id> <syncid> <num_shards> <version>
        // <lineage_id> <announced_ip> <announced_port>. It may only grow at the end, and the ip
        // is a bulk string because it is operator-supplied.
        std::string lineage_id = replica_manager_->GetLineageId();
        auto announced = cluster::ClusterFamily::AnnouncedNodeInfo(cntx->conn());

        auto* rb = static_cast<RedisReplyBuilder*>(builder);
        rb->StartArray(7);
        rb->SendSimpleString(replica_manager_->master_replid());
        rb->SendSimpleString(sync_id);
        rb->SendLong(flow_count);
        rb->SendLong(unsigned(DflyVersion::CURRENT_VER));
        rb->SendSimpleString(lineage_id);
        rb->SendBulkString(announced.ip);
        rb->SendLong(announced.port);
        return;
      }
    } else if (cmd == "LISTENING-PORT") {
      uint32_t replica_listening_port;
      if (!absl::SimpleAtoi(arg, &replica_listening_port)) {
        return cmd_cntx->SendError(kInvalidIntErr);
      }
      cntx->conn_state.replication_info.repl_listening_port = replica_listening_port;
      // We set a default value of ip_address here, because LISTENING-PORT is a mandatory field
      // but IP-ADDRESS is optional
      if (cntx->conn_state.replication_info.repl_ip_address.empty()) {
        cntx->conn_state.replication_info.repl_ip_address = cntx->conn()->RemoteEndpointAddress();
      }
    } else if (cmd == "IP-ADDRESS") {
      cntx->conn_state.replication_info.repl_ip_address = arg;
    } else if (cmd == "CLIENT-ID" && args.size() == 2) {
      auto info = dfly_cmd_->GetReplicaInfoFromConnection(&cntx->conn_state);
      DCHECK(info != nullptr);
      if (info) {
        info->SetId(arg);
      }
    } else if (cmd == "CLIENT-VERSION" && args.size() == 2) {
      unsigned version;
      if (!absl::SimpleAtoi(arg, &version)) {
        return cmd_cntx->SendError(kInvalidIntErr);
      }
      dfly_cmd_->SetDflyClientVersion(&cntx->conn_state, DflyVersion(version));
    } else if (cmd == "ACK" && args.size() == 2) {
      // Don't send error/Ok back through the socket, because we don't want to interleave with
      // the journal writes that we write into the same socket.

      if (!cntx->master_repl_flow) {
        LOG(ERROR) << "No replication flow assigned";
        return;
      }

      uint64_t ack;
      if (!absl::SimpleAtoi(arg, &ack)) {
        LOG(ERROR) << "Bad int in REPLCONF ACK command! arg=" << arg;
        return;
      }
      VLOG(2) << "Received client ACK=" << ack;
      cntx->master_repl_flow->last_acked_lsn = ack;
      return;
    } else {
      VLOG(1) << "Error " << cmd << " " << arg << " " << args.size();
      return err_cb();
    }
  }

  return builder->SendOk();
}

// WAIT <num_replicas> <timeout_ms>
void ReplicationFamily::Wait(facade::CmdArgParser parser, CommandContext* cmd_cntx) {
  const auto start = chrono::steady_clock::now();
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());

  if (!ServerState::IsMaster()) {
    return cmd_cntx->SendError("WAIT cannot be used with replica instances.");
  }

  using NonNegInt = facade::FInt<int64_t{0}, std::numeric_limits<int64_t>::max()>;
  auto [num_replicas_arg, timeout_arg] = parser.Next<NonNegInt, NonNegInt>();
  RETURN_ON_PARSE_ERROR(parser, cmd_cntx);

  const int64_t num_replicas = num_replicas_arg;
  const int64_t timeout_ms = timeout_arg;

  // WAIT does not use Transaction's cancellation machinery; bound even an infinite timeout.
  constexpr auto kMaxWaitDuration = chrono::minutes(10);
  const auto timeout = timeout_ms == 0 ? kMaxWaitDuration : chrono::milliseconds(timeout_ms);

  // Clamp before converting milliseconds to the clock's finer-grained duration.
  const auto max_timeout =
      chrono::duration_cast<chrono::milliseconds>(chrono::steady_clock::time_point::max() - start);
  const auto deadline = start + min(timeout, max_timeout);

  auto replicas = dfly_cmd_->GetReplicaInfoSnapshot();
  if (replicas.empty())
    return rb->SendLong(0);

  uint32_t shard_count = shard_set->size();
  std::vector<LSN> target_lsns(shard_count, 0);
  std::atomic<bool> role_changed{false};

  auto* tx = cmd_cntx->tx();
  bool can_block = !tx || !tx->IsMulti();

  // Global shard LSNs cover at least this connection's writes without per-write bookkeeping.
  // Role changes update threads independently, so check the role on each shard before access.
  shard_set->RunBriefInParallel([&](EngineShard* shard) {
    if (!ServerState::IsMaster()) {
      role_changed.store(true, std::memory_order_relaxed);
      return;
    }
    if (!shard->journal())
      return;

    LSN last_sent_lsn = journal::GetLsn();
    // Record the currently issued LSN for this shard.
    target_lsns[shard->shard_id()] = last_sent_lsn;

    // Check if all replicas are already caught up to the last sent LSN for this shard.
    ShardId sid = shard->shard_id();
    bool acked = rng::all_of(replicas, [sid, last_sent_lsn](const auto& replica) {
      return replica->GetFlow(sid).last_acked_lsn >= last_sent_lsn;
    });

    // MULTI never blocks, so there is no point in expediting ACKs.
    if (!acked && can_block) {
      // PING accelerates ACKs. Try to expedite the acknowledgment from replicas.
      // If the queue is full, then we rely on periodic replica ACKs to eventually catch up.
      shard->GetSecondaryQueue()->TryAdd([] {
        if (ServerState::IsMaster() && EngineShard::tlocal()->journal())
          journal::RecordEntry(0, journal::Op::PING, 0, nullopt, {});
      });
    }
  });

  while (true) {
    auto [count, running] = CountAckedReplicas(replicas, target_lsns, &role_changed);

    // A role change invalidates the captured journal epoch, even if this thread is still master.
    if (role_changed.load(std::memory_order_relaxed)) {
      return cmd_cntx->SendError("WAIT cannot be used with replica instances.");
    }

    // MULTI never blocks. Once all running replicas in our fixed snapshot ACK, no more can join.
    auto now = chrono::steady_clock::now();
    if (!can_block || count >= num_replicas || count == running || now >= deadline)
      return rb->SendLong(count);

    // Do not hold up takeover/shutdown, including demotion through a partial sync.
    if (!ServerState::IsMaster() || ServerState::tlocal()->gstate() != GlobalState::ACTIVE)
      return rb->SendLong(count);

    ThisFiber::SleepUntil(min(deadline, now + 100ms));
  }
}

void ReplicationFamily::Role(facade::CmdArgParser /*parser*/, CommandContext* cmd_cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());

  // is_master alone is insufficient here: the summary is taken under the replica manager lock.
  auto replica_info = replica_manager_->GetSummary();
  if (!replica_info) {
    rb->StartArray(2);
    rb->SendBulkString("master");
    auto vec = dfly_cmd_->GetReplicasRoleInfo();
    rb->StartArray(vec.size());
    for (auto& data : vec) {
      rb->StartArray(3);
      rb->SendBulkString(data.address);
      rb->SendBulkString(absl::StrCat(data.listening_port));
      rb->SendBulkString(data.state);
    }

  } else {
    rb->StartArray(4 + replica_info->cl_repl_summary.size() * 3);
    rb->SendBulkString(GetFlag(FLAGS_info_replication_valkey_compatible) ? "slave" : "replica");

    auto send_replica_info = [rb](const Replica::Summary& rinfo) {
      rb->SendBulkString(rinfo.host);
      rb->SendBulkString(absl::StrCat(rinfo.port));
      if (rinfo.full_sync_done) {
        rb->SendBulkString(GetFlag(FLAGS_info_replication_valkey_compatible) ? "online"
                                                                             : "stable_sync");
      } else if (rinfo.full_sync_in_progress) {
        rb->SendBulkString("full_sync");
      } else if (rinfo.master_link_established) {
        rb->SendBulkString("preparation");
      } else {
        rb->SendBulkString("connecting");
      }
    };
    send_replica_info(replica_info->summary);
    for (const auto& summary : replica_info->cl_repl_summary) {
      send_replica_info(summary);
    }
  }
}

void ReplicationFamily::Dfly(facade::CmdArgParser parser, CommandContext* cmd_cntx) {
  dfly_cmd_->Run(parser, cmd_cntx);
}

using EngineFunc = void (ReplicationFamily::*)(CmdArgParser, CommandContext*);

inline CommandId::Handler HandlerFunc(ReplicationFamily* se, EngineFunc f) {
  return [=](CmdArgParser parser, CommandContext* cntx) { return (se->*f)(parser, cntx); };
}

#define HFUNC(x) SetHandler(HandlerFunc(this, &ReplicationFamily::x))

namespace acl {
constexpr uint32_t kSlaveOf = ADMIN | SLOW | DANGEROUS;
constexpr uint32_t kReplicaOf = ADMIN | SLOW | DANGEROUS;
constexpr uint32_t kReplTakeOver = DANGEROUS;
constexpr uint32_t kReplConf = ADMIN | SLOW | DANGEROUS;
constexpr uint32_t kRole = ADMIN | FAST | DANGEROUS;
constexpr uint32_t kWait = SLOW | CONNECTION | BLOCKING;
// TODO(check this)
constexpr uint32_t kDfly = ADMIN;
}  // namespace acl

void ReplicationFamily::Register(CommandRegistry* registry) {
  using CI = CommandId;
  constexpr auto kReplicaOpts = CO::LOADING | CO::ADMIN | CO::GLOBAL_TRANS;
  *registry << CI{"SLAVEOF", kReplicaOpts, 3, 0, 0, acl::kSlaveOf}.HFUNC(ReplicaOf)
            << CI{"REPLICAOF", kReplicaOpts, -3, 0, 0, acl::kReplicaOf}.HFUNC(ReplicaOf)
            << CI{"ADDREPLICAOF", kReplicaOpts, 5, 0, 0, acl::kReplicaOf}.HFUNC(AddReplicaOf)
            << CI{"REPLTAKEOVER", CO::ADMIN | CO::GLOBAL_TRANS, -2, 0, 0, acl::kReplTakeOver}.HFUNC(
                   ReplTakeOver)
            << CI{"REPLCONF", CO::ADMIN | CO::LOADING, -1, 0, 0, acl::kReplConf}.HFUNC(ReplConf)
            << CI{"WAIT", CO::NOSCRIPT | CO::BLOCKING, 3, 0, 0, acl::kWait}.HFUNC(Wait)
            << CI{"ROLE", CO::LOADING | CO::FAST | CO::NOSCRIPT, 1, 0, 0, acl::kRole}.HFUNC(Role)
            << CI{"DFLY", CO::ADMIN | CO::GLOBAL_TRANS | CO::HIDDEN, -2, 0, 0, acl::kDfly}.HFUNC(
                   Dfly);
}

}  // namespace dfly
