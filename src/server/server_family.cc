// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/server_family.h"

#include <absl/cleanup/cleanup.h>
#include <absl/random/random.h>  // for master_replid_ generation.
#include <absl/strings/match.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_replace.h>
#include <absl/strings/strip.h>
#include <croncpp.h>  // cron::cronexpr
#include <sys/resource.h>
#include <sys/utsname.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <optional>

#include "absl/strings/ascii.h"
#include "facade/error.h"
#include "slowlog.h"
#include "util/fibers/synchronization.h"

extern "C" {
#include "redis/redis_aux.h"
}

#include "base/flags.h"
#include "base/logging.h"
#include "core/compact_object.h"
#include "facade/cmd_arg_parser.h"
#include "facade/dragonfly_connection.h"
#include "facade/reply_builder.h"
#include "io/file_util.h"
#include "io/proc_reader.h"
#include "search/doc_index.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/debugcmd.h"
#include "server/detail/save_stages_controller.h"
#include "server/detail/snapshot_storage.h"
#include "server/dflycmd.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/generic_family.h"
#include "server/journal/journal.h"
#include "server/main_service.h"
#include "server/memory_cmd.h"
#include "server/multi_command_squasher.h"
#include "server/protocol_client.h"
#include "server/rdb_load.h"
#include "server/rdb_save.h"
#include "server/script_mgr.h"
#include "server/server_state.h"
#include "server/snapshot.h"
#include "server/tiered_storage.h"
#include "server/transaction.h"
#include "server/version.h"
#include "strings/human_readable.h"
#include "util/accept_server.h"
#include "util/aws/aws.h"

using namespace std;

struct ReplicaOfFlag {
  string host;
  string port;

  bool has_value() const {
    return !host.empty() && !port.empty();
  }
};

static bool AbslParseFlag(std::string_view in, ReplicaOfFlag* flag, std::string* err);
static std::string AbslUnparseFlag(const ReplicaOfFlag& flag);

struct CronExprFlag {
  static constexpr std::string_view kCronPrefix = "0 "sv;
  std::optional<cron::cronexpr> cron_expr;
};

static bool AbslParseFlag(std::string_view in, CronExprFlag* flag, std::string* err);
static std::string AbslUnparseFlag(const CronExprFlag& flag);

ABSL_FLAG(string, dir, "", "working directory");
ABSL_FLAG(string, dbfilename, "dump-{timestamp}",
          "the filename to save/load the DB, instead of/with {timestamp} can be used {Y}, {m}, and "
          "{d} macros");
ABSL_FLAG(string, requirepass, "",
          "password for AUTH authentication. "
          "If empty can also be set with DFLY_PASSWORD environment variable.");
ABSL_FLAG(uint32_t, maxclients, 64000, "Maximum number of concurrent clients allowed.");

ABSL_FLAG(string, save_schedule, "", "the flag is deprecated, please use snapshot_cron instead");
ABSL_FLAG(CronExprFlag, snapshot_cron, {},
          "cron expression for the time to save a snapshot, crontab style");
ABSL_FLAG(bool, df_snapshot_format, true,
          "if true, save in dragonfly-specific snapshotting format");
ABSL_FLAG(int, epoll_file_threads, 0,
          "thread size for file workers when running in epoll mode, default is hardware concurrent "
          "threads");
ABSL_FLAG(ReplicaOfFlag, replicaof, ReplicaOfFlag{},
          "Specifies a host and port which point to a target master "
          "to replicate. "
          "Format should be <IPv4>:<PORT> or host:<PORT> or [<IPv6>]:<PORT>");
ABSL_FLAG(int32_t, slowlog_log_slower_than, 10000,
          "Add commands slower than this threshold to slow log. The value is expressed in "
          "microseconds and if it's negative - disables the slowlog.");
ABSL_FLAG(uint32_t, slowlog_max_len, 20, "Slow log maximum length.");

ABSL_FLAG(string, s3_endpoint, "", "endpoint for s3 snapshots, default uses aws regional endpoint");
ABSL_FLAG(bool, s3_use_https, true, "whether to use https for s3 endpoints");
// Disable EC2 metadata by default, or if a users credentials are invalid the
// AWS client will spent 30s trying to connect to inaccessable EC2 endpoints
// to load the credentials.
ABSL_FLAG(bool, s3_ec2_metadata, false,
          "whether to load credentials and configuration from EC2 metadata");
// Enables S3 payload signing over HTTP. This reduces the latency and resource
// usage when writing snapshots to S3, at the expense of security.
ABSL_FLAG(bool, s3_sign_payload, true,
          "whether to sign the s3 request payload when uploading snapshots");

ABSL_FLAG(bool, info_replication_valkey_compatible, true,
          "when true - output valkey compatible values for info-replication");

ABSL_FLAG(bool, managed_service_info, false,
          "Hides some implementation details from users when true (i.e. in managed service env)");

ABSL_DECLARE_FLAG(int32_t, port);
ABSL_DECLARE_FLAG(bool, cache_mode);
ABSL_DECLARE_FLAG(uint32_t, hz);
ABSL_DECLARE_FLAG(bool, tls);
ABSL_DECLARE_FLAG(string, tls_ca_cert_file);
ABSL_DECLARE_FLAG(string, tls_ca_cert_dir);
ABSL_DECLARE_FLAG(int, replica_priority);
ABSL_DECLARE_FLAG(double, rss_oom_deny_ratio);

bool AbslParseFlag(std::string_view in, ReplicaOfFlag* flag, std::string* err) {
#define RETURN_ON_ERROR(cond, m)                                           \
  do {                                                                     \
    if ((cond)) {                                                          \
      *err = m;                                                            \
      LOG(WARNING) << "Error in parsing arguments for --replicaof: " << m; \
      return false;                                                        \
    }                                                                      \
  } while (0)

  if (in.empty()) {  // on empty flag "parse" nothing. If we return false then DF exists.
    *flag = ReplicaOfFlag{};
    return true;
  }

  auto pos = in.find_last_of(':');
  RETURN_ON_ERROR(pos == string::npos, "missing ':'.");

  string_view ip = in.substr(0, pos);
  flag->port = in.substr(pos + 1);

  RETURN_ON_ERROR(ip.empty() || flag->port.empty(), "IP/host or port are empty.");

  // For IPv6: ip1.front == '[' AND ip1.back == ']'
  // For IPv4: ip1.front != '[' AND ip1.back != ']'
  // Together, this ip1.front == '[' iff ip1.back == ']', which can be implemented as XNOR (NOT XOR)
  RETURN_ON_ERROR(((ip.front() == '[') ^ (ip.back() == ']')), "unclosed brackets.");

  if (ip.front() == '[') {
    // shortest possible IPv6 is '::1' (loopback)
    RETURN_ON_ERROR(ip.length() <= 2, "IPv6 host name is too short");

    flag->host = ip.substr(1, ip.length() - 2);
  } else {
    flag->host = ip;
  }

  VLOG(1) << "--replicaof: Received " << flag->host << " :  " << flag->port;
  return true;
#undef RETURN_ON_ERROR
}

std::string AbslUnparseFlag(const ReplicaOfFlag& flag) {
  return (flag.has_value()) ? absl::StrCat(flag.host, ":", flag.port) : "";
}

bool AbslParseFlag(std::string_view in, CronExprFlag* flag, std::string* err) {
  if (in.empty()) {
    flag->cron_expr = std::nullopt;
    return true;
  }
  if (absl::StartsWith(in, "\"")) {
    *err = absl::StrCat("Could it be that you put quotes in the flagfile?");

    return false;
  }

  std::string raw_cron_expr = absl::StrCat(CronExprFlag::kCronPrefix, in);
  try {
    VLOG(1) << "creating cron from: '" << raw_cron_expr << "'";
    flag->cron_expr = cron::make_cron(raw_cron_expr);
    return true;
  } catch (const cron::bad_cronexpr& ex) {
    *err = ex.what();
  }
  return false;
}

std::string AbslUnparseFlag(const CronExprFlag& flag) {
  if (flag.cron_expr) {
    auto str_expr = to_cronstr(*flag.cron_expr);
    DCHECK(absl::StartsWith(str_expr, CronExprFlag::kCronPrefix));
    return str_expr.substr(CronExprFlag::kCronPrefix.size());
  }
  return "";
}

namespace dfly {

namespace fs = std::filesystem;

using absl::GetFlag;
using absl::StrCat;
using namespace facade;
using namespace util;
using detail::SaveStagesController;
using http::StringResponse;
using strings::HumanReadableNumBytes;

namespace {

const auto kRedisVersion = "7.4.0";

using EngineFunc = void (ServerFamily::*)(CmdArgList args, const CommandContext&);

inline CommandId::Handler3 HandlerFunc(ServerFamily* se, EngineFunc f) {
  return [=](CmdArgList args, const CommandContext& cntx) { return (se->*f)(args, cntx); };
}

using CI = CommandId;

struct CmdArgListFormatter {
  void operator()(std::string* out, MutableSlice arg) const {
    out->append(absl::StrCat("`", std::string_view(arg.data(), arg.size()), "`"));
  }
};

string UnknownCmd(string cmd, CmdArgList args) {
  return absl::StrCat("unknown command '", cmd, "' with args beginning with: ",
                      absl::StrJoin(args.begin(), args.end(), ", ", CmdArgListFormatter()));
}

bool IsS3Path(string_view path) {
  return absl::StartsWith(path, detail::kS3Prefix);
}

bool IsGCSPath(string_view path) {
  return absl::StartsWith(path, detail::kGCSPrefix);
}

// Check that if TLS is used at least one form of client authentication is
// enabled. That means either using a password or giving a root
// certificate for authenticating client certificates which will
// be required.
bool ValidateServerTlsFlags() {
  if (!absl::GetFlag(FLAGS_tls)) {
    return true;
  }

  bool has_auth = false;

  if (!dfly::GetPassword().empty()) {
    has_auth = true;
  }

  if (!(absl::GetFlag(FLAGS_tls_ca_cert_file).empty() &&
        absl::GetFlag(FLAGS_tls_ca_cert_dir).empty())) {
    has_auth = true;
  }

  if (!has_auth) {
    LOG(ERROR) << "TLS configured but no authentication method is used!";
    return false;
  }

  return true;
}

template <typename T> void UpdateMax(T* maxv, T current) {
  *maxv = std::max(*maxv, current);
}

void SetMasterFlagOnAllThreads(bool is_master) {
  auto cb = [is_master](unsigned, auto*) { ServerState::tlocal()->is_master = is_master; };
  shard_set->pool()->AwaitBrief(cb);
}

std::optional<cron::cronexpr> InferSnapshotCronExpr() {
  string save_time = GetFlag(FLAGS_save_schedule);
  auto cron_expr = GetFlag(FLAGS_snapshot_cron);

  if (!save_time.empty()) {
    LOG(ERROR) << "save_schedule flag is deprecated, please use snapshot_cron instead";
    exit(1);
  }

  if (cron_expr.cron_expr) {
    return std::move(cron_expr.cron_expr);
  }

  return std::nullopt;
}

void ClientSetName(CmdArgList args, SinkReplyBuilder* builder, ConnectionContext* cntx) {
  if (args.size() == 1) {
    cntx->conn()->SetName(string{ArgS(args, 0)});
    return builder->SendOk();
  } else {
    return builder->SendError(facade::kSyntaxErr);
  }
}

void ClientGetName(CmdArgList args, SinkReplyBuilder* builder, ConnectionContext* cntx) {
  if (!args.empty()) {
    return builder->SendError(facade::kSyntaxErr);
  }
  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  if (auto name = cntx->conn()->GetName(); !name.empty()) {
    return rb->SendBulkString(name);
  } else {
    return rb->SendNull();
  }
}

void ClientList(CmdArgList args, absl::Span<facade::Listener*> listeners, SinkReplyBuilder* builder,
                ConnectionContext* cntx) {
  if (!args.empty()) {
    return builder->SendError(facade::kSyntaxErr);
  }

  vector<string> client_info;
  absl::base_internal::SpinLock mu;

  // we can not preempt the connection traversal, so we need to use a spinlock.
  // alternatively we could lock when mutating the connection list, but it seems not important.
  auto cb = [&](unsigned thread_index, util::Connection* conn) {
    facade::Connection* dcon = static_cast<facade::Connection*>(conn);
    string info = dcon->GetClientInfo(thread_index);
    absl::base_internal::SpinLockHolder l(&mu);
    client_info.push_back(std::move(info));
  };

  for (auto* listener : listeners) {
    listener->TraverseConnections(cb);
  }

  string result = absl::StrJoin(client_info, "\n");
  result.append("\n");
  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  return rb->SendVerbatimString(result);
}

void ClientPauseCmd(CmdArgList args, vector<facade::Listener*> listeners, SinkReplyBuilder* builder,
                    ConnectionContext* cntx) {
  CmdArgParser parser(args);

  auto timeout = parser.Next<uint64_t>();
  ClientPause pause_state = ClientPause::ALL;
  if (parser.HasNext()) {
    pause_state = parser.MapNext("WRITE", ClientPause::WRITE, "ALL", ClientPause::ALL);
  }
  if (auto err = parser.Error(); err) {
    return builder->SendError(err->MakeReply());
  }

  const auto timeout_ms = timeout * 1ms;
  auto is_pause_in_progress = [end_time = chrono::steady_clock::now() + timeout_ms] {
    return ServerState::tlocal()->gstate() != GlobalState::SHUTTING_DOWN &&
           chrono::steady_clock::now() < end_time;
  };

  if (auto pause_fb_opt =
          Pause(listeners, cntx->ns, cntx->conn(), pause_state, std::move(is_pause_in_progress));
      pause_fb_opt) {
    pause_fb_opt->Detach();
    builder->SendOk();
  } else {
    builder->SendError("Failed to pause all running clients");
  }
}

void ClientTracking(CmdArgList args, SinkReplyBuilder* builder, ConnectionContext* cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  if (!rb->IsResp3())
    return builder->SendError(
        "Client tracking is currently not supported for RESP2. Please use RESP3.");

  CmdArgParser parser{args};
  if (!parser.HasAtLeast(1) || args.size() > 3)
    return builder->SendError(kSyntaxErr);

  bool is_on = false;
  using Tracking = ConnectionState::ClientTracking;
  Tracking::Options option = Tracking::NONE;
  if (parser.Check("ON")) {
    is_on = true;
  } else if (!parser.Check("OFF")) {
    return builder->SendError(kSyntaxErr);
  }

  bool noloop = false;

  if (parser.HasNext()) {
    if (parser.Check("OPTIN")) {
      option = Tracking::OPTIN;
    } else if (parser.Check("OPTOUT")) {
      option = Tracking::OPTOUT;
    } else if (parser.Check("NOLOOP")) {
      noloop = true;
    } else {
      return builder->SendError(kSyntaxErr);
    }
  }

  if (parser.HasNext()) {
    if (!noloop && parser.Check("NOLOOP")) {
      noloop = true;
    } else {
      return builder->SendError(kSyntaxErr);
    }
  }

  if (is_on) {
    ++cntx->subscriptions;
  }

  cntx->conn_state.tracking_info_.SetClientTracking(is_on);
  cntx->conn_state.tracking_info_.SetOption(option);
  cntx->conn_state.tracking_info_.SetNoLoop(noloop);
  return builder->SendOk();
}

void ClientCaching(CmdArgList args, SinkReplyBuilder* builder, Transaction* tx,
                   ConnectionContext* cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  if (!rb->IsResp3())
    return builder->SendError(
        "Client caching is currently not supported for RESP2. Please use RESP3.");

  if (args.size() != 1) {
    return builder->SendError(kSyntaxErr);
  }

  using Tracking = ConnectionState::ClientTracking;
  CmdArgParser parser{args};
  if (parser.Check("YES")) {
    if (!cntx->conn_state.tracking_info_.HasOption(Tracking::OPTIN)) {
      return builder->SendError(
          "ERR CLIENT CACHING YES is only valid when tracking is enabled in OPTIN mode");
    }
  } else if (parser.Check("NO")) {
    if (!cntx->conn_state.tracking_info_.HasOption(Tracking::OPTOUT)) {
      return builder->SendError(
          "ERR CLIENT CACHING NO is only valid when tracking is enabled in OPTOUT mode");
    }
    cntx->conn_state.tracking_info_.ResetCachingSequenceNumber();
  } else {
    return builder->SendError(kSyntaxErr);
  }

  bool is_multi = tx && tx->IsMulti();
  cntx->conn_state.tracking_info_.SetCachingSequenceNumber(is_multi);

  builder->SendOk();
}

void ClientSetInfo(CmdArgList args, SinkReplyBuilder* builder, ConnectionContext* cntx) {
  if (args.size() != 2) {
    return builder->SendError(kSyntaxErr);
  }

  auto* conn = cntx->conn();
  if (conn == nullptr) {
    return builder->SendError("No connection");
  }

  string type = absl::AsciiStrToUpper(ArgS(args, 0));
  string_view val = ArgS(args, 1);

  if (type == "LIB-NAME") {
    conn->SetLibName(string(val));
  } else if (type == "LIB-VER") {
    conn->SetLibVersion(string(val));
  } else {
    return builder->SendError(kSyntaxErr);
  }

  builder->SendOk();
}

void ClientId(CmdArgList args, SinkReplyBuilder* builder, ConnectionContext* cntx) {
  if (args.size() != 0) {
    return builder->SendError(kSyntaxErr);
  }

  return builder->SendLong(cntx->conn()->GetClientId());
}

void ClientKill(CmdArgList args, absl::Span<facade::Listener*> listeners, SinkReplyBuilder* builder,
                ConnectionContext* cntx) {
  std::function<bool(facade::Connection * conn)> evaluator;

  if (args.size() == 1) {
    string_view ip_port = ArgS(args, 0);
    if (ip_port.find(':') != ip_port.npos) {
      evaluator = [ip_port](facade::Connection* conn) {
        return conn->RemoteEndpointStr() == ip_port;
      };
    }
  } else if (args.size() == 2) {
    string filter_type = absl::AsciiStrToUpper(ArgS(args, 0));
    string_view filter_value = ArgS(args, 1);
    if (filter_type == "ADDR") {
      evaluator = [filter_value](facade::Connection* conn) {
        return conn->RemoteEndpointStr() == filter_value;
      };
    } else if (filter_type == "LADDR") {
      evaluator = [filter_value](facade::Connection* conn) {
        return conn->LocalBindStr() == filter_value;
      };
    } else if (filter_type == "ID") {
      uint32_t id;
      if (absl::SimpleAtoi(filter_value, &id)) {
        evaluator = [id](facade::Connection* conn) { return conn->GetClientId() == id; };
      }
    }
    // TODO: Add support for KILL USER/TYPE/SKIPME
  }

  if (!evaluator) {
    return builder->SendError(kSyntaxErr);
  }

  const bool is_admin_request = cntx->conn()->IsPrivileged();

  atomic<uint32_t> killed_connections = 0;
  atomic<uint32_t> kill_errors = 0;
  auto cb = [&](unsigned thread_index, util::Connection* conn) {
    facade::Connection* dconn = static_cast<facade::Connection*>(conn);
    if (evaluator(dconn)) {
      if (is_admin_request || !dconn->IsPrivileged()) {
        dconn->ShutdownSelf();
        killed_connections.fetch_add(1);
      } else {
        kill_errors.fetch_add(1);
      }
    }
  };

  for (auto* listener : listeners) {
    listener->TraverseConnections(cb);
  }

  if (kill_errors.load() == 0) {
    return builder->SendLong(killed_connections.load());
  } else {
    return builder->SendError(absl::StrCat("Killed ", killed_connections.load(),
                                           " client(s), but unable to kill ", kill_errors.load(),
                                           " admin client(s)."));
  }
}

std::string_view GetOSString() {
  // Call uname() only once since it can be expensive. Cache the final result in a static string.
  static string os_string = []() {
    utsname os_name;
    uname(&os_name);
    return StrCat(os_name.sysname, " ", os_name.release, " ", os_name.machine);
  }();

  return os_string;
}

string_view GetRedisMode() {
  return IsClusterEnabledOrEmulated() ? "cluster"sv : "standalone"sv;
}

struct ReplicaOfArgs {
  string host;
  uint16_t port;
  std::optional<cluster::SlotRange> slot_range;
  static optional<ReplicaOfArgs> FromCmdArgs(CmdArgList args, SinkReplyBuilder* builder);
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

optional<ReplicaOfArgs> ReplicaOfArgs::FromCmdArgs(CmdArgList args, SinkReplyBuilder* builder) {
  ReplicaOfArgs replicaof_args;
  CmdArgParser parser(args);

  if (parser.Check("NO")) {
    parser.ExpectTag("ONE");
    replicaof_args.port = 0;
  } else {
    replicaof_args.host = parser.Next<string>();
    replicaof_args.port = parser.Next<uint16_t>();
    if (auto err = parser.Error(); err || replicaof_args.port < 1) {
      builder->SendError("port is out of range");
      return nullopt;
    }
    if (parser.HasNext()) {
      auto [slot_start, slot_end] = parser.Next<SlotId, SlotId>();
      replicaof_args.slot_range = cluster::SlotRange{slot_start, slot_end};
      if (auto err = parser.Error(); err || !replicaof_args.slot_range->IsValid()) {
        builder->SendError("Invalid slot range");
        return nullopt;
      }
    }
  }

  if (auto err = parser.Error(); err) {
    builder->SendError(err->MakeReply());
    return nullopt;
  }
  return replicaof_args;
}

uint64_t GetDelayMs(uint64_t ts) {
  uint64_t now_ns = fb2::ProactorBase::GetMonotonicTimeNs();
  uint64_t delay_ns = 0;
  if (ts < now_ns - 1000000) {  // if more than 1ms has passed between ts and now_ns
    delay_ns = (now_ns - ts) / 1000000;
  }
  return delay_ns;
}

}  // namespace

void SlowLogGet(dfly::CmdArgList args, std::string_view sub_cmd, util::ProactorPool* pp,
                SinkReplyBuilder* builder) {
  size_t requested_slow_log_length = UINT32_MAX;
  size_t argc = args.size();
  if (argc >= 3) {
    builder->SendError(facade::UnknownSubCmd(sub_cmd, "SLOWLOG"), facade::kSyntaxErrType);
    return;
  } else if (argc == 2) {
    string_view length = facade::ArgS(args, 1);
    int64_t num;
    if ((!absl::SimpleAtoi(length, &num)) || (num < -1)) {
      builder->SendError("count should be greater than or equal to -1");
      return;
    }
    if (num >= 0) {
      requested_slow_log_length = num;
    }
  }

  // gather all the individual slowlogs from all the fibers and sort them by their timestamp
  std::vector<boost::circular_buffer<SlowLogEntry>> entries(pp->size());
  pp->AwaitFiberOnAll([&](auto index, auto* context) {
    auto shard_entries = ServerState::tlocal()->GetSlowLog().Entries();
    entries[index] = shard_entries;
  });

  std::vector<std::pair<SlowLogEntry, unsigned>> merged_slow_log;
  for (size_t i = 0; i < entries.size(); ++i) {
    for (const auto& log_item : entries[i]) {
      merged_slow_log.emplace_back(log_item, i);
    }
  }

  std::sort(merged_slow_log.begin(), merged_slow_log.end(), [](const auto& e1, const auto& e2) {
    return e1.first.unix_ts_usec > e2.first.unix_ts_usec;
  });

  requested_slow_log_length = std::min(merged_slow_log.size(), requested_slow_log_length);

  auto* rb = static_cast<facade::RedisReplyBuilder*>(builder);
  rb->StartArray(requested_slow_log_length);
  for (size_t i = 0; i < requested_slow_log_length; ++i) {
    const auto& entry = merged_slow_log[i].first;
    const auto& args = entry.cmd_args;

    rb->StartArray(6);

    rb->SendLong(entry.entry_id * pp->size() + merged_slow_log[i].second);
    rb->SendLong(entry.unix_ts_usec / 1000000);
    rb->SendLong(entry.exec_time_usec);

    // if we truncated the args, there is one pseudo-element containing the number of truncated
    // args that we must add, so the result length is increased by 1
    size_t len = args.size() + int(args.size() < entry.original_length);

    rb->StartArray(len);

    for (const auto& arg : args) {
      if (arg.second > 0) {
        auto suffix = absl::StrCat("... (", arg.second, " more bytes)");
        auto cmd_arg = arg.first.substr(0, kMaximumSlowlogArgLength - suffix.length());
        rb->SendBulkString(absl::StrCat(cmd_arg, suffix));
      } else {
        rb->SendBulkString(arg.first);
      }
    }
    // if we truncated arguments - add a special string to indicate that.
    if (args.size() < entry.original_length) {
      rb->SendBulkString(
          absl::StrCat("... (", entry.original_length - args.size(), " more arguments)"));
    }

    rb->SendBulkString(entry.client_ip);
    rb->SendBulkString(entry.client_name);
  }
}

std::optional<fb2::Fiber> Pause(std::vector<facade::Listener*> listeners, Namespace* ns,
                                facade::Connection* conn, ClientPause pause_state,
                                std::function<bool()> is_pause_in_progress) {
  // Track connections and set pause state to be able to wait untill all running transactions read
  // the new pause state. Exlude already paused commands from the busy count. Exlude tracking
  // blocked connections because: a) If the connection is blocked it is puased. b) We read pause
  // state after waking from blocking so if the trasaction was waken by another running
  //    command that did not pause on the new state yet we will pause after waking up.
  DispatchTracker tracker{std::move(listeners), conn, true /* ignore paused commands */,
                          true /*ignore blocking*/};
  shard_set->pool()->AwaitFiberOnAll([&tracker, pause_state](unsigned, util::ProactorBase*) {
    // Commands don't suspend before checking the pause state, so
    // it's impossible to deadlock on waiting for a command that will be paused.
    tracker.TrackOnThread();
    ServerState::tlocal()->SetPauseState(pause_state, true);
  });

  // Wait for all busy commands to finish running before replying to guarantee
  // that no more (write) operations will occur.
  const absl::Duration kDispatchTimeout = absl::Seconds(1);
  if (!tracker.Wait(kDispatchTimeout)) {
    LOG(WARNING) << "Couldn't wait for commands to finish dispatching in " << kDispatchTimeout;
    shard_set->pool()->AwaitBrief([pause_state](unsigned, util::ProactorBase*) {
      ServerState::tlocal()->SetPauseState(pause_state, false);
    });
    return std::nullopt;
  }

  // We should not expire/evict keys while clients are paused.
  shard_set->RunBriefInParallel(
      [ns](EngineShard* shard) { ns->GetDbSlice(shard->shard_id()).SetExpireAllowed(false); });

  return fb2::Fiber("client_pause", [is_pause_in_progress, pause_state, ns]() mutable {
    // On server shutdown we sleep 10ms to make sure all running task finish, therefore 10ms steps
    // ensure this fiber will not left hanging .
    constexpr auto step = 10ms;
    while (is_pause_in_progress()) {
      ThisFiber::SleepFor(step);
    }

    ServerState& etl = *ServerState::tlocal();
    if (etl.gstate() != GlobalState::SHUTTING_DOWN) {
      shard_set->pool()->AwaitFiberOnAll([pause_state](util::ProactorBase* pb) {
        ServerState::tlocal()->SetPauseState(pause_state, false);
      });
      shard_set->RunBriefInParallel(
          [ns](EngineShard* shard) { ns->GetDbSlice(shard->shard_id()).SetExpireAllowed(true); });
    }
  });
}

ServerFamily::ServerFamily(Service* service) : service_(*service) {
  start_time_ = time(NULL);
  last_save_info_.save_time = start_time_;
  script_mgr_.reset(new ScriptMgr());
  journal_.reset(new journal::Journal());

  {
    absl::InsecureBitGen eng;
    master_replid_ = GetRandomHex(eng, CONFIG_RUN_ID_SIZE);
    DCHECK_EQ(CONFIG_RUN_ID_SIZE, master_replid_.size());
  }

  if (auto ec =
          detail::ValidateFilename(GetFlag(FLAGS_dbfilename), GetFlag(FLAGS_df_snapshot_format));
      ec) {
    LOG(ERROR) << ec.Format();
    exit(1);
  }

  if (!ValidateServerTlsFlags()) {
    exit(1);
  }
  ValidateClientTlsFlags();
  dfly_cmd_ = make_unique<DflyCmd>(this);
}

ServerFamily::~ServerFamily() {
}

void SetMaxClients(std::vector<facade::Listener*>& listeners, uint32_t maxclients) {
  for (auto* listener : listeners) {
    if (!listener->IsPrivilegedInterface()) {
      listener->socket()->proactor()->Await(
          [listener, maxclients]() { listener->SetMaxClients(maxclients); });
    }
  }
}

void SetSlowLogMaxLen(util::ProactorPool& pool, uint32_t val) {
  pool.AwaitFiberOnAll(
      [&val](auto index, auto* context) { ServerState::tlocal()->GetSlowLog().ChangeLength(val); });
}

void SetSlowLogThreshold(util::ProactorPool& pool, int32_t val) {
  pool.AwaitFiberOnAll([val](auto index, auto* context) {
    ServerState::tlocal()->log_slower_than_usec = val < 0 ? UINT32_MAX : uint32_t(val);
  });
}

void ServerFamily::Init(util::AcceptServer* acceptor, std::vector<facade::Listener*> listeners) {
  CHECK(acceptor_ == nullptr);
  acceptor_ = acceptor;
  listeners_ = std::move(listeners);

  auto os_string = GetOSString();
  LOG_FIRST_N(INFO, 1) << "Host OS: " << os_string << " with " << shard_set->pool()->size()
                       << " threads";
  SetMaxClients(listeners_, absl::GetFlag(FLAGS_maxclients));
  config_registry.RegisterSetter<uint32_t>(
      "maxclients", [this](uint32_t val) { SetMaxClients(listeners_, val); });

  SetSlowLogThreshold(service_.proactor_pool(), absl::GetFlag(FLAGS_slowlog_log_slower_than));
  config_registry.RegisterMutable("slowlog_log_slower_than",
                                  [this](const absl::CommandLineFlag& flag) {
                                    auto res = flag.TryGet<int32_t>();
                                    if (res.has_value())
                                      SetSlowLogThreshold(service_.proactor_pool(), res.value());
                                    return res.has_value();
                                  });
  SetSlowLogMaxLen(service_.proactor_pool(), absl::GetFlag(FLAGS_slowlog_max_len));
  config_registry.RegisterSetter<uint32_t>(
      "slowlog_max_len", [this](uint32_t val) { SetSlowLogMaxLen(service_.proactor_pool(), val); });

  // We only reconfigure TLS when the 'tls' config key changes. Therefore to
  // update TLS certs, first update tls_cert_file, then set 'tls true'.
  config_registry.RegisterMutable("tls", [this](const absl::CommandLineFlag& flag) {
    if (!ValidateServerTlsFlags()) {
      return false;
    }
    for (facade::Listener* l : listeners_) {
      // Must reconfigure in the listener proactor to avoid a race.
      if (!l->socket()->proactor()->Await([l] { return l->ReconfigureTLS(); })) {
        return false;
      }
    }
    return true;
  });
  config_registry.RegisterMutable("tls_cert_file");
  config_registry.RegisterMutable("tls_key_file");
  config_registry.RegisterMutable("tls_ca_cert_file");
  config_registry.RegisterMutable("tls_ca_cert_dir");
  config_registry.RegisterMutable("replica_priority");
  config_registry.RegisterMutable("lua_undeclared_keys_shas");
  config_registry.RegisterMutable("list_rdb_encode_v2");

  pb_task_ = shard_set->pool()->GetNextProactor();
  if (pb_task_->GetKind() == ProactorBase::EPOLL) {
    fq_threadpool_.reset(new fb2::FiberQueueThreadPool(absl::GetFlag(FLAGS_epoll_file_threads)));
  }

  string flag_dir = GetFlag(FLAGS_dir);
  if (IsS3Path(flag_dir)) {
#ifdef WITH_AWS
    shard_set->pool()->GetNextProactor()->Await([&] { util::aws::Init(); });
    snapshot_storage_ = std::make_shared<detail::AwsS3SnapshotStorage>(
        absl::GetFlag(FLAGS_s3_endpoint), absl::GetFlag(FLAGS_s3_use_https),
        absl::GetFlag(FLAGS_s3_ec2_metadata), absl::GetFlag(FLAGS_s3_sign_payload));
#else
    LOG(ERROR) << "Compiled without AWS support";
    exit(1);
#endif
  } else if (IsGCSPath(flag_dir)) {
    auto gcs = std::make_shared<detail::GcsSnapshotStorage>();
    auto ec = shard_set->pool()->GetNextProactor()->Await([&] { return gcs->Init(3000); });
    if (ec) {
      LOG(ERROR) << "Failed to initialize GCS snapshot storage: " << ec.message();
      exit(1);
    }
    snapshot_storage_ = std::move(gcs);
  } else if (fq_threadpool_) {
    snapshot_storage_ = std::make_shared<detail::FileSnapshotStorage>(fq_threadpool_.get());
  } else {
    snapshot_storage_ = std::make_shared<detail::FileSnapshotStorage>(nullptr);
  }

  // check for '--replicaof' before loading anything
  if (ReplicaOfFlag flag = GetFlag(FLAGS_replicaof); flag.has_value()) {
    service_.proactor_pool().GetNextProactor()->Await(
        [this, &flag]() { this->Replicate(flag.host, flag.port); });
  } else {  // load from snapshot only if --replicaof is empty
    LoadFromSnapshot();
  }

  const auto create_snapshot_schedule_fb = [this] {
    snapshot_schedule_fb_ =
        service_.proactor_pool().GetNextProactor()->LaunchFiber([this] { SnapshotScheduling(); });
  };
  config_registry.RegisterMutable(
      "snapshot_cron", [this, create_snapshot_schedule_fb](const absl::CommandLineFlag& flag) {
        JoinSnapshotSchedule();
        create_snapshot_schedule_fb();
        return true;
      });
  create_snapshot_schedule_fb();
}

void ServerFamily::LoadFromSnapshot() {
  {
    util::fb2::LockGuard lk{loading_stats_mu_};
    loading_stats_.restore_count++;
  }

  const auto load_path_result =
      snapshot_storage_->LoadPath(GetFlag(FLAGS_dir), GetFlag(FLAGS_dbfilename));

  if (load_path_result) {
    const std::string load_path = *load_path_result;
    if (!load_path.empty()) {
      auto future = Load(load_path, LoadExistingKeys::kFail);
      load_fiber_ = service_.proactor_pool().GetNextProactor()->LaunchFiber([future]() mutable {
        // Wait for load to finish in a dedicated fiber.
        // Failure to load on start causes Dragonfly to exit with an error code.
        if (!future.has_value() || future->Get()) {
          // Error was already printed to log at this point.
          exit(1);
        }
      });
    }
  } else {
    if (std::error_code(load_path_result.error()) == std::errc::no_such_file_or_directory) {
      LOG(WARNING) << "Load snapshot: No snapshot found";
    } else {
      util::fb2::LockGuard lk{loading_stats_mu_};
      loading_stats_.failed_restore_count++;
      LOG(ERROR) << "Failed to load snapshot: " << load_path_result.error().Format();
    }
  }
}

void ServerFamily::JoinSnapshotSchedule() {
  schedule_done_.Notify();
  snapshot_schedule_fb_.JoinIfNeeded();
  schedule_done_.Reset();
}

void ServerFamily::Shutdown() {
  VLOG(1) << "ServerFamily::Shutdown";

  load_fiber_.JoinIfNeeded();

  JoinSnapshotSchedule();

  bg_save_fb_.JoinIfNeeded();

  if (save_on_shutdown_ && !absl::GetFlag(FLAGS_dbfilename).empty()) {
    shard_set->pool()->GetNextProactor()->Await([this]() ABSL_LOCKS_EXCLUDED(loading_stats_mu_) {
      GenericError ec = DoSave();

      util::fb2::LockGuard lk{loading_stats_mu_};
      loading_stats_.backup_count++;

      if (ec) {
        loading_stats_.failed_backup_count++;
        LOG(WARNING) << "Failed to perform snapshot " << ec.Format();
      }
    });
  }

  pb_task_->Await([this] {
    auto ec = journal_->Close();
    LOG_IF(ERROR, ec) << "Error closing journal " << ec;

    util::fb2::LockGuard lk(replicaof_mu_);
    if (replica_) {
      replica_->Stop();
    }
    StopAllClusterReplicas();

    dfly_cmd_->Shutdown();
    DebugCmd::Shutdown();
  });
}

bool ServerFamily::HasPrivilegedInterface() {
  for (auto* listener : listeners_) {
    if (listener->IsPrivilegedInterface()) {
      return true;
    }
  }
  return false;
}

void ServerFamily::UpdateMemoryGlobalStats() {
  ShardId sid = EngineShard::tlocal()->shard_id();
  if (sid != 0) {  // This function is executed periodicaly on all shards. To ensure the logic
                   // bellow runs only on one shard we return is the shard is not 0.
    return;
  }

  uint64_t mem_current = used_mem_current.load(std::memory_order_relaxed);
  if (mem_current > used_mem_peak.load(memory_order_relaxed)) {
    used_mem_peak.store(mem_current, memory_order_relaxed);
  }

  io::Result<io::StatusData> sdata_res = io::ReadStatusInfo();
  if (sdata_res) {
    size_t total_rss = FetchRssMemory(sdata_res.value());
    rss_mem_current.store(total_rss, memory_order_relaxed);
    if (rss_mem_peak.load(memory_order_relaxed) < total_rss) {
      rss_mem_peak.store(total_rss, memory_order_relaxed);
    }
    double rss_oom_deny_ratio = ServerState::tlocal()->rss_oom_deny_ratio;
    if (rss_oom_deny_ratio > 0) {
      size_t memory_limit = max_memory_limit * rss_oom_deny_ratio;
      if (total_rss > memory_limit && accepting_connections_ && HasPrivilegedInterface()) {
        for (auto* listener : listeners_) {
          if (!listener->IsPrivilegedInterface()) {
            listener->socket()->proactor()->Await([listener]() { listener->pause_accepting(); });
          }
        }
        accepting_connections_ = false;

      } else if (total_rss < memory_limit && !accepting_connections_) {
        for (auto* listener : listeners_) {
          if (!listener->IsPrivilegedInterface()) {
            listener->socket()->proactor()->Await([listener]() { listener->resume_accepting(); });
          }
        }
        accepting_connections_ = true;
      }
    }
  }
}

struct AggregateLoadResult {
  AggregateError first_error;
  std::atomic<size_t> keys_read;
};

void ServerFamily::FlushAll(Namespace* ns) {
  const CommandId* cid = service_.FindCmd("FLUSHALL");
  boost::intrusive_ptr<Transaction> flush_trans(new Transaction{cid});
  flush_trans->InitByArgs(ns, 0, {});
  VLOG(1) << "Performing flush";
  error_code ec = Drakarys(flush_trans.get(), DbSlice::kDbAll);
  if (ec) {
    LOG(ERROR) << "Error flushing db " << ec.message();
  }
}

// Load starts as many fibers as there are files to load each one separately.
// It starts one more fiber that waits for all load fibers to finish and returns the first
// error (if any occured) with a future.
std::optional<fb2::Future<GenericError>> ServerFamily::Load(string_view load_path,
                                                            LoadExistingKeys existing_keys) {
  std::string path(load_path);

  if (load_path.empty()) {
    fs::path dir_path(GetFlag(FLAGS_dir));
    string filename = GetFlag(FLAGS_dbfilename);
    dir_path.append(filename);
    path = dir_path.generic_string();
  }

  DCHECK_GT(shard_count(), 0u);

  // TODO: to move it to helio.
  auto immediate = [](auto val) {
    fb2::Future<GenericError> future;
    future.Resolve(val);
    return future;
  };

  if (ServerState::tlocal() && !ServerState::tlocal()->is_master) {
    return immediate(string("Replica cannot load data"));
  }

  auto expand_result = snapshot_storage_->ExpandSnapshot(path);
  if (!expand_result) {
    LOG(ERROR) << "Failed to load snapshot: " << expand_result.error().Format();

    return immediate(expand_result.error());
  }

  auto new_state = service_.SwitchState(GlobalState::ACTIVE, GlobalState::LOADING);
  if (new_state != GlobalState::LOADING) {
    LOG(WARNING) << new_state << " in progress, ignored";
    return {};
  }

  auto& pool = service_.proactor_pool();

  const vector<string>& paths = *expand_result;

  LOG(INFO) << "Loading " << path;

  vector<fb2::Fiber> load_fibers;
  load_fibers.reserve(paths.size());

  auto aggregated_result = std::make_shared<AggregateLoadResult>();

  for (auto& path : paths) {
    // For single file, choose thread that does not handle shards if possible.
    // This will balance out the CPU during the load.
    ProactorBase* proactor;
    if (paths.size() == 1 && shard_count() < pool.size()) {
      proactor = pool.at(shard_count());
    } else {
      proactor = pool.GetNextProactor();
    }

    auto load_func = [this, aggregated_result, existing_keys, path = std::move(path)]() {
      auto load_result = LoadRdb(path, existing_keys);
      if (load_result.has_value())
        aggregated_result->keys_read.fetch_add(*load_result);
      else
        aggregated_result->first_error = load_result.error();
    };
    load_fibers.push_back(proactor->LaunchFiber(std::move(load_func)));
  }

  fb2::Future<GenericError> future;

  // Run fiber that empties the channel and sets ec_promise.
  auto load_join_func = [this, aggregated_result, load_fibers = std::move(load_fibers),
                         future]() mutable {
    for (auto& fiber : load_fibers) {
      fiber.Join();
    }

    if (aggregated_result->first_error) {
      LOG(ERROR) << "Rdb load failed: " << (*aggregated_result->first_error).message();
    } else {
      RdbLoader::PerformPostLoad(&service_);
      LOG(INFO) << "Load finished, num keys read: " << aggregated_result->keys_read;
    }

    service_.SwitchState(GlobalState::LOADING, GlobalState::ACTIVE);
    future.Resolve(*(aggregated_result->first_error));
  };
  pool.GetNextProactor()->Dispatch(std::move(load_join_func));

  return future;
}

void ServerFamily::SnapshotScheduling() {
  const std::optional<cron::cronexpr> cron_expr = InferSnapshotCronExpr();
  if (!cron_expr) {
    return;
  }

  ServerState* ss = ServerState::tlocal();
  do {
    if (schedule_done_.WaitFor(100ms)) {
      return;
    }
  } while (ss->gstate() == GlobalState::LOADING);

  while (true) {
    const std::chrono::time_point now = std::chrono::system_clock::now();
    const std::chrono::time_point next = cron::cron_next(cron_expr.value(), now);

    if (schedule_done_.WaitFor(next - now)) {
      break;
    };

    GenericError ec = DoSave();

    util::fb2::LockGuard lk{loading_stats_mu_};
    loading_stats_.backup_count++;

    if (ec) {
      loading_stats_.failed_backup_count++;
      LOG(WARNING) << "Failed to perform snapshot " << ec.Format();
    }
  }
}

io::Result<size_t> ServerFamily::LoadRdb(const std::string& rdb_file,
                                         LoadExistingKeys existing_keys) {
  VLOG(1) << "Loading data from " << rdb_file;
  CHECK(fb2::ProactorBase::IsProactorThread()) << "must be called from proactor thread";

  error_code ec;
  io::ReadonlyFileOrError res = snapshot_storage_->OpenReadFile(rdb_file);
  if (res) {
    io::FileSource fs(*res);

    RdbLoader loader{&service_};
    if (existing_keys == LoadExistingKeys::kOverride) {
      loader.SetOverrideExistingKeys(true);
    }

    ec = loader.Load(&fs);
    if (!ec) {
      VLOG(1) << "Done loading RDB from " << rdb_file << ", keys loaded: " << loader.keys_loaded();
      VLOG(1) << "Loading finished after " << strings::HumanReadableElapsedTime(loader.load_time());
      return loader.keys_loaded();
    }
  } else {
    ec = res.error();
  }
  return nonstd::make_unexpected(ec);
}

enum MetricType { COUNTER, GAUGE, SUMMARY, HISTOGRAM };

const char* MetricTypeName(MetricType type) {
  switch (type) {
    case MetricType::COUNTER:
      return "counter";
    case MetricType::GAUGE:
      return "gauge";
    case MetricType::SUMMARY:
      return "summary";
    case MetricType::HISTOGRAM:
      return "histogram";
  }
  return "unknown";
}

inline string GetMetricFullName(string_view metric_name) {
  return StrCat("dragonfly_", metric_name);
}

void AppendMetricHeader(string_view metric_name, string_view metric_help, MetricType type,
                        string* dest) {
  const auto full_metric_name = GetMetricFullName(metric_name);
  absl::StrAppend(dest, "# HELP ", full_metric_name, " ", metric_help, "\n");
  absl::StrAppend(dest, "# TYPE ", full_metric_name, " ", MetricTypeName(type), "\n");
}

void AppendLabelTupple(absl::Span<const string_view> label_names,
                       absl::Span<const string_view> label_values, string* dest) {
  if (label_names.empty())
    return;

  absl::StrAppend(dest, "{");
  for (size_t i = 0; i < label_names.size(); ++i) {
    if (i > 0) {
      absl::StrAppend(dest, ", ");
    }
    absl::StrAppend(dest, label_names[i], "=\"", label_values[i], "\"");
  }

  absl::StrAppend(dest, "}");
}

void AppendMetricValue(string_view metric_name, const absl::AlphaNum& value,
                       absl::Span<const string_view> label_names,
                       absl::Span<const string_view> label_values, string* dest) {
  absl::StrAppend(dest, GetMetricFullName(metric_name));
  AppendLabelTupple(label_names, label_values, dest);
  absl::StrAppend(dest, " ", value, "\n");
}

void AppendMetricWithoutLabels(string_view name, string_view help, const absl::AlphaNum& value,
                               MetricType type, string* dest) {
  AppendMetricHeader(name, help, type, dest);
  AppendMetricValue(name, value, {}, {}, dest);
}

void PrintPrometheusMetrics(uint64_t uptime, const Metrics& m, DflyCmd* dfly_cmd,
                            StringResponse* resp) {
  // Server metrics
  AppendMetricHeader("version", "", MetricType::GAUGE, &resp->body());
  AppendMetricValue("version", 1, {"version"}, {GetVersion()}, &resp->body());

  bool is_master = ServerState::tlocal()->is_master;

  AppendMetricWithoutLabels("master", "1 if master 0 if replica", is_master ? 1 : 0,
                            MetricType::GAUGE, &resp->body());
  AppendMetricWithoutLabels("uptime_in_seconds", "", uptime, MetricType::COUNTER, &resp->body());

  // Clients metrics
  const auto& conn_stats = m.facade_stats.conn_stats;
  AppendMetricWithoutLabels("max_clients", "Maximal number of clients", GetFlag(FLAGS_maxclients),
                            MetricType::GAUGE, &resp->body());
  AppendMetricWithoutLabels("connected_clients", "", conn_stats.num_conns, MetricType::GAUGE,
                            &resp->body());
  AppendMetricWithoutLabels("client_read_buffer_bytes", "", conn_stats.read_buf_capacity,
                            MetricType::GAUGE, &resp->body());
  AppendMetricWithoutLabels("blocked_clients", "", conn_stats.num_blocked_clients,
                            MetricType::GAUGE, &resp->body());
  AppendMetricWithoutLabels("dispatch_queue_bytes", "", conn_stats.dispatch_queue_bytes,
                            MetricType::GAUGE, &resp->body());
  AppendMetricWithoutLabels("pipeline_queue_length", "", conn_stats.dispatch_queue_entries,
                            MetricType::GAUGE, &resp->body());
  AppendMetricWithoutLabels("send_delay_seconds", "",
                            double(GetDelayMs(m.oldest_pending_send_ts)) / 1000.0,
                            MetricType::GAUGE, &resp->body());

  AppendMetricWithoutLabels("pipeline_throttle_total", "", conn_stats.pipeline_throttle_count,
                            MetricType::COUNTER, &resp->body());
  AppendMetricWithoutLabels("pipeline_cmd_cache_bytes", "", conn_stats.pipeline_cmd_cache_bytes,
                            MetricType::GAUGE, &resp->body());
  AppendMetricWithoutLabels("pipeline_commands_total", "", conn_stats.pipelined_cmd_cnt,
                            MetricType::COUNTER, &resp->body());
  AppendMetricWithoutLabels("pipeline_commands_duration_seconds", "",
                            conn_stats.pipelined_cmd_latency * 1e-6, MetricType::COUNTER,
                            &resp->body());
  AppendMetricWithoutLabels("commands_squashing_replies_bytes", "",
                            MultiCommandSquasher::GetRepliesMemSize(), MetricType::GAUGE,
                            &resp->body());
  string connections_libs;
  AppendMetricHeader("connections_libs", "Total number of connections by libname:ver",
                     MetricType::GAUGE, &connections_libs);
  for (const auto& [lib, count] : m.connections_lib_name_ver_map) {
    AppendMetricValue("connections_libs", count, {"lib"}, {lib}, &connections_libs);
  }
  absl::StrAppend(&resp->body(), connections_libs);

  // Memory metrics
  auto sdata_res = io::ReadStatusInfo();
  AppendMetricWithoutLabels("memory_used_bytes", "", m.heap_used_bytes, MetricType::GAUGE,
                            &resp->body());
  AppendMetricWithoutLabels("memory_used_peak_bytes", "", used_mem_peak.load(memory_order_relaxed),
                            MetricType::GAUGE, &resp->body());
  AppendMetricWithoutLabels("memory_fiberstack_vms_bytes",
                            "virtual memory size used by all the fibers", m.worker_fiber_stack_size,
                            MetricType::GAUGE, &resp->body());
  AppendMetricWithoutLabels("fibers_count", "", m.worker_fiber_count, MetricType::GAUGE,
                            &resp->body());
  AppendMetricWithoutLabels("blocked_tasks", "", m.blocked_tasks, MetricType::GAUGE, &resp->body());

  AppendMetricWithoutLabels("memory_max_bytes", "", max_memory_limit, MetricType::GAUGE,
                            &resp->body());

  if (m.events.insertion_rejections | m.coordinator_stats.oom_error_cmd_cnt) {
    AppendMetricHeader("oom_errors_total", "Rejected requests due to out of memory errors",
                       MetricType::COUNTER, &resp->body());
    AppendMetricValue("oom_errors_total", m.events.insertion_rejections, {"type"}, {"insert"},
                      &resp->body());
    AppendMetricValue("oom_errors_total", m.coordinator_stats.oom_error_cmd_cnt, {"type"}, {"cmd"},
                      &resp->body());
  }
  if (sdata_res.has_value()) {
    size_t rss = FetchRssMemory(sdata_res.value());
    AppendMetricWithoutLabels("used_memory_rss_bytes", "", rss, MetricType::GAUGE, &resp->body());
    AppendMetricWithoutLabels("swap_memory_bytes", "", sdata_res->vm_swap, MetricType::GAUGE,
                              &resp->body());
  } else {
    LOG_FIRST_N(ERROR, 10) << "Error fetching /proc/self/status stats. error "
                           << sdata_res.error().message();
  }
  AppendMetricWithoutLabels("tls_bytes", "", m.tls_bytes, MetricType::GAUGE, &resp->body());
  AppendMetricWithoutLabels("snapshot_serialization_bytes", "", m.serialization_bytes,
                            MetricType::GAUGE, &resp->body());

  DbStats total;
  for (const auto& db_stats : m.db_stats) {
    total += db_stats;
  }

  {
    string type_used_memory_metric;
    bool added = false;
    AppendMetricHeader("type_used_memory", "Memory used per type", MetricType::GAUGE,
                       &type_used_memory_metric);

    for (unsigned type = 0; type < total.memory_usage_by_type.size(); type++) {
      size_t mem = total.memory_usage_by_type[type];
      if (mem > 0) {
        AppendMetricValue("type_used_memory", mem, {"type"}, {ObjTypeToString(type)},
                          &type_used_memory_metric);
        added = true;
      }
    }
    if (added)
      absl::StrAppend(&resp->body(), type_used_memory_metric);
  }

  // Stats metrics
  AppendMetricWithoutLabels("connections_received_total", "", conn_stats.conn_received_cnt,
                            MetricType::COUNTER, &resp->body());

  AppendMetricWithoutLabels("commands_processed_total", "", conn_stats.command_cnt,
                            MetricType::COUNTER, &resp->body());
  AppendMetricWithoutLabels("keyspace_hits_total", "", m.events.hits, MetricType::COUNTER,
                            &resp->body());
  AppendMetricWithoutLabels("keyspace_misses_total", "", m.events.misses, MetricType::COUNTER,
                            &resp->body());
  AppendMetricWithoutLabels("keyspace_mutations_total", "", m.events.mutations, MetricType::COUNTER,
                            &resp->body());
  AppendMetricWithoutLabels("lua_interpreter_cnt", "", m.lua_stats.interpreter_cnt,
                            MetricType::GAUGE, &resp->body());
  AppendMetricWithoutLabels("used_memory_lua", "", m.lua_stats.used_bytes, MetricType::GAUGE,
                            &resp->body());
  AppendMetricWithoutLabels("lua_blocked_total", "", m.lua_stats.blocked_cnt, MetricType::COUNTER,
                            &resp->body());

  AppendMetricWithoutLabels("backups_total", "", m.loading_stats.backup_count, MetricType::COUNTER,
                            &resp->body());
  AppendMetricWithoutLabels("failed_backups_total", "", m.loading_stats.failed_backup_count,
                            MetricType::COUNTER, &resp->body());
  AppendMetricWithoutLabels("restores_total", "", m.loading_stats.restore_count,
                            MetricType::COUNTER, &resp->body());
  AppendMetricWithoutLabels("failed_restores_total", "", m.loading_stats.failed_restore_count,
                            MetricType::COUNTER, &resp->body());

  // Net metrics
  AppendMetricWithoutLabels("net_input_bytes_total", "", conn_stats.io_read_bytes,
                            MetricType::COUNTER, &resp->body());
  AppendMetricWithoutLabels("net_output_bytes_total", "", m.facade_stats.reply_stats.io_write_bytes,
                            MetricType::COUNTER, &resp->body());
  {
    AppendMetricWithoutLabels("reply_duration_seconds", "",
                              m.facade_stats.reply_stats.send_stats.total_duration * 1e-6,
                              MetricType::COUNTER, &resp->body());
    AppendMetricWithoutLabels("reply_total", "", m.facade_stats.reply_stats.send_stats.count,
                              MetricType::COUNTER, &resp->body());
  }

  AppendMetricWithoutLabels("script_error_total", "", m.facade_stats.reply_stats.script_error_count,
                            MetricType::COUNTER, &resp->body());

  AppendMetricHeader("listener_accept_error_total", "Listener accept errors", MetricType::COUNTER,
                     &resp->body());
  AppendMetricValue("listener_accept_error_total", m.refused_conn_max_clients_reached_count,
                    {"reason"}, {"limit_reached"}, &resp->body());

  // DB stats
  AppendMetricWithoutLabels("expired_keys_total", "", m.events.expired_keys, MetricType::COUNTER,
                            &resp->body());
  AppendMetricWithoutLabels("evicted_keys_total", "", m.events.evicted_keys, MetricType::COUNTER,
                            &resp->body());

  // Command stats
  if (!m.cmd_stats_map.empty()) {
    string command_metrics;

    AppendMetricHeader("commands", "Metrics for all commands ran", MetricType::COUNTER,
                       &command_metrics);
    for (const auto& [name, stat] : m.cmd_stats_map) {
      const auto calls = stat.first;
      const double duration_seconds = stat.second * 1e-6;
      AppendMetricValue("commands_total", calls, {"cmd"}, {name}, &command_metrics);
      AppendMetricValue("commands_duration_seconds", duration_seconds, {"cmd"}, {name},
                        &command_metrics);
    }
    absl::StrAppend(&resp->body(), command_metrics);
  }

  if (m.replica_side_info) {  // slave side
    auto& replica_info = *m.replica_side_info;
    AppendMetricWithoutLabels("replica_reconnect_count", "Number of replica reconnects",
                              replica_info.reconnect_count, MetricType::COUNTER, &resp->body());
  } else {  // Master side
    string replication_lag_metrics;
    vector<ReplicaRoleInfo> replicas_info = dfly_cmd->GetReplicasRoleInfo();

    ReplicationMemoryStats repl_mem;
    dfly_cmd->GetReplicationMemoryStats(&repl_mem);
    AppendMetricWithoutLabels(
        "replication_streaming_bytes", "Stable sync replication memory  usage",
        repl_mem.streamer_buf_capacity_bytes, MetricType::GAUGE, &resp->body());
    AppendMetricWithoutLabels("replication_full_sync_bytes", "Full sync memory usage",
                              repl_mem.full_sync_buf_bytes, MetricType::GAUGE, &resp->body());

    AppendMetricHeader("connected_replica_lag_records", "Lag in records of a connected replica.",
                       MetricType::GAUGE, &replication_lag_metrics);
    for (const auto& replica : replicas_info) {
      AppendMetricValue("connected_replica_lag_records", replica.lsn_lag,
                        {"replica_ip", "replica_port", "replica_state"},
                        {replica.address, absl::StrCat(replica.listening_port), replica.state},
                        &replication_lag_metrics);
    }
    absl::StrAppend(&resp->body(), replication_lag_metrics);
  }

  AppendMetricWithoutLabels("fiber_switch_total", "", m.fiber_switch_cnt, MetricType::COUNTER,
                            &resp->body());
  double delay_seconds = m.fiber_switch_delay_usec * 1e-6;
  AppendMetricWithoutLabels("fiber_switch_delay_seconds_total", "", delay_seconds,
                            MetricType::COUNTER, &resp->body());

  AppendMetricWithoutLabels("fiber_longrun_total", "", m.fiber_longrun_cnt, MetricType::COUNTER,
                            &resp->body());
  double longrun_seconds = m.fiber_longrun_usec * 1e-6;
  AppendMetricWithoutLabels("fiber_longrun_seconds", "", longrun_seconds, MetricType::COUNTER,
                            &resp->body());
  AppendMetricWithoutLabels("tx_queue_len", "", m.tx_queue_len, MetricType::GAUGE, &resp->body());

  {
    bool added = false;
    string str;
    AppendMetricHeader("transaction_widths_total", "Transaction counts by their widths",
                       MetricType::COUNTER, &str);

    for (unsigned width = 0; width < shard_set->size(); ++width) {
      uint64_t count = m.coordinator_stats.tx_width_freq_arr[width];

      if (count > 0) {
        AppendMetricValue("transaction_widths_total", count, {"width"}, {StrCat("w", width + 1)},
                          &str);
        added = true;
      }
    }
    if (added)
      absl::StrAppend(&resp->body(), str);
  }

  string db_key_metrics;
  string db_key_expire_metrics;

  AppendMetricHeader("db_keys", "Total number of keys by DB", MetricType::GAUGE, &db_key_metrics);
  AppendMetricHeader("db_keys_expiring", "Total number of expiring keys by DB", MetricType::GAUGE,
                     &db_key_expire_metrics);

  for (size_t i = 0; i < m.db_stats.size(); ++i) {
    AppendMetricValue("db_keys", m.db_stats[i].key_count, {"db"}, {StrCat("db", i)},
                      &db_key_metrics);
    AppendMetricValue("db_keys_expiring", m.db_stats[i].expire_count, {"db"}, {StrCat("db", i)},
                      &db_key_expire_metrics);
  }

  absl::StrAppend(&resp->body(), db_key_metrics);
  absl::StrAppend(&resp->body(), db_key_expire_metrics);
}

void ServerFamily::ConfigureMetrics(util::HttpListenerBase* http_base) {
  // The naming of the metrics should be compatible with redis_exporter, see
  // https://github.com/oliver006/redis_exporter/blob/master/exporter/exporter.go#L111

  auto cb = [this](const util::http::QueryArgs& args, util::HttpContext* send) {
    StringResponse resp = util::http::MakeStringResponse(boost::beast::http::status::ok);
    util::http::SetMime(util::http::kTextMime, &resp);
    uint64_t uptime = time(NULL) - start_time_;
    PrintPrometheusMetrics(uptime, this->GetMetrics(&namespaces->GetDefaultNamespace()),
                           this->dfly_cmd_.get(), &resp);

    return send->Invoke(std::move(resp));
  };

  http_base->RegisterCb("/metrics", cb);
}

void ServerFamily::PauseReplication(bool pause) {
  util::fb2::LockGuard lk(replicaof_mu_);

  // Switch to primary mode.
  if (!ServerState::tlocal()->is_master) {
    auto repl_ptr = replica_;
    CHECK(repl_ptr);
    repl_ptr->Pause(pause);
  }
}

std::optional<ReplicaOffsetInfo> ServerFamily::GetReplicaOffsetInfo() {
  util::fb2::LockGuard lk(replicaof_mu_);

  // Switch to primary mode.
  if (!ServerState::tlocal()->is_master) {
    auto repl_ptr = replica_;
    CHECK(repl_ptr);
    return ReplicaOffsetInfo{repl_ptr->GetSyncId(), repl_ptr->GetReplicaOffset()};
  }
  return nullopt;
}

vector<facade::Listener*> ServerFamily::GetNonPriviligedListeners() const {
  std::vector<facade::Listener*> listeners;
  listeners.reserve(listeners.size());
  for (facade::Listener* listener : listeners_) {
    if (!listener->IsPrivilegedInterface()) {
      listeners.push_back(listener);
    }
  }
  return listeners;
}

optional<Replica::Summary> ServerFamily::GetReplicaSummary() const {
  util::fb2::LockGuard lk(replicaof_mu_);
  if (replica_ == nullptr) {
    return nullopt;
  } else {
    return replica_->GetSummary();
  }
}

void ServerFamily::OnClose(ConnectionContext* cntx) {
  dfly_cmd_->OnClose(cntx->conn_state.replication_info.repl_session_id);
}

void ServerFamily::StatsMC(std::string_view section, SinkReplyBuilder* builder) {
  if (!section.empty()) {
    return builder->SendError("");
  }
  string info;

#define ADD_LINE(name, val) absl::StrAppend(&info, "STAT " #name " ", val, "\r\n")

  time_t now = time(NULL);
  struct rusage ru;
  getrusage(RUSAGE_SELF, &ru);

  auto dbl_time = [](const timeval& tv) -> double {
    return tv.tv_sec + double(tv.tv_usec) / 1000000.0;
  };

  double utime = dbl_time(ru.ru_utime);
  double systime = dbl_time(ru.ru_stime);
  auto kind = ProactorBase::me()->GetKind();
  const char* multiplex_api = (kind == ProactorBase::IOURING) ? "iouring" : "epoll";

  Metrics m = GetMetrics(&namespaces->GetDefaultNamespace());
  uint64_t uptime = time(NULL) - start_time_;

  ADD_LINE(pid, getpid());
  ADD_LINE(uptime, uptime);
  ADD_LINE(time, now);
  ADD_LINE(version, kGitTag);
  ADD_LINE(libevent, multiplex_api);
  ADD_LINE(pointer_size, sizeof(void*));
  ADD_LINE(rusage_user, utime);
  ADD_LINE(rusage_system, systime);
  ADD_LINE(max_connections, -1);
  ADD_LINE(curr_connections, m.facade_stats.conn_stats.num_conns);
  ADD_LINE(total_connections, -1);
  ADD_LINE(rejected_connections, -1);
  ADD_LINE(bytes_read, m.facade_stats.conn_stats.io_read_bytes);
  ADD_LINE(bytes_written, m.facade_stats.reply_stats.io_write_bytes);
  ADD_LINE(limit_maxbytes, -1);

  absl::StrAppend(&info, "END\r\n");

  MCReplyBuilder* mc_builder = static_cast<MCReplyBuilder*>(builder);
  mc_builder->SendRaw(info);

#undef ADD_LINE
}

GenericError ServerFamily::DoSave(bool ignore_state) {
  const CommandId* cid = service().FindCmd("SAVE");
  CHECK_NOTNULL(cid);
  boost::intrusive_ptr<Transaction> trans(new Transaction{cid});
  trans->InitByArgs(&namespaces->GetDefaultNamespace(), 0, {});
  return DoSave(absl::GetFlag(FLAGS_df_snapshot_format), {}, trans.get(), ignore_state);
}

GenericError ServerFamily::DoSaveCheckAndStart(bool new_version, string_view basename,
                                               Transaction* trans, bool ignore_state) {
  auto state = ServerState::tlocal()->gstate();

  // In some cases we want to create a snapshot even if server is not active, f.e in takeover
  if (!ignore_state && (state != GlobalState::ACTIVE && state != GlobalState::SHUTTING_DOWN)) {
    return GenericError{make_error_code(errc::operation_in_progress),
                        StrCat(GlobalStateName(state), " - can not save database")};
  }
  {
    util::fb2::LockGuard lk(save_mu_);
    if (save_controller_) {
      return GenericError{make_error_code(errc::operation_in_progress),
                          "SAVING - can not save database"};
    }

    VLOG(1) << "Saving snapshot to " << basename;

    save_controller_ = make_unique<SaveStagesController>(detail::SaveStagesInputs{
        new_version, basename, trans, &service_, fq_threadpool_.get(), snapshot_storage_});

    auto res = save_controller_->InitResourcesAndStart();

    if (res) {
      DCHECK_EQ(res->error, true);
      last_save_info_.SetLastSaveError(*res);
      save_controller_.reset();
      return res->error;
    }
  }
  return {};
}

GenericError ServerFamily::WaitUntilSaveFinished(Transaction* trans, bool ignore_state) {
  save_controller_->WaitAllSnapshots();
  detail::SaveInfo save_info;

  VLOG(1) << "Before WaitUntilSaveFinished::Finalize";
  {
    util::fb2::LockGuard lk(save_mu_);
    save_info = save_controller_->Finalize();

    if (save_info.error) {
      last_save_info_.SetLastSaveError(save_info);
    } else {
      last_save_info_.save_time = save_info.save_time;
      last_save_info_.success_duration_sec = save_info.duration_sec;
      last_save_info_.file_name = save_info.file_name;
      last_save_info_.freq_map = save_info.freq_map;
    }
    save_controller_.reset();
  }

  return save_info.error;
}

GenericError ServerFamily::DoSave(bool new_version, string_view basename, Transaction* trans,
                                  bool ignore_state) {
  if (auto ec = DoSaveCheckAndStart(new_version, basename, trans, ignore_state); ec) {
    return ec;
  }

  return WaitUntilSaveFinished(trans, ignore_state);
}

bool ServerFamily::TEST_IsSaving() const {
  std::atomic_bool is_saving{false};
  shard_set->pool()->AwaitFiberOnAll([&](auto*) {
    if (SliceSnapshot::IsSnaphotInProgress())
      is_saving.store(true, std::memory_order_relaxed);
  });
  return is_saving.load(std::memory_order_relaxed);
}

error_code ServerFamily::Drakarys(Transaction* transaction, DbIndex db_ind) {
  VLOG(1) << "Drakarys";

  transaction->Execute(
      [db_ind](Transaction* t, EngineShard* shard) {
        t->GetDbSlice(shard->shard_id()).FlushDb(db_ind);
        return OpStatus::OK;
      },
      true);

  return error_code{};
}

LastSaveInfo ServerFamily::GetLastSaveInfo() const {
  util::fb2::LockGuard lk(save_mu_);
  return last_save_info_;
}

void ServerFamily::DbSize(CmdArgList args, const CommandContext& cmd_cntx) {
  atomic_ulong num_keys{0};

  shard_set->RunBriefInParallel(
      [&](EngineShard* shard) {
        auto db_size = cmd_cntx.conn_cntx->ns->GetDbSlice(shard->shard_id())
                           .DbSize(cmd_cntx.conn_cntx->conn_state.db_index);
        num_keys.fetch_add(db_size, memory_order_relaxed);
      },
      [](ShardId) { return true; });

  return cmd_cntx.rb->SendLong(num_keys.load(memory_order_relaxed));
}

void ServerFamily::CancelBlockingOnThread(std::function<OpStatus(ArgSlice)> status_cb) {
  auto cb = [status_cb](unsigned thread_index, util::Connection* conn) {
    if (auto fcntx = static_cast<facade::Connection*>(conn)->cntx(); fcntx) {
      auto* cntx = static_cast<ConnectionContext*>(fcntx);
      if (cntx->transaction) {
        cntx->transaction->CancelBlocking(status_cb);
      }
    }
  };

  for (auto* listener : listeners_) {
    listener->TraverseConnectionsOnThread(cb, UINT32_MAX, nullptr);
  }
}

string GetPassword() {
  string flag = GetFlag(FLAGS_requirepass);
  if (!flag.empty()) {
    return flag;
  }

  const char* env_var = getenv("DFLY_PASSWORD");
  if (env_var) {
    return env_var;
  }

  return "";
}

void ServerFamily::SendInvalidationMessages() const {
  // send invalidation message (caused by flushdb) to all the clients which
  // turned on client tracking
  auto cb = [](unsigned thread_index, util::Connection* conn) {
    facade::ConnectionContext* fc = static_cast<facade::Connection*>(conn)->cntx();
    if (fc) {
      ConnectionContext* cntx = static_cast<ConnectionContext*>(fc);
      if (cntx->conn_state.tracking_info_.IsTrackingOn()) {
        facade::Connection::InvalidationMessage x;
        x.invalidate_due_to_flush = true;
        cntx->conn()->SendInvalidationMessageAsync(x);
      }
    }
  };
  for (auto* listener : listeners_) {
    listener->TraverseConnections(cb);
  }
}

void ServerFamily::FlushDb(CmdArgList args, const CommandContext& cmd_cntx) {
  DCHECK(cmd_cntx.tx);
  Drakarys(cmd_cntx.tx, cmd_cntx.tx->GetDbIndex());
  SendInvalidationMessages();
  cmd_cntx.rb->SendOk();
}

void ServerFamily::FlushAll(CmdArgList args, const CommandContext& cmd_cntx) {
  if (args.size() > 1) {
    cmd_cntx.rb->SendError(kSyntaxErr);
    return;
  }

  DCHECK(cmd_cntx.tx);
  Drakarys(cmd_cntx.tx, DbSlice::kDbAll);
  SendInvalidationMessages();
  cmd_cntx.rb->SendOk();
}

bool ServerFamily::DoAuth(ConnectionContext* cntx, std::string_view username,
                          std::string_view password) {
  const auto* registry = ServerState::tlocal()->user_registry;
  CHECK(registry);
  const bool is_authorized = registry->AuthUser(username, password);
  if (is_authorized) {
    cntx->authed_username = username;
    auto cred = registry->GetCredentials(username);
    cntx->acl_commands = cred.acl_commands;
    cntx->keys = std::move(cred.keys);
    cntx->pub_sub = std::move(cred.pub_sub);
    cntx->ns = &namespaces->GetOrInsert(cred.ns);
    cntx->authenticated = true;
  }
  return is_authorized;
}

void ServerFamily::Auth(CmdArgList args, const CommandContext& cmd_cntx) {
  if (args.size() > 2) {
    return cmd_cntx.rb->SendError(kSyntaxErr);
  }

  ConnectionContext* cntx = cmd_cntx.conn_cntx;
  // non admin port auth
  if (!cntx->conn()->IsPrivileged()) {
    const bool one_arg = args.size() == 1;
    std::string_view username = one_arg ? "default" : facade::ToSV(args[0]);
    const size_t index = one_arg ? 0 : 1;
    std::string_view password = facade::ToSV(args[index]);
    if (DoAuth(cntx, username, password)) {
      return cmd_cntx.rb->SendOk();
    }
    auto& log = ServerState::tlocal()->acl_log;
    using Reason = acl::AclLog::Reason;
    log.Add(*cntx, "AUTH", Reason::AUTH, std::string(username));
    return cmd_cntx.rb->SendError(facade::kAuthRejected);
  }

  if (!cntx->req_auth) {
    return cmd_cntx.rb->SendError(
        "AUTH <password> called without any password configured for "
        "admin port. Are you sure your configuration is correct?");
  }

  string_view pass = ArgS(args, 0);
  if (pass == GetPassword()) {
    cntx->authenticated = true;
    cmd_cntx.rb->SendOk();
  } else {
    cmd_cntx.rb->SendError(facade::kAuthRejected);
  }
}

void ServerFamily::Client(CmdArgList args, const CommandContext& cmd_cntx) {
  string sub_cmd = absl::AsciiStrToUpper(ArgS(args, 0));
  CmdArgList sub_args = args.subspan(1);
  auto* builder = cmd_cntx.rb;
  auto* cntx = cmd_cntx.conn_cntx;

  if (sub_cmd == "SETNAME") {
    return ClientSetName(sub_args, builder, cntx);
  } else if (sub_cmd == "GETNAME") {
    return ClientGetName(sub_args, builder, cntx);
  } else if (sub_cmd == "LIST") {
    return ClientList(sub_args, absl::MakeSpan(listeners_), builder, cntx);
  } else if (sub_cmd == "PAUSE") {
    return ClientPauseCmd(sub_args, GetNonPriviligedListeners(), builder, cntx);
  } else if (sub_cmd == "TRACKING") {
    return ClientTracking(sub_args, builder, cntx);
  } else if (sub_cmd == "KILL") {
    return ClientKill(sub_args, absl::MakeSpan(listeners_), builder, cntx);
  } else if (sub_cmd == "CACHING") {
    return ClientCaching(sub_args, builder, cmd_cntx.tx, cntx);
  } else if (sub_cmd == "SETINFO") {
    return ClientSetInfo(sub_args, builder, cntx);
  } else if (sub_cmd == "ID") {
    return ClientId(sub_args, builder, cntx);
  }

  LOG_FIRST_N(ERROR, 10) << "Subcommand " << sub_cmd << " not supported";
  return builder->SendError(UnknownSubCmd(sub_cmd, "CLIENT"), kSyntaxErrType);
}

void ServerFamily::Config(CmdArgList args, const CommandContext& cmd_cntx) {
  string sub_cmd = absl::AsciiStrToUpper(ArgS(args, 0));

  auto* builder = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  if (sub_cmd == "HELP") {
    string_view help_arr[] = {
        "CONFIG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        "GET <pattern>",
        "    Return parameters matching the glob-like <pattern> and their values.",
        "SET <directive> <value>",
        "    Set the configuration <directive> to <value>.",
        "RESETSTAT",
        "    Reset statistics reported by the INFO command.",
        "HELP",
        "    Prints this help.",
    };

    return builder->SendSimpleStrArr(help_arr);
  }

  if (sub_cmd == "SET") {
    if (args.size() != 3) {
      return builder->SendError(WrongNumArgsError("config|set"));
    }

    string param = absl::AsciiStrToLower(ArgS(args, 1));

    ConfigRegistry::SetResult result = config_registry.Set(param, ArgS(args, 2));

    const char kErrPrefix[] = "CONFIG SET failed (possibly related to argument '";
    switch (result) {
      case ConfigRegistry::SetResult::OK:
        return builder->SendOk();
      case ConfigRegistry::SetResult::UNKNOWN:
        return builder->SendError(
            absl::StrCat("Unknown option or number of arguments for CONFIG SET - '", param, "'"),
            kConfigErrType);

      case ConfigRegistry::SetResult::READONLY:
        return builder->SendError(
            absl::StrCat(kErrPrefix, param, "') - can't set immutable config"), kConfigErrType);

      case ConfigRegistry::SetResult::INVALID:
        return builder->SendError(absl::StrCat(kErrPrefix, param, "') - argument can not be set"),
                                  kConfigErrType);
    }
    ABSL_UNREACHABLE();
  }

  if (sub_cmd == "GET" && args.size() == 2) {
    vector<string> res;
    string_view param = ArgS(args, 1);

    // Support 'databases' for backward compatibility.
    if (param == "databases") {
      res.emplace_back(param);
      res.push_back(absl::StrCat(absl::GetFlag(FLAGS_dbnum)));
    } else {
      vector<string> names = config_registry.List(param);

      for (const auto& name : names) {
        auto value = config_registry.Get(name);
        DCHECK(value.has_value());
        if (value.has_value()) {
          res.push_back(name);
          res.push_back(*value);
        }
      }
    }
    auto* rb = static_cast<RedisReplyBuilder*>(builder);
    return rb->SendBulkStrArr(res, RedisReplyBuilder::MAP);
  }

  if (sub_cmd == "RESETSTAT") {
    ResetStat(cmd_cntx.conn_cntx->ns);
    return builder->SendOk();
  } else {
    return builder->SendError(UnknownSubCmd(sub_cmd, "CONFIG"), kSyntaxErrType);
  }
}

void ServerFamily::Debug(CmdArgList args, const CommandContext& cmd_cntx) {
  DebugCmd dbg_cmd{this, &service_.cluster_family(), cmd_cntx.conn_cntx};

  return dbg_cmd.Run(args, cmd_cntx.rb);
}

void ServerFamily::Memory(CmdArgList args, const CommandContext& cmd_cntx) {
  MemoryCmd mem_cmd{this, cmd_cntx.rb, cmd_cntx.conn_cntx};

  return mem_cmd.Run(args);
}

void ServerFamily::BgSaveFb(boost::intrusive_ptr<Transaction> trans) {
  GenericError ec = WaitUntilSaveFinished(trans.get());
  if (ec) {
    LOG(INFO) << "Error in BgSaveFb: " << ec.Format();
  }
}

std::optional<ServerFamily::VersionBasename> ServerFamily::GetVersionAndBasename(
    CmdArgList args, SinkReplyBuilder* builder) {
  if (args.size() > 2) {
    builder->SendError(kSyntaxErr);
    return {};
  }

  bool new_version = absl::GetFlag(FLAGS_df_snapshot_format);

  if (args.size() >= 1) {
    string sub_cmd = absl::AsciiStrToUpper(ArgS(args, 0));
    if (sub_cmd == "DF") {
      new_version = true;
    } else if (sub_cmd == "RDB") {
      new_version = false;
    } else {
      builder->SendError(UnknownSubCmd(sub_cmd, "SAVE"), kSyntaxErrType);
      return {};
    }
  }

  string_view basename;
  if (args.size() == 2) {
    basename = ArgS(args, 1);
  }

  return ServerFamily::VersionBasename{new_version, basename};
}

// BGSAVE [DF|RDB] [basename]
// TODO add missing [SCHEDULE]
void ServerFamily::BgSave(CmdArgList args, const CommandContext& cmd_cntx) {
  auto maybe_res = GetVersionAndBasename(args, cmd_cntx.rb);
  if (!maybe_res) {
    return;
  }

  const auto [version, basename] = *maybe_res;

  if (auto ec = DoSaveCheckAndStart(version, basename, cmd_cntx.tx); ec) {
    cmd_cntx.rb->SendError(ec.Format());
    return;
  }
  bg_save_fb_.JoinIfNeeded();
  bg_save_fb_ = fb2::Fiber("bg_save_fiber", &ServerFamily::BgSaveFb, this,
                           boost::intrusive_ptr<Transaction>(cmd_cntx.tx));
  cmd_cntx.rb->SendOk();
}

// SAVE [DF|RDB] [basename]
// Allows saving the snapshot of the dataset on disk, potentially overriding the format
// and the snapshot name.
void ServerFamily::Save(CmdArgList args, const CommandContext& cmd_cntx) {
  auto maybe_res = GetVersionAndBasename(args, cmd_cntx.rb);
  if (!maybe_res) {
    return;
  }

  const auto [version, basename] = *maybe_res;

  GenericError ec = DoSave(version, basename, cmd_cntx.tx);
  if (ec) {
    cmd_cntx.rb->SendError(ec.Format());
  } else {
    cmd_cntx.rb->SendOk();
  }
}

static void MergeDbSliceStats(const DbSlice::Stats& src, Metrics* dest) {
  if (src.db_stats.size() > dest->db_stats.size())
    dest->db_stats.resize(src.db_stats.size());

  for (size_t i = 0; i < src.db_stats.size(); ++i)
    dest->db_stats[i] += src.db_stats[i];

  dest->events += src.events;
  dest->small_string_bytes += src.small_string_bytes;
}

void ServerFamily::ResetStat(Namespace* ns) {
  shard_set->pool()->AwaitBrief(
      [registry = service_.mutable_registry(), ns](unsigned index, auto*) {
        registry->ResetCallStats(index);
        ns->GetCurrentDbSlice().ResetEvents();
        facade::ResetStats();
        ServerState::tlocal()->exec_freq_count.clear();
      });
}

Metrics ServerFamily::GetMetrics(Namespace* ns) const {
  Metrics result;
  util::fb2::Mutex mu;

  uint64_t start = absl::GetCurrentTimeNanos();

  auto cmd_stat_cb = [&dest = result.cmd_stats_map](string_view name, const CmdCallStats& stat) {
    auto& [calls, sum] = dest[absl::AsciiStrToLower(name)];
    calls += stat.first;
    sum += stat.second;
  };

  auto cb = [&](unsigned index, ProactorBase* pb) {
    EngineShard* shard = EngineShard::tlocal();
    ServerState* ss = ServerState::tlocal();

    lock_guard lk(mu);

    result.fiber_switch_cnt += fb2::FiberSwitchEpoch();
    result.fiber_switch_delay_usec += fb2::FiberSwitchDelayUsec();
    result.fiber_longrun_cnt += fb2::FiberLongRunCnt();
    result.fiber_longrun_usec += fb2::FiberLongRunSumUsec();
    result.worker_fiber_stack_size += fb2::WorkerFibersStackSize();
    result.worker_fiber_count += fb2::WorkerFibersCount();
    result.blocked_tasks += TaskQueue::blocked_submitters();

    result.coordinator_stats.Add(ss->stats);

    result.qps += uint64_t(ss->MovingSum6());
    result.facade_stats += *tl_facade_stats;
    result.serialization_bytes += SliceSnapshot::GetThreadLocalMemoryUsage();

    if (shard) {
      result.heap_used_bytes += shard->UsedMemory();
      MergeDbSliceStats(ns->GetDbSlice(shard->shard_id()).GetStats(), &result);
      result.shard_stats += shard->stats();

      if (shard->tiered_storage()) {
        result.tiered_stats += shard->tiered_storage()->GetStats();
      }

      if (shard->search_indices()) {
        result.search_stats += shard->search_indices()->GetStats();
      }

      result.traverse_ttl_per_sec += shard->GetMovingSum6(EngineShard::TTL_TRAVERSE);
      result.delete_ttl_per_sec += shard->GetMovingSum6(EngineShard::TTL_DELETE);
      if (result.tx_queue_len < shard->txq()->size())
        result.tx_queue_len = shard->txq()->size();
    }  // if (shard)

    result.tls_bytes += Listener::TLSUsedMemoryThreadLocal();
    result.refused_conn_max_clients_reached_count += Listener::RefusedConnectionMaxClientsCount();

    result.lua_stats += InterpreterManager::tl_stats();

    auto connections_lib_name_ver_map = facade::Connection::GetLibStatsTL();
    for (auto& [k, v] : connections_lib_name_ver_map) {
      result.connections_lib_name_ver_map[k] += v;
    }

    auto& send_list = facade::SinkReplyBuilder::pending_list;
    if (!send_list.empty()) {
      DCHECK(std::is_sorted(send_list.begin(), send_list.end(),
                            [](const auto& left, const auto& right) {
                              return left.timestamp_ns < right.timestamp_ns;
                            }));

      auto& oldest_member = send_list.front();
      result.oldest_pending_send_ts =
          min<uint64_t>(result.oldest_pending_send_ts, oldest_member.timestamp_ns);
    }
    service_.mutable_registry()->MergeCallStats(index, cmd_stat_cb);
  };  // cb

  service_.proactor_pool().AwaitFiberOnAll(std::move(cb));

  uint64_t after_cb = absl::GetCurrentTimeNanos();

  // Normalize moving average stats
  result.qps /= 6;
  result.traverse_ttl_per_sec /= 6;
  result.delete_ttl_per_sec /= 6;

  bool is_master = ServerState::tlocal() && ServerState::tlocal()->is_master;

  if (!is_master) {
    auto info = GetReplicaSummary();
    if (info) {
      result.replica_side_info = {
          .reconnect_count = info->reconnect_count,
      };
    }
  }

  {
    util::fb2::LockGuard lk{loading_stats_mu_};
    result.loading_stats = loading_stats_;
  }

  // Update peak stats. We rely on the fact that GetMetrics is called frequently enough to
  // update peak_stats_ from it.
  util::fb2::LockGuard lk{peak_stats_mu_};
  UpdateMax(&peak_stats_.conn_dispatch_queue_bytes,
            result.facade_stats.conn_stats.dispatch_queue_bytes);
  UpdateMax(&peak_stats_.conn_read_buf_capacity, result.facade_stats.conn_stats.read_buf_capacity);

  result.peak_stats = peak_stats_;

  uint64_t delta_ms = (absl::GetCurrentTimeNanos() - start) / 1'000'000;
  if (delta_ms > 30) {
    uint64_t cb_dur = (after_cb - start) / 1'000'000;
    LOG(INFO) << "GetMetrics took " << delta_ms << " ms, out of which callback took " << cb_dur
              << " ms";
  }
  return result;
}

void ServerFamily::Info(CmdArgList args, const CommandContext& cmd_cntx) {
  if (args.size() > 1) {
    return cmd_cntx.rb->SendError(kSyntaxErr);
  }

  string section;

  if (args.size() == 1) {
    section = absl::AsciiStrToUpper(ArgS(args, 0));
  }

  string info;

  auto should_enter = [&](string_view name, bool hidden = false) {
    if ((!hidden && section.empty()) || section == "ALL" || section == name) {
      auto normalized_name = string{name.substr(0, 1)} + absl::AsciiStrToLower(name.substr(1));
      absl::StrAppend(&info, info.empty() ? "" : "\r\n", "# ", normalized_name, "\r\n");
      return true;
    }
    return false;
  };

  auto append = [&info](absl::AlphaNum a1, absl::AlphaNum a2) {
    absl::StrAppend(&info, a1, ":", a2, "\r\n");
  };

  ServerState* ss = ServerState::tlocal();

  bool show_managed_info =
      !absl::GetFlag(FLAGS_managed_service_info) || cmd_cntx.conn_cntx->conn()->IsPrivileged();

  if (should_enter("SERVER")) {
    auto kind = ProactorBase::me()->GetKind();
    const char* multiplex_api = (kind == ProactorBase::IOURING) ? "iouring" : "epoll";

    append("redis_version", kRedisVersion);
    append("dragonfly_version", GetVersion());
    append("redis_mode", GetRedisMode());
    append("arch_bits", 64);

    if (show_managed_info) {
      append("os", GetOSString());
      append("thread_count", service_.proactor_pool().size());
    }
    append("multiplexing_api", multiplex_api);
    append("tcp_port", GetFlag(FLAGS_port));

    uint64_t uptime = time(NULL) - start_time_;
    append("uptime_in_seconds", uptime);
    append("uptime_in_days", uptime / (3600 * 24));
  }

  Metrics m;
  // Save time by not calculating metrics if we don't need them.
  if (!(section == "SERVER" || section == "REPLICATION")) {
    m = GetMetrics(cmd_cntx.conn_cntx->ns);
  }

  DbStats total;
  for (const auto& db_stats : m.db_stats)
    total += db_stats;

  if (should_enter("CLIENTS")) {
    append("connected_clients", m.facade_stats.conn_stats.num_conns);
    append("max_clients", GetFlag(FLAGS_maxclients));
    append("client_read_buffer_bytes", m.facade_stats.conn_stats.read_buf_capacity);
    append("blocked_clients", m.facade_stats.conn_stats.num_blocked_clients);
    append("pipeline_queue_length", m.facade_stats.conn_stats.dispatch_queue_entries);

    append("send_delay_ms", GetDelayMs(m.oldest_pending_send_ts));
    append("timeout_disconnects", m.coordinator_stats.conn_timeout_events);
  }

  if (should_enter("MEMORY")) {
    append("used_memory", m.heap_used_bytes);
    append("used_memory_human", HumanReadableNumBytes(m.heap_used_bytes));
    const auto ump = used_mem_peak.load(memory_order_relaxed);
    append("used_memory_peak", ump);
    append("used_memory_peak_human", HumanReadableNumBytes(ump));

    // Virtual memory size, upper bound estimation on the RSS memory used by the fiber stacks.
    append("fibers_stack_vms", m.worker_fiber_stack_size);
    append("fibers_count", m.worker_fiber_count);

    size_t rss = rss_mem_current.load(memory_order_relaxed);
    append("used_memory_rss", rss);
    append("used_memory_rss_human", HumanReadableNumBytes(rss));
    append("used_memory_peak_rss", rss_mem_peak.load(memory_order_relaxed));

    append("maxmemory", max_memory_limit);
    append("maxmemory_human", HumanReadableNumBytes(max_memory_limit));

    append("used_memory_lua", m.lua_stats.used_bytes);

    // Blob - all these cases where the key/objects are represented by a single blob allocated on
    // heap. For example, strings or intsets. members of lists, sets, zsets etc
    // are not accounted for to avoid complex computations. In some cases, when number of members
    // is known we approximate their allocations by taking 16 bytes per member.
    append("object_used_memory", total.obj_memory_usage);

    for (unsigned type = 0; type < total.memory_usage_by_type.size(); type++) {
      size_t mem = total.memory_usage_by_type[type];
      if (mem > 0) {
        append(absl::StrCat("type_used_memory_", ObjTypeToString(type)), mem);
      }
    }
    append("table_used_memory", total.table_mem_usage);
    append("num_buckets", total.bucket_count);
    append("num_entries", total.key_count);
    append("inline_keys", total.inline_keys);
    append("listpack_blobs", total.listpack_blob_cnt);
    append("listpack_bytes", total.listpack_bytes);
    append("small_string_bytes", m.small_string_bytes);
    append("pipeline_cache_bytes", m.facade_stats.conn_stats.pipeline_cmd_cache_bytes);
    append("dispatch_queue_bytes", m.facade_stats.conn_stats.dispatch_queue_bytes);
    append("dispatch_queue_subscriber_bytes",
           m.facade_stats.conn_stats.dispatch_queue_subscriber_bytes);
    append("dispatch_queue_peak_bytes", m.peak_stats.conn_dispatch_queue_bytes);
    append("client_read_buffer_peak_bytes", m.peak_stats.conn_read_buf_capacity);
    append("tls_bytes", m.tls_bytes);
    append("snapshot_serialization_bytes", m.serialization_bytes);
    append("commands_squashing_replies_bytes", MultiCommandSquasher::GetRepliesMemSize());

    if (GetFlag(FLAGS_cache_mode)) {
      append("cache_mode", "cache");
      // PHP Symphony needs this field to work.
      append("maxmemory_policy", "eviction");
    } else {
      append("cache_mode", "store");
      // Compatible with redis based frameworks.
      append("maxmemory_policy", "noeviction");
    }

    if (!m.replica_side_info) {  // master
      ReplicationMemoryStats repl_mem;
      dfly_cmd_->GetReplicationMemoryStats(&repl_mem);
      append("replication_streaming_buffer_bytes", repl_mem.streamer_buf_capacity_bytes);
      append("replication_full_sync_buffer_bytes", repl_mem.full_sync_buf_bytes);
    }

    {
      util::fb2::LockGuard lk{save_mu_};
      if (save_controller_) {
        append("save_buffer_bytes", save_controller_->GetSaveBuffersSize());
      }
    }
  }

  if (should_enter("STATS")) {
    auto& conn_stats = m.facade_stats.conn_stats;
    auto& reply_stats = m.facade_stats.reply_stats;

    append("total_connections_received", conn_stats.conn_received_cnt);
    append("total_commands_processed", conn_stats.command_cnt);
    append("instantaneous_ops_per_sec", m.qps);
    append("total_pipelined_commands", conn_stats.pipelined_cmd_cnt);
    append("total_pipelined_squashed_commands", m.coordinator_stats.squashed_commands);
    append("pipeline_throttle_total", conn_stats.pipeline_throttle_count);
    append("pipelined_latency_usec", conn_stats.pipelined_cmd_latency);
    append("total_net_input_bytes", conn_stats.io_read_bytes);
    append("connection_migrations", conn_stats.num_migrations);
    append("total_net_output_bytes", reply_stats.io_write_bytes);
    append("rdb_save_usec", m.coordinator_stats.rdb_save_usec);
    append("rdb_save_count", m.coordinator_stats.rdb_save_count);
    append("big_value_preemptions", m.coordinator_stats.big_value_preemptions);
    append("compressed_blobs", m.coordinator_stats.compressed_blobs);
    append("instantaneous_input_kbps", -1);
    append("instantaneous_output_kbps", -1);
    append("rejected_connections", -1);
    append("expired_keys", m.events.expired_keys);
    append("evicted_keys", m.events.evicted_keys);
    append("hard_evictions", m.events.hard_evictions);
    append("garbage_checked", m.events.garbage_checked);
    append("garbage_collected", m.events.garbage_collected);
    append("bump_ups", m.events.bumpups);
    append("stash_unloaded", m.events.stash_unloaded);
    append("oom_rejections", m.events.insertion_rejections + m.coordinator_stats.oom_error_cmd_cnt);
    append("traverse_ttl_sec", m.traverse_ttl_per_sec);
    append("delete_ttl_sec", m.delete_ttl_per_sec);
    append("keyspace_hits", m.events.hits);
    append("keyspace_misses", m.events.misses);
    append("keyspace_mutations", m.events.mutations);
    append("total_reads_processed", conn_stats.io_read_cnt);
    append("total_writes_processed", reply_stats.io_write_cnt);
    append("defrag_attempt_total", m.shard_stats.defrag_attempt_total);
    append("defrag_realloc_total", m.shard_stats.defrag_realloc_total);
    append("defrag_task_invocation_total", m.shard_stats.defrag_task_invocation_total);
    append("reply_count", reply_stats.send_stats.count);
    append("reply_latency_usec", reply_stats.send_stats.total_duration);

    // Number of connections that are currently blocked on grabbing interpreter.
    append("blocked_on_interpreter", m.coordinator_stats.blocked_on_interpreter);
    append("lua_interpreter_cnt", m.lua_stats.interpreter_cnt);

    // Total number of events of when a connection was blocked on grabbing interpreter.
    append("lua_blocked_total", m.lua_stats.blocked_cnt);
  }

  if (should_enter("TIERED", true)) {
    append("tiered_entries", total.tiered_entries);
    append("tiered_entries_bytes", total.tiered_used_bytes);

    append("tiered_total_stashes", m.tiered_stats.total_stashes);
    append("tiered_total_fetches", m.tiered_stats.total_fetches);
    append("tiered_total_cancels", m.tiered_stats.total_cancels);
    append("tiered_total_deletes", m.tiered_stats.total_deletes);
    append("tiered_total_uploads", m.tiered_stats.total_uploads);
    append("tiered_total_stash_overflows", m.tiered_stats.total_stash_overflows);
    append("tiered_heap_buf_allocations", m.tiered_stats.total_heap_buf_allocs);
    append("tiered_registered_buf_allocations", m.tiered_stats.total_registered_buf_allocs);

    append("tiered_allocated_bytes", m.tiered_stats.allocated_bytes);
    append("tiered_capacity_bytes", m.tiered_stats.capacity_bytes);

    append("tiered_pending_read_cnt", m.tiered_stats.pending_read_cnt);
    append("tiered_pending_stash_cnt", m.tiered_stats.pending_stash_cnt);

    append("tiered_small_bins_cnt", m.tiered_stats.small_bins_cnt);
    append("tiered_small_bins_entries_cnt", m.tiered_stats.small_bins_entries_cnt);
    append("tiered_small_bins_filling_bytes", m.tiered_stats.small_bins_filling_bytes);
    append("tiered_cold_storage_bytes", m.tiered_stats.cold_storage_bytes);
    append("tiered_offloading_steps", m.tiered_stats.total_offloading_steps);
    append("tiered_offloading_stashes", m.tiered_stats.total_offloading_stashes);
    append("tiered_ram_hits", m.events.ram_hits);
    append("tiered_ram_cool_hits", m.events.ram_cool_hits);
    append("tiered_ram_misses", m.events.ram_misses);
  }

  if (should_enter("PERSISTENCE", true)) {
    size_t current_snap_keys = 0;
    size_t total_snap_keys = 0;
    double perc = 0;
    bool is_saving = false;
    uint32_t curent_durration_sec = 0;
    {
      util::fb2::LockGuard lk{save_mu_};
      if (save_controller_) {
        is_saving = true;
        curent_durration_sec = save_controller_->GetCurrentSaveDuration();
        auto res = save_controller_->GetCurrentSnapshotProgress();
        if (res.total_keys != 0) {
          current_snap_keys = res.current_keys;
          total_snap_keys = res.total_keys;
          perc = (static_cast<double>(current_snap_keys) / total_snap_keys) * 100;
        }
      }
    }

    append("current_snapshot_perc", perc);
    append("current_save_keys_processed", current_snap_keys);
    append("current_save_keys_total", total_snap_keys);

    auto save_info = GetLastSaveInfo();
    // when last success save
    append("last_success_save", save_info.save_time);
    append("last_saved_file", save_info.file_name);
    append("last_success_save_duration_sec", save_info.success_duration_sec);

    unsigned is_loading = (ss->gstate() == GlobalState::LOADING);
    append("loading", is_loading);
    append("saving", is_saving);
    append("current_save_duration_sec", curent_durration_sec);

    for (const auto& k_v : save_info.freq_map) {
      append(StrCat("rdb_", k_v.first), k_v.second);
    }
    append("rdb_changes_since_last_success_save", m.events.update);

    // when last failed save
    append("last_failed_save", save_info.last_error_time);
    append("last_error", save_info.last_error.Format());
    append("last_failed_save_duration_sec", save_info.failed_duration_sec);
  }

  if (should_enter("TRANSACTION", true)) {
    append("tx_shard_polls", m.shard_stats.poll_execution_total);
    append("tx_shard_optimistic_total", m.shard_stats.tx_optimistic_total);
    append("tx_shard_ooo_total", m.shard_stats.tx_ooo_total);
    append("tx_global_total", m.coordinator_stats.tx_global_cnt);
    append("tx_normal_total", m.coordinator_stats.tx_normal_cnt);
    append("tx_inline_runs_total", m.coordinator_stats.tx_inline_runs);
    append("tx_schedule_cancel_total", m.coordinator_stats.tx_schedule_cancel_cnt);
    append("tx_batch_scheduled_items_total", m.shard_stats.tx_batch_scheduled_items_total);
    append("tx_batch_schedule_calls_total", m.shard_stats.tx_batch_schedule_calls_total);
    append("tx_with_freq", absl::StrJoin(m.coordinator_stats.tx_width_freq_arr, ","));
    append("tx_queue_len", m.tx_queue_len);

    append("eval_io_coordination_total", m.coordinator_stats.eval_io_coordination_cnt);
    append("eval_shardlocal_coordination_total",
           m.coordinator_stats.eval_shardlocal_coordination_cnt);
    append("eval_squashed_flushes", m.coordinator_stats.eval_squashed_flushes);
    append("multi_squash_execution_total", m.coordinator_stats.multi_squash_executions);
    append("multi_squash_execution_hop_usec", m.coordinator_stats.multi_squash_exec_hop_usec);
    append("multi_squash_execution_reply_usec", m.coordinator_stats.multi_squash_exec_reply_usec);
  }

  if (should_enter("REPLICATION")) {
    bool is_master = true;
    // Thread local var is_master is updated under mutex replicaof_mu_ together with replica_,
    // ensuring eventual consistency of is_master. When determining if the server is a replica and
    // accessing the replica_ object, we must lock replicaof_mu_. Using is_master alone is
    // insufficient in this scenario.
    // Please note that we we do not use Metrics object here.
    {
      fb2::LockGuard lk(replicaof_mu_);
      is_master = !replica_;
    }
    if (is_master) {
      vector<ReplicaRoleInfo> replicas_info = dfly_cmd_->GetReplicasRoleInfo();
      append("role", "master");
      append("connected_slaves", replicas_info.size());

      if (show_managed_info) {
        for (size_t i = 0; i < replicas_info.size(); i++) {
          auto& r = replicas_info[i];
          // e.g. slave0:ip=172.19.0.3,port=6379,state=full_sync
          append(StrCat("slave", i), StrCat("ip=", r.address, ",port=", r.listening_port,
                                            ",state=", r.state, ",lag=", r.lsn_lag));
        }
      }
      append("master_replid", master_replid_);
    } else {
      append("role", GetFlag(FLAGS_info_replication_valkey_compatible) ? "slave" : "replica");

      auto replication_info_cb = [&](const Replica::Summary& rinfo) {
        append("master_host", rinfo.host);
        append("master_port", rinfo.port);

        const char* link = rinfo.master_link_established ? "up" : "down";
        append("master_link_status", link);
        append("master_last_io_seconds_ago", rinfo.master_last_io_sec);
        append("master_sync_in_progress", rinfo.full_sync_in_progress);
        append("master_replid", rinfo.master_id);
        if (rinfo.full_sync_done)
          append("slave_repl_offset", rinfo.repl_offset_sum);
        append("slave_priority", GetFlag(FLAGS_replica_priority));
        append("slave_read_only", 1);
      };
      fb2::LockGuard lk(replicaof_mu_);

      replication_info_cb(replica_->GetSummary());

      // Special case, when multiple masters replicate to a single replica.
      for (const auto& replica : cluster_replicas_) {
        replication_info_cb(replica->GetSummary());
      }
    }
  }

  if (should_enter("COMMANDSTATS", true)) {
    auto append_sorted = [&append](string_view prefix, auto display) {
      sort(display.begin(), display.end());
      for (const auto& k_v : display) {
        append(StrCat(prefix, k_v.first), k_v.second);
      }
    };

    vector<pair<string_view, string>> commands;
    for (const auto& [name, stats] : m.cmd_stats_map) {
      const auto calls = stats.first, sum = stats.second;
      commands.push_back(
          {name, absl::StrJoin({absl::StrCat("calls=", calls), absl::StrCat("usec=", sum),
                                absl::StrCat("usec_per_call=", static_cast<double>(sum) / calls)},
                               ",")});
    }

    auto unknown_cmd = service_.UknownCmdMap();

    append_sorted("cmdstat_", std::move(commands));
    append_sorted("unknown_",
                  vector<pair<string_view, uint64_t>>(unknown_cmd.cbegin(), unknown_cmd.cend()));
  }

  if (should_enter("MODULES")) {
    append("module",
           "name=ReJSON,ver=20000,api=1,filters=0,usedby=[search],using=[],options=[handle-io-"
           "errors]");
    append("module",
           "name=search,ver=20000,api=1,filters=0,usedby=[],using=[ReJSON],options=[handle-io-"
           "errors]");
  }

  if (should_enter("SEARCH", true)) {
    append("search_memory", m.search_stats.used_memory);
    append("search_num_indices", m.search_stats.num_indices);
    append("search_num_entries", m.search_stats.num_entries);
  }

  if (should_enter("ERRORSTATS", true)) {
    for (const auto& k_v : m.facade_stats.reply_stats.err_count) {
      append(k_v.first, k_v.second);
    }
  }

  if (should_enter("KEYSPACE")) {
    for (size_t i = 0; i < m.db_stats.size(); ++i) {
      const auto& stats = m.db_stats[i];
      bool show = (i == 0) || (stats.key_count > 0);
      if (show) {
        string val = StrCat("keys=", stats.key_count, ",expires=", stats.expire_count,
                            ",avg_ttl=-1");  // TODO
        append(StrCat("db", i), val);
      }
    }
  }

#ifndef __APPLE__
  if (should_enter("CPU")) {
    struct rusage ru, cu, tu;
    getrusage(RUSAGE_SELF, &ru);
    getrusage(RUSAGE_CHILDREN, &cu);
    getrusage(RUSAGE_THREAD, &tu);
    append("used_cpu_sys", StrCat(ru.ru_stime.tv_sec, ".", ru.ru_stime.tv_usec));
    append("used_cpu_user", StrCat(ru.ru_utime.tv_sec, ".", ru.ru_utime.tv_usec));
    append("used_cpu_sys_children", StrCat(cu.ru_stime.tv_sec, ".", cu.ru_stime.tv_usec));
    append("used_cpu_user_children", StrCat(cu.ru_utime.tv_sec, ".", cu.ru_utime.tv_usec));
    append("used_cpu_sys_main_thread", StrCat(tu.ru_stime.tv_sec, ".", tu.ru_stime.tv_usec));
    append("used_cpu_user_main_thread", StrCat(tu.ru_utime.tv_sec, ".", tu.ru_utime.tv_usec));
  }
#endif

  if (should_enter("CLUSTER")) {
    append("cluster_enabled", IsClusterEnabledOrEmulated());
  }
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  rb->SendVerbatimString(info);
}

void ServerFamily::Hello(CmdArgList args, const CommandContext& cmd_cntx) {
  // If no arguments are provided default to RESP2.
  bool is_resp3 = false;
  bool has_auth = false;
  bool has_setname = false;
  string_view username;
  string_view password;
  string_view clientname;

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  if (args.size() > 0) {
    string_view proto_version = ArgS(args, 0);
    is_resp3 = proto_version == "3";
    bool valid_proto_version = proto_version == "2" || is_resp3;
    if (!valid_proto_version) {
      rb->SendError(UnknownCmd("HELLO", args));
      return;
    }

    for (uint32_t i = 1; i < args.size(); i++) {
      auto sub_cmd = ArgS(args, i);
      auto moreargs = args.size() - 1 - i;
      if (absl::EqualsIgnoreCase(sub_cmd, "AUTH") && moreargs >= 2) {
        has_auth = true;
        username = ArgS(args, i + 1);
        password = ArgS(args, i + 2);
        i += 2;
      } else if (absl::EqualsIgnoreCase(sub_cmd, "SETNAME") && moreargs > 0) {
        has_setname = true;
        clientname = ArgS(args, i + 1);
        i += 1;
      } else {
        rb->SendError(kSyntaxErr);
        return;
      }
    }
  }

  auto* cntx = cmd_cntx.conn_cntx;
  if (has_auth && !DoAuth(cntx, username, password)) {
    return rb->SendError(facade::kAuthRejected);
  }

  if (cntx->req_auth && !cntx->authenticated) {
    rb->SendError(
        "-NOAUTH HELLO must be called with the client already "
        "authenticated, otherwise the HELLO <proto> AUTH <user> <pass> "
        "option can be used to authenticate the client and "
        "select the RESP protocol version at the same time");
    return;
  }

  if (has_setname) {
    cntx->conn()->SetName(string{clientname});
  }

  int proto_version = 2;
  if (is_resp3) {
    proto_version = 3;
    rb->SetRespVersion(RespVersion::kResp3);
  } else {
    // Issuing hello 2 again is valid and should switch back to RESP2
    rb->SetRespVersion(RespVersion::kResp2);
  }

  SinkReplyBuilder::ReplyAggregator agg(rb);
  rb->StartCollection(7, RedisReplyBuilder::MAP);
  rb->SendBulkString("server");
  rb->SendBulkString("redis");
  rb->SendBulkString("version");
  rb->SendBulkString(kRedisVersion);
  rb->SendBulkString("dragonfly_version");
  rb->SendBulkString(GetVersion());
  rb->SendBulkString("proto");
  rb->SendLong(proto_version);
  rb->SendBulkString("id");
  rb->SendLong(cntx->conn()->GetClientId());
  rb->SendBulkString("mode");
  rb->SendBulkString(GetRedisMode());
  rb->SendBulkString("role");
  rb->SendBulkString((*ServerState::tlocal()).is_master ? "master" : "slave");
}

void ServerFamily::AddReplicaOf(CmdArgList args, const CommandContext& cmd_cntx) {
  util::fb2::LockGuard lk(replicaof_mu_);
  if (ServerState::tlocal()->is_master) {
    cmd_cntx.rb->SendError("Calling ADDREPLICAOFF allowed only after server is already a replica");
    return;
  }
  CHECK(replica_);

  auto replicaof_args = ReplicaOfArgs::FromCmdArgs(args, cmd_cntx.rb);
  if (!replicaof_args.has_value()) {
    return;
  }
  if (replicaof_args->IsReplicaOfNoOne()) {
    return cmd_cntx.rb->SendError("ADDREPLICAOF does not support no one");
  }
  LOG(INFO) << "Add Replica " << *replicaof_args;

  auto add_replica = make_unique<Replica>(replicaof_args->host, replicaof_args->port, &service_,
                                          master_replid(), replicaof_args->slot_range);
  error_code ec = add_replica->Start(cmd_cntx.rb);
  if (!ec) {
    cluster_replicas_.push_back(std::move(add_replica));
  }
}

void ServerFamily::ReplicaOfInternal(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder,
                                     ActionOnConnectionFail on_err) {
  std::shared_ptr<Replica> new_replica;
  {
    util::fb2::LockGuard lk(replicaof_mu_);  // Only one REPLICAOF command can run at a time

    // We should not execute replica of command while loading from snapshot.
    ServerState* ss = ServerState::tlocal();
    if (ss->is_master && ss->gstate() == GlobalState::LOADING) {
      builder->SendError(kLoadingErr);
      return;
    }

    auto replicaof_args = ReplicaOfArgs::FromCmdArgs(args, builder);
    if (!replicaof_args.has_value()) {
      return;
    }

    LOG(INFO) << "Replicating " << *replicaof_args;

    // If NO ONE was supplied, just stop the current replica (if it exists)
    if (replicaof_args->IsReplicaOfNoOne()) {
      if (!ss->is_master) {
        CHECK(replica_);

        SetMasterFlagOnAllThreads(true);  // Flip flag before clearing replica
        replica_->Stop();
        replica_.reset();

        StopAllClusterReplicas();
      }

      // May not switch to ACTIVE if the process is, for example, shutting down at the same time.
      service_.SwitchState(GlobalState::LOADING, GlobalState::ACTIVE);

      return builder->SendOk();
    }

    // If any replication is in progress, stop it, cancellation should kick in immediately
    if (replica_)
      replica_->Stop();
    StopAllClusterReplicas();

    // First, switch into the loading state
    if (auto new_state = service_.SwitchState(GlobalState::ACTIVE, GlobalState::LOADING);
        new_state != GlobalState::LOADING) {
      LOG(WARNING) << new_state << " in progress, ignored";
      builder->SendError("Invalid state");
      return;
    }

    // If we are called by "Replicate", tx will be null but we do not need
    // to flush anything.
    if (tx) {
      Drakarys(tx, DbSlice::kDbAll);
    }

    // Create a new replica and assing it
    new_replica = make_shared<Replica>(replicaof_args->host, replicaof_args->port, &service_,
                                       master_replid(), replicaof_args->slot_range);

    replica_ = new_replica;

    // TODO: disconnect pending blocked clients (pubsub, blocking commands)
    SetMasterFlagOnAllThreads(false);  // Flip flag after assiging replica

  }  // release the lock, lk.unlock()
  // We proceed connecting below without the lock to allow interrupting the replica immediately.
  // From this point and onward, it should be highly responsive.

  error_code ec{};
  switch (on_err) {
    case ActionOnConnectionFail::kReturnOnError:
      ec = new_replica->Start(builder);
      break;
    case ActionOnConnectionFail::kContinueReplication:  // set DF to replicate, and forget about it
      new_replica->EnableReplication(builder);
      break;
  };

  // If the replication attempt failed, clean up global state. The replica should have stopped
  // internally.
  util::fb2::LockGuard lk(replicaof_mu_);  // Only one REPLICAOF command can run at a time
  if (ec && replica_ == new_replica) {
    service_.SwitchState(GlobalState::LOADING, GlobalState::ACTIVE);
    SetMasterFlagOnAllThreads(true);
    replica_.reset();
  }
}

void ServerFamily::StopAllClusterReplicas() {
  // Stop all cluster replication.
  for (auto& replica : cluster_replicas_) {
    replica->Stop();
    replica.reset();
  }
  cluster_replicas_.clear();
}

void ServerFamily::ReplicaOf(CmdArgList args, const CommandContext& cmd_cntx) {
  ReplicaOfInternal(args, cmd_cntx.tx, cmd_cntx.rb, ActionOnConnectionFail::kReturnOnError);
}

void ServerFamily::Replicate(string_view host, string_view port) {
  StringVec replicaof_params{string(host), string(port)};

  CmdArgVec args_vec;
  for (auto& s : replicaof_params) {
    args_vec.emplace_back(MutableSlice{s.data(), s.size()});
  }
  CmdArgList args_list = absl::MakeSpan(args_vec);
  io::NullSink sink;
  facade::RedisReplyBuilder rb(&sink);
  ReplicaOfInternal(args_list, nullptr, &rb, ActionOnConnectionFail::kContinueReplication);
}

// REPLTAKEOVER <seconds> [SAVE]
// SAVE is used only by tests.
void ServerFamily::ReplTakeOver(CmdArgList args, const CommandContext& cmd_cntx) {
  VLOG(1) << "ReplTakeOver start";

  CmdArgParser parser{args};

  int timeout_sec = parser.Next<int>();
  bool save_flag = static_cast<bool>(parser.Check("SAVE"));

  auto* builder = cmd_cntx.rb;
  if (parser.HasNext())
    return builder->SendError(absl::StrCat("Unsupported option:", string_view(parser.Next())));

  if (auto err = parser.Error(); err)
    return builder->SendError(err->MakeReply());

  // We allow zero timeouts for tests.
  if (timeout_sec < 0) {
    return builder->SendError("timeout is negative");
  }

  // We return OK, to support idempotency semantics.
  if (ServerState::tlocal()->is_master)
    return builder->SendOk();

  util::fb2::LockGuard lk(replicaof_mu_);

  auto repl_ptr = replica_;
  CHECK(repl_ptr);

  auto info = replica_->GetSummary();
  if (!info.full_sync_done) {
    return builder->SendError("Full sync not done");
  }

  std::error_code ec = replica_->TakeOver(ArgS(args, 0), save_flag);
  if (ec)
    return builder->SendError("Couldn't execute takeover");

  LOG(INFO) << "Takeover successful, promoting this instance to master.";
  SetMasterFlagOnAllThreads(true);
  replica_->Stop();
  replica_.reset();
  return builder->SendOk();
}

void ServerFamily::ReplConf(CmdArgList args, const CommandContext& cmd_cntx) {
  auto* builder = cmd_cntx.rb;
  {
    util::fb2::LockGuard lk(replicaof_mu_);
    if (!ServerState::tlocal()->is_master) {
      return builder->SendError("Replicating a replica is unsupported");
    }
  }

  auto err_cb = [&]() mutable {
    LOG(ERROR) << "Error in receiving command: " << args;
    builder->SendError(kSyntaxErr);
  };

  if (args.size() % 2 == 1)
    return err_cb();

  auto* cntx = cmd_cntx.conn_cntx;
  for (unsigned i = 0; i < args.size(); i += 2) {
    DCHECK_LT(i + 1, args.size());

    string cmd = absl::AsciiStrToUpper(ArgS(args, i));
    std::string_view arg = ArgS(args, i + 1);
    if (cmd == "CAPA") {
      if (arg == "dragonfly" && args.size() == 2 && i == 0) {
        auto [sid, flow_count] = dfly_cmd_->CreateSyncSession(&cntx->conn_state);
        cntx->conn()->SetName(absl::StrCat("repl_ctrl_", sid));

        string sync_id = absl::StrCat("SYNC", sid);
        cntx->conn_state.replication_info.repl_session_id = sid;

        cntx->replica_conn = true;

        // The response for 'capa dragonfly' is: <masterid> <syncid> <numthreads> <version>
        auto* rb = static_cast<RedisReplyBuilder*>(builder);
        rb->StartArray(4);
        rb->SendSimpleString(master_replid_);
        rb->SendSimpleString(sync_id);
        rb->SendLong(flow_count);
        rb->SendLong(unsigned(DflyVersion::CURRENT_VER));
        return;
      }
    } else if (cmd == "LISTENING-PORT") {
      uint32_t replica_listening_port;
      if (!absl::SimpleAtoi(arg, &replica_listening_port)) {
        builder->SendError(kInvalidIntErr);
        return;
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
        info->id = arg;
      }
    } else if (cmd == "CLIENT-VERSION" && args.size() == 2) {
      unsigned version;
      if (!absl::SimpleAtoi(arg, &version)) {
        return builder->SendError(kInvalidIntErr);
      }
      dfly_cmd_->SetDflyClientVersion(&cntx->conn_state, DflyVersion(version));
    } else if (cmd == "ACK" && args.size() == 2) {
      // Don't send error/Ok back through the socket, because we don't want to interleave with
      // the journal writes that we write into the same socket.

      if (!cntx->replication_flow) {
        LOG(ERROR) << "No replication flow assigned";
        return;
      }

      uint64_t ack;
      if (!absl::SimpleAtoi(arg, &ack)) {
        LOG(ERROR) << "Bad int in REPLCONF ACK command! arg=" << arg;
        return;
      }
      VLOG(2) << "Received client ACK=" << ack;
      cntx->replication_flow->last_acked_lsn = ack;
      return;
    } else {
      VLOG(1) << "Error " << cmd << " " << arg << " " << args.size();
      return err_cb();
    }
  }

  return builder->SendOk();
}

void ServerFamily::Role(CmdArgList args, const CommandContext& cmd_cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  util::fb2::LockGuard lk(replicaof_mu_);
  // Thread local var is_master is updated under mutex replicaof_mu_ together with replica_,
  // ensuring eventual consistency of is_master. When determining if the server is a replica and
  // accessing the replica_ object, we must lock replicaof_mu_. Using is_master alone is
  // insufficient in this scenario.
  if (!replica_) {
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
    rb->StartArray(4 + cluster_replicas_.size() * 3);
    rb->SendBulkString(GetFlag(FLAGS_info_replication_valkey_compatible) ? "slave" : "replica");

    auto send_replica_info = [rb](Replica::Summary rinfo) {
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
    send_replica_info(replica_->GetSummary());
    for (const auto& replica : cluster_replicas_) {
      send_replica_info(replica->GetSummary());
    }
  }
}

void ServerFamily::Script(CmdArgList args, const CommandContext& cmd_cntx) {
  script_mgr_->Run(std::move(args), cmd_cntx.tx, cmd_cntx.rb, cmd_cntx.conn_cntx);
}

void ServerFamily::LastSave(CmdArgList args, const CommandContext& cmd_cntx) {
  time_t save_time;
  {
    util::fb2::LockGuard lk(save_mu_);
    save_time = last_save_info_.save_time;
  }
  cmd_cntx.rb->SendLong(save_time);
}

void ServerFamily::Latency(CmdArgList args, const CommandContext& cmd_cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  string sub_cmd = absl::AsciiStrToUpper(ArgS(args, 0));

  if (sub_cmd == "LATEST") {
    return rb->SendEmptyArray();
  }

  LOG_FIRST_N(ERROR, 10) << "Subcommand " << sub_cmd << " not supported";
  rb->SendError(kSyntaxErr);
}

void ServerFamily::ShutdownCmd(CmdArgList args, const CommandContext& cmd_cntx) {
  if (args.size() > 1) {
    cmd_cntx.rb->SendError(kSyntaxErr);
    return;
  }

  if (args.size() == 1) {
    auto sub_cmd = ArgS(args, 0);
    if (absl::EqualsIgnoreCase(sub_cmd, "SAVE")) {
    } else if (absl::EqualsIgnoreCase(sub_cmd, "NOSAVE")) {
      save_on_shutdown_ = false;
    } else {
      cmd_cntx.rb->SendError(kSyntaxErr);
      return;
    }
  }

  CHECK_NOTNULL(acceptor_)->Stop();
  cmd_cntx.rb->SendOk();
}

void ServerFamily::Dfly(CmdArgList args, const CommandContext& cmd_cntx) {
  dfly_cmd_->Run(args, cmd_cntx.tx, static_cast<RedisReplyBuilder*>(cmd_cntx.rb),
                 cmd_cntx.conn_cntx);
}

void ServerFamily::SlowLog(CmdArgList args, const CommandContext& cmd_cntx) {
  string sub_cmd = absl::AsciiStrToUpper(ArgS(args, 0));
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  if (sub_cmd == "HELP") {
    string_view help[] = {
        "SLOWLOG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        "GET [<count>]",
        "    Return top <count> entries from the slowlog (default: 10, -1 mean all).",
        "    Entries are made of:",
        "    id, timestamp, time in microseconds, arguments array, client IP and port,",
        "    client name",
        "LEN",
        "    Return the length of the slowlog.",
        "RESET",
        "    Reset the slowlog.",
        "HELP",
        "    Prints this help.",
    };

    rb->SendSimpleStrArr(help);
    return;
  }

  if (sub_cmd == "LEN") {
    vector<int> lengths(service_.proactor_pool().size());
    service_.proactor_pool().AwaitFiberOnAll([&lengths](auto index, auto* context) {
      lengths[index] = ServerState::tlocal()->GetSlowLog().Length();
    });
    int sum = std::accumulate(lengths.begin(), lengths.end(), 0);
    return rb->SendLong(sum);
  }

  if (sub_cmd == "RESET") {
    service_.proactor_pool().AwaitFiberOnAll(
        [](auto index, auto* context) { ServerState::tlocal()->GetSlowLog().Reset(); });
    return rb->SendOk();
  }

  if (sub_cmd == "GET") {
    return SlowLogGet(args, sub_cmd, &service_.proactor_pool(), rb);
  }
  rb->SendError(UnknownSubCmd(sub_cmd, "SLOWLOG"), kSyntaxErrType);
}

void ServerFamily::Module(CmdArgList args, const CommandContext& cmd_cntx) {
  string sub_cmd = absl::AsciiStrToUpper(ArgS(args, 0));
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);

  if (sub_cmd != "LIST")
    return rb->SendError(kSyntaxErr);

  rb->StartArray(2);

  // Json
  rb->StartCollection(2, RedisReplyBuilder::MAP);
  rb->SendSimpleString("name");
  rb->SendSimpleString("ReJSON");
  rb->SendSimpleString("ver");
  rb->SendLong(20'000);

  // Search
  rb->StartCollection(2, RedisReplyBuilder::MAP);
  rb->SendSimpleString("name");
  rb->SendSimpleString("search");
  rb->SendSimpleString("ver");
  rb->SendLong(20'000);  // we target v2
}

#define HFUNC(x) SetHandler(HandlerFunc(this, &ServerFamily::x))

namespace acl {
constexpr uint32_t kAuth = FAST | CONNECTION;
constexpr uint32_t kBGSave = ADMIN | SLOW | DANGEROUS;
constexpr uint32_t kClient = SLOW | CONNECTION;
constexpr uint32_t kConfig = ADMIN | SLOW | DANGEROUS;
constexpr uint32_t kDbSize = KEYSPACE | READ | FAST;
constexpr uint32_t kDebug = ADMIN | SLOW | DANGEROUS;
constexpr uint32_t kFlushDB = KEYSPACE | WRITE | SLOW | DANGEROUS;
constexpr uint32_t kFlushAll = KEYSPACE | WRITE | SLOW | DANGEROUS;
constexpr uint32_t kInfo = SLOW | DANGEROUS;
constexpr uint32_t kHello = FAST | CONNECTION;
constexpr uint32_t kLastSave = ADMIN | FAST | DANGEROUS;
constexpr uint32_t kLatency = ADMIN | SLOW | DANGEROUS;
constexpr uint32_t kMemory = READ | SLOW;
constexpr uint32_t kSave = ADMIN | SLOW | DANGEROUS;
constexpr uint32_t kShutDown = ADMIN | SLOW | DANGEROUS;
constexpr uint32_t kSlaveOf = ADMIN | SLOW | DANGEROUS;
constexpr uint32_t kReplicaOf = ADMIN | SLOW | DANGEROUS;
constexpr uint32_t kReplTakeOver = DANGEROUS;
constexpr uint32_t kReplConf = ADMIN | SLOW | DANGEROUS;
constexpr uint32_t kRole = ADMIN | FAST | DANGEROUS;
constexpr uint32_t kSlowLog = ADMIN | SLOW | DANGEROUS;
constexpr uint32_t kScript = SLOW | SCRIPTING;
constexpr uint32_t kModule = ADMIN | SLOW | DANGEROUS;
// TODO(check this)
constexpr uint32_t kDfly = ADMIN;
}  // namespace acl

void ServerFamily::Register(CommandRegistry* registry) {
  constexpr auto kReplicaOpts = CO::LOADING | CO::ADMIN | CO::GLOBAL_TRANS;
  constexpr auto kMemOpts = CO::LOADING | CO::READONLY | CO::FAST;
  registry->StartFamily();
  *registry
      << CI{"AUTH", CO::NOSCRIPT | CO::FAST | CO::LOADING, -2, 0, 0, acl::kAuth}.HFUNC(Auth)
      << CI{"BGSAVE", CO::ADMIN | CO::GLOBAL_TRANS, -1, 0, 0, acl::kBGSave}.HFUNC(BgSave)
      << CI{"CLIENT", CO::NOSCRIPT | CO::LOADING, -2, 0, 0, acl::kClient}.HFUNC(Client)
      << CI{"CONFIG", CO::ADMIN | CO::DANGEROUS, -2, 0, 0, acl::kConfig}.HFUNC(Config)
      << CI{"DBSIZE", CO::READONLY | CO::FAST | CO::LOADING, 1, 0, 0, acl::kDbSize}.HFUNC(DbSize)
      << CI{"DEBUG", CO::ADMIN | CO::LOADING, -2, 0, 0, acl::kDebug}.HFUNC(Debug)
      << CI{"FLUSHDB", CO::WRITE | CO::GLOBAL_TRANS | CO::DANGEROUS, 1, 0, 0, acl::kFlushDB}.HFUNC(
             FlushDb)
      << CI{"FLUSHALL", CO::WRITE | CO::GLOBAL_TRANS | CO::DANGEROUS, -1, 0, 0, acl::kFlushAll}
             .HFUNC(FlushAll)
      << CI{"INFO", CO::LOADING, -1, 0, 0, acl::kInfo}.HFUNC(Info)
      << CI{"HELLO", CO::LOADING, -1, 0, 0, acl::kHello}.HFUNC(Hello)
      << CI{"LASTSAVE", CO::LOADING | CO::FAST, 1, 0, 0, acl::kLastSave}.HFUNC(LastSave)
      << CI{"LATENCY", CO::NOSCRIPT | CO::LOADING | CO::FAST, -2, 0, 0, acl::kLatency}.HFUNC(
             Latency)
      << CI{"MEMORY", kMemOpts, -2, 0, 0, acl::kMemory}.HFUNC(Memory)
      << CI{"SAVE", CO::ADMIN | CO::GLOBAL_TRANS, -1, 0, 0, acl::kSave}.HFUNC(Save)
      << CI{"SHUTDOWN",    CO::ADMIN | CO::NOSCRIPT | CO::LOADING | CO::DANGEROUS, -1, 0, 0,
            acl::kShutDown}
             .HFUNC(ShutdownCmd)
      << CI{"SLAVEOF", kReplicaOpts, 3, 0, 0, acl::kSlaveOf}.HFUNC(ReplicaOf)
      << CI{"REPLICAOF", kReplicaOpts, -3, 0, 0, acl::kReplicaOf}.HFUNC(ReplicaOf)
      << CI{"ADDREPLICAOF", kReplicaOpts, 5, 0, 0, acl::kReplicaOf}.HFUNC(AddReplicaOf)
      << CI{"REPLTAKEOVER", CO::ADMIN | CO::GLOBAL_TRANS, -2, 0, 0, acl::kReplTakeOver}.HFUNC(
             ReplTakeOver)
      << CI{"REPLCONF", CO::ADMIN | CO::LOADING, -1, 0, 0, acl::kReplConf}.HFUNC(ReplConf)
      << CI{"ROLE", CO::LOADING | CO::FAST | CO::NOSCRIPT, 1, 0, 0, acl::kRole}.HFUNC(Role)
      << CI{"SLOWLOG", CO::ADMIN | CO::FAST, -2, 0, 0, acl::kSlowLog}.HFUNC(SlowLog)
      << CI{"SCRIPT", CO::NOSCRIPT | CO::NO_KEY_TRANSACTIONAL, -2, 0, 0, acl::kScript}.HFUNC(Script)
      << CI{"DFLY", CO::ADMIN | CO::GLOBAL_TRANS | CO::HIDDEN, -2, 0, 0, acl::kDfly}.HFUNC(Dfly)
      << CI{"MODULE", CO::ADMIN, 2, 0, 0, acl::kModule}.HFUNC(Module);
}

}  // namespace dfly
