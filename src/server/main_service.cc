// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/main_service.h"

#include "absl/strings/str_split.h"
#include "facade/resp_expr.h"
#include "util/fibers/detail/fiber_interface.h"
#include "util/fibers/proactor_base.h"
#include "util/fibers/synchronization.h"

#ifdef __FreeBSD__
#include <pthread_np.h>
#elif defined(__linux__)
#include "util/fibers/uring_proactor.h"
#endif

extern "C" {
#include "redis/redis_aux.h"
}

#include <absl/cleanup/cleanup.h>
#include <absl/functional/bind_front.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/str_format.h>
#include <xxhash.h>

#include <csignal>
#include <filesystem>

#include "base/cycle_clock.h"
#include "base/flag_utils.h"
#include "base/flags.h"
#include "base/logging.h"
#include "core/search/vector_utils.h"
#include "facade/dragonfly_connection.h"
#include "facade/error.h"
#include "facade/reply_builder.h"
#include "facade/reply_capture.h"
#include "server/acl/acl_commands_def.h"
#include "server/acl/acl_family.h"
#include "server/acl/user_registry.h"
#include "server/acl/validator.h"
#include "server/bitops_family.h"
#include "server/bloom_family.h"
#include "server/channel_store.h"
#include "server/cluster/cluster_family.h"
#include "server/error.h"
#include "server/generic_family.h"
#include "server/geo_family.h"
#include "server/hll_family.h"
#include "server/hset_family.h"
#include "server/http_api.h"
#include "server/json_family.h"
#include "server/list_family.h"
#include "server/multi_command_squasher.h"
#include "server/namespaces.h"
#include "server/script_mgr.h"
#include "server/search/search_family.h"
#include "server/server_state.h"
#include "server/set_family.h"
#include "server/stream_family.h"
#include "server/string_family.h"
#include "server/tiered_storage.h"
#include "server/transaction.h"
#include "server/version.h"
#include "server/zset_family.h"
#include "strings/human_readable.h"
#include "util/html/sorted_table.h"
#include "util/varz.h"

using namespace std;
using facade::ErrorReply;

ABSL_FLAG(int32_t, port, 6379,
          "Redis port. 0 disables the port, -1 will bind on a random available port.");

ABSL_FLAG(uint16_t, announce_port, 0,
          "Port that Dragonfly announces to cluster clients and replication master");

ABSL_FLAG(uint32_t, memcached_port, 0, "Memcached port");

ABSL_FLAG(uint32_t, num_shards, 0, "Number of database shards, 0 - to choose automatically");

ABSL_FLAG(bool, multi_exec_squash, true,
          "Whether multi exec will squash single shard commands to optimize performance");

ABSL_RETIRED_FLAG(bool, track_exec_frequencies, true,
                  "DEPRECATED. Whether to track exec frequencies for multi exec");
ABSL_FLAG(bool, lua_resp2_legacy_float, false,
          "Return rounded down integers instead of floats for lua scripts with RESP2");
ABSL_FLAG(uint32_t, multi_eval_squash_buffer, 4096, "Max buffer for squashed commands per script");

ABSL_DECLARE_FLAG(bool, primary_port_http_enabled);
ABSL_FLAG(bool, admin_nopass, false,
          "If set, would enable open admin access to console on the assigned port, without "
          "authorization needed.");

ABSL_FLAG(bool, expose_http_api, false,
          "If set, will expose a POST /api handler for sending redis commands as json array.");

ABSL_FLAG(facade::MemoryBytesFlag, maxmemory, facade::MemoryBytesFlag{},
          "Limit on maximum-memory that is used by the database, until data starts to be evicted "
          "(according to eviction policy). With tiering, this value defines only the size in RAM, "
          "and not the whole dataset (RAM + SSD). "
          "Must be *at least* 256MiB per proactor thread. "
          "Can be any human‑readable bytes values (supports K/M/G/T/P/E with optional B, "
          "case‑insensitive, both 'GiB' & 'GB' possible). Examples: 300000000, 512MB, 2G, 1.25GiB. "
          "0 - value will be automatically defined based on the env (ex: machine's capacity). "
          "default: 0");

ABSL_RETIRED_FLAG(
    double, oom_deny_ratio, 1.1,
    "commands with flag denyoom will return OOM when the ratio between maxmemory and used "
    "memory is above this value");

ABSL_FLAG(uint32_t, shard_thread_busy_polling_usec, 0,
          "If non-zero, overrides the busy polling parameter for shard threads.");

ABSL_FLAG(string, huffman_table, "",
          "a comma separated map: domain1:code1,domain2:code2,... where "
          "domain can currently be only KEYS or STRINGS, code is a base64-encoded huffman table"
          " exported via "
          "DEBUG COMPRESSION EXPORT. if the flag is empty no huffman compression is applied.");

ABSL_FLAG(bool, jsonpathv2, true,
          "If true uses Dragonfly jsonpath implementation, "
          "otherwise uses legacy jsoncons implementation.");

ABSL_FLAG(uint32_t, uring_wake_mode, 1,
          "0 - use eventfd, 1 - use io_uring, 2 - use io_uring with immediate flush of the "
          "notification");

ABSL_FLAG(uint32_t, uring_submit_threshold, 1u << 31, "");

ABSL_FLAG(uint32_t, scheduler_background_budget, 50'000, "Background fiber budget in nanoseconds");
ABSL_FLAG(uint32_t, scheduler_background_sleep_prob, 50,
          "Sleep probability of background fibers on reaching budget");
ABSL_FLAG(uint32_t, scheduler_background_warrant, 5,
          "Percentage of guaranteed cpu time for background fibers");

ABSL_FLAG(uint32_t, squash_stats_latency_lower_limit, 0,
          "If set, will not track latency stats below this threshold (usec). ");

namespace {

struct ShutdownWatchdog {
  util::fb2::Fiber watchdog_fb;
  util::fb2::Done watchdog_done;
  util::ProactorPool& pool;

  explicit ShutdownWatchdog(util::ProactorPool& pp);
  void Disarm();
};

ShutdownWatchdog::ShutdownWatchdog(util::ProactorPool& pp) : pool{pp} {
  watchdog_fb = pool.GetNextProactor()->LaunchFiber("shutdown_watchdog", [&] {
    if (!watchdog_done.WaitFor(20s)) {
      LOG(ERROR) << "Deadlock detected during shutdown";
      absl::SetFlag(&FLAGS_alsologtostderr, true);
      util::fb2::Mutex m;
      pool.AwaitFiberOnAll([&m](unsigned index, auto*) {
        util::ThisFiber::SetName(absl::StrFormat("print_stack_fib_%u", index));
        std::unique_lock lk(m);
        LOG(ERROR) << "Proactor " << index << ":\n";
        util::fb2::detail::FiberInterface::PrintAllFiberStackTraces();
      });
    }
  });
}

void ShutdownWatchdog::Disarm() {
  watchdog_done.Notify();
  watchdog_fb.JoinIfNeeded();
}

std::optional<ShutdownWatchdog> shutdown_watchdog = std::nullopt;

}  // namespace

namespace dfly {

#if defined(__linux__)
#if __GLIBC__ == 2 && __GLIBC_MINOR__ < 30
#include <sys/syscall.h>
#define gettid() syscall(SYS_gettid)
#endif

#elif defined(__FreeBSD__)

#define gettid() pthread_getthreadid_np()

#elif defined(__APPLE__)

inline unsigned gettid() {
  uint64_t tid;
  pthread_threadid_np(NULL, &tid);
  return tid;
}

#endif

using namespace util;
using absl::GetFlag;
using absl::StrCat;
using base::VarzValue;
using ::boost::intrusive_ptr;
using namespace facade;
namespace h2 = boost::beast::http;

namespace {

std::optional<VarzFunction> engine_varz;

constexpr size_t kMaxThreadSize = 1024;

// Unwatch all keys for a connection and unregister from DbSlices.
// Used by UNWATCH, DICARD and EXEC.
void UnwatchAllKeys(Namespace* ns, ConnectionState::ExecInfo* exec_info) {
  if (!exec_info->watched_keys.empty()) {
    auto cb = [&](EngineShard* shard) {
      ns->GetDbSlice(shard->shard_id()).UnregisterConnectionWatches(exec_info);
    };
    shard_set->RunBriefInParallel(std::move(cb));
  }
  exec_info->ClearWatched();
}

void MultiCleanup(ConnectionContext* cntx) {
  auto& exec_info = cntx->conn_state.exec_info;
  if (auto* borrowed = exec_info.preborrowed_interpreter; borrowed) {
    ServerState::tlocal()->ReturnInterpreter(borrowed);
    exec_info.preborrowed_interpreter = nullptr;
  }
  UnwatchAllKeys(cntx->ns, &exec_info);
  exec_info.Clear();
}

void DeactivateMonitoring(ConnectionContext* server_ctx) {
  if (server_ctx->monitor) {
    // remove monitor on this connection
    server_ctx->ChangeMonitor(false /*start*/);
  }
}

// The format of the message that are sending is
// +"time of day" [db-number <lua|unix:path|connection info] "command" "arg1" .. "argM"
std::string CreateMonitorTimestamp() {
  timeval tv;

  gettimeofday(&tv, nullptr);
  return absl::StrCat(tv.tv_sec, ".", tv.tv_usec, absl::kZeroPad6);
}

auto CmdEntryToMonitorFormat(std::string_view str) -> std::string {
  // This code is based on Redis impl for it at sdscatrepr@sds.c
  std::string result = absl::StrCat("\"");

  for (auto c : str) {
    switch (c) {
      case '\\':
        absl::StrAppend(&result, "\\\\");
        break;
      case '"':
        absl::StrAppend(&result, "\\\"");
        break;
      case '\n':
        absl::StrAppend(&result, "\\n");
        break;
      case '\r':
        absl::StrAppend(&result, "\\r");
        break;
      case '\t':
        absl::StrAppend(&result, "\\t");
        break;
      case '\a':
        absl::StrAppend(&result, "\\a");
        break;
      case '\b':
        absl::StrAppend(&result, "\\b");
        break;
      default:
        if (isprint(c)) {
          result += c;
        } else {
          absl::StrAppendFormat(&result, "\\x%02x", c);
        }
        break;
    }
  }
  absl::StrAppend(&result, "\"");
  return result;
}

std::string MakeMonitorMessage(const ConnectionContext* cntx, const CommandId* cid,
                               CmdArgList tail_args) {
  std::string message = absl::StrCat(CreateMonitorTimestamp(), " [", cntx->conn_state.db_index);

  if (cntx->conn_state.squashing_info)
    cntx = cntx->conn_state.squashing_info->owner;

  string endpoint;
  if (cntx->conn_state.script_info) {
    endpoint = "lua";
  } else if (const auto* conn = cntx->conn(); conn != nullptr) {
    endpoint = conn->RemoteEndpointStr();
  } else {
    endpoint = "REPLICATION:0";
  }
  absl::StrAppend(&message, " ", endpoint, "] ");

  absl::StrAppend(&message, "\"", cid->name(), "\"");

  if (cid->name() == "AUTH")
    return message;

  for (auto arg : tail_args)
    absl::StrAppend(&message, " ", CmdEntryToMonitorFormat(facade::ToSV(arg)));

  return message;
}

void SendMonitor(const std::string& msg) {
  const auto& monitor_repo = ServerState::tlocal()->Monitors();
  const auto& monitors = monitor_repo.monitors();
  if (monitors.empty()) {
    return;
  }
  VLOG(2) << "Thread " << ProactorBase::me()->GetPoolIndex() << " sending monitor message '" << msg
          << "' for " << monitors.size();

  for (auto monitor_conn : monitors) {
    monitor_conn->SendMonitorMessageAsync(msg);
  }
}

void DispatchMonitor(ConnectionContext* cntx, const CommandId* cid, CmdArgList tail_args) {
  //  We have connections waiting to get the info on the last command, send it to them
  string monitor_msg = MakeMonitorMessage(cntx, cid, tail_args);

  VLOG(2) << "Sending command '" << monitor_msg << "' to the clients that registered on it";

  shard_set->pool()->DispatchBrief(
      [msg = std::move(monitor_msg)](unsigned idx, util::ProactorBase*) { SendMonitor(msg); });
}

class InterpreterReplier : public RedisReplyBuilder {
 public:
  explicit InterpreterReplier(ObjectExplorer* explr) : RedisReplyBuilder(nullptr), explr_(explr) {
  }

  void SendError(std::string_view str, std::string_view type) final;

  void SendBulkString(std::string_view str) final;
  void SendSimpleString(std::string_view str) final;

  void SendNullArray() final;
  void SendNull() final;
  void SendLong(long val) final;
  void SendDouble(double val) final;

  void StartCollection(unsigned len, CollectionType type) final;

 private:
  void PostItem();

  ObjectExplorer* explr_;
  vector<pair<unsigned, unsigned>> array_len_;
  unsigned num_elems_ = 0;
};

// Serialized result of script invocation to Redis protocol
class EvalSerializer : public ObjectExplorer {
 public:
  explicit EvalSerializer(RedisReplyBuilder* rb, bool float_as_int)
      : rb_(rb), float_as_int_(float_as_int) {
  }

  void OnBool(bool b) final {
    if (b) {
      rb_->SendLong(1);
    } else {
      rb_->SendNull();
    }
  }

  void OnString(string_view str) final {
    rb_->SendBulkString(str);
  }

  void OnDouble(double d) final {
    if (float_as_int_ || GetFlag(FLAGS_lua_resp2_legacy_float)) {
      const long val = d >= 0 ? static_cast<long>(floor(d)) : static_cast<long>(ceil(d));
      rb_->SendLong(val);
    } else {
      rb_->SendDouble(d);
    }
  }

  void OnInt(int64_t val) final {
    rb_->SendLong(val);
  }

  void OnArrayStart(unsigned len) final {
    rb_->StartArray(len);
  }

  void OnArrayEnd() final {
  }

  void OnMapStart(unsigned len) final {
    rb_->StartCollection(len, CollectionType::MAP);
  }

  void OnMapEnd() final {
  }

  void OnNil() final {
    rb_->SendNull();
  }

  void OnStatus(string_view str) {
    rb_->SendSimpleString(str);
  }

  void OnError(string_view str) {
    if (!str.empty() && str.front() != '-') {
      rb_->SendError(absl::StrCat("-", str));
    } else {
      rb_->SendError(str);
    }
  }

 private:
  RedisReplyBuilder* rb_;
  bool float_as_int_;
};

void InterpreterReplier::PostItem() {
  if (array_len_.empty()) {
    DCHECK_EQ(0u, num_elems_);
    ++num_elems_;
  } else {
    ++num_elems_;

    while (num_elems_ == array_len_.back().second) {
      num_elems_ = array_len_.back().first;
      explr_->OnArrayEnd();

      array_len_.pop_back();
      if (array_len_.empty())
        break;
    }
  }
}

void InterpreterReplier::SendError(string_view str, std::string_view type) {
  DCHECK(array_len_.empty());
  DVLOG(1) << "Lua/df_call error " << str;
  if (!str.empty() && str.front() != '-') {
    explr_->OnError(absl::StrCat("-ERR ", str));
  } else {
    explr_->OnError(str);
  }
}

void InterpreterReplier::SendSimpleString(string_view str) {
  if (array_len_.empty())
    explr_->OnStatus(str);
  else
    explr_->OnString(str);
  PostItem();
}

void InterpreterReplier::SendNullArray() {
  SendSimpleStrArr(ArgSlice{});
  PostItem();
}

void InterpreterReplier::SendNull() {
  explr_->OnNil();
  PostItem();
}

void InterpreterReplier::SendLong(long val) {
  explr_->OnInt(val);
  PostItem();
}

void InterpreterReplier::SendDouble(double val) {
  explr_->OnDouble(val);
  PostItem();
}

void InterpreterReplier::SendBulkString(string_view str) {
  explr_->OnString(str);
  PostItem();
}

void InterpreterReplier::StartCollection(unsigned len, CollectionType type) {
  if (type == CollectionType::MAP)
    len *= 2;
  explr_->OnArrayStart(len);

  if (len == 0) {
    explr_->OnArrayEnd();
    PostItem();
  } else {
    array_len_.emplace_back(num_elems_ + 1, len);
    num_elems_ = 0;
  }
}

bool IsSHA(string_view str) {
  return std::all_of(str.begin(), str.end(),
                     [](unsigned char c) { return absl::ascii_isxdigit(c); });
}

optional<ErrorReply> EvalValidator(CmdArgList args) {
  string_view num_keys_str = ArgS(args, 1);
  int32_t num_keys;

  if (!absl::SimpleAtoi(num_keys_str, &num_keys) || num_keys < 0)
    return ErrorReply{facade::kInvalidIntErr};

  if (unsigned(num_keys) > args.size() - 2)
    return ErrorReply{"Number of keys can't be greater than number of args", kSyntaxErrType};

  return nullopt;
}

void TxTable(const http::QueryArgs& args, HttpContext* send) {
  using html::SortedTable;

  http::StringResponse resp = http::MakeStringResponse(h2::status::ok);
  resp.body() = SortedTable::HtmlStart();
  SortedTable::StartTable({"ShardId", "TID", "TxId", "Armed"}, &resp.body());

  if (shard_set) {
    vector<string> rows(shard_set->size());

    shard_set->RunBriefInParallel([&](EngineShard* shard) {
      ShardId sid = shard->shard_id();

      absl::AlphaNum tid(gettid());
      absl::AlphaNum sid_an(sid);

      string& mine = rows[sid];
      TxQueue* queue = shard->txq();

      if (!queue->Empty()) {
        auto cur = queue->Head();
        do {
          auto value = queue->At(cur);
          Transaction* trx = std::get<Transaction*>(value);

          absl::AlphaNum an2(trx->txid());
          absl::AlphaNum an3(trx->DEBUG_IsArmedInShard(sid));
          SortedTable::Row({sid_an.Piece(), tid.Piece(), an2.Piece(), an3.Piece()}, &mine);
          cur = queue->Next(cur);
        } while (cur != queue->Head());
      }
    });

    for (const auto& s : rows) {
      resp.body().append(s);
    }
  }

  SortedTable::EndTable(&resp.body());
  send->Invoke(std::move(resp));
}

void ClusterHtmlPage(const http::QueryArgs& args, HttpContext* send,
                     cluster::ClusterFamily* cluster_family) {
  http::StringResponse resp = http::MakeStringResponse(h2::status::ok);
  resp.body() = R"(
<html>
  <head>
    <style>
.title_text {
    color: #09bc8d;
    font-weight: bold;
    min-width: 150px;
    display: inline-block;
    margin-bottom: 6px;
}

.value_text {
  color: #d60f96;
}

.master {
    border: 1px solid gray;
    margin-bottom: 10px;
    padding: 10px;
    border-radius: 8px;
}

.master h3 {
    padding-left: 20px;
}
    </style>
  </head>
  <body>
    <h1>Cluster Info</h1>
)";

  auto print_kv = [&](string_view k, string_view v) {
    resp.body() += absl::StrCat("<div><span class='title_text'>", k,
                                "</span><span class='value_text'>", v, "</span></div>\n");
  };

  auto print_kb = [&](string_view k, bool v) { print_kv(k, v ? "True" : "False"); };

  print_kv("Mode", IsClusterEmulated() ? "Emulated" : IsClusterEnabled() ? "Enabled" : "Disabled");

  if (IsClusterEnabledOrEmulated()) {
    print_kb("Lock on hashtags", LockTagOptions::instance().enabled);
  }

  if (IsClusterEnabled()) {
    if (cluster::ClusterConfig::Current() == nullptr) {
      resp.body() += "<h2>Not yet configured.</h2>\n";
    } else {
      auto config = cluster::ClusterConfig::Current()->GetConfig();
      for (const auto& shard : config) {
        resp.body() += "<div class='master'>\n";
        resp.body() += "<h3>Master</h3>\n";
        print_kv("ID", shard.master.id);
        print_kv("IP", shard.master.ip);
        print_kv("Port", absl::StrCat(shard.master.port));

        resp.body() += "<h3>Replicas</h3>\n";
        if (shard.replicas.empty()) {
          resp.body() += "<p>None</p>\n";
        } else {
          for (const auto& replica : shard.replicas) {
            resp.body() += "<h4>Replica</h4>\n";
            print_kv("ID", replica.id);
            print_kv("IP", replica.ip);
            print_kv("Port", absl::StrCat(replica.port));
          }
        }

        resp.body() += "<h3>Slots</h3>\n";
        for (const auto& slot : shard.slot_ranges) {
          resp.body() +=
              absl::StrCat("<div>[<span class='value_text'>", slot.start,
                           "</span>-<span class='value_text'>", slot.end, "</span>]</div>");
        }

        resp.body() += "</div>\n";
      }
    }
  }

  resp.body() += "  </body>\n</html>\n";
  send->Invoke(std::move(resp));
}

enum class ExecScriptUse {
  NONE = 0,
  SCRIPT_LOAD = 1,
  SCRIPT_RUN = 2,
};

ExecScriptUse DetermineScriptPresense(const std::vector<StoredCmd>& body) {
  bool script_load = false;
  for (const auto& scmd : body) {
    if (scmd.Cid()->MultiControlKind() == CO::MultiControlKind::EVAL) {
      return ExecScriptUse::SCRIPT_RUN;
    }

    if ((scmd.Cid()->name() == "SCRIPT") && (absl::AsciiStrToUpper(scmd.FirstArg()) == "LOAD")) {
      script_load = true;
    }
  }

  if (script_load)
    return ExecScriptUse::SCRIPT_LOAD;

  return ExecScriptUse::NONE;
}

// Returns the multi mode for that transaction. Returns NOT_DETERMINED if no scheduling
// is required.
Transaction::MultiMode DeduceExecMode(ExecScriptUse state,
                                      const ConnectionState::ExecInfo& exec_info,
                                      const ScriptMgr& script_mgr) {
  // Check if script most LIKELY has global eval transactions
  bool contains_global = false;
  bool contains_admin_cmd = false;
  Transaction::MultiMode multi_mode = Transaction::LOCK_AHEAD;

  if (state == ExecScriptUse::SCRIPT_RUN) {
    contains_global = script_mgr.AreGlobalByDefault();
  }

  bool transactional = contains_global;
  if (!transactional) {
    for (const auto& scmd : exec_info.body) {
      // We can only tell if eval is transactional based on they keycount
      if (absl::StartsWith(scmd.Cid()->name(), "EVAL")) {
        CmdArgVec arg_vec{};
        auto args = scmd.ArgList(&arg_vec);
        auto keys = DetermineKeys(scmd.Cid(), args);
        transactional |= (keys && keys.value().NumArgs() > 0);
      } else {
        transactional |= scmd.Cid()->IsTransactional();
      }
      contains_global |= scmd.Cid()->opt_mask() & CO::GLOBAL_TRANS;
      contains_admin_cmd |= scmd.Cid()->opt_mask() & CO::ADMIN;

      // We can't run no-key-transactional commands in lock-ahead mode currently,
      // because it means we have to schedule on all shards
      if (scmd.Cid()->opt_mask() & CO::NO_KEY_TRANSACTIONAL)
        contains_global = true;

      if (contains_global)
        break;
    }
  }

  // multi/exec contains commands like ping that do not affect db state.
  if (!transactional && exec_info.watched_keys.empty())
    return Transaction::NOT_DETERMINED;

  if (contains_admin_cmd) {
    multi_mode = Transaction::NON_ATOMIC;
  }
  // Atomic modes fall back to GLOBAL if they contain global commands.
  else if (contains_global && multi_mode == Transaction::LOCK_AHEAD) {
    multi_mode = Transaction::GLOBAL;
  }

  return multi_mode;
}

string CreateExecDescriptor(const std::vector<StoredCmd>& stored_cmds, unsigned num_uniq_shards) {
  string result;
  size_t max_len = std::min<size_t>(20u, stored_cmds.size());
  absl::StrAppend(&result, "EXEC/", num_uniq_shards, "/", max_len);

  return result;
}

string ConnectionLogContext(const facade::Connection* conn) {
  if (conn == nullptr) {
    return "(null-conn)";
  }
  return absl::StrCat("(", conn->RemoteEndpointStr(), ")");
}

string FailedCommandToString(std::string_view command, facade::CmdArgList args,
                             std::string_view reason) {
  string result;
  absl::StrAppend(&result, " ", command);

  if (command != "AUTH" && command != "ACL SETUSER") {
    for (auto arg : args) {
      absl::StrAppend(&result, " ", absl::CHexEscape(arg));
    }
  }

  absl::StrAppend(&result, " failed with reason: ", reason);

  return result;
}

thread_local uint32_t squash_stats_latency_lower_limit_cached;

void UpdateFromFlagsOnThread() {
  if (uint32_t poll = GetFlag(FLAGS_shard_thread_busy_polling_usec);
      poll > 0 && EngineShard::tlocal())
    ProactorBase::me()->SetBusyPollUsec(poll);
  squash_stats_latency_lower_limit_cached = GetFlag(FLAGS_squash_stats_latency_lower_limit);
}

std::vector<std::string> GetMutableFlagNames() {
  return base::GetFlagNames(FLAGS_shard_thread_busy_polling_usec,
                            FLAGS_squash_stats_latency_lower_limit);
}

void UpdateUringFlagsOnThread() {
#ifdef __linux__
  if (auto* pb = ProactorBase::me(); pb->GetKind() == fb2::ProactorBase::IOURING) {
    fb2::UringProactor* up = static_cast<fb2::UringProactor*>(pb);
    uint32_t mode = absl::GetFlag(FLAGS_uring_wake_mode);
    uint32_t threshold = absl::GetFlag(FLAGS_uring_submit_threshold);

    up->ConfigureMsgRing(mode > 0);
    up->ConfigureSubmitWakeup(mode == 2);
    up->SetSubmitQueueThreshold(threshold);
  }
#endif
}

void UpdateSchedulerFlagsOnThread() {
  using fb2::detail::Scheduler;
  auto* sched = util::fb2::detail::FiberScheduler();
  sched->UpdateConfig(&Scheduler::Config::budget_background_fib,
                      GetFlag(FLAGS_scheduler_background_budget));
  sched->UpdateConfig(&Scheduler::Config::background_sleep_prob,
                      GetFlag(FLAGS_scheduler_background_sleep_prob));
  sched->UpdateConfig(&Scheduler::Config::background_warrant_pct,
                      GetFlag(FLAGS_scheduler_background_warrant));
}

void SetHuffmanTable(const std::string& huffman_table) {
  if (huffman_table.empty())
    return;
  vector<string_view> parts = absl::StrSplit(huffman_table, ',');
  for (const auto& part : parts) {
    vector<string_view> kv = absl::StrSplit(part, ':');
    if (kv.size() != 2 || kv[0].empty() || kv[1].empty()) {
      LOG(ERROR) << "Invalid huffman table entry" << part;
      continue;
    }
    string domain_str = absl::AsciiStrToUpper(kv[0]);
    CompactObj::HuffmanDomain domain;

    if (domain_str == "KEYS") {
      domain = CompactObj::HUFF_KEYS;
    } else if (domain_str == "STRINGS") {
      domain = CompactObj::HUFF_STRING_VALUES;
    } else {
      LOG(ERROR) << "Unknown huffman domain: " << kv[0];
      continue;
    }

    string unescaped;
    if (!absl::Base64Unescape(kv[1], &unescaped)) {
      LOG(ERROR) << "Failed to decode base64 huffman table for domain " << kv[0] << " with value "
                 << kv[1];
      continue;
    }

    atomic_bool success = true;
    shard_set->RunBriefInParallel([&](auto* shard) {
      if (!CompactObj::InitHuffmanThreadLocal(domain, unescaped)) {
        success = false;
      }
    });
    LOG_IF(ERROR, !success) << "Failed to set huffman table for domain " << kv[0] << " with value "
                            << kv[1];
  }
}

string_view CommandOptName(CO::CommandOpt opt, bool enabled) {
  using namespace CO;
  if (!enabled) {
    if (opt == FAST)
      return "SLOW";
    return "";
  }

  switch (opt) {
    case JOURNALED:
      return "write";
    case READONLY:
      return "readonly";
    case DENYOOM:
      return "denyoom";
    case FAST:
      return "fast";
    case LOADING:
      return "loading";
    case DANGEROUS:
      return "dangerous";
    case ADMIN:
      return "admin";
    case NOSCRIPT:
      return "noscript";
    case BLOCKING:
      return "blocking";
    case HIDDEN:
    case INTERLEAVED_KEYS:
    case GLOBAL_TRANS:
    case STORE_LAST_KEY:
    case VARIADIC_KEYS:
    case NO_AUTOJOURNAL:
    case NO_KEY_TRANSACTIONAL:
    case NO_KEY_TX_SPAN_ALL:
    case IDEMPOTENT:
      return "";
  }
  return "";
}

}  // namespace

Service::Service(ProactorPool* pp)
    : pp_(*pp),
      acl_family_(&user_registry_, pp),
      server_family_(this),
      cluster_family_(&server_family_) {
  CHECK(pp);
  CHECK(shard_set == NULL);

#ifdef PRINT_STACKTRACES_ON_SIGNAL
  LOG(INFO) << "PRINT STACKTRACES REGISTERED";
  ProactorBase::RegisterSignal({SIGUSR1}, pp_.GetNextProactor(), [this](int signal) {
    LOG(INFO) << "Received " << strsignal(signal);
    base::SetVLogLevel("uring_proactor", 2);

    util::fb2::Mutex m;
    pp_.AwaitFiberOnAll([&m](unsigned index, util::ProactorBase* base) {
      util::fb2::LockGuard lk(m);
      util::fb2::detail::FiberInterface::PrintAllFiberStackTraces();
    });
  });
#endif

  CHECK(shard_set == nullptr);
  shard_set = new EngineShardSet(pp);

  // We support less than 1024 threads and we support less than 1024 shards.
  // For example, Scan uses 10 bits in cursor to encode shard id it currently traverses.
  CHECK_LT(pp->size(), kMaxThreadSize);
  RegisterCommands();

  exec_cid_ = FindCmd("EXEC");

  engine_varz.emplace("engine", [this] { return GetVarzStats(); });
}

Service::~Service() {
#ifdef PRINT_STACKTRACES_ON_SIGNAL
  ProactorBase::ClearSignal({SIGUSR1}, true);
#endif

  delete shard_set;
  shard_set = nullptr;
}

void RegisterMutableFlags(ConfigRegistry* reg, absl::Span<const std::string> names,
                          std::function<void()> f) {
  auto cb = [f](auto&&) {
    shard_set->pool()->AwaitBrief([f](unsigned tid, auto*) { f(); });
    return true;
  };
  for (std::string_view name : names)
    reg->RegisterMutable(name, cb);
}

void Service::Init(util::AcceptServer* acceptor, std::vector<facade::Listener*> listeners) {
  InitRedisTables();
  facade::Connection::Init(pp_.size());

#if defined(WITH_SEARCH)
  // Initialize SimSIMD runtime if needed (explicit, avoids implicit static initializers)
  dfly::search::InitSimSIMD();
#endif

  config_registry.RegisterMutable("dbfilename");
  config_registry.Register("dbnum");  // equivalent to databases in redis.
  config_registry.Register("dir");
  config_registry.RegisterMutable("enable_heartbeat_eviction");
  config_registry.RegisterMutable("enable_heartbeat_rss_eviction");
  config_registry.RegisterMutable("masterauth");
  config_registry.RegisterMutable("masteruser");
  config_registry.RegisterMutable("max_eviction_per_heartbeat");
  config_registry.RegisterMutable("max_segment_to_consider");
  config_registry.RegisterMutable("pipeline_squash");
  config_registry.RegisterMutable("lua_mem_gc_threshold");
  config_registry.RegisterMutable("background_debug_jobs");

  // Register ServerState flags
  RegisterMutableFlags(&config_registry, ServerState::GetMutableFlagNames(),
                       []() { ServerState::tlocal()->UpdateFromFlags(); });
  // Register Connection flags
  RegisterMutableFlags(&config_registry, facade::Connection::GetMutableFlagNames(),
                       []() { facade::Connection::UpdateFromFlags(); });
  // Register tiered storage flags
  RegisterMutableFlags(&config_registry, TieredStorage::GetMutableFlagNames(), []() {
    if (auto* es = EngineShard::tlocal(); es && es->tiered_storage()) {
      es->tiered_storage()->UpdateFromFlags();
    }
  });
  // Register main service flags
  RegisterMutableFlags(&config_registry, GetMutableFlagNames(),
                       []() { UpdateFromFlagsOnThread(); });
  // Register squsher flags
  RegisterMutableFlags(&config_registry, MultiCommandSquasher::GetMutableFlagNames(),
                       []() { MultiCommandSquasher::UpdateFromFlags(); });
  // Register uring proactor flags
  RegisterMutableFlags(&config_registry,
                       base::GetFlagNames(FLAGS_uring_wake_mode, FLAGS_uring_submit_threshold),
                       []() { UpdateUringFlagsOnThread(); });
  // Register scheduler flags
  RegisterMutableFlags(
      &config_registry,
      base::GetFlagNames(FLAGS_scheduler_background_budget, FLAGS_scheduler_background_sleep_prob,
                         FLAGS_scheduler_background_warrant),
      []() { UpdateSchedulerFlagsOnThread(); });

  config_registry.RegisterSetter<MemoryBytesFlag>("maxmemory", [](const MemoryBytesFlag& flag) {
    // TODO: reduce code reliance on constant direct access of max_memory_limit
    max_memory_limit.store(flag.value, memory_order_relaxed);
  });

  config_registry.RegisterMutable("replica_partial_sync");
  config_registry.RegisterMutable("background_snapshotting");
  config_registry.RegisterMutable("replication_timeout");
  config_registry.RegisterMutable("migration_finalization_timeout_ms");
  config_registry.RegisterMutable("slot_migration_throttle_us");
  config_registry.RegisterMutable("table_growth_margin");
  config_registry.RegisterMutable("tcp_keepalive");
  config_registry.RegisterMutable("timeout");
  config_registry.RegisterMutable("send_timeout");
  config_registry.RegisterMutable("managed_service_info");
#ifdef WITH_SEARCH
  config_registry.RegisterMutable("MAXSEARCHRESULTS");
  config_registry.RegisterMutable("search_query_string_bytes");
#endif

  config_registry.RegisterMutable(
      "notify_keyspace_events", [pool = &pp_](const absl::CommandLineFlag& flag) {
        auto res = flag.TryGet<std::string>();
        if (!res.has_value() || (!res->empty() && !absl::EqualsIgnoreCase(*res, "EX"))) {
          return false;
        }

        pool->AwaitBrief([&res](unsigned, auto*) {
          auto* shard = EngineShard::tlocal();
          if (shard) {
            auto shard_id = shard->shard_id();
            auto& db_slice = namespaces->GetDefaultNamespace().GetDbSlice(shard_id);
            db_slice.SetNotifyKeyspaceEvents(*res);
          }
        });

        return true;
      });

  config_registry.RegisterMutable("aclfile");
  config_registry.RegisterSetter<uint32_t>("acllog_max_len", [](uint32_t val) {
    shard_set->pool()->AwaitFiberOnAll(
        [val](auto index, auto* context) { ServerState::tlocal()->acl_log.SetTotalEntries(val); });
  });

  uint32_t shard_num = GetFlag(FLAGS_num_shards);
  if (shard_num == 0 || shard_num > pp_.size()) {
    LOG_IF(WARNING, shard_num > pp_.size())
        << "Requested num_shards (" << shard_num << ") is bigger than thread count (" << pp_.size()
        << "), using num_shards=" << pp_.size();
    shard_num = pp_.size();
  }

  // We assume that listeners.front() is the main_listener
  // see dfly_main RunEngine. In unit tests, listeners are empty.
  facade::Listener* main_listener = listeners.empty() ? nullptr : listeners.front();

  ChannelStore* cs = new ChannelStore{};
  // Must initialize before the shard_set because EngineShard::Init references ServerState.
  pp_.AwaitBrief([&](uint32_t index, ProactorBase* pb) {
    tl_facade_stats = new FacadeStats;
    ServerState::Init(index, shard_num, main_listener, &user_registry_);
    ServerState::tlocal()->UpdateChannelStore(cs);
  });

  const auto tcp_disabled = GetFlag(FLAGS_port) == 0u;
  // We assume that listeners.front() is the main_listener
  // see dfly_main RunEngine
  if (!tcp_disabled && main_listener) {
    acl_family_.Init(main_listener, &user_registry_);
  }

  // Initialize shard_set with a callback running once in a while in the shard threads.
  shard_set->Init(shard_num, [this] {
    server_family_.GetDflyCmd()->BreakStalledFlowsInShard();
    server_family_.UpdateMemoryGlobalStats();
  });
  // InitThreadLocals might block
  pp_.AwaitFiberOnAll(
      [&](uint32_t index, ProactorBase* pb) { sharding::InitThreadLocals(shard_set->size()); });
  Transaction::Init(shard_num);

  shard_set->pool()->AwaitBrief([](unsigned, auto*) {
    facade::Connection::UpdateFromFlags();
    UpdateFromFlagsOnThread();
    UpdateUringFlagsOnThread();
    UpdateSchedulerFlagsOnThread();
  });
  SetHuffmanTable(GetFlag(FLAGS_huffman_table));

  // Requires that shard_set will be initialized before because server_family_.Init might
  // load the snapshot.
  server_family_.Init(acceptor, std::move(listeners));
}

void Service::Shutdown() {
  VLOG(1) << "Service::Shutdown";

  // We mark that we are shutting down. After this incoming requests will be
  // rejected.
  mu_.lock();
  global_state_ = GlobalState::SHUTTING_DOWN;
  mu_.unlock();

  pp_.AwaitFiberOnAll([](ProactorBase* pb) {
    ServerState::tlocal()->EnterLameDuck();
    facade::Connection::ShutdownThreadLocal();
  });

  config_registry.Reset();

  // to shutdown all the runtime components that depend on EngineShard
  cluster_family_.Shutdown();

  server_family_.Shutdown();

  shutdown_watchdog.emplace(pp_);

  engine_varz.reset();

  ChannelStore::Destroy();

  shard_set->PreShutdown();
  shard_set->Shutdown();

#ifdef WITH_SEARCH
  SearchFamily::Shutdown();
#endif

  Transaction::Shutdown();

  pp_.AwaitFiberOnAll([](ProactorBase* pb) { ServerState::tlocal()->Destroy(); });

  // wait for all the pending callbacks to stop.
  ThisFiber::SleepFor(10ms);
  facade::Connection::Shutdown();

  shutdown_watchdog->Disarm();
}

OpResult<KeyIndex> Service::FindKeys(const CommandId* cid, CmdArgList args) {
  // Sharded pub-sub acts as if it's sharded by its channel name (just for checks)
  if (cid->PubSubKind() == CO::PubSubKind::SHARDED) {
    // SPUBLISH has only one key, the rest is data
    if (cid->name() == registry_.RenamedOrOriginal("SPUBLISH"))
      return KeyIndex(0, 1);
    return {KeyIndex(0, args.size())};  // sub/unsub list of channels
  }

  return DetermineKeys(cid, args);
}

optional<ErrorReply> Service::CheckKeysOwnership(const CommandId& cid, CmdArgList args,
                                                 const ConnectionContext& dfly_cntx) {
  if (dfly_cntx.is_replicating) {
    // Always allow commands on the replication port, as it might be for future-owned keys.
    return nullopt;
  }

  if (cid.first_key_pos() == 0 && cid.PubSubKind() != CO::PubSubKind::SHARDED) {
    return nullopt;  // No key command.
  }

  OpResult<KeyIndex> key_index_res = FindKeys(&cid, args);

  if (!key_index_res) {
    return ErrorReply{key_index_res.status()};
  }

  const auto& key_index = *key_index_res;

  UniqueSlotChecker slot_checker;
  for (string_view key : key_index.Range(args)) {
    slot_checker.Add(key);
  }

  if (slot_checker.IsCrossSlot()) {
    return ErrorReply{kCrossSlotError};
  }

  optional<SlotId> keys_slot = slot_checker.GetUniqueSlotId();

  if (keys_slot.has_value()) {
    if (auto error = cluster::SlotOwnershipError(*keys_slot);
        !error.status.has_value() || error.status.value() != facade::OpStatus::OK) {
      return ErrorReply{std::move(error)};
    }
  }

  return nullopt;
}

// TODO(kostas) refactor. Almost 1-1 with CheckKeyOwnership() above.
std::optional<facade::ErrorReply> Service::TakenOverSlotError(const CommandId& cid, CmdArgList args,
                                                              const ConnectionContext& dfly_cntx) {
  if (cid.first_key_pos() == 0 && cid.PubSubKind() != CO::PubSubKind::SHARDED) {
    return nullopt;  // No key command.
  }

  OpResult<KeyIndex> key_index_res = FindKeys(&cid, args);

  if (!key_index_res) {
    return ErrorReply{key_index_res.status()};
  }

  const auto& key_index = *key_index_res;

  UniqueSlotChecker slot_checker;
  for (string_view key : key_index.Range(args)) {
    slot_checker.Add(key);
  }

  if (slot_checker.IsCrossSlot()) {
    return ErrorReply{kCrossSlotError};
  }

  optional<SlotId> keys_slot = slot_checker.GetUniqueSlotId();
  if (!keys_slot.has_value()) {
    return nullopt;
  }

  if (auto error = cluster::SlotOwnershipError(*keys_slot);
      !error.status.has_value() || error.status.value() != facade::OpStatus::OK) {
    return ErrorReply{std::move(error)};
  }
  const auto cluster_config = cluster::ClusterConfig::Current();
  if (!cluster_config)
    return facade::ErrorReply{facade::kClusterNotConfigured};

  // Moved regardless, we have been taken over
  cluster::ClusterNodeInfo redirect = cluster_config->GetMasterNodeForSlot(*keys_slot);
  return facade::ErrorReply{
      absl::StrCat("-MOVED ", *keys_slot, " ", redirect.ip, ":", redirect.port), "MOVED"};
}

// Return OK if all keys are allowed to be accessed: either declared in EVAL or
// transaction is running in global or non-atomic mode.
optional<ErrorReply> CheckKeysDeclared(const ConnectionState::ScriptInfo& eval_info,
                                       const CommandId* cid, CmdArgList args,
                                       Transaction::MultiMode multi_mode) {
  // We either scheduled on all shards or re-schedule for each operation,
  // so we are not restricted to any keys.
  if (multi_mode == Transaction::GLOBAL || multi_mode == Transaction::NON_ATOMIC)
    return nullopt;

  OpResult<KeyIndex> key_index_res = DetermineKeys(cid, args);
  if (!key_index_res)
    return ErrorReply{key_index_res.status()};

  // TODO: Switch to transaction internal locked keys once single hop multi transactions are merged
  // const auto& locked_keys = trans->GetMultiKeys();
  const auto& locked_tags = eval_info.lock_tags;
  for (string_view key : key_index_res->Range(args)) {
    if (!locked_tags.contains(LockTag{key})) {
      return ErrorReply(absl::StrCat(kUndeclaredKeyErr, ", key: ", key));
    }
  }

  return nullopt;
}

static optional<ErrorReply> VerifyConnectionAclStatus(const CommandId* cid,
                                                      const ConnectionContext* cntx,
                                                      string_view error_msg, ArgSlice tail_args) {
  // If we are on a squashed context we need to use the owner, because the
  // context we are operating on is a stub and the acl username is not copied
  // See: MultiCommandSquasher::SquashedHopCb
  if (cntx->conn_state.squashing_info)
    cntx = cntx->conn_state.squashing_info->owner;

  if (!acl::IsUserAllowedToInvokeCommand(*cntx, *cid, tail_args)) {
    return ErrorReply(absl::StrCat("-NOPERM ", cntx->authed_username, " ", error_msg));
  }
  return nullopt;
}

bool ShouldDenyOnOOM(const CommandId* cid, uint64_t curr_time_ns) {
  ServerState& etl = *ServerState::tlocal();
  if ((cid->opt_mask() & CO::DENYOOM) && etl.is_master) {
    auto memory_stats = etl.GetMemoryUsage(curr_time_ns);

    size_t limit = max_memory_limit.load(memory_order_relaxed);
    if (memory_stats.used_mem > limit ||
        (etl.rss_oom_deny_ratio > 0 && memory_stats.rss_mem > (limit * etl.rss_oom_deny_ratio))) {
      DLOG(WARNING) << "Out of memory, used " << memory_stats.used_mem << " ,rss "
                    << memory_stats.rss_mem << " ,limit " << limit;
      etl.stats.oom_error_cmd_cnt++;
      return true;
    }
  }
  return false;
}

optional<ErrorReply> Service::VerifyCommandExecution(const CommandContext& cmd_cntx,
                                                     CmdArgList tail_args) {
  DCHECK_NE(cmd_cntx.start_time_ns, 0u);
  if (ShouldDenyOnOOM(cmd_cntx.cid, cmd_cntx.start_time_ns)) {
    return facade::ErrorReply{OpStatus::OUT_OF_MEMORY};
  }

  return VerifyConnectionAclStatus(cmd_cntx.cid, cmd_cntx.server_conn_cntx(),
                                   "ACL rules changed between the MULTI and EXEC", tail_args);
}

std::optional<ErrorReply> Service::VerifyCommandState(const CommandId& cid, CmdArgList tail_args,
                                                      const ConnectionContext& dfly_cntx) {
  ServerState& etl = *ServerState::tlocal();

  // If there is no connection owner, it means the command it being called
  // from another command or used internally, therefore is always permitted.
  if (dfly_cntx.conn() != nullptr && !dfly_cntx.conn()->IsPrivileged() && cid.IsRestricted()) {
    VLOG(1) << "Non-admin attempt to execute " << cid.name() << " " << tail_args << " "
            << ConnectionLogContext(dfly_cntx.conn());
    return ErrorReply{"Cannot execute restricted command (admin only)", kRestrictDenied};
  }

  if (auto err = cid.Validate(tail_args); err)
    return err;

  // Check if the command is allowed to execute under this global state
  bool allowed_by_state = true;
  const GlobalState gstate = etl.gstate();
  switch (gstate) {
    case GlobalState::LOADING:
      allowed_by_state = dfly_cntx.journal_emulated || (cid.opt_mask() & CO::LOADING);
      break;
    case GlobalState::SHUTTING_DOWN:
      allowed_by_state = false;
      break;
    case GlobalState::TAKEN_OVER:
      // Only PING, admin commands, and all commands via admin connections are allowed
      // we prohibit even read commands, because read commands running in pipeline can take a while
      // to send all data to a client which leads to fail in takeover
      allowed_by_state =
          dfly_cntx.conn()->IsPrivileged() || (cid.opt_mask() & CO::ADMIN) || cid.name() == "PING";
      break;
    default:
      break;
  }

  if (!allowed_by_state) {
    VLOG(1) << "Command " << cid.name() << " not executed because global state is " << gstate;

    if (gstate == GlobalState::LOADING) {
      return ErrorReply(kLoadingErr);
    }

    if (gstate == GlobalState::TAKEN_OVER) {
      if (IsClusterEnabled()) {
        if (auto err = TakenOverSlotError(cid, tail_args, dfly_cntx); err) {
          return err;
        }
      }
      return ErrorReply(kLoadingErr);
    }

    return ErrorReply{StrCat("Can not execute during ", GlobalStateName(gstate))};
  }

  string_view cmd_name{cid.name()};

  if (dfly_cntx.req_auth && !dfly_cntx.authenticated) {
    if (cmd_name != "AUTH" && cmd_name != "QUIT" && cmd_name != "HELLO") {
      return ErrorReply{"-NOAUTH Authentication required."};
    }
  }

  // only reset and quit are allow if this connection is used for monitoring
  if (dfly_cntx.monitor && (cmd_name != "RESET" && cmd_name != "QUIT"))
    return ErrorReply{"Replica can't interact with the keyspace"};

  bool is_write_cmd = cid.IsJournaled();
  bool is_trans_cmd = cid.MultiControlKind() == CO::MultiControlKind::EXEC;
  bool under_script = dfly_cntx.conn_state.script_info != nullptr;
  bool multi_active = dfly_cntx.conn_state.exec_info.IsCollecting() && !is_trans_cmd;

  if (!etl.is_master && is_write_cmd && !dfly_cntx.is_replicating)
    return ErrorReply{"-READONLY You can't write against a read only replica."};

  if (multi_active) {
    if (cmd_name == "WATCH" || cmd_name == "FLUSHALL" || cmd_name == "FLUSHDB" ||
        absl::EndsWith(cmd_name, "SUBSCRIBE"))
      return ErrorReply{absl::StrCat("'", cmd_name, "' not allowed inside a transaction")};
  }

  if (IsClusterEnabled()) {
    if (auto err = CheckKeysOwnership(cid, tail_args, dfly_cntx); err)
      return err;
  }

  if (under_script && (cid.opt_mask() & CO::NOSCRIPT))
    return ErrorReply{"This Redis command is not allowed from script"};

  if (under_script) {
    DCHECK(dfly_cntx.transaction);
    // The following commands access shards arbitrarily without having keys, so they can only be run
    // non atomically or globally.
    Transaction::MultiMode mode = dfly_cntx.transaction->GetMultiMode();
    bool shard_access = (cid.opt_mask()) & (CO::GLOBAL_TRANS | CO::NO_KEY_TRANSACTIONAL);
    if (shard_access && (mode != Transaction::GLOBAL && mode != Transaction::NON_ATOMIC))
      return ErrorReply("This Redis command is not allowed from script");

    if (cid.IsTransactional()) {
      auto err = CheckKeysDeclared(*dfly_cntx.conn_state.script_info, &cid, tail_args, mode);

      if (err.has_value()) {
        VLOG(1) << "CheckKeysDeclared failed with error " << err->ToSv() << " for command "
                << cid.name();
        return err;
      }
    }

    if (dfly_cntx.conn_state.script_info->read_only && is_write_cmd) {
      return ErrorReply{"Write commands are not allowed from read-only scripts"};
    }
  }

  return VerifyConnectionAclStatus(&cid, &dfly_cntx, "has no ACL permissions", tail_args);
}

DispatchResult Service::DispatchCommand(facade::ParsedArgs args, SinkReplyBuilder* builder,
                                        facade::ConnectionContext* cntx) {
  DCHECK(!args.empty());
  DCHECK_NE(0u, shard_set->size()) << "Init was not called";

  absl::Cleanup clear_last_error([builder]() { builder->ConsumeLastError(); });
  ServerState& etl = *ServerState::tlocal();

  string cmd = absl::AsciiStrToUpper(args.Front());
  const auto [cid, args_no_cmd] = registry_.FindExtended(cmd, args.Tail());

  if (cid == nullptr) {
    auto reply = ReportUnknownCmd(cmd);
    builder->SendError(reply.ToSv(), reply.kind);
    return DispatchResult::ERROR;
  }

  ConnectionContext* dfly_cntx = static_cast<ConnectionContext*>(cntx);
  bool under_script = bool(dfly_cntx->conn_state.script_info);
  bool under_exec = dfly_cntx->conn_state.exec_info.IsRunning();
  bool dispatching_in_multi = under_script || under_exec;

  CmdArgVec tmp_vec;
  ArgSlice tail_args = args_no_cmd.ToSlice(&tmp_vec);

  if (VLOG_IS_ON(2) && cntx->conn() /* no owner in replica context */) {
    LOG(INFO) << "Got (" << cntx->conn()->GetClientId() << "): " << (under_script ? "LUA " : "")
              << cid->name() << " " << tail_args << " in dbid=" << dfly_cntx->conn_state.db_index;
  }

  // Don't interrupt running multi commands or admin connections.
  if (etl.IsPaused() && !dispatching_in_multi && cntx->conn() && !cntx->conn()->IsPrivileged()) {
    bool is_write = cid->IsJournaled();
    is_write |= cid->name() == "PUBLISH" || cid->name() == "EVAL" || cid->name() == "EVALSHA";
    is_write |= cid->name() == "EXEC" && dfly_cntx->conn_state.exec_info.is_write;

    cntx->paused = true;
    etl.AwaitPauseState(is_write);
    cntx->paused = false;
  }

  if (auto err = VerifyCommandState(*cid, tail_args, *dfly_cntx); err) {
    LOG_IF(WARNING, cntx->replica_conn || !cntx->conn() /* no owner in replica context */)
        << "VerifyCommandState error: " << err->ToSv();
    if (auto& exec_info = dfly_cntx->conn_state.exec_info; exec_info.IsCollecting())
      exec_info.state = ConnectionState::ExecInfo::EXEC_ERROR;

    // We need to skip this because ACK's should not be replied to
    // Bonus points because this allows to continue replication with ACL users who got
    // their access revoked and reinstated

    if (cid->name() == "REPLCONF") {
      DCHECK_GE(args_no_cmd.size(), 1u);
      if (absl::EqualsIgnoreCase(args_no_cmd.Front(), "ACK")) {
        server_family_.GetDflyCmd()->OnClose(
            dfly_cntx->conn_state.replication_info.repl_session_id);
        return DispatchResult::ERROR;
      }
    }
    DCHECK(!err->status);
    builder->SendError(err->ToSv(), err->kind);
    return DispatchResult::ERROR;
  }

  VLOG_IF(1, cid->opt_mask() & CO::CommandOpt::DANGEROUS)
      << "Executing dangerous command " << cid->name() << " "
      << ConnectionLogContext(dfly_cntx->conn());

  bool is_trans_cmd = cid->MultiControlKind() == CO::MultiControlKind::EXEC;
  if (dfly_cntx->conn_state.exec_info.IsCollecting() && !is_trans_cmd) {
    // TODO: protect against aggregating huge transactions.
    auto& exec_info = dfly_cntx->conn_state.exec_info;
    const size_t old_size = exec_info.GetStoredCmdBytes();
    exec_info.AddStoredCmd(cid, tail_args);  // Deep copy of args.
    etl.stats.stored_cmd_bytes += exec_info.GetStoredCmdBytes() - old_size;
    if (cid->IsJournaled()) {
      exec_info.is_write = true;
    }
    builder->SendSimpleString("QUEUED");
    return DispatchResult::OK;
  }

  // Create command transaction
  intrusive_ptr<Transaction> dist_trans;

  if (dispatching_in_multi) {
    DCHECK(dfly_cntx->transaction);
    if (cid->IsTransactional()) {
      dfly_cntx->transaction->MultiSwitchCmd(cid);
      OpStatus status = dfly_cntx->transaction->InitByArgs(
          dfly_cntx->ns, dfly_cntx->conn_state.db_index, tail_args);

      if (status != OpStatus::OK) {
        builder->SendError(StatusToMsg(status));
        return DispatchResult::ERROR;
      }
    }
  } else {
    DCHECK(dfly_cntx->transaction == nullptr);

    if (cid->IsTransactional()) {
      dist_trans.reset(new Transaction{cid});

      if (!dist_trans->IsMulti()) {  // Multi command initialize themself based on their mode.
        CHECK(dfly_cntx->ns != nullptr);
        if (auto st =
                dist_trans->InitByArgs(dfly_cntx->ns, dfly_cntx->conn_state.db_index, tail_args);
            st != OpStatus::OK) {
          builder->SendError(StatusToMsg(st));
          return DispatchResult::ERROR;
        }
      }

      dfly_cntx->transaction = dist_trans.get();
      dfly_cntx->last_command_debug.shards_count = dfly_cntx->transaction->GetUniqueShardCnt();
    } else {
      dfly_cntx->transaction = nullptr;
    }
  }

  DispatchResult res = DispatchResult::ERROR;
  if (dfly_cntx->cmnd_ctx) {
    // Currently only used by memcache flow.

    dfly_cntx->cmnd_ctx->cid = cid;
    dfly_cntx->cmnd_ctx->tx = dfly_cntx->transaction;
    res = InvokeCmd(tail_args, dfly_cntx->cmnd_ctx);
  } else {
    // TODO: eventually Resp flow should be migrated to use CommandContext too.
    CommandContext command_ctx{cid, dfly_cntx->transaction, builder, dfly_cntx};
    res = InvokeCmd(tail_args, &command_ctx);
  }

  if ((res != DispatchResult::OK) && (res != DispatchResult::OOM)) {
    builder->SendError("Internal Error");
    builder->CloseConnection();
  }

  if (!dispatching_in_multi) {
    dfly_cntx->transaction = nullptr;
  }
  return res;
}

class ReplyGuard {
 public:
  ReplyGuard(const CommandContext& cmd_cntx) {
    const bool is_script = bool(cmd_cntx.server_conn_cntx()->conn_state.script_info);
    cid_name_ = cmd_cntx.cid->name();
    const bool is_one_of = (cid_name_ == "REPLCONF" || cid_name_ == "DFLY");
    bool is_mcache = cmd_cntx.mc_command() != nullptr;
    const bool is_no_reply_memcache =
        is_mcache &&
        (static_cast<MCReplyBuilder*>(cmd_cntx.rb())->NoReply() || cid_name_ == "QUIT");
    const bool should_dcheck = !is_one_of && !is_script && !is_no_reply_memcache;
    if (should_dcheck) {
      cmd_cntx_ = &cmd_cntx;
      replies_recorded_ = cmd_cntx.rb()->RepliesRecorded();
    }
  }

  ~ReplyGuard() {
    if (cmd_cntx_ && !cmd_cntx_->IsDeferredReply()) {
      auto* rb = cmd_cntx_->rb();
      DCHECK_GT(rb->RepliesRecorded(), replies_recorded_) << cid_name_ << " " << typeid(*rb).name();
    }
  }

 private:
  const CommandContext* cmd_cntx_ = nullptr;
  size_t replies_recorded_ = 0;
  std::string_view cid_name_;
};

OpResult<void> OpTrackKeys(const OpArgs slice_args, const facade::Connection::WeakRef& conn_ref,
                           const ShardArgs& args) {
  if (conn_ref.IsExpired()) {
    DVLOG(2) << "Connection expired, exiting TrackKey function.";
    return OpStatus::OK;
  }

  DVLOG(2) << "Start tracking keys for client ID: " << conn_ref.GetClientId();

  auto& db_slice = slice_args.GetDbSlice();
  // TODO: There is a bug here that we track all arguments instead of tracking only keys.
  for (auto key : args) {
    DVLOG(2) << "Inserting client ID " << conn_ref.GetClientId()
             << " into the tracking client set of key " << key;
    db_slice.TrackKey(conn_ref, key);
  }

  return OpStatus::OK;
}

DispatchResult Service::InvokeCmd(CmdArgList tail_args, CommandContext* cmd_cntx) {
  auto* cid = cmd_cntx->cid;
  DCHECK(cid);
  DCHECK(!cid->Validate(tail_args));

  cmd_cntx->start_time_ns = absl::GetCurrentTimeNanos();

  ConnectionContext* cntx = cmd_cntx->server_conn_cntx();
  auto* builder = cmd_cntx->rb();
  DCHECK(builder);
  DCHECK(cntx);

  if (auto err = VerifyCommandExecution(*cmd_cntx, tail_args); err) {
    // We need to skip this because ACK's should not be replied to
    // Bonus points because this allows to continue replication with ACL users who got
    // their access revoked and reinstated
    if (cid->name() == "REPLCONF" && absl::EqualsIgnoreCase(ArgS(tail_args, 0), "ACK")) {
      return DispatchResult::OK;
    }
    cmd_cntx->SendError(*err);
    // return ERROR only for internal error aborts
    builder->ConsumeLastError();

    return err->status == OpStatus::OUT_OF_MEMORY ? DispatchResult::OOM : DispatchResult::OK;
  }

  // We are not sending any admin command in the monitor, and we do not want to
  // do any processing if we don't have any waiting connections with monitor
  // enabled on them - see https://redis.io/commands/monitor/
  // For EXEC command specifically, we dispatch monitor after executing all queued commands
  // to preserve correct ordering (MULTI, commands, EXEC) instead of (MULTI, EXEC, commands)
  bool should_dispatch_monitor =
      !ServerState::tlocal()->Monitors().Empty() && cid->CanBeMonitored();
  if (should_dispatch_monitor) {
    DispatchMonitor(cntx, cid, tail_args);
  }

  ServerState::tlocal()->RecordCmd(cntx->has_main_or_memcache_listener);
  auto& info = cntx->conn_state.tracking_info_;
  Transaction* tx = cmd_cntx->tx;
  if (tx) {
    // Reset it, because in multi/exec the transaction pointer is the same and
    // we will end up triggerring the callback on the following commands. To avoid this
    // we reset it.
    tx->SetTrackingCallback({});
    if (cid->IsReadOnly() && info.ShouldTrackKeys()) {
      auto conn = cntx->conn()->Borrow();
      tx->SetTrackingCallback([conn](Transaction* trans) {
        auto* shard = EngineShard::tlocal();
        OpTrackKeys(trans->GetOpArgs(shard), conn, trans->GetShardArgs(shard->shard_id()));
      });
    }
  }

#ifndef NDEBUG
  // Verifies that we reply to the client when needed.
  ReplyGuard reply_guard(*cmd_cntx);
#endif
  auto last_error = builder->ConsumeLastError();
  DCHECK(last_error.empty());
  try {
    cid->Invoke(tail_args, cmd_cntx);
  } catch (std::exception& e) {
    LOG(ERROR) << "Internal error, system probably unstable " << e.what();
    return DispatchResult::ERROR;
  }

  DispatchResult res = DispatchResult::OK;
  if (std::string reason = builder->ConsumeLastError(); !reason.empty()) {
    // Set flag if OOM reported
    if (reason == kOutOfMemory) {
      res = DispatchResult::OOM;
    }
    VLOG(2) << FailedCommandToString(cid->name(), tail_args, reason);
    if (!absl::StartsWith(reason, "-BUSYGROUP")) {
      LOG_EVERY_T(WARNING, 1) << FailedCommandToString(cid->name(), tail_args, reason);
    }
  }

  auto cid_name = cid->name();
  if ((!tx && cid_name != "MULTI") || (tx && !tx->IsMulti())) {
    // Each time we execute a command we need to increase the sequence number in
    // order to properly track clients when OPTIN is used.
    // We don't do this for `multi/exec` because it would break the
    // semantics, i.e, CACHING should stick for all commands following
    // the CLIENT CACHING ON within a multi/exec block
    cntx->conn_state.tracking_info_.IncrementSequenceNumber();
  }

  cmd_cntx->RecordLatency(tail_args);

  if (tx && !cntx->conn_state.exec_info.IsRunning() && cntx->conn_state.script_info == nullptr) {
    cntx->last_command_debug.clock = tx->txid();
  }

  return res;
}

DispatchManyResult Service::DispatchManyCommands(std::function<facade::ParsedArgs()> arg_gen,
                                                 unsigned count, SinkReplyBuilder* builder,
                                                 facade::ConnectionContext* cntx) {
  ConnectionContext* dfly_cntx = static_cast<ConnectionContext*>(cntx);
  DCHECK(!dfly_cntx->conn_state.exec_info.IsRunning());
  DCHECK_EQ(builder->GetProtocol(), Protocol::REDIS);
  DCHECK_GT(count, 1u);

  auto* ss = dfly::ServerState::tlocal();
  // Don't even start when paused. We can only continue if DispatchTracker is aware of us running.
  if (ss->IsPaused())
    return {.processed = 0, .account_in_stats = false};

  vector<StoredCmd> stored_cmds;
  intrusive_ptr<Transaction> dist_trans;
  uint32_t dispatched = 0;
  MultiCommandSquasher::Stats stats;

  uint64_t start_cycles = base::CycleClock::Now();

  auto perform_squash = [&] {
    if (stored_cmds.empty())
      return;

    if (!dist_trans) {
      dist_trans.reset(new Transaction{exec_cid_});
      dist_trans->StartMultiNonAtomic();
    } else {
      // Reset to original command id as it's changed during squashing
      dist_trans->MultiSwitchCmd(exec_cid_);
    }

    dfly_cntx->transaction = dist_trans.get();
    MultiCommandSquasher::Opts opts;
    opts.verify_commands = true;
    opts.max_squash_size = ss->max_squash_cmd_num;

    stats += MultiCommandSquasher::Execute(absl::MakeSpan(stored_cmds),
                                           static_cast<RedisReplyBuilder*>(builder), dfly_cntx,
                                           this, opts);
    dfly_cntx->transaction = nullptr;

    dispatched += stored_cmds.size();
    stored_cmds.clear();
  };

  for (unsigned i = 0; i < count; i++) {
    ParsedArgs args = arg_gen();
    string cmd = absl::AsciiStrToUpper(args.Front());
    const auto [cid, tail_args] = registry_.FindExtended(cmd, args.Tail());

    // MULTI...EXEC commands need to be collected into a single context, so squashing is not
    // possible
    const bool is_multi = dfly_cntx->conn_state.exec_info.IsCollecting() ||
                          (cid != nullptr && cid->MultiControlKind() == CO::MultiControlKind::EXEC);

    // Generally, executing any multi-transactions (including eval) is not possible because they
    // might request a stricter multi mode than non-atomic which is used for squashing.
    // TODO: By allowing promoting non-atomic multit transactions to lock-ahead for specific command
    // invocations, we can potentially execute multiple eval in parallel, which is very powerful
    // paired with shardlocal eval
    const bool is_eval = cid != nullptr && cid->MultiControlKind() == CO::MultiControlKind::EVAL;
    const bool is_blocking = cid != nullptr && cid->IsBlocking();

    if (!is_multi && !is_eval && !is_blocking && cid != nullptr) {
      stored_cmds.reserve(count);
      stored_cmds.emplace_back(cid, tail_args);  // Shallow copy
      continue;
    }

    // Squash accumulated commands
    perform_squash();

    // Stop accumulating when a pause is requested, fall back to regular dispatch
    if (ss->IsPaused())
      break;

    // Dispatch non squashed command only after all squshed commands were executed and replied
    DispatchCommand(args, builder, cntx);
    dispatched++;
  }

  perform_squash();

  if (dist_trans)
    dist_trans->UnlockMulti();

  uint64_t total_usec = base::CycleClock::ToUsec(base::CycleClock::Now() - start_cycles);
  bool account_in_stats = total_usec > squash_stats_latency_lower_limit_cached;
  if (account_in_stats) {
    auto* ss = ServerState::tlocal();
    ss->stats.multi_squash_exec_hop_usec += stats.hop_usec;
    ss->stats.multi_squash_exec_reply_usec += stats.reply_usec;
    ss->stats.multi_squash_hops += stats.hops;
    ss->stats.squashed_commands += stats.squashed_commands;
  } else {
    ss->stats.squash_stats_ignored++;
  }
  return {.processed = dispatched, .account_in_stats = account_in_stats};
}

void Service::DispatchMC(facade::ParsedCommand* parsed_cmd) {
  absl::InlinedVector<string_view, 8> args;
  MCReplyBuilder* mc_builder = static_cast<MCReplyBuilder*>(parsed_cmd->rb());
  CommandContext* cmd_ctx = static_cast<CommandContext*>(parsed_cmd);
  const auto& cmd = *parsed_cmd->mc_command();
  string_view value = cmd.value();
  auto* cntx = cmd_ctx->server_conn_cntx();
  char cmd_name[16];
  char ttl[absl::numbers_internal::kFastToBufferSize];
  char store_opt[32] = {0};
  char ttl_op[] = "EXAT";

  mc_builder->SetNoreply(cmd.no_reply);
  mc_builder->SetMeta(cmd.meta);
  if (cmd.meta) {
    mc_builder->SetBase64(cmd.base64);
    mc_builder->SetReturnMCFlag(cmd.return_flags);
    mc_builder->SetReturnValue(cmd.return_value);
    mc_builder->SetReturnVersion(cmd.return_version);
  }

  switch (cmd.type) {
    case MemcacheParser::REPLACE:
      strcpy(cmd_name, "SET");
      strcpy(store_opt, "XX");
      break;
    case MemcacheParser::SET:
      strcpy(cmd_name, "SET");
      break;
    case MemcacheParser::ADD:
      strcpy(cmd_name, "SET");
      strcpy(store_opt, "NX");
      break;
    case MemcacheParser::DELETE:
      strcpy(cmd_name, "DEL");
      break;
    case MemcacheParser::INCR:
      strcpy(cmd_name, "INCRBY");
      absl::numbers_internal::FastIntToBuffer(cmd.delta, store_opt);
      break;
    case MemcacheParser::DECR:
      strcpy(cmd_name, "DECRBY");
      absl::numbers_internal::FastIntToBuffer(cmd.delta, store_opt);
      break;
    case MemcacheParser::APPEND:
      strcpy(cmd_name, "APPEND");
      break;
    case MemcacheParser::PREPEND:
      strcpy(cmd_name, "PREPEND");
      break;
    case MemcacheParser::GATS:
      [[fallthrough]];
    case MemcacheParser::GAT:
      strcpy(cmd_name, "GAT");
      break;
    case MemcacheParser::GET:
      [[fallthrough]];
    case MemcacheParser::GETS:
      strcpy(cmd_name, "MGET");
      break;
    case MemcacheParser::FLUSHALL:
      strcpy(cmd_name, "FLUSHDB");
      break;
    case MemcacheParser::QUIT:
      strcpy(cmd_name, "QUIT");
      break;
    case MemcacheParser::STATS:
      server_family_.StatsMC(cmd.key(), cmd_ctx);
      return;
    case MemcacheParser::VERSION:
      mc_builder->SendSimpleString("VERSION 1.6.0 DF");
      return;
    default:
      mc_builder->SendClientError("bad command line format");
      return;
  }

  args.emplace_back(cmd_name, strlen(cmd_name));

  // if expire_ts is greater than month it's a unix timestamp
  // https://github.com/memcached/memcached/blob/master/doc/protocol.txt#L139
  constexpr uint32_t kExpireLimit = 60 * 60 * 24 * 30;
  const uint64_t expire_ts = cmd.expire_ts && cmd.expire_ts <= kExpireLimit
                                 ? cmd.expire_ts + time(nullptr)
                                 : cmd.expire_ts;

  // For GAT/GATS commands, the expiry precedes the keys which will be looked up:
  // GAT|GATS <expiry> key [key...]
  if (cmd.type == MemcacheParser::GAT || cmd.type == MemcacheParser::GATS) {
    char* next = absl::numbers_internal::FastIntToBuffer(expire_ts, ttl);
    args.emplace_back(ttl, next - ttl);
  }

  if (!cmd.backed_args->empty()) {
    args.emplace_back(cmd.key());
  }

  ConnectionContext* dfly_cntx = static_cast<ConnectionContext*>(cntx);
  if (MemcacheParser::IsStoreCmd(cmd.type)) {
    args.emplace_back(value);

    if (store_opt[0]) {
      args.emplace_back(store_opt, strlen(store_opt));
    }

    if (expire_ts && memcmp(cmd_name, "SET", 3) == 0) {
      char* next = absl::numbers_internal::FastIntToBuffer(expire_ts, ttl);
      args.emplace_back(ttl_op, 4);
      args.emplace_back(ttl, next - ttl);
    }
  } else if (cmd.type < MemcacheParser::QUIT) {  // read commands
    if (cmd.size() > 1) {
      auto it = cmd.backed_args->begin();
      ++it;  // skip first key
      for (auto end = cmd.backed_args->end(); it != end; ++it) {
        args.emplace_back(*it);
      }
    }
  } else {  // write commands.
    if (store_opt[0]) {
      args.emplace_back(store_opt, strlen(store_opt));
    }
  }
  dfly_cntx->cmnd_ctx = static_cast<CommandContext*>(parsed_cmd);
  DispatchCommand(ParsedArgs{args}, mc_builder, cntx);
}

ErrorReply Service::ReportUnknownCmd(string_view cmd_name) {
  constexpr uint8_t kMaxUknownCommands = 64;
  constexpr uint8_t kMaxUknownCommandLength = 20;

  lock_guard lk(mu_);
  if (unknown_cmds_.size() <= kMaxUknownCommands && cmd_name.size() <= kMaxUknownCommandLength)
    unknown_cmds_[cmd_name]++;

  return ErrorReply{StrCat("unknown command `", cmd_name, "`"), "unknown_cmd"};
}

bool RequirePrivilegedAuth() {
  return !GetFlag(FLAGS_admin_nopass);
}

facade::ConnectionContext* Service::CreateContext(facade::Connection* owner) {
  auto cred = user_registry_.GetCredentials("default");
  ConnectionContext* res = new ConnectionContext{owner, std::move(cred)};
  res->ns = &namespaces->GetOrInsert("");

  if (owner->socket()->IsUDS()) {
    res->req_auth = false;
    res->skip_acl_validation = true;
  } else if (owner->IsPrivileged() && RequirePrivilegedAuth()) {
    res->req_auth = !GetPassword().empty();
  } else if (!owner->IsPrivileged()) {
    // Memcached protocol doesn't support authentication, so we don't require it
    if (owner->GetProtocol() == Protocol::MEMCACHE) {
      res->req_auth = false;
      res->authenticated = true;  // Automatically authenticated for Memcached protocol
    } else {
      res->req_auth = !user_registry_.AuthUser("default", "");
    }
  }

  // a bit of a hack. I set up breaker callback here for the owner.
  // Should work though it's confusing to have it here.
  owner->RegisterBreakHook([res](uint32_t) {
    if (res->transaction)
      res->transaction->CancelBlocking(nullptr);
  });

  return res;
}

facade::ParsedCommand* Service::AllocateParsedCommand() {
  return new CommandContext{};
}

const CommandId* Service::FindCmd(std::string_view cmd) const {
  return registry_.Find(registry_.RenamedOrOriginal(cmd));
}

bool Service::IsLocked(Namespace* ns, DbIndex db_index, std::string_view key) const {
  ShardId sid = Shard(key, shard_count());
  bool is_open = pp_.at(sid)->AwaitBrief([db_index, key, ns, sid] {
    return ns->GetDbSlice(sid).CheckLock(IntentLock::EXCLUSIVE, db_index, key);
  });
  return !is_open;
}

bool Service::IsShardSetLocked() const {
  std::atomic_uint res{0};

  shard_set->RunBriefInParallel([&](EngineShard* shard) {
    bool unlocked = shard->shard_lock()->Check(IntentLock::SHARED);
    res.fetch_add(!unlocked, memory_order_relaxed);
  });

  return res.load() != 0;
}

absl::flat_hash_map<std::string, unsigned> Service::UknownCmdMap() const {
  lock_guard lk(mu_);
  return unknown_cmds_;
}

void Service::Quit(CmdArgList args, CommandContext* cmd_cntx) {
  if (cmd_cntx->rb()->GetProtocol() == Protocol::REDIS)
    cmd_cntx->rb()->SendOk();

  cmd_cntx->rb()->CloseConnection();

  auto* cntx = cmd_cntx->server_conn_cntx();
  DeactivateMonitoring(cntx);
  cntx->conn()->ShutdownSelfBlocking();
}

void Service::Multi(CmdArgList args, CommandContext* cmd_cntx) {
  auto& conn_state = cmd_cntx->server_conn_cntx()->conn_state;
  if (conn_state.exec_info.IsCollecting()) {
    return cmd_cntx->SendError("MULTI calls can not be nested");
  }
  conn_state.exec_info.state = ConnectionState::ExecInfo::EXEC_COLLECT;
  // TODO: to protect against huge exec transactions.
  return cmd_cntx->rb()->SendOk();
}

void Service::Watch(CmdArgList args, CommandContext* cmd_cntx) {
  auto* cntx = cmd_cntx->server_conn_cntx();
  auto& exec_info = cntx->conn_state.exec_info;

  // Skip if EXEC will already fail due previous WATCH.
  if (exec_info.watched_dirty.load(memory_order_relaxed)) {
    return cmd_cntx->rb()->SendOk();
  }

  atomic_uint32_t keys_existed = 0;
  auto cb = [&](Transaction* t, EngineShard* shard) {
    ShardId shard_id = shard->shard_id();
    ShardArgs largs = t->GetShardArgs(shard_id);
    for (auto k : largs) {
      t->GetDbSlice(shard_id).RegisterWatchedKey(cntx->db_index(), k, &exec_info);
    }

    auto res = GenericFamily::OpExists(t->GetOpArgs(shard), largs);
    keys_existed.fetch_add(res.value_or(0), memory_order_relaxed);
    return OpStatus::OK;
  };
  cmd_cntx->tx->ScheduleSingleHop(std::move(cb));

  // Duplicate keys are stored to keep correct count.
  exec_info.watched_existed += keys_existed.load(memory_order_relaxed);
  for (string_view key : args) {
    exec_info.watched_keys.emplace_back(cntx->db_index(), key);
  }

  return cmd_cntx->rb()->SendOk();
}

void Service::Unwatch(CmdArgList args, CommandContext* cmd_cntx) {
  auto* cntx = cmd_cntx->server_conn_cntx();
  UnwatchAllKeys(cntx->ns, &cntx->conn_state.exec_info);
  return cmd_cntx->rb()->SendOk();
}

optional<CapturingReplyBuilder::Payload> Service::FlushEvalAsyncCmds(ConnectionContext* cntx,
                                                                     bool force) {
  auto& info = cntx->conn_state.script_info;
  auto* tx = cntx->transaction;
  size_t used_mem = info->async_cmds_heap_mem + info->async_cmds.size() * sizeof(StoredCmd);

  if ((info->async_cmds.empty() || !force) && used_mem < info->async_cmds_heap_limit)
    return nullopt;

  ++ServerState::tlocal()->stats.eval_squashed_flushes;

  auto* eval_cid = registry_.Find("EVAL");
  DCHECK(eval_cid);
  tx->MultiSwitchCmd(eval_cid);

  CapturingReplyBuilder crb{ReplyMode::ONLY_ERR};
  MultiCommandSquasher::Opts opts;
  opts.verify_commands = true;
  opts.error_abort = true;
  opts.max_squash_size = ServerState::tlocal()->max_squash_cmd_num;
  MultiCommandSquasher::Execute(absl::MakeSpan(info->async_cmds), &crb, cntx, this, opts);

  info->async_cmds_heap_mem = 0;
  info->async_cmds.clear();

  auto reply = crb.Take();
  return CapturingReplyBuilder::TryExtractError(reply) ? make_optional(std::move(reply)) : nullopt;
}

void Service::CallFromScript(Interpreter::CallArgs& ca, CommandContext* cmd_cntx) {
  auto* tx = cmd_cntx->tx;
  DCHECK(tx);
  DVLOG(2) << "CallFromScript " << ca.args[0];

  InterpreterReplier replier(ca.translator);
  optional<ErrorReply> findcmd_err;
  auto* cntx = cmd_cntx->server_conn_cntx();
  if (ca.async) {
    auto& info = cntx->conn_state.script_info;
    string cmd = absl::AsciiStrToUpper(ca.args[0]);

    // Full command verification happens during squashed execution
    if (auto* cid = registry_.Find(cmd); cid != nullptr) {
      auto reply_mode = ca.error_abort ? ReplyMode::ONLY_ERR : ReplyMode::NONE;
      info->async_cmds.emplace_back(cid, ca.args.subspan(1), reply_mode);
      info->async_cmds_heap_mem += info->async_cmds.back().UsedMemory();
    } else if (ca.error_abort) {  // If we don't abort on errors, we can ignore it completely
      findcmd_err = ReportUnknownCmd(ca.args[0]);
    }
  }

  if (auto err = FlushEvalAsyncCmds(cntx, !ca.async || findcmd_err.has_value()); err) {
    CapturingReplyBuilder::Apply(std::move(*err), &replier);  // forward error to lua
    *ca.requested_abort = true;
    return;
  }

  if (findcmd_err.has_value()) {
    auto* prev = cmd_cntx->SwapReplier(&replier);
    cmd_cntx->SendError(*findcmd_err);
    *ca.requested_abort |= ca.error_abort;
    cmd_cntx->SwapReplier(prev);
  }

  if (ca.async)
    return;

  DispatchCommand(ParsedArgs{ca.args}, &replier, cntx);
}

void Service::Eval(CmdArgList args, CommandContext* cmd_cntx, bool read_only) {
  string_view body = ArgS(args, 0);

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  if (body.empty()) {
    return rb->SendNull();
  }

  auto* cntx = cmd_cntx->server_conn_cntx();
  BorrowedInterpreter interpreter{cmd_cntx->tx, &cntx->conn_state};
  auto res = server_family_.script_mgr()->Insert(body, interpreter);
  if (!res)
    return cmd_cntx->SendError(res.error().Format(), facade::kScriptErrType);

  string sha{std::move(res.value())};

  CallSHA(args, sha, interpreter, read_only, cmd_cntx);
}

void Service::EvalRo(CmdArgList args, CommandContext* cmd_cntx) {
  Eval(args, cmd_cntx, true);
}

void Service::EvalSha(CmdArgList args, CommandContext* cmd_cntx, bool read_only) {
  string sha = absl::AsciiStrToLower(ArgS(args, 0));
  auto* cntx = cmd_cntx->server_conn_cntx();
  BorrowedInterpreter interpreter{cmd_cntx->tx, &cntx->conn_state};
  CallSHA(args, sha, interpreter, read_only, cmd_cntx);
}

void Service::EvalShaRo(CmdArgList args, CommandContext* cmd_cntx) {
  EvalSha(args, cmd_cntx, true);
}

void Service::CallSHA(CmdArgList args, string_view sha, Interpreter* interpreter, bool read_only,
                      CommandContext* cmd_cntx) {
  uint32_t num_keys;
  CHECK(absl::SimpleAtoi(ArgS(args, 1), &num_keys));  // we already validated this

  EvalArgs ev_args;
  ev_args.sha = sha;
  ev_args.keys = args.subspan(2, num_keys);
  ev_args.args = args.subspan(2 + num_keys);

  uint64_t start = absl::GetCurrentTimeNanos();
  EvalInternal(args, ev_args, interpreter, read_only, cmd_cntx);

  uint64_t end = absl::GetCurrentTimeNanos();
  ServerState::tlocal()->RecordCallLatency(sha, (end - start) / 1000);
}

void LoadScript(string_view sha, ScriptMgr* script_mgr, Interpreter* interpreter) {
  if (interpreter->Exists(sha))
    return;

  auto script_data = script_mgr->Find(sha);
  if (!script_data) {
    LOG(DFATAL) << "Script " << sha << " not found in script mgr";
    return;
  }

  string err;
  Interpreter::AddResult add_res = interpreter->AddFunction(sha, script_data->body, &err);
  if (add_res != Interpreter::ADD_OK) {
    LOG(DFATAL) << "Error adding " << sha << " to database, err " << err;
  }
}

// Determine multi mode based on script params.
Transaction::MultiMode DetermineMultiMode(ScriptMgr::ScriptParams params) {
  if (params.atomic && params.undeclared_keys)
    return Transaction::GLOBAL;
  else if (params.atomic)
    return Transaction::LOCK_AHEAD;
  else
    return Transaction::NON_ATOMIC;
}

// Starts multi transaction. Returns true if transaction was scheduled.
// Skips scheduling if multi mode requires declaring keys, but no keys were declared.
bool StartMulti(ConnectionContext* cntx, Transaction::MultiMode tx_mode, CmdArgList keys) {
  Transaction* tx = cntx->transaction;
  Namespace* ns = cntx->ns;
  const DbIndex dbid = cntx->db_index();

  switch (tx_mode) {
    case Transaction::GLOBAL:
      tx->StartMultiGlobal(ns, dbid);
      return true;
    case Transaction::LOCK_AHEAD:
      if (keys.empty())
        return false;
      tx->StartMultiLockedAhead(ns, dbid, keys);
      return true;
    case Transaction::NON_ATOMIC:
      tx->StartMultiNonAtomic();
      return true;
    default:
      LOG(FATAL) << "Invalid mode";
  };

  return false;
}

static bool CanRunSingleShardMulti(optional<ShardId> sid, Transaction::MultiMode multi_mode,
                                   const Transaction& tx) {
  if (!sid.has_value() || multi_mode != Transaction::LOCK_AHEAD) {
    return false;
  }

  if (tx.GetMultiMode() != Transaction::NOT_DETERMINED) {
    // We may be running EVAL under MULTI. Currently RunSingleShardMulti() will attempt to lock
    // keys, in which case will be already locked by MULTI. We could optimize this path as well
    // though.
    return false;
  }

  return true;
}

void Service::EvalInternal(CmdArgList args, const EvalArgs& eval_args, Interpreter* interpreter,
                           bool read_only, CommandContext* cmd_cntx) {
  DCHECK(!eval_args.sha.empty());

  // Sanitizing the input to avoid code injection.
  if (eval_args.sha.size() != 40 || !IsSHA(eval_args.sha)) {
    return cmd_cntx->SendError(facade::kScriptNotFound);
  }

  auto* ss = ServerState::tlocal();
  auto params = ss->GetScriptParams(eval_args.sha);
  if (!params) {
    return cmd_cntx->SendError(facade::kScriptNotFound);
  }

  LoadScript(eval_args.sha, server_family_.script_mgr(), interpreter);

  string error;
  auto* conn_cntx = cmd_cntx->server_conn_cntx();
  DCHECK(!conn_cntx->conn_state.script_info);  // we should not call eval from the script.

  // TODO: to determine whether the script is RO by scanning all "redis.p?call" calls
  // and checking whether all invocations consist of RO commands.
  // we can do it once during script insertion into script mgr.
  auto& sinfo = conn_cntx->conn_state.script_info;
  sinfo = make_unique<ConnectionState::ScriptInfo>();
  sinfo->lock_tags.reserve(eval_args.keys.size());
  sinfo->read_only = read_only;

  optional<ShardId> sid;

  UniqueSlotChecker slot_checker;
  for (size_t i = 0; i < eval_args.keys.size(); ++i) {
    string_view key = ArgS(eval_args.keys, i);
    slot_checker.Add(key);
    sinfo->lock_tags.insert(LockTag(key));

    ShardId cur_sid = Shard(key, shard_count());
    if (i == 0) {
      sid = cur_sid;
    }
    if (sid.has_value() && *sid != cur_sid) {
      sid = nullopt;
    }
  }

  sinfo->async_cmds_heap_limit = GetFlag(FLAGS_multi_eval_squash_buffer);
  Transaction* tx = cmd_cntx->tx;
  CHECK(tx != nullptr);

  Interpreter::RunResult result;
  Transaction::MultiMode script_mode = DetermineMultiMode(*params);

  interpreter->SetGlobalArray("KEYS", eval_args.keys);
  interpreter->SetGlobalArray("ARGV", eval_args.args);

  absl::Cleanup clean = [interpreter, &sinfo]() {
    interpreter->ResetStack();
    sinfo.reset();
  };

  if (CanRunSingleShardMulti(sid, script_mode, *tx)) {
    // If script runs on a single shard, we run it remotely to save hops.
    interpreter->SetRedisFunc([cmd_cntx, this](Interpreter::CallArgs args) {
      // Disable squashing, as we're using the squashing mechanism to run remotely.
      args.async = false;
      CallFromScript(args, cmd_cntx);
    });

    ++ss->stats.eval_shardlocal_coordination_cnt;
    tx->PrepareMultiForScheduleSingleHop(conn_cntx->ns, *sid, conn_cntx->db_index(), args);
    tx->ScheduleSingleHop([&](Transaction*, EngineShard*) {
      boost::intrusive_ptr<Transaction> stub_tx =
          new Transaction{tx, *sid, slot_checker.GetUniqueSlotId()};
      conn_cntx->transaction = stub_tx.get();

      result = interpreter->RunFunction(eval_args.sha, &error);

      conn_cntx->transaction = tx;
      return OpStatus::OK;
    });

    if (*sid != ss->thread_index()) {
      VLOG(2) << "Migrating connection " << conn_cntx->conn() << " from "
              << ProactorBase::me()->GetPoolIndex() << " to " << *sid;
      conn_cntx->conn()->RequestAsyncMigration(shard_set->pool()->at(*sid), false);
    }
  } else {
    Transaction::MultiMode tx_mode = tx->GetMultiMode();
    bool scheduled = false;

    // Check if eval is already part of a running multi transaction
    if (tx_mode != Transaction::NOT_DETERMINED) {
      if (tx_mode > script_mode) {
        string err = StrCat(
            "Multi mode conflict when running eval in multi transaction. Multi mode is: ", tx_mode,
            " eval mode is: ", script_mode);
        return cmd_cntx->SendError(err);
      }
    } else {
      scheduled = StartMulti(conn_cntx, script_mode, eval_args.keys);
    }

    ++ss->stats.eval_io_coordination_cnt;
    interpreter->SetRedisFunc(
        [cmd_cntx, this](Interpreter::CallArgs args) { CallFromScript(args, cmd_cntx); });

    result = interpreter->RunFunction(eval_args.sha, &error);

    if (auto err = FlushEvalAsyncCmds(conn_cntx, true); err) {
      auto err_ref = CapturingReplyBuilder::TryExtractError(*err);
      result = Interpreter::RUN_ERR;
      error = absl::StrCat(err_ref->first);
    }

    // Conclude the transaction.
    if (scheduled)
      tx->UnlockMulti();
  }

  if (result == Interpreter::RUN_ERR) {
    string resp = StrCat("Error running script (call to ", eval_args.sha, "): ", error);
    server_family_.script_mgr()->OnScriptError(eval_args.sha, error);
    return cmd_cntx->SendError(resp, facade::kScriptErrType);
  }

  CHECK(result == Interpreter::RUN_OK);

  // TODO(vlad): Investigate if using ReplyScope here is possible with a different serialization
  // strategy due to currently SerializeResult destructuring a value while serializing
  auto* builder = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  SinkReplyBuilder::ReplyAggregator agg(builder);
  EvalSerializer ser{builder, params->float_as_int};
  if (!interpreter->IsResultSafe()) {
    builder->SendError("reached lua stack limit");
  } else {
    interpreter->SerializeResult(&ser);
  }
}

void Service::Discard(CmdArgList args, CommandContext* cmd_cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  auto* cntx = cmd_cntx->server_conn_cntx();
  if (!cntx->conn_state.exec_info.IsCollecting()) {
    return rb->SendError("DISCARD without MULTI");
  }

  MultiCleanup(cntx);
  rb->SendOk();
}

// Return true if non of the connections watched keys expired.
bool CheckWatchedKeyExpiry(ConnectionContext* cntx, const CommandId* exists_cid,
                           const CommandId* exec_cid) {
  auto& exec_info = cntx->conn_state.exec_info;
  auto* tx = cntx->transaction;

  CmdArgVec str_list(exec_info.watched_keys.size());
  for (size_t i = 0; i < str_list.size(); i++) {
    auto& [db, s] = exec_info.watched_keys[i];
    str_list[i] = MutableSlice{s.data(), s.size()};
  }

  atomic_uint32_t watch_exist_count{0};
  auto cb = [&watch_exist_count](Transaction* t, EngineShard* shard) {
    ShardArgs args = t->GetShardArgs(shard->shard_id());
    auto res = GenericFamily::OpExists(t->GetOpArgs(shard), args);
    watch_exist_count.fetch_add(res.value_or(0), memory_order_relaxed);

    return OpStatus::OK;
  };

  tx->MultiSwitchCmd(exists_cid);
  tx->InitByArgs(cntx->ns, cntx->conn_state.db_index, CmdArgList{str_list});
  OpStatus status = tx->ScheduleSingleHop(std::move(cb));
  CHECK_EQ(OpStatus::OK, status);

  // Reset cid to EXEC as it was before
  tx->MultiSwitchCmd(exec_cid);

  // The comparison can still be true even if a key expired due to another one being created.
  // So we have to check the watched_dirty flag, which is set if a key expired.
  return watch_exist_count.load() == exec_info.watched_existed &&
         !exec_info.watched_dirty.load(memory_order_relaxed);
}

// Check if exec_info watches keys on dbs other than db_indx.
bool IsWatchingOtherDbs(DbIndex db_indx, const ConnectionState::ExecInfo& exec_info) {
  return std::any_of(exec_info.watched_keys.begin(), exec_info.watched_keys.end(),
                     [db_indx](const auto& pair) { return pair.first != db_indx; });
}

template <typename F> void IterateAllKeys(const ConnectionState::ExecInfo* exec_info, F&& f) {
  for (auto& [dbid, key] : exec_info->watched_keys)
    f(MutableSlice{key.data(), key.size()});

  CmdArgVec arg_vec{};

  for (const auto& scmd : exec_info->body) {
    if (!scmd.Cid()->IsTransactional())
      continue;

    auto args = scmd.ArgList(&arg_vec);
    auto key_res = DetermineKeys(scmd.Cid(), args);
    if (!key_res.ok())
      continue;

    for (unsigned i : key_res->Range())
      f(arg_vec[i]);
  }
}

CmdArgVec CollectAllKeys(ConnectionState::ExecInfo* exec_info) {
  CmdArgVec out;
  out.reserve(exec_info->watched_keys.size() + exec_info->body.size());

  IterateAllKeys(exec_info, [&out](MutableSlice key) { out.push_back(key); });

  return out;
}

void Service::Exec(CmdArgList args, CommandContext* cmd_cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  auto* cntx = cmd_cntx->server_conn_cntx();
  auto& exec_info = cntx->conn_state.exec_info;

  // Clean the context no matter the outcome
  absl::Cleanup exec_clear = [cntx] { MultiCleanup(cntx); };

  if (exec_info.state == ConnectionState::ExecInfo::EXEC_ERROR) {
    return rb->SendError("-EXECABORT Transaction discarded because of previous errors");
  }

  // Check basic invariants
  if (!exec_info.IsCollecting()) {
    return rb->SendError("EXEC without MULTI");
  }

  if (IsWatchingOtherDbs(cntx->db_index(), exec_info)) {
    return rb->SendError("Dragonfly does not allow WATCH and EXEC on different databases");
  }

  if (exec_info.watched_dirty.load(memory_order_relaxed)) {
    return rb->SendNull();
  }

  cmd_cntx->exec_body_len = exec_info.body.size();

  auto keys = CollectAllKeys(&exec_info);
  if (IsClusterEnabled()) {
    UniqueSlotChecker slot_checker;
    for (const auto& s : keys) {
      slot_checker.Add(s);
    }

    if (slot_checker.IsCrossSlot()) {
      return rb->SendError(kCrossSlotError);
    }
  }

  // The transaction can contain script load script execution, determine their presence ahead to
  // customize logic below.
  ExecScriptUse state = DetermineScriptPresense(exec_info.body);

  // We borrow a single interpreter for all the EVALs/Script load inside. Returned by MultiCleanup
  if (state != ExecScriptUse::NONE) {
    exec_info.preborrowed_interpreter =
        BorrowedInterpreter(cmd_cntx->tx, &cntx->conn_state).Release();
  }

  // Determine according multi mode, not only only flag, but based on presence of global commands
  // and scripts
  Transaction::MultiMode multi_mode = DeduceExecMode(state, exec_info, *script_mgr());

  bool scheduled = false;
  if (multi_mode != Transaction::NOT_DETERMINED) {
    scheduled = StartMulti(cntx, multi_mode, keys);
  }

  // EXEC should not run if any of the watched keys expired.
  if (!exec_info.watched_keys.empty() &&
      !CheckWatchedKeyExpiry(cntx, registry_.Find("EXISTS"), exec_cid_)) {
    cmd_cntx->tx->UnlockMulti();
    return rb->SendNull();
  }

  exec_info.state = ConnectionState::ExecInfo::EXEC_RUNNING;

  VLOG(2) << "StartExec " << exec_info.body.size();

  // Make sure we flush whatever responses we aggregated in the reply builder.
  SinkReplyBuilder::ReplyAggregator agg(rb);
  rb->StartArray(exec_info.body.size());

  if (!exec_info.body.empty()) {
    string descr = CreateExecDescriptor(exec_info.body, cmd_cntx->tx->GetUniqueShardCnt());
    ServerState::tlocal()->exec_freq_count[descr]++;

    if (GetFlag(FLAGS_multi_exec_squash) && state != ExecScriptUse::SCRIPT_RUN &&
        !cntx->conn_state.tracking_info_.IsTrackingOn()) {
      MultiCommandSquasher::Opts opts;
      opts.max_squash_size = ServerState::tlocal()->max_squash_cmd_num;
      MultiCommandSquasher::Execute(absl::MakeSpan(exec_info.body), rb, cntx, this, opts);
    } else {
      CmdArgVec arg_vec;
      DCHECK_EQ(cmd_cntx->cid, exec_cid_);

      for (const auto& scmd : exec_info.body) {
        CmdArgList args = scmd.ArgList(&arg_vec);

        if (scmd.Cid()->IsTransactional()) {
          cmd_cntx->tx->MultiSwitchCmd(scmd.Cid());
          OpStatus st = cmd_cntx->tx->InitByArgs(cntx->ns, cntx->conn_state.db_index, args);
          if (st != OpStatus::OK) {
            cmd_cntx->SendError(st);
            break;
          }
        }

        // TODO: we will have to create a CommandContext per command if we want to support async
        // execution inside exec.
        cmd_cntx->cid = scmd.Cid();
        auto invoke_res = InvokeCmd(args, cmd_cntx);
        if ((invoke_res != DispatchResult::OK) ||
            rb->GetError())  // checks for i/o error, not logical error.
          break;
      }
      cmd_cntx->cid = exec_cid_;
    }
  }

  if (scheduled) {
    VLOG(2) << "Exec unlocking " << exec_info.body.size() << " commands";
    cmd_cntx->tx->UnlockMulti();
  }

  // Dispatch EXEC to monitor after all queued commands have been executed
  // to preserve correct ordering (MULTI, commands, EXEC)
  if (!ServerState::tlocal()->Monitors().Empty() && (exec_cid_->opt_mask() & CO::ADMIN) == 0) {
    DispatchMonitor(cntx, exec_cid_, args);
  }

  VLOG(2) << "Exec completed";
}

void Service::Publish(CmdArgList args, CommandContext* cmd_cntx) {
  bool sharded = cmd_cntx->cid->PubSubKind() == CO::PubSubKind::SHARDED;
  if (!sharded && IsClusterEnabled())
    return cmd_cntx->SendError("PUBLISH is not supported in cluster mode yet");

  string_view channel = ArgS(args, 0);
  string_view messages[] = {ArgS(args, 1)};

  auto* cs = ServerState::tlocal()->channel_store();
  cmd_cntx->rb()->SendLong(cs->SendMessages(channel, messages, sharded));
}

void Service::Subscribe(CmdArgList args, CommandContext* cmd_cntx) {
  bool sharded = cmd_cntx->cid->PubSubKind() == CO::PubSubKind::SHARDED;
  if (!sharded && IsClusterEnabled())
    return cmd_cntx->SendError("SUBSCRIBE is not supported in cluster mode yet");

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  auto* conn_cntx = cmd_cntx->server_conn_cntx();
  conn_cntx->ChangeSubscription(true /*add*/, true /* reply*/, sharded, args, rb);
}

void Service::Unsubscribe(CmdArgList args, CommandContext* cmd_cntx) {
  bool sharded = cmd_cntx->cid->PubSubKind() == CO::PubSubKind::SHARDED;
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  auto* conn_cntx = cmd_cntx->server_conn_cntx();
  if (!sharded && IsClusterEnabled())
    return rb->SendError("UNSUBSCRIBE is not supported in cluster mode yet");

  if (args.size() == 0) {
    conn_cntx->UnsubscribeAll(true, rb);
  } else {
    conn_cntx->ChangeSubscription(false, true, sharded, args, rb);
  }
}

void Service::PSubscribe(CmdArgList args, CommandContext* cmd_cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());

  if (IsClusterEnabled()) {
    return rb->SendError("PSUBSCRIBE is not supported in cluster mode yet");
  }
  cmd_cntx->server_conn_cntx()->ChangePSubscription(true, true, args, rb);
}

void Service::PUnsubscribe(CmdArgList args, CommandContext* cmd_cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  if (IsClusterEnabled()) {
    return rb->SendError("PUNSUBSCRIBE is not supported in cluster mode yet");
  }
  auto* conn_cntx = cmd_cntx->server_conn_cntx();
  if (args.size() == 0) {
    conn_cntx->PUnsubscribeAll(true, rb);
  } else {
    conn_cntx->ChangePSubscription(false, true, args, rb);
  }
}

// Not a real implementation. Serves as a decorator to accept some function commands
// for testing.
void Service::Function(CmdArgList args, CommandContext* cmd_cntx) {
  string sub_cmd = absl::AsciiStrToUpper(ArgS(args, 0));

  if (sub_cmd == "FLUSH") {
    return cmd_cntx->rb()->SendOk();
  }

  string err = UnknownSubCmd(sub_cmd, "FUNCTION");
  return cmd_cntx->SendError(err, kSyntaxErrType);
}

void Service::PubsubChannels(string_view pattern, SinkReplyBuilder* builder) {
  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  rb->SendBulkStrArr(ServerState::tlocal()->channel_store()->ListChannels(pattern));
}

void Service::PubsubPatterns(SinkReplyBuilder* builder) {
  size_t pattern_count = ServerState::tlocal()->channel_store()->PatternCount();
  builder->SendLong(pattern_count);
}

void Service::PubsubNumSub(CmdArgList args, SinkReplyBuilder* builder) {
  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  rb->StartArray(args.size() * 2);
  for (string_view channel : args) {
    rb->SendBulkString(channel);
    rb->SendLong(ServerState::tlocal()->channel_store()->FetchSubscribers(channel).size());
  }
}

void Service::Monitor(CmdArgList args, CommandContext* cmd_cntx) {
  VLOG(1) << "starting monitor on this connection: "
          << cmd_cntx->server_conn_cntx()->conn()->GetClientId();
  // we are registering the current connection for all threads so they will be aware of
  // this connection, to send to it any command
  cmd_cntx->rb()->SendOk();
  cmd_cntx->server_conn_cntx()->ChangeMonitor(true /* start */);
}

void Service::Pubsub(CmdArgList args, CommandContext* cmd_cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());

  if (args.size() < 1) {
    rb->SendError(WrongNumArgsError(cmd_cntx->cid->name()));
    return;
  }

  string subcmd = absl::AsciiStrToUpper(ArgS(args, 0));

  if (subcmd == "HELP") {
    string_view help_arr[] = {
        "PUBSUB <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        "CHANNELS [<pattern>]",
        "\tReturn the currently active channels matching a <pattern> (default: '*').",
        "NUMPAT",
        "\tReturn number of subscriptions to patterns.",
        "NUMSUB [<channel> <channel...>]",
        "\tReturns the number of subscribers for the specified channels, excluding",
        "\tpattern subscriptions.",
        "SHARDCHANNELS [pattern]",
        "\tReturns a list of active shard channels, optionally matching the specified pattern ",
        "(default: '*').",
        "SHARDNUMSUB [<channel> <channel...>]",
        "\tReturns the number of subscribers for the specified shard channels, excluding",
        "\tpattern subscriptions.",
        "HELP",
        "\tPrints this help."};

    rb->SendSimpleStrArr(help_arr);
    return;
  }

  // Don't allow SHARD subcommands in non cluster mode
  if (!IsClusterEnabledOrEmulated() && ((subcmd == "SHARDCHANNELS") || (subcmd == "SHARDNUMSUB"))) {
    auto err = absl::StrCat("PUBSUB ", subcmd, " is not supported in non cluster mode");
    return rb->SendError(err);
  }

  if (subcmd == "CHANNELS" || subcmd == "SHARDCHANNELS") {
    string_view pattern;
    if (args.size() > 1) {
      pattern = ArgS(args, 1);
    }
    PubsubChannels(pattern, rb);
  } else if (subcmd == "NUMPAT") {
    PubsubPatterns(rb);
  } else if (subcmd == "NUMSUB" || subcmd == "SHARDNUMSUB") {
    args.remove_prefix(1);
    PubsubNumSub(args, rb);
  } else {
    rb->SendError(UnknownSubCmd(subcmd, "PUBSUB"));
  }
}

void Service::Command(CmdArgList args, CommandContext* cmd_cntx) {
  unsigned cmd_cnt = 0;
  registry_.Traverse([&](string_view name, const CommandId& cd) {
    if ((cd.opt_mask() & CO::HIDDEN) == 0) {
      ++cmd_cnt;
    }
  });

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  auto serialize_command = [rb, this](string_view name, const CommandId& cid) {
    rb->StartArray(7);
    rb->SendSimpleString(cid.name());
    rb->SendLong(cid.arity());

    vector<string> opts;
    for (uint32_t i = 0; i < 32; i++) {
      unsigned obit = (1u << i);
      if (auto name = CommandOptName(CO::CommandOpt{obit}, cid.opt_mask() & obit); !name.empty())
        opts.emplace_back(name);
    }
    rb->SendSimpleStrArr(opts);

    rb->SendLong(cid.first_key_pos());
    rb->SendLong(cid.last_key_pos());
    rb->SendLong(cid.opt_mask() & CO::INTERLEAVED_KEYS ? 2 : 1);

    {
      const auto& table = acl_family_.GetRevTable();
      vector<string> cats;
      for (uint32_t i = 0; i < 32; i++) {
        if (cid.acl_categories() & (1 << i)) {
          cats.emplace_back("@" + table[i]);
        }
      }
      rb->SendSimpleStrArr(cats);
    }
  };

  // If no arguments are specified, reply with all commands
  if (args.empty()) {
    rb->StartArray(cmd_cnt);
    registry_.Traverse([&](string_view name, const CommandId& cid) {
      if (cid.opt_mask() & CO::HIDDEN)
        return;
      serialize_command(name, cid);
    });
    return;
  }

  string subcmd = absl::AsciiStrToUpper(ArgS(args, 0));

  // COUNT
  if (subcmd == "COUNT") {
    return rb->SendLong(cmd_cnt);
  }

  bool sufficient_args = (args.size() == 2);

  // INFO [cmd]
  if (subcmd == "INFO" && sufficient_args) {
    string cmd = absl::AsciiStrToUpper(ArgS(args, 1));

    if (const auto* cid = registry_.Find(cmd); cid) {
      rb->StartArray(1);
      serialize_command(cmd, *cid);
    } else {
      rb->SendNull();
    }

    return;
  }

  sufficient_args = (args.size() == 1);
  if (subcmd == "DOCS" && sufficient_args) {
    // Returning an error here forces the interactive CLI client to fall back to static hints and
    // tab completion
    return rb->SendError("COMMAND DOCS Not Implemented");
  }

  if (subcmd == "HELP" && sufficient_args) {
    // Return help information for supported COMMAND subcommands
    constexpr string_view help[] = {
        "(no subcommand)",
        "    Return details about all commands.",
        "INFO command-name",
        "    Return details about specified command.",
        "COUNT",
        "    Return the total number of commands in this server.",
    };
    return rb->SendSimpleStrArr(help);
  }

  return rb->SendError(kSyntaxErr, kSyntaxErrType);
}

VarzValue::Map Service::GetVarzStats() {
  VarzValue::Map res;

  Metrics m = server_family_.GetMetrics(&namespaces->GetDefaultNamespace());
  DbStats db_stats;
  for (const auto& s : m.db_stats) {
    db_stats += s;
  }

  res.emplace_back("keys", VarzValue::FromInt(db_stats.key_count));
  res.emplace_back("obj_mem_usage", VarzValue::FromInt(db_stats.obj_memory_usage));
  double load = double(db_stats.key_count) / (1 + db_stats.prime_capacity);
  res.emplace_back("table_load_factor", VarzValue::FromDouble(load));

  return res;
}

GlobalState Service::SwitchState(GlobalState from, GlobalState to) {
  util::fb2::LockGuard lk(mu_);
  if (global_state_ != from) {
    return global_state_;
  }

  VLOG(1) << "Switching state from " << from << " to " << to;
  global_state_ = to;

  pp_.Await([&](ProactorBase*) {
    ServerState::tlocal()->set_gstate(to);
    auto* es = EngineShard::tlocal();
    if (es && to == GlobalState::ACTIVE) {
      DbSlice& db = namespaces->GetDefaultNamespace().GetDbSlice(es->shard_id());
      DCHECK(db.IsLoadRefCountZero());
    }
  });
  return to;
}

bool Service::RequestLoadingState() {
  if (SwitchState(GlobalState::ACTIVE, GlobalState::LOADING) == GlobalState::LOADING) {
    util::fb2::LockGuard lk(mu_);
    loading_state_counter_++;
    return true;
  }
  return false;
}

void Service::RemoveLoadingState() {
  bool switch_state = false;
  {
    util::fb2::LockGuard lk(mu_);
    CHECK_GT(loading_state_counter_, 0u);
    --loading_state_counter_;
    switch_state = loading_state_counter_ == 0;
  }
  if (switch_state) {
    SwitchState(GlobalState::LOADING, GlobalState::ACTIVE);
  }
}

bool Service::IsLoadingExclusively() {
  util::fb2::LockGuard lk(mu_);
  return global_state_ == GlobalState::LOADING && loading_state_counter_ == 0;
}

void Service::ConfigureHttpHandlers(util::HttpListenerBase* base, bool is_privileged) {
  // We skip authentication on privileged listener if the flag admin_nopass is set
  // We also skip authentication if requirepass is empty
  const bool should_skip_auth =
      (is_privileged && !RequirePrivilegedAuth()) || GetPassword().empty();
  if (!should_skip_auth) {
    base->SetAuthFunctor([pass = GetPassword()](std::string_view path, std::string_view username,
                                                std::string_view password) {
      if (path == "/metrics")
        return true;
      const bool pass_verified = pass.empty() ? true : password == pass;
      return username == "default" && pass_verified;
    });
  }
  server_family_.ConfigureMetrics(base);
  base->RegisterCb("/txz", TxTable);
  base->RegisterCb("/clusterz", [this](const http::QueryArgs& args, HttpContext* send) {
    return ClusterHtmlPage(args, send, &cluster_family_);
  });

  if (GetFlag(FLAGS_expose_http_api)) {
    base->RegisterCb("/api",
                     [this](const http::QueryArgs& args, HttpRequest&& req, HttpContext* send) {
                       HttpAPI(args, std::move(req), this, send);
                     });
  }
}

void Service::OnConnectionClose(facade::ConnectionContext* cntx) {
  ConnectionContext* server_cntx = static_cast<ConnectionContext*>(cntx);
  ConnectionState& conn_state = server_cntx->conn_state;
  VLOG_IF(1, conn_state.replication_info.repl_session_id)
      << "OnConnectionClose: " << server_cntx->conn()->GetName()
      << ", repl_session_id: " << conn_state.replication_info.repl_session_id;

  if (conn_state.subscribe_info) {  // Clean-ups related to PUBSUB
    if (!conn_state.subscribe_info->channels.empty()) {
      server_cntx->UnsubscribeAll(false, nullptr);
    }

    if (conn_state.subscribe_info) {
      DCHECK(!conn_state.subscribe_info->patterns.empty());
      server_cntx->PUnsubscribeAll(false, nullptr);
    }

    DCHECK(!conn_state.subscribe_info);
  }

  UnwatchAllKeys(server_cntx->ns, &conn_state.exec_info);

  DeactivateMonitoring(server_cntx);

  server_family_.OnClose(server_cntx);

  conn_state.tracking_info_.SetClientTracking(false);
}

void Service::RegisterTieringFlags() {
#ifdef WITH_TIERING
  // TODO(vlad): Introduce templatable flag cache
  auto update_tiered_storage = [](auto) {
    shard_set->pool()->AwaitBrief([](unsigned, auto*) {
      if (auto* es = EngineShard::tlocal(); es && es->tiered_storage()) {
        es->tiered_storage()->UpdateFromFlags();
      }
    });
  };
  config_registry.RegisterSetter<bool>("tiered_experimental_cooling", update_tiered_storage);
  config_registry.RegisterSetter<unsigned>("tiered_storage_write_depth", update_tiered_storage);
  config_registry.RegisterSetter<float>("tiered_offload_threshold", update_tiered_storage);
  config_registry.RegisterSetter<float>("tiered_upload_threshold", update_tiered_storage);
#endif
}

Service::ContextInfo Service::GetContextInfo(facade::ConnectionContext* cntx) const {
  ConnectionContext* server_cntx = static_cast<ConnectionContext*>(cntx);
  return {.db_index = server_cntx->db_index(),
          .async_dispatch = server_cntx->async_dispatch,
          .conn_closing = server_cntx->conn_closing,
          .subscribers = bool(server_cntx->conn_state.subscribe_info),
          .blocked = server_cntx->blocked};
}

#define HFUNC(x) SetHandler(&Service::x)
#define MFUNC(x) \
  SetHandler([this](CmdArgList sp, CommandContext* cntx) { this->x(std::move(sp), cntx); })

namespace acl {
constexpr uint32_t kQuit = FAST | CONNECTION;
constexpr uint32_t kMulti = FAST | TRANSACTION;
constexpr uint32_t kWatch = FAST | TRANSACTION;
constexpr uint32_t kUnwatch = FAST | TRANSACTION;
constexpr uint32_t kDiscard = FAST | TRANSACTION;
constexpr uint32_t kEval = SLOW | SCRIPTING;
constexpr uint32_t kEvalRo = SLOW | SCRIPTING;
constexpr uint32_t kEvalSha = SLOW | SCRIPTING;
constexpr uint32_t kEvalShaRo = SLOW | SCRIPTING;
constexpr uint32_t kExec = SLOW | TRANSACTION;
constexpr uint32_t kPublish = PUBSUB | FAST;
constexpr uint32_t kSubscribe = PUBSUB | SLOW;
constexpr uint32_t kUnsubscribe = PUBSUB | SLOW;
constexpr uint32_t kPSubscribe = PUBSUB | SLOW;
constexpr uint32_t kPUnsubsribe = PUBSUB | SLOW;
constexpr uint32_t kFunction = SLOW;
constexpr uint32_t kMonitor = ADMIN | SLOW | DANGEROUS;
constexpr uint32_t kPubSub = SLOW;
constexpr uint32_t kCommand = SLOW | CONNECTION;
}  // namespace acl

void Service::Register(CommandRegistry* registry) {
  using CI = CommandId;
  registry->StartFamily();
  *registry
      << CI{"QUIT", CO::FAST, 1, 0, 0, acl::kQuit}.HFUNC(Quit)
      << CI{"MULTI", CO::NOSCRIPT | CO::FAST | CO::LOADING, 1, 0, 0, acl::kMulti}.HFUNC(Multi)
      << CI{"WATCH", CO::LOADING, -2, 1, -1, acl::kWatch}.HFUNC(Watch)
      << CI{"UNWATCH", CO::LOADING, 1, 0, 0, acl::kUnwatch}.HFUNC(Unwatch)
      << CI{"DISCARD", CO::NOSCRIPT | CO::FAST | CO::LOADING, 1, 0, 0, acl::kDiscard}.MFUNC(Discard)
      << CI{"EVAL", CO::NOSCRIPT | CO::VARIADIC_KEYS, -3, 3, 3, acl::kEval}
             .MFUNC(Eval)
             .SetValidator(&EvalValidator)
      << CI{"EVAL_RO", CO::NOSCRIPT | CO::READONLY | CO::VARIADIC_KEYS, -3, 3, 3, acl::kEvalRo}
             .MFUNC(EvalRo)
             .SetValidator(&EvalValidator)
      << CI{"EVALSHA", CO::NOSCRIPT | CO::VARIADIC_KEYS, -3, 3, 3, acl::kEvalSha}
             .MFUNC(EvalSha)
             .SetValidator(&EvalValidator)
      << CI{"EVALSHA_RO",   CO::NOSCRIPT | CO::READONLY | CO::VARIADIC_KEYS, -3, 3, 3,
            acl::kEvalShaRo}
             .MFUNC(EvalShaRo)
             .SetValidator(&EvalValidator)
      << CI{"EXEC", CO::LOADING | CO::NOSCRIPT, 1, 0, 0, acl::kExec}.MFUNC(Exec)
      << CI{"PUBLISH", CO::LOADING | CO::FAST, 3, 0, 0, acl::kPublish}.MFUNC(Publish)
      << CI{"SPUBLISH", CO::LOADING | CO::FAST, 3, 0, 0, acl::kPublish}.MFUNC(Publish)
      << CI{"SUBSCRIBE", CO::NOSCRIPT | CO::LOADING, -2, 0, 0, acl::kSubscribe}.MFUNC(Subscribe)
      << CI{"SSUBSCRIBE", CO::NOSCRIPT | CO::LOADING, -2, 0, 0, acl::kSubscribe}.MFUNC(Subscribe)
      << CI{"UNSUBSCRIBE", CO::NOSCRIPT | CO::LOADING, -1, 0, 0, acl::kUnsubscribe}.MFUNC(
             Unsubscribe)
      << CI{"SUNSUBSCRIBE", CO::NOSCRIPT | CO::LOADING, -1, 0, 0, acl::kUnsubscribe}.MFUNC(
             Unsubscribe)
      << CI{"PSUBSCRIBE", CO::NOSCRIPT | CO::LOADING, -2, 0, 0, acl::kPSubscribe}.MFUNC(PSubscribe)
      << CI{"PUNSUBSCRIBE", CO::NOSCRIPT | CO::LOADING, -1, 0, 0, acl::kPUnsubsribe}.MFUNC(
             PUnsubscribe)
      << CI{"FUNCTION", CO::NOSCRIPT, 2, 0, 0, acl::kFunction}.MFUNC(Function)
      << CI{"MONITOR", CO::ADMIN, 1, 0, 0, acl::kMonitor}.MFUNC(Monitor)
      << CI{"PUBSUB", CO::LOADING | CO::FAST, -1, 0, 0, acl::kPubSub}.MFUNC(Pubsub)
      << CI{"COMMAND", CO::LOADING | CO::NOSCRIPT, -1, 0, 0, acl::kCommand}.MFUNC(Command);
}

void Service::RegisterCommands() {
  Register(&registry_);
  server_family_.Register(&registry_);
  GenericFamily::Register(&registry_);
  ListFamily::Register(&registry_);
  StringFamily::Register(&registry_);

#ifdef WITH_COLLECTION_CMDS
  SetFamily::Register(&registry_);
  HSetFamily::Register(&registry_);
  ZSetFamily::Register(&registry_);
  StreamFamily::Register(&registry_);
#endif

#ifdef WITH_EXTENSION_CMDS
  GeoFamily::Register(&registry_);
  BitOpsFamily::Register(&registry_);
  HllFamily::Register(&registry_);
  BloomFamily::Register(&registry_);
  JsonFamily::Register(&registry_);
#endif

#ifdef WITH_SEARCH
  SearchFamily::Register(&registry_);
#endif

  cluster_family_.Register(&registry_);

  // AclFamily should always be registered last
  // If we add a new familly, register that first above and *not* below
  acl_family_.Register(&registry_);

  // Only after all the commands are registered
  registry_.Init(pp_.size());

  using CI = CommandId;
  if (VLOG_IS_ON(2)) {
    LOG(INFO) << "Multi-key commands are: ";
    registry_.Traverse([](std::string_view key, const CI& cid) {
      if (cid.is_multi_key()) {
        string key_len;
        if (cid.last_key_pos() < 0)
          key_len = "unlimited";
        else
          key_len = StrCat(cid.last_key_pos() - cid.first_key_pos() + 1);
        LOG(INFO) << "    " << key << ": with " << key_len << " keys";
      }
    });

    LOG(INFO) << "Non-transactional commands are: ";
    registry_.Traverse([](std::string_view name, const CI& cid) {
      if (cid.IsTransactional()) {
        LOG(INFO) << "    " << name;
      }
    });
  }
}

const acl::AclFamily* Service::TestInit() {
  acl_family_.Init(nullptr, &user_registry_);
  return &acl_family_;
}

}  // namespace dfly
