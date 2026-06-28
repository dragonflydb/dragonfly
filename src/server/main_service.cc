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
#include "facade/cmd_arg_parser.h"
#include "facade/dragonfly_connection.h"
#include "facade/dragonfly_listener.h"
#include "facade/error.h"
#include "facade/reply_builder.h"
#include "facade/reply_capture.h"
#include "server/acl/acl_commands_def.h"
#include "server/acl/acl_family.h"
#include "server/acl/user_registry.h"
#include "server/acl/validator.h"
#include "server/channel_store.h"
#include "server/cluster/cluster_family.h"
#include "server/command_families.h"
#include "server/dflycmd.h"
#include "server/error.h"
#include "server/generic_family.h"
#include "server/hset_family.h"
#include "server/http_api.h"
#include "server/multi_command_squasher.h"
#include "server/namespaces.h"
#include "server/script_mgr.h"
#include "server/search/search_family.h"
#include "server/server_state.h"
#include "server/set_family.h"
#include "server/sharding.h"
#include "server/stream_family.h"
#include "server/tiered_storage.h"
#include "server/transaction.h"
#include "server/version.h"
#include "server/zset_family.h"
#include "strings/human_readable.h"
#include "util/html/sorted_table.h"
#include "util/varz.h"

using namespace std;
namespace rng = std::ranges;
using facade::ErrorReply;

ABSL_FLAG(int32_t, port, 6379,
          "Redis port. 0 disables the port, -1 will bind on a random available port.");

ABSL_FLAG(uint16_t, announce_port, 0,
          "Port that Dragonfly announces to cluster clients and replication master");

ABSL_FLAG(uint32_t, memcached_port, 0, "Memcached port");

ABSL_FLAG(uint32_t, num_shards, 0, "Number of database shards, 0 - to choose automatically");

ABSL_FLAG(bool, multi_exec_squash, true,
          "Whether multi exec will squash single shard commands to optimize performance");

ABSL_FLAG(bool, lua_resp2_legacy_float, false,
          "Return rounded down integers instead of floats for lua scripts with RESP2");
ABSL_FLAG(uint32_t, multi_eval_squash_buffer, 4096, "Max buffer for squashed commands per script");

ABSL_DECLARE_FLAG(bool, primary_port_http_enabled);
ABSL_FLAG(size_t, listpack_max_field_len, 64,
          "Maximum length of a hash field or value to be stored in listpack encoding");
ABSL_FLAG(size_t, listpack_max_bytes, 1024,
          "Maximum total bytes of a hash in listpack encoding before converting to a hash table");
ABSL_FLAG(bool, admin_nopass, false,
          "If set, would enable open admin access to console on the assigned port, without "
          "authorization needed.");

ABSL_FLAG(bool, expose_http_api, false,
          "If set, will expose a POST /api handler for sending redis commands as json array.");

ABSL_FLAG(strings::MemoryBytesFlag, maxmemory, strings::MemoryBytesFlag{},
          "Limit on maximum-memory that is used by the database, until data starts to be evicted "
          "(according to eviction policy). With tiering, this value defines only the size in RAM, "
          "and not the whole dataset (RAM + SSD). "
          "Must be *at least* 256MiB per proactor thread. "
          "Can be any human‑readable bytes values (supports K/M/G/T/P/E with optional B, "
          "case‑insensitive, both 'GiB' & 'GB' possible). Examples: 300000000, 512MB, 2G, 1.25GiB. "
          "0 - value will be automatically defined based on the env (ex: machine's capacity). "
          "default: 0");

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

ABSL_FLAG(uint32_t, scheduler_background_budget, 50'000, "Background fiber budget in nanoseconds");
ABSL_FLAG(uint32_t, scheduler_background_sleep_prob, 50,
          "Sleep probability of background fibers on reaching budget");
ABSL_FLAG(uint32_t, scheduler_background_warrant, 5,
          "Percentage of guaranteed cpu time for background fibers");

ABSL_RETIRED_FLAG(uint32_t, squash_stats_latency_lower_limit, 0,
                  "Deprecated. Squash latency stats are now tracked unconditionally; "
                  "use the pipeline_latency_seconds histogram for percentiles instead.");

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
#ifdef USE_ABSL_LOG
      absl::SetStderrThreshold(absl::LogSeverityAtLeast::kInfo);
#else
      absl::SetFlag(&FLAGS_alsologtostderr, true);
#endif
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

namespace rng = std::ranges;
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
      ns->GetDbSlice(shard->shard_id())
          .UnregisterConnectionWatches(exec_info->watched_keys, &exec_info->watched_dirty);
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
                               const facade::ParsedArgs& tail_args) {
  std::string message = absl::StrCat(CreateMonitorTimestamp(), " [", cntx->conn_state.db_index);

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
    absl::StrAppend(&message, " ", CmdEntryToMonitorFormat(arg));

  return message;
}

void DispatchMonitor(ConnectionContext* cntx, const CommandId* cid,
                     const facade::ParsedArgs& tail_args) {
  auto cb = [msg = MakeMonitorMessage(cntx, cid, tail_args)](unsigned idx, util::ProactorBase*) {
    const auto& monitors = ServerState::tlocal()->Monitors().monitors();
    if (monitors.empty())
      return;

    VLOG(2) << "Sending command '" << msg << "' from " << ProactorBase::me()->GetPoolIndex()
            << " to " << monitors.size() << " monitors";
    for (auto monitor_conn : monitors)
      monitor_conn->SendMonitorMessageAsync(msg);
  };
  shard_set->pool()->DispatchBrief(std::move(cb));
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
    if (str.find_first_of("\r\n") == string_view::npos) {
      rb_->SendSimpleString(str);
      return;
    }
    rb_->SendSimpleString(StripCRLF(str));
  }

  void OnError(string_view str) {
    std::string buf;
    if (str.find_first_of("\r\n") != string_view::npos) {
      buf = StripCRLF(str);
      str = buf;
    }
    if (!str.empty() && str.front() == '-') {
      rb_->SendError(str);
    } else {
      rb_->SendError(absl::StrCat("-", str));
    }
  }

 private:
  static std::string StripCRLF(string_view str) {
    std::string out;
    out.reserve(str.size());
    for (char c : str)
      if (c != '\r' && c != '\n')
        out += c;
    return out;
  }

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
  return rng::all_of(str, [](unsigned char c) { return absl::ascii_isxdigit(c); });
}

optional<ErrorReply> EvalValidator(const ParsedArgs& args) {
  facade::CmdArgParser parser{args};
  parser.Skip(1);  // script body / sha
  uint32_t num_keys = parser.Next<uint32_t>();

  if (auto err = parser.TakeError(); err)
    return err.MakeReply();

  if (!parser.HasAtLeast(num_keys))
    return ErrorReply{"Number of keys can't be greater than number of args", kSyntaxErrType};

  return nullopt;
}

enum class ExecScriptUse : uint8_t {
  NONE = 0,
  SCRIPT_LOAD = 1,
  SCRIPT_RUN = 2,
};

ExecScriptUse DetermineScriptPresense(const std::vector<StoredCmd>& body) {
  bool script_load = false;
  for (const auto& scmd : body) {
    if (scmd.Cid()->IsEvalGroup()) {
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
        auto args = scmd.Slice(&arg_vec);
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

string FailedCommandToString(std::string_view command, const facade::ParsedArgs& args,
                             std::string_view reason) {
  constexpr size_t kMaxArgCount = 31;
  constexpr size_t kMaxArgLength = 128;
  constexpr size_t kMaxReasonLength = 256;

  string result;
  absl::StrAppend(&result, " ", command);

  auto AppendArg = [&](string_view arg) {
    if (arg.size() > kMaxArgLength) {
      absl::StrAppend(&result, " ", absl::CHexEscape(arg.substr(0, kMaxArgLength)), "... (",
                      arg.size() - kMaxArgLength, " more bytes)");
    } else {
      absl::StrAppend(&result, " ", absl::CHexEscape(arg));
    }
  };

  const bool is_eval = absl::StartsWith(command, "EVAL");

  if (command == "AUTH" || absl::StartsWith(command, "ACL")) {
    // skip all args to protect passwords
  } else if (is_eval) {
    // log only script/SHA and numkeys, skip KEYS and ARGV to avoid PII leaks
    for (size_t i = 0; i < std::min(args.size(), size_t{2}); ++i) {
      AppendArg(args[i]);
    }
  } else {
    size_t num_args = std::min(args.size(), kMaxArgCount);
    for (size_t i = 0; i < num_args; ++i) {
      AppendArg(args[i]);
    }
    if (args.size() > kMaxArgCount) {
      absl::StrAppend(&result, " ... (", args.size() - kMaxArgCount, " more arguments)");
    }
  }

  // Lua error messages for EVAL can contain user-supplied data (PII).
  // "Error running script (call to <sha>): <lua_error>" — strip everything after "): ".
  string_view safe_reason = reason.substr(0, kMaxReasonLength);
  if (is_eval && absl::StartsWith(reason, "Error running script (call to ")) {
    auto sep = reason.find("): ");
    if (sep != string_view::npos)
      safe_reason = reason.substr(0, sep + 1);
  }
  absl::StrAppend(&result, " failed with reason: ", safe_reason);

  return result;
}

void UpdateFromFlagsOnThread() {
  if (uint32_t poll = GetFlag(FLAGS_shard_thread_busy_polling_usec);
      poll > 0 && EngineShard::tlocal())
    ProactorBase::me()->SetBusyPollUsec(poll);
}

std::vector<std::string> GetMutableFlagNames() {
  return base::GetFlagNames(FLAGS_shard_thread_busy_polling_usec);
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

OpResult<void> OpTrackKeys(const OpArgs slice_args, const facade::Connection::WeakRef& conn_ref,
                           const ShardArgs& args) {
  if (conn_ref.IsExpired()) {
    DVLOG(2) << "Connection expired, exiting TrackKey function.";
    return OpStatus::OK;
  }

  DVLOG(2) << "Start tracking keys for client ID: " << conn_ref.GetClientId();

  // TODO: There is a bug here that we track all arguments instead of tracking only keys.
  auto& db_slice = slice_args.GetDbSlice();
  for (auto key : args)
    db_slice.TrackKey(conn_ref, key);

  return OpStatus::OK;
}

void TrackIfNeeded(CommandContext* cmd_cntx) {
  auto* cntx = cmd_cntx->server_conn_cntx();
  auto& info = cntx->conn_state.tracking_info_;

  if (!info.IsTrackingOn()) {
    return;
  }

  if (auto* tx = cmd_cntx->tx(); tx) {
    // Reset it, because in multi/exec the transaction pointer is the same and
    // we will end up triggerring the callback on the following commands. To avoid this
    // we reset it.
    tx->SetTrackingCallback({});
    if (cmd_cntx->cid()->IsReadOnly() && info.ShouldTrackKeys()) {
      auto conn = cntx->conn()->Borrow();
      tx->SetTrackingCallback([conn](Transaction* trans) {
        auto* shard = EngineShard::tlocal();
        OpTrackKeys(trans->GetOpArgs(shard), conn, trans->GetShardArgs(shard->shard_id()));
      });
    }
  }
}

// Check CLIENT PAUSE state and block if needed
void CheckPauseState(facade::Connection* conn, ConnectionContext* dfly_cntx, const CommandId* cid) {
  auto& etl = *ServerState::tlocal();
  if (etl.IsPaused() && !conn->IsPrivileged()) {
    bool is_write = cid->IsJournaled();
    // PUBLISH and writable EVAL/EVALSHA (not the *_RO variants) count as writes here.
    is_write |= cid->IsPublish() || (cid->IsEvalGroup() && !cid->IsReadOnly());
    is_write |= cid->IsExec() && dfly_cntx->conn_state.exec_info.is_write;

    dfly_cntx->paused = true;
    etl.AwaitPauseState(is_write);
    dfly_cntx->paused = false;
  }
}

// Prepare transaction for DispatchCommand.
//
// Return value:
//   first  - newly created top-level transaction (or nullptr if none).
//   second - result: overall status of preparation.
pair<intrusive_ptr<Transaction>, OpStatus> PrepareTransaction(const CommandId* cid,
                                                              const facade::ParsedArgs& tail_args,
                                                              CommandContext* cmd_ctx) {
  auto* dfly_cntx = cmd_ctx->server_conn_cntx();
  bool init = false;
  intrusive_ptr<Transaction> res;
  if (dfly_cntx->transaction) {  // Existing transaction context (e.g., MULTI/EXEC or script)
    DCHECK(dfly_cntx->transaction->IsMulti());  // dispatching in multi
    if (cid->IsTransactional()) {
      dfly_cntx->transaction->MultiSwitchCmd(cid);
      init = true;
    }
  } else {
    if (cid->IsTransactional()) {
      res.reset(new Transaction{cid});
      init = !res->IsMulti();  // Multi command initialize themselves based on their mode
    }
    dfly_cntx->transaction = res.get();
  }

  cmd_ctx->SetupTx(cid, dfly_cntx->transaction);

  if (init) {
    DCHECK(cmd_ctx->tx());
    if (auto st =
            cmd_ctx->tx()->InitByArgs(dfly_cntx->ns, dfly_cntx->conn_state.db_index, tail_args);
        st != OpStatus::OK) {
      if (res) {
        dfly_cntx->transaction = nullptr;
      }
      return {nullptr, st};
    }

    if (res)  // new transaction
      dfly_cntx->last_cmd_stats.shards_count = cmd_ctx->tx()->GetUniqueShardCnt();
  }

  return {std::move(res), OpStatus::OK};
}

void StoreInMultiBlock(ConnectionContext* dfly_cntx, const CommandId* cid,
                       facade::ParsedCommand* parsed_cmd, uint8_t tail_index) {
  // TODO: protect against aggregating huge transactions.
  auto& exec_info = dfly_cntx->conn_state.exec_info;
  const size_t old_size = exec_info.GetStoredCmdBytes();

  // Moves arguments from parsed_cmd to body.
  exec_info.body.emplace_back(cid, parsed_cmd, tail_index);
  exec_info.stored_cmd_bytes += exec_info.body.back().UsedMemory();
  exec_info.is_write |= cid->IsJournaled();
  ServerState::tlocal()->stats.stored_cmd_bytes += exec_info.GetStoredCmdBytes() - old_size;
}

bool ShouldLogError(const CommandId& cid, string_view reason, const facade::ParsedArgs& tail_args) {
  if (absl::StartsWith(reason, "-BUSYGROUP"))
    return false;

  if (cid.name() != "CLIENT")
    return true;
  return tail_args.empty() || !absl::EqualsIgnoreCase(tail_args.Front(), "maint_notifications");
}

string_view McTypeToCmdName(MemcacheParser::CmdType type) {
  using MP = MemcacheParser;
  switch (type) {
    case MP::SET:
    case MP::ADD:
    case MP::REPLACE:
      return "SET";
    case MP::DELETE:
      return "DEL";
    case MP::INCR:
      return "INCR";
    case MP::DECR:
      return "DECR";
    case MP::APPEND:
      return "APPEND";
    case MP::PREPEND:
      return "PREPEND";
    case MP::GET:
    case MP::GETS:
      return "MGET";
    case MP::GAT:
    case MP::GATS:
      return "GAT";
    case MP::FLUSHALL:
      return "FLUSHDB";
    case MP::QUIT:
      return "QUIT";
    default:
      return {};
  }
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
  server.max_map_field_len = absl::GetFlag(FLAGS_listpack_max_field_len);
  server.max_listpack_map_bytes = absl::GetFlag(FLAGS_listpack_max_bytes);
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

  // Register scheduler flags
  RegisterMutableFlags(
      &config_registry,
      base::GetFlagNames(FLAGS_scheduler_background_budget, FLAGS_scheduler_background_sleep_prob,
                         FLAGS_scheduler_background_warrant),
      []() { UpdateSchedulerFlagsOnThread(); });

  config_registry.RegisterSetter<strings::MemoryBytesFlag>(
      "maxmemory", [](const strings::MemoryBytesFlag& flag) {
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

  // Create Global ChannelStore
  channel_store = new ChannelStore{};

  // Must initialize before the shard_set because EngineShard::Init references ServerState.
  pp_.AwaitBrief([&](uint32_t index, ProactorBase* pb) {
    tl_facade_stats = new FacadeStats;
    ServerState::Init(index, shard_num, main_listener, &user_registry_);
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

  shard_set->pool()->AwaitBrief([](unsigned, auto*) {
    facade::Connection::UpdateFromFlags();
    UpdateFromFlagsOnThread();
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

  shard_set->PreShutdown();
  shard_set->Shutdown();

  delete channel_store;
  channel_store = nullptr;

  pp_.AwaitFiberOnAll([](ProactorBase* pb) {
#if defined(DFLY_USE_SSL)
    // Explicitly release OpenSSL thread-local state here.
    // This prevents a potential crash during thread exit where the allocator (e.g. mimalloc)
    // might tear down the thread's heap before OpenSSL tries to free its internal state.
    OPENSSL_thread_stop();
#endif
    ServerState::tlocal()->Destroy();
  });

  // wait for all the pending callbacks to stop.
  ThisFiber::SleepFor(10ms);
  facade::Connection::Shutdown();

  shutdown_watchdog->Disarm();
}

OpResult<KeyIndex> Service::FindKeys(const CommandId* cid, const facade::ParsedArgs& args) {
  // Sharded pub-sub acts as if it's sharded by its channel name (just for checks)
  if (cid->IsShardedPubSub()) {
    // SPUBLISH has only one key, the rest is data
    if (cid->IsSPublish())
      return KeyIndex(0, 1);
    return {KeyIndex(0, args.size())};  // sub/unsub list of channels
  }

  return DetermineKeys(cid, args);
}

optional<ErrorReply> Service::CheckKeysOwnership(const CommandId& cid,
                                                 const facade::ParsedArgs& args,
                                                 const ConnectionContext& dfly_cntx) {
  if (dfly_cntx.is_replicating) {
    // Always allow commands on the replication port, as it might be for future-owned keys.
    return nullopt;
  }

  if (cid.first_key_pos() == 0 && !cid.IsShardedPubSub()) {
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
std::optional<facade::ErrorReply> Service::TakenOverSlotError(const CommandId& cid,
                                                              const facade::ParsedArgs& args,
                                                              const ConnectionContext& dfly_cntx) {
  if (cid.first_key_pos() == 0 && !cid.IsShardedPubSub()) {
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
                                       const CommandId* cid, const facade::ParsedArgs& args,
                                       Transaction::MultiMode multi_mode) {
  // We either scheduled on all shards or re-schedule for each operation,
  // so we are not restricted to any keys.
  if (multi_mode == Transaction::GLOBAL || multi_mode == Transaction::NON_ATOMIC)
    return nullopt;

  OpResult<KeyIndex> key_index_res = DetermineKeys(cid, args);
  if (!key_index_res)
    return ErrorReply{key_index_res.status()};

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
                                                      string_view error_msg,
                                                      const facade::ParsedArgs& tail_args) {
  if (!acl::IsUserAllowedToInvokeCommand(*cntx, *cid, tail_args)) {
    return ErrorReply(absl::StrCat("-NOPERM ", cntx->authed_username, " ", error_msg));
  }
  return nullopt;
}

std::optional<ErrorReply> Service::VerifyCommandState(const CommandId& cid,
                                                      const facade::ParsedArgs& tail_args,
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
      allowed_by_state = dfly_cntx.is_replicating || (cid.opt_mask() & CO::LOADING);
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
    if (cmd_name != "AUTH" && !cid.IsQuit() && cmd_name != "HELLO") {
      return ErrorReply{"-NOAUTH Authentication required.", facade::kNoAuthErrType};
    }
  }

  // only reset and quit are allow if this connection is used for monitoring.
  // In Valkey monitor connections are marked as replica connections, so they get this unrelated
  // error message.
  if (dfly_cntx.monitor && (cmd_name != "RESET" && !cid.IsQuit()))
    return ErrorReply{"Replica can't interact with the keyspace"};

  bool is_write_cmd = cid.IsJournaled();
  bool is_trans_cmd = cid.IsExecGroup();
  bool under_script = dfly_cntx.conn_state.script_info != nullptr;
  bool multi_active = dfly_cntx.conn_state.exec_info.IsCollecting() && !is_trans_cmd;

  if (!etl.is_master && is_write_cmd && !dfly_cntx.is_replicating)
    return ErrorReply{"-READONLY You can't write against a read only replica."};

  if (multi_active) {
    if (cmd_name == "WATCH" || cmd_name == "FLUSHALL" || cmd_name == "FLUSHDB" ||
        cid.IsSubscribeFamily())
      return ErrorReply{absl::StrCat("'", cmd_name, "' not allowed inside a transaction")};
  }

  if (IsClusterEnabled()) {
    if (auto err = CheckKeysOwnership(cid, tail_args, dfly_cntx); err)
      return err;
  }

  if (under_script && (cid.opt_mask() & CO::NOSCRIPT))
    return ErrorReply{"This Redis command is not allowed from script"};

  if (under_script) {
    auto* tx = dfly_cntx.transaction;
    DCHECK(tx);
    // The following commands access shards arbitrarily without having keys, so they can only be run
    // non atomically or globally.
    Transaction::MultiMode mode = tx->GetMultiMode();
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

DispatchResult Service::DispatchCommand(facade::ParsedArgs args, facade::ParsedCommand* parsed_cmd,
                                        facade::AsyncPreference async_pref) {
  DCHECK_NE(0u, shard_set->size()) << "Init was not called";

  const CommandId* cid = nullptr;
  ParsedArgs args_no_cmd;

  if (parsed_cmd->mc_command()) {
    auto mc_res = HandleMemcacheCommand(parsed_cmd, async_pref);
    if (std::holds_alternative<facade::DispatchResult>(mc_res)) {
      return std::get<facade::DispatchResult>(mc_res);
    }
    cid = std::get<CommandId*>(mc_res);
    args_no_cmd = args;  // MC args have no command name prefix
  } else {
    DCHECK(!args.empty());
    std::tie(cid, args_no_cmd) = registry_.FindExtended(args);
  }

  if (cid == nullptr) {
    if (async_pref != AsyncPreference::ONLY_SYNC) {
      parsed_cmd->SetDeferredReply();
    }
    if (parsed_cmd->mc_command()) {
      parsed_cmd->SendSimpleString("CLIENT_ERROR bad command line format");
    } else {
      parsed_cmd->SendError(ReportUnknownCmd(absl::AsciiStrToUpper(args.Front())));
    }
    return DispatchResult::ERROR;
  }

  // Determine if command should run async
  switch (async_pref) {
    case AsyncPreference::ONLY_SYNC:
      break;
    case AsyncPreference::ONLY_ASYNC:
      if (!cid->SupportsAsync())
        return DispatchResult::WOULD_BLOCK;
      [[fallthrough]];
    case AsyncPreference::PREFER_ASYNC:
      if (cid->SupportsAsync())
        parsed_cmd->SetDeferredReply();
      break;
  };

  CommandContext* cmd_cntx = static_cast<CommandContext*>(parsed_cmd);
  ConnectionContext* dfly_cntx = cmd_cntx->server_conn_cntx();

  if (dfly_cntx->async_dispatch && cid->IsBlocking()) {
    ++ServerState::tlocal()->stats.blocking_commands_in_pipelines;
    cmd_cntx->conn()->FlushReplies();
  }

  cmd_cntx->SetTailArgs(args_no_cmd);

  ArgSlice tail_args;
  if (cmd_cntx->IsDeferredReply()) {
    args_no_cmd.ToVec(&cmd_cntx->arg_slice_backing);  // Ensure lifetime
    tail_args = cmd_cntx->arg_slice_backing;
  } else {
    tail_args = args_no_cmd.ToSlice(&cmd_cntx->arg_slice_backing);
  }

  // Block on CLIENT PAUSE if needed
  if (auto* conn = cmd_cntx->conn(); conn /* replica context doesn't have an owner */) {
    if (VLOG_IS_ON(2)) {
      bool under_script = bool(dfly_cntx->conn_state.script_info);
      LOG(INFO) << "Got (" << conn->GetClientId() << "): " << (under_script ? "LUA " : "")
                << cid->name() << " " << args_no_cmd
                << " in dbid=" << dfly_cntx->conn_state.db_index;
    }

    // Check pause state only if it is a top level transaction.
    if (dfly_cntx->transaction == nullptr)
      CheckPauseState(conn, dfly_cntx, cid);
  }

  // Verify command state
  if (auto err = VerifyCommandState(*cid, args_no_cmd, *dfly_cntx); err) {
    LOG_IF(WARNING, dfly_cntx->replica_conn || !dfly_cntx->conn() /* no owner in replica context */)
        << "VerifyCommandState error: " << err->ToSv();
    if (auto& exec_info = dfly_cntx->conn_state.exec_info; exec_info.IsCollecting())
      exec_info.state = ConnectionState::ExecInfo::EXEC_ERROR;

    // We need to skip this because ACK's should not be replied to
    // Bonus points because this allows to continue replication with ACL users who got
    // their access revoked and reinstated

    if (cid->IsReplConf()) {
      DCHECK_GE(args_no_cmd.size(), 1u);
      // We should not reply to REPLCONF ACKS.
      if (absl::EqualsIgnoreCase(args_no_cmd.Front(), "ACK")) {
        server_family_.GetDflyCmd()->OnClose(
            dfly_cntx->conn_state.replication_info.repl_session_id);
        return DispatchResult::ERROR;
      }
    }
    DCHECK(!err->status);
    cmd_cntx->SendError(*err);
    return DispatchResult::ERROR;
  }

  VLOG_IF(1, cid->opt_mask() & CO::CommandOpt::DANGEROUS)
      << "Executing dangerous command " << cid->name() << " "
      << ConnectionLogContext(dfly_cntx->conn());

  // If inside MULTI block, store command
  bool is_trans_cmd = cid->IsExecGroup();
  if (dfly_cntx->conn_state.exec_info.IsCollecting() && !is_trans_cmd) {
    uint8_t tail_index = args.size() - args_no_cmd.size();
    StoreInMultiBlock(dfly_cntx, cid, parsed_cmd, tail_index);
    cmd_cntx->SendSimpleString("QUEUED");
    return DispatchResult::OK;
  }

  auto [dispatched_tx, status] = PrepareTransaction(cid, args_no_cmd, cmd_cntx);
  if (status != OpStatus::OK) {
    DCHECK(!dispatched_tx);
    cmd_cntx->SendError(StatusToMsg(status));
    return DispatchResult::ERROR;
  }

  DispatchResult res = InvokeCmd(tail_args, cmd_cntx);
  if (dispatched_tx) {
    DCHECK(dfly_cntx->transaction == dispatched_tx.get());
    // A new top-level transaction is created here only for top-level commands. Nested commands
    // (EXEC body, script CALLs, squashed sub-commands) reuse an existing transaction or run on a
    // different dispatch path, so recording last-command stats here naturally excludes them and
    // avoids racing on the shared ConnectionContext from squashing shard threads.
    dfly_cntx->last_cmd_stats.clock = dispatched_tx->txid();
    dfly_cntx->transaction = nullptr;
  }

  if ((res != DispatchResult::OK) && (res != DispatchResult::OOM)) {
    cmd_cntx->SendError("Internal Error");
    dfly_cntx->conn()->MarkForClose();
  }

  return res;
}

std::variant<CommandId*, facade::DispatchResult> Service::HandleMemcacheCommand(
    ParsedCommand* parsed_cmd, AsyncPreference async_pref) {
  auto* mc = parsed_cmd->mc_command();
  DCHECK(mc != nullptr);

  if (mc->type == MemcacheParser::STATS || mc->type == MemcacheParser::VERSION) {
    if (async_pref == AsyncPreference::ONLY_ASYNC)
      return {facade::DispatchResult::WOULD_BLOCK};

    auto* cmd_ctx = static_cast<CommandContext*>(parsed_cmd);
    if (mc->type == MemcacheParser::STATS) {
      server_family_.StatsMC(mc->key(), cmd_ctx);
    } else {
      cmd_ctx->SendSimpleString("VERSION 1.6.0 DF");
    }
    return {facade::DispatchResult::OK};  // replied to the client
  }

  string_view cmd_name = McTypeToCmdName(mc->type);
  if (cmd_name.empty())
    return nullptr;

  return registry_.Find(cmd_name);
}

class ReplyGuard {
 public:
  explicit ReplyGuard(const CommandContext& cmd_cntx) {
    const bool is_script = bool(cmd_cntx.server_conn_cntx()->conn_state.script_info);
    cid_name_ = cmd_cntx.cid()->name();
    const bool is_one_of = (cmd_cntx.cid()->IsReplConf() || cid_name_ == "DFLY");
    bool is_mcache = cmd_cntx.mc_command() != nullptr;
    const bool is_no_reply_memcache =
        (is_mcache && cmd_cntx.mc_command()->cmd_flags.no_reply) || cmd_cntx.cid()->IsQuit();
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

DispatchResult Service::InvokeCmd(CmdArgList tail_args, CommandContext* cmd_cntx) {
  auto* cid = cmd_cntx->cid();
  DCHECK(cid);
  DCHECK(!cid->Validate(tail_args));

  cmd_cntx->start_time_usec = base::CycleClock::ToUsec(base::CycleClock::Now());

  ConnectionContext* cntx = cmd_cntx->server_conn_cntx();
  auto* builder = cmd_cntx->rb();
  DCHECK(builder);
  DCHECK(cntx);

  ServerState& ss = *ServerState::tlocal();

  if ((cid->opt_mask() & CO::DENYOOM) && ss.ShouldDenyOnOOM(cmd_cntx->start_time_usec)) {
    cmd_cntx->SendError(ErrorReply{OpStatus::OUT_OF_MEMORY});
    return DispatchResult::OOM;
  }

  bool has_monitors = !ss.Monitors().Empty();
  if (has_monitors && cid->CanBeMonitored()) {
    DispatchMonitor(cntx, cid, tail_args);
  }

  ss.RecordCmd(cntx->has_main_or_memcache_listener);
  TrackIfNeeded(cmd_cntx);
  auto* tx = cmd_cntx->tx();

#ifndef NDEBUG
  // Verifies that we reply to the client when needed.
  ReplyGuard reply_guard(*cmd_cntx);
#endif
  builder->ConsumeLastError();  // throw away last error
  DispatchResult res = DispatchResult::OK;
  try {
    cid->Invoke(tail_args, cmd_cntx);
  } catch (std::exception& e) {
    LOG(ERROR) << "Internal error, system probably unstable " << e.what();
    res = DispatchResult::ERROR;
  }

  if (res == DispatchResult::OK) {
    if (std::string reason = builder->ConsumeLastError(); !reason.empty()) {
      // Set flag if OOM reported
      if (reason == kOutOfMemory) {
        res = DispatchResult::OOM;
      }
      VLOG(2) << FailedCommandToString(cid->name(), tail_args, reason);
      if (ShouldLogError(*cid, reason, tail_args)) {
        LOG_EVERY_T(WARNING, 1) << FailedCommandToString(cid->name(), tail_args, reason);
      }
    }

    if (cntx->conn_state.tracking_info_.IsTrackingOn()) {
      if ((!tx && !cid->IsMulti()) || (tx && !tx->IsMulti())) {
        // Each time we execute a command we need to increase the sequence number in
        // order to properly track clients when OPTIN is used.
        // We don't do this for `multi/exec` because it would break the
        // semantics, i.e, CACHING should stick for all commands following
        // the CLIENT CACHING ON within a multi/exec block
        cntx->conn_state.tracking_info_.IncrementSequenceNumber();
      }
    }

    cmd_cntx->RecordLatency(tail_args);
  }

  // For EVAL[] and EXEC/DISCARD, clean up state.
  // We don't do it directly in commands to allow some introspection after execution (slowlog).
  if (cid->IsExecGroup() && !cid->IsMulti())
    MultiCleanup(cntx);
  else if (cid->IsEvalGroup())
    cntx->conn_state.script_info.reset();

  return res;
}

uint32_t Service::DispatchSquashedBatch(facade::ParsedCommand* first, unsigned count,
                                        facade::ConnectionContext* cntx) {
  auto* dfly_cntx = static_cast<ConnectionContext*>(cntx);
  DCHECK(!dfly_cntx->conn_state.exec_info.IsRunning());

  auto* rb = static_cast<RedisReplyBuilder*>(first->rb());
  auto* ss = ServerState::tlocal();

  // Don't even start when paused. We can only continue if DispatchTracker is aware of us running.
  if (ss->IsPaused())
    return 0;

  vector<CmdRef> cmd_refs;
  cmd_refs.reserve(count);
  intrusive_ptr<Transaction> dist_trans;
  unsigned dispatched = 0;
  MultiCommandSquasher::Stats stats;

  auto perform_squash = [&] {
    if (cmd_refs.empty())
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
    opts.pipeline_mode = true;
    opts.max_squash_size = ss->max_squash_cmd_num;

    auto cmd_gen = [it = cmd_refs.begin(), end = cmd_refs.end()]() mutable -> CmdRef {
      return (it == end) ? CmdRef{} : *it++;
    };
    stats += MultiCommandSquasher::Execute(std::move(cmd_gen), rb, dfly_cntx, this, opts);
    dfly_cntx->transaction = nullptr;

    dispatched += cmd_refs.size();
    cmd_refs.clear();
  };

  auto* cmd = first;
  for (unsigned i = 0; i < count && cmd; i++) {
    auto* cmd_cntx = static_cast<CommandContext*>(cmd);

    ParsedArgs args{*cmd_cntx};
    const auto [cid, tail_args] = registry_.FindExtended(args);

    // Stop the batch at the first command that can't join it;
    // the connection's regular dispatch path then handles exceptions
    // (and the rest of the pipeline) with the real reply builder, after the replies squashed so
    // far are flushed in order. Commands that stop the batch:
    //  - unknown commands (cid == nullptr): dispatched standalone to produce their error reply;
    //  - MULTI/EXEC and the commands queued between them: sequential, stored in ExecInfo;
    //  - EVAL: scripts may require a stricter multi mode than the non-atomic squashing tx;
    //  - blocking commands: prior replies must be flushed before the fiber blocks;
    //  - QUIT: closes the reply builder, dropping any deferred replies not yet sent;
    //  - subscribe/unsubscribe: emit one reply per channel (multiple top-level replies), which the
    //    CapturingReplyBuilder backing deferred replies cannot represent;
    //  - admin commands (e.g. REPLCONF, DFLY): control commands that may produce no top-level
    //    reply (e.g. REPLCONF ACK), which a deferred (captured) reply cannot represent.
    if (cid == nullptr)
      break;

    const bool is_multi = dfly_cntx->conn_state.exec_info.IsCollecting() || cid->IsExecGroup();
    const bool is_eval = cid->IsEvalGroup();
    if (is_multi || is_eval || cid->IsBlocking() || (cid->opt_mask() & CO::ADMIN) ||
        cid->IsQuit() || cid->IsSubscribeFamily())
      break;

    if (auto err = VerifyCommandState(*cid, tail_args, *dfly_cntx); err) {
      CapturingReplyBuilder crb{ReplyMode::FULL, rb->GetRespVersion()};
      crb.SendError(std::move(*err));
      cmd_cntx->Resolve(crb.Take());
      ++dispatched;
      cmd = cmd->next;
      continue;
    }

    cmd_refs.push_back(CmdRef{cid, tail_args, ReplyMode::FULL, cmd_cntx});
    cmd = cmd->next;
  }

  perform_squash();

  if (dist_trans)
    dist_trans->UnlockMulti();

  ss->stats.multi_squash_exec_hop_usec += stats.hop_usec;
  ss->stats.multi_squash_exec_reply_usec += stats.reply_usec;
  ss->stats.multi_squash_hops += stats.hops;
  ss->stats.squashed_commands += stats.squashed_commands;

  return dispatched;
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

  return res;
}

facade::ParsedCommand* Service::AllocateParsedCommand() {
  return new CommandContext{};
}

const CommandId* Service::FindCmd(std::string_view cmd) const {
  return registry_.Find(cmd);
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

void Service::Quit(CmdArgParser, CommandContext* cmd_cntx) {
  if (cmd_cntx->rb()->GetProtocol() == Protocol::REDIS)
    cmd_cntx->rb()->SendOk();

  auto* cntx = cmd_cntx->server_conn_cntx();
  DeactivateMonitoring(cntx);
  cmd_cntx->conn()->MarkForClose();
}

void Service::Multi(CmdArgParser, CommandContext* cmd_cntx) {
  auto& conn_state = cmd_cntx->server_conn_cntx()->conn_state;
  if (conn_state.exec_info.IsCollecting()) {
    return cmd_cntx->SendError("MULTI calls can not be nested");
  }
  conn_state.exec_info.state = ConnectionState::ExecInfo::EXEC_COLLECT;
  // TODO: to protect against huge exec transactions.
  return cmd_cntx->rb()->SendOk();
}

void Service::Watch(CmdArgParser parser, CommandContext* cmd_cntx) {
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
      t->GetDbSlice(shard_id).RegisterWatchedKey(cntx->db_index(), k, &exec_info.watched_dirty);
    }

    auto res = GenericFamily::OpExists(t->GetOpArgs(shard), largs);
    keys_existed.fetch_add(res.value_or(0), memory_order_relaxed);
    return OpStatus::OK;
  };
  cmd_cntx->tx()->ScheduleSingleHop(std::move(cb));

  // Duplicate keys are stored to keep correct count.
  exec_info.watched_existed += keys_existed.load(memory_order_relaxed);
  for (string_view key : parser.UnparsedArgs()) {
    exec_info.watched_keys.emplace_back(cntx->db_index(), key);
  }

  return cmd_cntx->rb()->SendOk();
}

void Service::Unwatch(CmdArgParser, CommandContext* cmd_cntx) {
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

  for (auto& scmd : info->async_cmds) {
    auto ref = scmd.Ref();
    if (auto err = VerifyCommandState(*ref.cid, ref.args, *cntx); err) {
      info->async_cmds_heap_mem = 0;
      info->async_cmds.clear();
      CapturingReplyBuilder crb{ReplyMode::ONLY_ERR};
      crb.SendError(std::move(*err));
      auto reply = crb.Take();
      return make_optional(std::move(reply));
    }
  }

  CapturingReplyBuilder crb{ReplyMode::ONLY_ERR};
  MultiCommandSquasher::Opts opts;
  opts.error_abort = true;
  opts.max_squash_size = ServerState::tlocal()->max_squash_cmd_num;
  auto cmd_gen = [it = info->async_cmds.begin(), end = info->async_cmds.end()]() mutable -> CmdRef {
    return (it == end) ? CmdRef{} : (it++)->Ref();
  };
  MultiCommandSquasher::Execute(std::move(cmd_gen), &crb, cntx, this, opts);

  info->async_cmds_heap_mem = 0;
  info->async_cmds.clear();

  auto reply = crb.Take();
  return CapturingReplyBuilder::TryExtractError(reply) ? make_optional(std::move(reply)) : nullopt;
}

void Service::TryEnqueueEvalAsyncCmd(const Interpreter::CallArgs& ca, CommandContext* cmd_cntx,
                                     facade::RedisReplyBuilder* replier) {
  using CT = Interpreter::CallArgs::Type;
  auto* cntx = cmd_cntx->server_conn_cntx();
  auto& info = cntx->conn_state.script_info;

  bool abort_on_error = (ca.call_type & CT::PCALL) == 0;
  bool async_call = ca.call_type & CT::ACALL;
  bool tx_call = ca.call_type & (CT::LOCK | CT::UNLOCK);

  optional<ErrorReply> early_async_error;

  // Full command verification happens during squashed execution
  if (async_call) {
    ParsedArgs parsed_args{*ca.args};
    if (auto [cid, tail] = registry_.FindExtended(parsed_args); cid != nullptr) {
      auto reply_mode = abort_on_error ? ReplyMode::ONLY_ERR : ReplyMode::NONE;
      CmdArgVec scratch;
      info->async_cmds.emplace_back(cid, tail.ToSlice(&scratch), reply_mode);
      info->async_cmds_heap_mem += info->async_cmds.back().UsedMemory();
    } else if (abort_on_error) {  // If we don't abort on errors, we can ignore it completely
      early_async_error = ReportUnknownCmd(ca.args->at(0));
    }
  }

  bool need_flush = !async_call || early_async_error.has_value() || tx_call;
  if (auto err = FlushEvalAsyncCmds(cntx, need_flush); err) {
    CapturingReplyBuilder::Apply(std::move(*err), replier);  // forward error to lua
    *ca.requested_abort = true;
    return;
  }

  if (early_async_error.has_value()) {
    auto* prev = cmd_cntx->SwapReplier(replier);
    cmd_cntx->SendError(*early_async_error);
    *ca.requested_abort |= abort_on_error;
    cmd_cntx->SwapReplier(prev);
  }
}

void Service::CallFromScript(Interpreter::CallArgs& ca, CommandContext* cmd_cntx) {
  using CT = Interpreter::CallArgs::Type;  // TODO: use c++20 using enum
  auto* tx = cmd_cntx->tx();
  DCHECK(tx);

  auto* cntx = cmd_cntx->server_conn_cntx();
  auto& info = cntx->conn_state.script_info;
  info->stats.num_commands++;

  InterpreterReplier replier(ca.translator);
  TryEnqueueEvalAsyncCmd(ca, cmd_cntx, &replier);

  // Handle unlock/lock or default call
  switch (ca.call_type) {
    case CT::UNLOCK:
      tx->UnlockMulti(true);
      tx->StartMultiNonAtomic();
      info->lock_tags.clear();
      info->key_backing.clear();
      return;
    case CT::LOCK:
      if (tx->GetMultiMode() != Transaction::NON_ATOMIC)
        return;

      info->key_backing.resize(ca.args->size());
      for (size_t i = 0; i < ca.args->size(); i++) {
        info->key_backing[i] = ca.args->at(i);  // copy key
        info->lock_tags.insert(LockTag(info->key_backing[i]));
      }

      {
        CmdArgVec keys(info->key_backing.begin(), info->key_backing.end());
        tx->MultiSwitchCmd(registry_.Find("EVAL"));
        tx->StartMultiLockedAhead(cntx->ns, cntx->db_index(), keys, false);
      }
      return;
    case CT::ACALL:
    case CT::APCALL:  // was handled above
      return;
    default: {
      // Save EVAL's state that DispatchCommand overwrites.
      auto saved_tail = cmd_cntx->tail_args();
      CmdArgVec saved_backing;
      saved_backing.swap(cmd_cntx->arg_slice_backing);

      auto* prev = cmd_cntx->SwapReplier(&replier);
      DispatchCommand(ParsedArgs{*ca.args}, cmd_cntx, AsyncPreference::ONLY_SYNC);
      cmd_cntx->SwapReplier(prev);

      saved_backing.swap(cmd_cntx->arg_slice_backing);
      cmd_cntx->SetTailArgs(saved_tail);
    }
  }
}

void Service::Eval(CmdArgList args, CommandContext* cmd_cntx, bool read_only) {
  string_view body = ArgS(args, 0);

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  if (body.empty()) {
    return rb->SendNull();
  }

  auto* cntx = cmd_cntx->server_conn_cntx();
  BorrowedInterpreter interpreter{cmd_cntx->tx(), &cntx->conn_state};
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
  BorrowedInterpreter interpreter{cmd_cntx->tx(), &cntx->conn_state};
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
  DCHECK(tx);
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

// `multi_mode` is the deduced multi mode that is not yet set on the transaction
static bool CanRunSingleShardMulti(bool one_shard, Transaction::MultiMode multi_mode,
                                   const Transaction& tx) {
  if (tx.GetMultiMode() != Transaction::NOT_DETERMINED) {
    // We may be running EVAL under MULTI. Currently RunSingleShardMulti() will attempt to lock
    // keys, in which case will be already locked by MULTI. We could optimize this path as well
    // though.
    return false;
  }

  // If we have only a single shard, we can run a global command without hops
  if (shard_set->size() == 1 && multi_mode == Transaction::GLOBAL)
    return true;

  return one_shard && multi_mode == Transaction::LOCK_AHEAD;
}

void Service::EvalInternal(CmdArgList args, const EvalArgs& eval_args, Interpreter* interpreter,
                           bool read_only, CommandContext* cmd_cntx) {
  const static size_t kShaSize = 40;
  static_assert(sizeof(ConnectionState::ScriptInfo::Stats::sha) == kShaSize);

  // Sanitizing the input to avoid code injection.
  if (eval_args.sha.size() != kShaSize || !IsSHA(eval_args.sha)) {
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
  memcpy(sinfo->stats.sha, eval_args.sha.data(), eval_args.sha.size());

  optional<ShardId> sid{nullopt};
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
  Transaction* tx = cmd_cntx->tx();
  CHECK(tx != nullptr);

  Interpreter::RunResult result;
  Transaction::MultiMode script_mode = DetermineMultiMode(*params);

  interpreter->SetGlobalArray("KEYS", eval_args.keys);
  interpreter->SetGlobalArray("ARGV", eval_args.args);

  // Reset cid to EVAL[] as the context is reused during command dispatch
  absl::Cleanup clean = [interpreter, cmd_cntx, cid = cmd_cntx->cid()]() {
    interpreter->ResetStack();
    cmd_cntx->SetupTx(cid, cmd_cntx->tx());
  };

  if (CanRunSingleShardMulti(sid.has_value(), script_mode, *tx)) {
    sinfo->stats.tx_shards = 1;
    // It might be that there are no declared keys, but there is only a single shard
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
    DCHECK(sid.has_value() || shard_set->size() == 1);
    ShardId real_sid = sid.value_or(ShardId(0));
#pragma GCC diagnostic pop

    // If script runs on a single shard, we run it remotely to save hops.
    interpreter->SetRedisFunc([cmd_cntx, this](Interpreter::CallArgs args) {
      // Disable squashing, as we're using the squashing mechanism to run remotely.
      args.call_type =
          static_cast<Interpreter::CallArgs::Type>(args.call_type & ~Interpreter::CallArgs::ACALL);
      CallFromScript(args, cmd_cntx);
    });

    ++ss->stats.eval_shardlocal_coordination_cnt;
    tx->PrepareSingleSquash(conn_cntx->ns, real_sid, conn_cntx->db_index(), eval_args.keys,
                            script_mode);

    tx->ScheduleSingleHop([&](Transaction*, EngineShard*) {
      boost::intrusive_ptr<Transaction> stub_tx =
          new Transaction{tx, real_sid, slot_checker.GetUniqueSlotId()};
      conn_cntx->transaction = stub_tx.get();

      result = interpreter->RunFunction(eval_args.sha, &error);

      conn_cntx->transaction = tx;
      return OpStatus::OK;
    });

    // Migration only makes sense if there are distinct shards
    if (sid.has_value() && *sid != ss->thread_index()) {
      VLOG(2) << "Migrating connection " << conn_cntx->conn() << " from "
              << ProactorBase::me()->GetPoolIndex() << " to " << real_sid;
      conn_cntx->conn()->RequestAsyncMigration(shard_set->pool()->at(real_sid), false);
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
      sinfo->stats.tx_shards = tx->GetUniqueShardCnt();
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

  sinfo->stats.tx_mode = script_mode;

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

void Service::Discard(CmdArgParser, CommandContext* cmd_cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  auto* cntx = cmd_cntx->server_conn_cntx();
  if (!cntx->conn_state.exec_info.IsCollecting()) {
    return rb->SendError("DISCARD without MULTI");
  }

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
  return rng::any_of(exec_info.watched_keys,
                     [db_indx](const auto& pair) { return pair.first != db_indx; });
}

template <typename F> void IterateAllKeys(const ConnectionState::ExecInfo* exec_info, F&& f) {
  for (auto& [dbid, key] : exec_info->watched_keys)
    f(MutableSlice{key.data(), key.size()});

  CmdArgVec arg_vec{};

  for (const auto& scmd : exec_info->body) {
    if (!scmd.Cid()->IsTransactional())
      continue;

    auto args = scmd.Slice(&arg_vec);
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

void Service::Exec(CmdArgParser, CommandContext* cmd_cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  auto* cntx = cmd_cntx->server_conn_cntx();
  auto& exec_info = cntx->conn_state.exec_info;

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
        BorrowedInterpreter(cmd_cntx->tx(), &cntx->conn_state).Release();
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
    cmd_cntx->tx()->UnlockMulti();
    return rb->SendNull();
  }

  exec_info.state = ConnectionState::ExecInfo::EXEC_RUNNING;

  VLOG(2) << "StartExec " << exec_info.body.size();

  // Make sure we flush whatever responses we aggregated in the reply builder.
  SinkReplyBuilder::ReplyAggregator agg(rb);
  rb->StartArray(exec_info.body.size());

  if (!exec_info.body.empty()) {
    string descr = CreateExecDescriptor(exec_info.body, cmd_cntx->tx()->GetUniqueShardCnt());
    ServerState::tlocal()->exec_freq_count[descr]++;

    if (GetFlag(FLAGS_multi_exec_squash) && state != ExecScriptUse::SCRIPT_RUN &&
        !cntx->conn_state.tracking_info_.IsTrackingOn()) {
      auto cmd_gen = [it = exec_info.body.begin(), end = exec_info.body.end()]() mutable -> CmdRef {
        return (it == end) ? CmdRef{} : (it++)->Ref();
      };
      MultiCommandSquasher::Opts opts;
      opts.max_squash_size = ServerState::tlocal()->max_squash_cmd_num;
      MultiCommandSquasher::Execute(std::move(cmd_gen), rb, cntx, this, opts);
    } else {
      CmdArgVec arg_vec;
      DCHECK_EQ(cmd_cntx->cid(), exec_cid_);

      for (const auto& scmd : exec_info.body) {
        CmdArgList args = scmd.Slice(&arg_vec);

        if (scmd.Cid()->IsTransactional()) {
          cmd_cntx->tx()->MultiSwitchCmd(scmd.Cid());
          OpStatus st = cmd_cntx->tx()->InitByArgs(cntx->ns, cntx->conn_state.db_index, args);
          if (st != OpStatus::OK) {
            cmd_cntx->SendError(st);
            break;
          }
        }

        // TODO: we will have to create a CommandContext per command if we want to support async
        // execution inside exec.
        cmd_cntx->UpdateCid(scmd.Cid());
        cmd_cntx->SetTailArgs(scmd.Args());
        auto invoke_res = InvokeCmd(args, cmd_cntx);
        if ((invoke_res != DispatchResult::OK) ||
            rb->GetError())  // checks for i/o error, not logical error.
          break;
      }
      cmd_cntx->UpdateCid(exec_cid_);
    }
  }

  if (scheduled) {
    VLOG(2) << "Exec unlocking " << exec_info.body.size() << " commands";
    cmd_cntx->tx()->UnlockMulti();
  }

  // Dispatch at the end manually to have (MULTI, cmds..., EXEC) order
  if (!ServerState::tlocal()->Monitors().Empty()) {
    LOG_IF(DFATAL, exec_cid_->opt_mask() & CO::ADMIN) << "EXEC should be non admin command";
    DispatchMonitor(cntx, exec_cid_, {});
  }

  VLOG(2) << "Exec completed";
}

void Service::Publish(CmdArgParser parser, CommandContext* cmd_cntx) {
  bool sharded = cmd_cntx->cid()->IsShardedPubSub();
  if (!sharded && IsClusterEnabled())
    return cmd_cntx->SendError("PUBLISH is not supported in cluster mode yet");

  string_view channel = parser.Next();
  string_view messages[] = {parser.Next()};

  cmd_cntx->SendLong(channel_store->SendMessages(channel, messages, sharded));
}

void Service::Subscribe(CmdArgParser parser, CommandContext* cmd_cntx) {
  bool sharded = cmd_cntx->cid()->IsShardedPubSub();
  if (!sharded && IsClusterEnabled())
    return cmd_cntx->SendError("SUBSCRIBE is not supported in cluster mode yet");

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  auto* conn_cntx = cmd_cntx->server_conn_cntx();
  conn_cntx->ChangeSubscription(true /*add*/, true /* reply*/, sharded, parser.UnparsedArgs(), rb);
}

void Service::Unsubscribe(CmdArgParser parser, CommandContext* cmd_cntx) {
  bool sharded = cmd_cntx->cid()->IsShardedPubSub();
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  auto* conn_cntx = cmd_cntx->server_conn_cntx();
  if (!sharded && IsClusterEnabled())
    return rb->SendError("UNSUBSCRIBE is not supported in cluster mode yet");

  ParsedArgs channels = parser.UnparsedArgs();
  if (channels.empty()) {
    conn_cntx->UnsubscribeAll(true, rb);
  } else {
    conn_cntx->ChangeSubscription(false, true, sharded, channels, rb);
  }
}

void Service::PSubscribe(CmdArgParser parser, CommandContext* cmd_cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());

  if (IsClusterEnabled()) {
    return rb->SendError("PSUBSCRIBE is not supported in cluster mode yet");
  }
  cmd_cntx->server_conn_cntx()->ChangePSubscription(true, true, parser.UnparsedArgs(), rb);
}

void Service::PUnsubscribe(CmdArgParser parser, CommandContext* cmd_cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
  if (IsClusterEnabled()) {
    return rb->SendError("PUNSUBSCRIBE is not supported in cluster mode yet");
  }
  auto* conn_cntx = cmd_cntx->server_conn_cntx();
  ParsedArgs patterns = parser.UnparsedArgs();
  if (patterns.empty()) {
    conn_cntx->PUnsubscribeAll(true, rb);
  } else {
    conn_cntx->ChangePSubscription(false, true, patterns, rb);
  }
}

// Not a real implementation. Serves as a decorator to accept some function commands
// for testing.
void Service::Function(CmdArgParser parser, CommandContext* cmd_cntx) {
  string sub_cmd = absl::AsciiStrToUpper(parser.Next());

  if (sub_cmd == "FLUSH") {
    return cmd_cntx->rb()->SendOk();
  }

  string err = UnknownSubCmd(sub_cmd, "FUNCTION");
  return cmd_cntx->SendError(err, kSyntaxErrType);
}

void Service::PubsubChannels(string_view pattern, SinkReplyBuilder* builder) {
  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  rb->SendBulkStrArr(channel_store->ListChannels(pattern));
}

void Service::PubsubPatterns(SinkReplyBuilder* builder) {
  size_t pattern_count = channel_store->PatternCount();
  builder->SendLong(pattern_count);
}

void Service::PubsubNumSub(ParsedArgs channels, SinkReplyBuilder* builder) {
  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  rb->StartArray(channels.size() * 2);
  for (string_view channel : channels) {
    rb->SendBulkString(channel);
    rb->SendLong(channel_store->FetchSubscribers(channel).size());
  }
}

void Service::Monitor(CmdArgParser, CommandContext* cmd_cntx) {
  VLOG(1) << "starting monitor on this connection: "
          << cmd_cntx->server_conn_cntx()->conn()->GetClientId();
  // we are registering the current connection for all threads so they will be aware of
  // this connection, to send to it any command
  cmd_cntx->rb()->SendOk();
  cmd_cntx->server_conn_cntx()->ChangeMonitor(true /* start */);
}

void Service::Pubsub(CmdArgParser parser, CommandContext* cmd_cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());

  if (!parser.HasNext()) {
    rb->SendError(WrongNumArgsError(cmd_cntx->cid()->name()));
    return;
  }

  string subcmd = absl::AsciiStrToUpper(parser.Next());

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
    string_view pattern = parser.NextOrDefault();
    PubsubChannels(pattern, rb);
  } else if (subcmd == "NUMPAT") {
    PubsubPatterns(rb);
  } else if (subcmd == "NUMSUB" || subcmd == "SHARDNUMSUB") {
    PubsubNumSub(parser.UnparsedArgs(), rb);
  } else {
    rb->SendError(UnknownSubCmd(subcmd, "PUBSUB"));
  }
}

void Service::Command(CmdArgParser parser, CommandContext* cmd_cntx) {
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
    rb->SendLong(cid.interleaved_step() ? cid.interleaved_step() : 1);

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
  if (!parser.HasNext()) {
    rb->StartArray(cmd_cnt);
    registry_.Traverse([&](string_view name, const CommandId& cid) {
      if (cid.opt_mask() & CO::HIDDEN)
        return;
      serialize_command(name, cid);
    });
    return;
  }

  string subcmd = absl::AsciiStrToUpper(parser.Next());

  // COUNT
  if (subcmd == "COUNT") {
    return rb->SendLong(cmd_cnt);
  }

  // INFO [cmd]
  if (subcmd == "INFO" && parser.HasNext()) {
    string cmd = absl::AsciiStrToUpper(parser.Next());

    if (const auto* cid = registry_.Find(cmd); cid) {
      rb->StartArray(1);
      serialize_command(cmd, *cid);
    } else {
      rb->SendNull();
    }

    return;
  }

  if (subcmd == "DOCS" && !parser.HasNext()) {
    // Returning an error here forces the interactive CLI client to fall back to static hints and
    // tab completion
    return rb->SendError("COMMAND DOCS Not Implemented");
  }

  if (subcmd == "HELP" && !parser.HasNext()) {
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
  GlobalState prev = global_state_;
  if (global_state_ != from) {
    return prev;
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
  return prev;
}

bool Service::RequestLoadingState() {
  GlobalState prev = SwitchState(GlobalState::ACTIVE, GlobalState::LOADING);
  if (prev == GlobalState::ACTIVE || prev == GlobalState::LOADING) {
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
      if (path == "/metrics" || path == "/healthz")
        return true;
      const bool pass_verified = pass.empty() ? true : password == pass;
      return username == "default" && pass_verified;
    });
  }
  server_family_.ConfigureMetrics(base);

  base->RegisterCb("/healthz", [](const http::QueryArgs&, HttpContext* send) {
    const bool healthy = ServerState::tlocal()->gstate() == GlobalState::ACTIVE;
    http::StringResponse resp =
        http::MakeStringResponse(healthy ? h2::status::ok : h2::status::service_unavailable);
    http::SetMime(http::kTextMime, &resp);
    resp.body() = healthy ? "OK\n" : "Service Unavailable\n";
    return send->Invoke(std::move(resp));
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

Service::ContextInfo Service::GetContextInfo(facade::ConnectionContext* cntx) const {
  ConnectionContext* server_cntx = static_cast<ConnectionContext*>(cntx);
  bool is_scheduled = server_cntx->transaction && server_cntx->transaction->IsScheduled() &&
                      !server_cntx->transaction->Blocker()->IsCompleted();

  return {.db_index = server_cntx->db_index(),
          .async_dispatch = server_cntx->async_dispatch,
          .conn_closing = server_cntx->conn_closing,
          .has_subscribers = bool(server_cntx->conn_state.subscribe_info),
          .is_blocked = server_cntx->blocked,
          .is_scheduled = is_scheduled};
}

#define HFUNC(x) SetHandler(&Service::x)
#define MFUNC_OLD(x) \
  SetHandler([this](CmdArgList sp, CommandContext* cntx) { this->x(std::move(sp), cntx); })

#define MFUNC(x) \
  SetHandler(    \
      [this](CmdArgList, CommandContext* cntx) { this->x(MakeParserFromContext(cntx), cntx); })

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
             .MFUNC_OLD(Eval)
             .SetValidator(&EvalValidator)
      << CI{"EVAL_RO", CO::NOSCRIPT | CO::READONLY | CO::VARIADIC_KEYS, -3, 3, 3, acl::kEvalRo}
             .MFUNC_OLD(EvalRo)
             .SetValidator(&EvalValidator)
      << CI{"EVALSHA", CO::NOSCRIPT | CO::VARIADIC_KEYS, -3, 3, 3, acl::kEvalSha}
             .MFUNC_OLD(EvalSha)
             .SetValidator(&EvalValidator)
      << CI{"EVALSHA_RO",   CO::NOSCRIPT | CO::READONLY | CO::VARIADIC_KEYS, -3, 3, 3,
            acl::kEvalShaRo}
             .MFUNC_OLD(EvalShaRo)
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
  RegisterListFamily(&registry_);
  RegisterStringFamily(&registry_);

#ifdef WITH_COLLECTION_CMDS
  SetFamily::Register(&registry_);
  HSetFamily::Register(&registry_);
  ZSetFamily::Register(&registry_);
  StreamFamily::Register(&registry_);
#endif

#ifdef WITH_EXTENSION_CMDS
  RegisterGeoFamily(&registry_);
  RegisterBitopsFamily(&registry_);
  RegisterHllFamily(&registry_);
  RegisterBloomFamily(&registry_);
  RegisterCmsFamily(&registry_);
  RegisterTopkFamily(&registry_);
  RegisterCuckooFilterFamily(&registry_);
  RegisterJsonFamily(&registry_);
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
