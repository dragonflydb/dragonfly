// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/main_service.h"

#include "absl/strings/str_split.h"
#include "facade/resp_expr.h"
#include "util/fibers/synchronization.h"

#ifdef __FreeBSD__
#include <pthread_np.h>
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

#include "base/flags.h"
#include "base/logging.h"
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
#include "server/conn_context.h"
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
#include "server/transaction.h"
#include "server/version.h"
#include "server/zset_family.h"
#include "strings/human_readable.h"
#include "util/html/sorted_table.h"
#include "util/varz.h"

using namespace std;
using facade::operator""_KB;
using facade::ErrorReply;

ABSL_FLAG(int32_t, port, 6379,
          "Redis port. 0 disables the port, -1 will bind on a random available port.");

ABSL_FLAG(uint16_t, announce_port, 0,
          "Port that Dragonfly announces to cluster clients and replication master");

ABSL_FLAG(uint32_t, memcached_port, 0, "Memcached port");

ABSL_FLAG(uint32_t, num_shards, 0, "Number of database shards, 0 - to choose automatically");

ABSL_RETIRED_FLAG(uint32_t, multi_exec_mode, 2, "DEPRECATED. Sets multi exec atomicity mode");

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

ABSL_FLAG(dfly::MemoryBytesFlag, maxmemory, dfly::MemoryBytesFlag{},
          "Limit on maximum-memory that is used by the database. "
          "0 - means the program will automatically determine its maximum memory usage. "
          "default: 0");

ABSL_RETIRED_FLAG(
    double, oom_deny_ratio, 1.1,
    "commands with flag denyoom will return OOM when the ratio between maxmemory and used "
    "memory is above this value");

ABSL_FLAG(double, rss_oom_deny_ratio, 1.25,
          "When the ratio between maxmemory and RSS memory exceeds this value, commands marked as "
          "DENYOOM will fail with OOM error and new connections to non-admin port will be "
          "rejected. Negative value disables this feature.");

ABSL_FLAG(size_t, serialization_max_chunk_size, 64_KB,
          "Maximum size of a value that may be serialized at once during snapshotting or full "
          "sync. Values bigger than this threshold will be serialized using streaming "
          "serialization. 0 - to disable streaming mode");
ABSL_FLAG(uint32_t, max_squashed_cmd_num, 100,
          "Max number of commands squashed in a single shard during squash optimizaiton");

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
  if (!monitors.empty()) {
    VLOG(2) << "Thread " << ProactorBase::me()->GetPoolIndex() << " sending monitor message '"
            << msg << "' for " << monitors.size();

    for (auto monitor_conn : monitors) {
      // never preempts, so we can iterate safely.
      monitor_conn->SendMonitorMessageAsync(msg);
    }
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
  explicit EvalSerializer(RedisReplyBuilder* rb) : rb_(rb) {
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
    if (rb_->IsResp3() || !absl::GetFlag(FLAGS_lua_resp2_legacy_float)) {
      rb_->SendDouble(d);
    } else {
      long val = d >= 0 ? static_cast<long>(floor(d)) : static_cast<long>(ceil(d));
      rb_->SendLong(val);
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
    rb_->StartCollection(len, RedisReplyBuilder::MAP);
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
    rb_->SendError(str);
  }

 private:
  RedisReplyBuilder* rb_;
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
  explr_->OnError(str);
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
  if (type == MAP)
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
    if (CO::IsEvalKind(scmd.Cid()->name())) {
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

  for (auto arg : args) {
    absl::StrAppend(&result, " ", absl::CHexEscape(arg));
  }

  absl::StrAppend(&result, " failed with reason: ", reason);

  return result;
}

void SetRssOomDenyRatioOnAllThreads(double ratio) {
  auto cb = [ratio](unsigned, auto*) { ServerState::tlocal()->rss_oom_deny_ratio = ratio; };
  shard_set->pool()->AwaitBrief(cb);
}

void SetSerializationMaxChunkSize(size_t val) {
  auto cb = [val](unsigned, auto*) { ServerState::tlocal()->serialization_max_chunk_size = val; };
  shard_set->pool()->AwaitBrief(cb);
}

void SetMaxSquashedCmdNum(int32_t val) {
  auto cb = [val](unsigned, auto*) { ServerState::tlocal()->max_squash_cmd_num = val; };
  shard_set->pool()->AwaitBrief(cb);
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

void Service::Init(util::AcceptServer* acceptor, std::vector<facade::Listener*> listeners) {
  InitRedisTables();
  facade::Connection::Init(pp_.size());

  config_registry.RegisterSetter<MemoryBytesFlag>(
      "maxmemory", [](const MemoryBytesFlag& flag) { max_memory_limit = flag.value; });

  config_registry.RegisterMutable("dbfilename");
  config_registry.Register("dbnum");  // equivalent to databases in redis.
  config_registry.Register("dir");
  config_registry.RegisterMutable("enable_heartbeat_eviction");
  config_registry.RegisterMutable("masterauth");
  config_registry.RegisterMutable("masteruser");
  config_registry.RegisterMutable("max_eviction_per_heartbeat");
  config_registry.RegisterMutable("max_segment_to_consider");

  config_registry.RegisterSetter<double>("rss_oom_deny_ratio",
                                         [](double val) { SetRssOomDenyRatioOnAllThreads(val); });
  config_registry.RegisterSetter<size_t>("serialization_max_chunk_size",
                                         [](size_t val) { SetSerializationMaxChunkSize(val); });

  config_registry.RegisterMutable("pipeline_squash");

  config_registry.RegisterSetter<uint32_t>("pipeline_queue_limit", [](uint32_t val) {
    shard_set->pool()->AwaitBrief(
        [val](unsigned tid, auto*) { facade::Connection::SetMaxQueueLenThreadLocal(tid, val); });
  });

  config_registry.RegisterSetter<size_t>("pipeline_buffer_limit", [](size_t val) {
    shard_set->pool()->AwaitBrief(
        [val](unsigned tid, auto*) { facade::Connection::SetPipelineBufferLimit(tid, val); });
  });

  config_registry.RegisterSetter<uint32_t>("max_squashed_cmd_num",
                                           [](uint32_t val) { SetMaxSquashedCmdNum(val); });

  config_registry.RegisterMutable("replica_partial_sync");
  config_registry.RegisterMutable("replication_timeout");
  config_registry.RegisterMutable("migration_finalization_timeout_ms");
  config_registry.RegisterMutable("table_growth_margin");
  config_registry.RegisterMutable("tcp_keepalive");
  config_registry.RegisterMutable("timeout");
  config_registry.RegisterMutable("send_timeout");
  config_registry.RegisterMutable("managed_service_info");

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
  Transaction::Init(shard_num);

  SetRssOomDenyRatioOnAllThreads(absl::GetFlag(FLAGS_rss_oom_deny_ratio));
  SetSerializationMaxChunkSize(absl::GetFlag(FLAGS_serialization_max_chunk_size));
  SetMaxSquashedCmdNum(absl::GetFlag(FLAGS_max_squashed_cmd_num));

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
  engine_varz.reset();

  ChannelStore::Destroy();

  shard_set->PreShutdown();
  shard_set->Shutdown();
  Transaction::Shutdown();

  pp_.AwaitFiberOnAll([](ProactorBase* pb) { ServerState::tlocal()->Destroy(); });

  // wait for all the pending callbacks to stop.
  ThisFiber::SleepFor(10ms);
  facade::Connection::Shutdown();
}

OpResult<KeyIndex> Service::FindKeys(const CommandId* cid, CmdArgList args) {
  if (!cid->IsShardedPSub()) {
    return DetermineKeys(cid, args);
  }

  // Sharded pub sub
  // Command form: SPUBLISH shardchannel message
  if (cid->name() == registry_.RenamedOrOriginal("SPUBLISH")) {
    return {KeyIndex(0, 1)};
  }

  return {KeyIndex(0, args.size())};
}

optional<ErrorReply> Service::CheckKeysOwnership(const CommandId* cid, CmdArgList args,
                                                 const ConnectionContext& dfly_cntx) {
  if (dfly_cntx.is_replicating) {
    // Always allow commands on the replication port, as it might be for future-owned keys.
    return nullopt;
  }

  if (cid->first_key_pos() == 0 && !cid->IsShardedPSub()) {
    return nullopt;  // No key command.
  }

  OpResult<KeyIndex> key_index_res = FindKeys(cid, args);

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

bool ShouldDenyOnOOM(const CommandId* cid) {
  ServerState& etl = *ServerState::tlocal();
  if ((cid->opt_mask() & CO::DENYOOM) && etl.is_master) {
    uint64_t start_ns = absl::GetCurrentTimeNanos();
    auto memory_stats = etl.GetMemoryUsage(start_ns);

    if (memory_stats.used_mem > max_memory_limit ||
        (etl.rss_oom_deny_ratio > 0 &&
         memory_stats.rss_mem > (max_memory_limit * etl.rss_oom_deny_ratio))) {
      DLOG(WARNING) << "Out of memory, used " << memory_stats.used_mem << " ,rss "
                    << memory_stats.rss_mem << " ,limit " << max_memory_limit;
      etl.stats.oom_error_cmd_cnt++;
      return true;
    }
  }
  return false;
}

optional<ErrorReply> Service::VerifyCommandExecution(const CommandId* cid,
                                                     const ConnectionContext* cntx,
                                                     CmdArgList tail_args) {
  if (ShouldDenyOnOOM(cid)) {
    return facade::ErrorReply{kOutOfMemory};
  }

  return VerifyConnectionAclStatus(cid, cntx, "ACL rules changed between the MULTI and EXEC",
                                   tail_args);
}

std::optional<ErrorReply> Service::VerifyCommandState(const CommandId* cid, CmdArgList tail_args,
                                                      const ConnectionContext& dfly_cntx) {
  DCHECK(cid);

  ServerState& etl = *ServerState::tlocal();

  // If there is no connection owner, it means the command it being called
  // from another command or used internally, therefore is always permitted.
  if (dfly_cntx.conn() != nullptr && !dfly_cntx.conn()->IsPrivileged() && cid->IsRestricted()) {
    VLOG(1) << "Non-admin attempt to execute " << cid->name() << " " << tail_args << " "
            << ConnectionLogContext(dfly_cntx.conn());
    return ErrorReply{"Cannot execute restricted command (admin only)", kRestrictDenied};
  }

  if (auto err = cid->Validate(tail_args); err)
    return err;

  bool is_trans_cmd = CO::IsTransKind(cid->name());
  bool under_script = dfly_cntx.conn_state.script_info != nullptr;
  bool is_write_cmd = cid->IsWriteOnly();
  bool multi_active = dfly_cntx.conn_state.exec_info.IsCollecting() && !is_trans_cmd;

  // Check if the command is allowed to execute under this global state
  bool allowed_by_state = true;
  const GlobalState gstate = etl.gstate();
  switch (gstate) {
    case GlobalState::LOADING:
      allowed_by_state = dfly_cntx.journal_emulated || (cid->opt_mask() & CO::LOADING);
      break;
    case GlobalState::SHUTTING_DOWN:
      allowed_by_state = false;
      break;
    case GlobalState::TAKEN_OVER:
      // Only PING, admin commands, and all commands via admin connections are allowed
      // we prohibit even read commands, because read commands running in pipeline can take a while
      // to send all data to a client which leads to fail in takeover
      allowed_by_state = dfly_cntx.conn()->IsPrivileged() || (cid->opt_mask() & CO::ADMIN) ||
                         cid->name() == "PING";
      break;
    default:
      break;
  }

  if (!allowed_by_state) {
    VLOG(1) << "Command " << cid->name() << " not executed because global state is " << gstate;

    if (gstate == GlobalState::LOADING) {
      return ErrorReply(kLoadingErr);
    }

    return ErrorReply{StrCat("Can not execute during ", GlobalStateName(gstate))};
  }

  string_view cmd_name{cid->name()};

  if (dfly_cntx.req_auth && !dfly_cntx.authenticated) {
    if (cmd_name != "AUTH" && cmd_name != "QUIT" && cmd_name != "HELLO") {
      return ErrorReply{"-NOAUTH Authentication required."};
    }
  }

  // only reset and quit are allow if this connection is used for monitoring
  if (dfly_cntx.monitor && (cmd_name != "RESET" && cmd_name != "QUIT"))
    return ErrorReply{"Replica can't interact with the keyspace"};

  if (!etl.is_master && is_write_cmd && !dfly_cntx.is_replicating)
    return ErrorReply{"-READONLY You can't write against a read only replica."};

  if (multi_active) {
    if (absl::EndsWith(cmd_name, "SUBSCRIBE"))
      return ErrorReply{absl::StrCat("Can not call ", cmd_name, " within a transaction")};

    if (cmd_name == "WATCH" || cmd_name == "FLUSHALL" || cmd_name == "FLUSHDB")
      return ErrorReply{absl::StrCat("'", cmd_name, "' inside MULTI is not allowed")};
  }

  if (IsClusterEnabled()) {
    if (auto err = CheckKeysOwnership(cid, tail_args, dfly_cntx); err)
      return err;
  }

  if (under_script && (cid->opt_mask() & CO::NOSCRIPT))
    return ErrorReply{"This Redis command is not allowed from script"};

  if (under_script) {
    DCHECK(dfly_cntx.transaction);
    // The following commands access shards arbitrarily without having keys, so they can only be run
    // non atomically or globally.
    Transaction::MultiMode mode = dfly_cntx.transaction->GetMultiMode();
    bool shard_access = (cid->opt_mask()) & (CO::GLOBAL_TRANS | CO::NO_KEY_TRANSACTIONAL);
    if (shard_access && (mode != Transaction::GLOBAL && mode != Transaction::NON_ATOMIC))
      return ErrorReply("This Redis command is not allowed from script");

    if (cid->IsTransactional()) {
      auto err = CheckKeysDeclared(*dfly_cntx.conn_state.script_info, cid, tail_args, mode);

      if (err.has_value()) {
        VLOG(1) << "CheckKeysDeclared failed with error " << err->ToSv() << " for command "
                << cid->name();
        return err.value();
      }
    }

    if (dfly_cntx.conn_state.script_info->read_only && is_write_cmd) {
      return ErrorReply{"Write commands are not allowed from read-only scripts"};
    }
  }

  return VerifyConnectionAclStatus(cid, &dfly_cntx, "has no ACL permissions", tail_args);
}

void Service::DispatchCommand(ArgSlice args, SinkReplyBuilder* builder,
                              facade::ConnectionContext* cntx) {
  DCHECK(!args.empty());
  DCHECK_NE(0u, shard_set->size()) << "Init was not called";

  absl::Cleanup clear_last_error([builder]() { builder->ConsumeLastError(); });
  ServerState& etl = *ServerState::tlocal();

  string cmd = absl::AsciiStrToUpper(args[0]);
  const auto [cid, args_no_cmd] = registry_.FindExtended(cmd, args.subspan(1));

  if (cid == nullptr) {
    return builder->SendError(ReportUnknownCmd(cmd));
  }

  ConnectionContext* dfly_cntx = static_cast<ConnectionContext*>(cntx);
  bool under_script = bool(dfly_cntx->conn_state.script_info);
  bool under_exec = dfly_cntx->conn_state.exec_info.IsRunning();
  bool dispatching_in_multi = under_script || under_exec;

  if (VLOG_IS_ON(2) && cntx->conn() /* no owner in replica context */) {
    LOG(INFO) << "Got (" << cntx->conn()->GetClientId() << "): " << (under_script ? "LUA " : "")
              << args << " in dbid=" << dfly_cntx->conn_state.db_index;
  }

  // Don't interrupt running multi commands or admin connections.
  if (etl.IsPaused() && !dispatching_in_multi && cntx->conn() && !cntx->conn()->IsPrivileged()) {
    bool is_write = cid->IsWriteOnly();
    is_write |= cid->name() == "PUBLISH" || cid->name() == "EVAL" || cid->name() == "EVALSHA";
    is_write |= cid->name() == "EXEC" && dfly_cntx->conn_state.exec_info.is_write;

    cntx->paused = true;
    etl.AwaitPauseState(is_write);
    cntx->paused = false;
  }

  if (auto err = VerifyCommandState(cid, args_no_cmd, *dfly_cntx); err) {
    if (auto& exec_info = dfly_cntx->conn_state.exec_info; exec_info.IsCollecting())
      exec_info.state = ConnectionState::ExecInfo::EXEC_ERROR;

    // We need to skip this because ACK's should not be replied to
    // Bonus points because this allows to continue replication with ACL users who got
    // their access revoked and reinstated
    if (cid->name() == "REPLCONF" && absl::EqualsIgnoreCase(ArgS(args_no_cmd, 0), "ACK")) {
      server_family_.GetDflyCmd()->OnClose(dfly_cntx->conn_state.replication_info.repl_session_id);
      return;
    }
    builder->SendError(std::move(*err));
    return;
  }

  VLOG_IF(1, cid->opt_mask() & CO::CommandOpt::DANGEROUS)
      << "Executing dangerous command " << cid->name() << " "
      << ConnectionLogContext(dfly_cntx->conn());

  bool is_trans_cmd = CO::IsTransKind(cid->name());
  if (dfly_cntx->conn_state.exec_info.IsCollecting() && !is_trans_cmd) {
    // TODO: protect against aggregating huge transactions.
    dfly_cntx->conn_state.exec_info.body.emplace_back(cid, true, args_no_cmd);
    if (cid->IsWriteOnly()) {
      dfly_cntx->conn_state.exec_info.is_write = true;
    }
    return builder->SendSimpleString("QUEUED");
  }

  // Create command transaction
  intrusive_ptr<Transaction> dist_trans;

  if (dispatching_in_multi) {
    DCHECK(dfly_cntx->transaction);
    if (cid->IsTransactional()) {
      dfly_cntx->transaction->MultiSwitchCmd(cid);
      OpStatus status = dfly_cntx->transaction->InitByArgs(
          dfly_cntx->ns, dfly_cntx->conn_state.db_index, args_no_cmd);

      if (status != OpStatus::OK)
        return builder->SendError(status);
    }
  } else {
    DCHECK(dfly_cntx->transaction == nullptr);

    if (cid->IsTransactional()) {
      dist_trans.reset(new Transaction{cid});

      if (!dist_trans->IsMulti()) {  // Multi command initialize themself based on their mode.
        CHECK(dfly_cntx->ns != nullptr);
        if (auto st =
                dist_trans->InitByArgs(dfly_cntx->ns, dfly_cntx->conn_state.db_index, args_no_cmd);
            st != OpStatus::OK)
          return builder->SendError(st);
      }

      dfly_cntx->transaction = dist_trans.get();
      dfly_cntx->last_command_debug.shards_count = dfly_cntx->transaction->GetUniqueShardCnt();
    } else {
      dfly_cntx->transaction = nullptr;
    }
  }

  dfly_cntx->cid = cid;

  if (!InvokeCmd(cid, args_no_cmd, CommandContext{dfly_cntx->transaction, builder, dfly_cntx})) {
    builder->SendError("Internal Error");
    builder->CloseConnection();
  }

  if (!dispatching_in_multi) {
    dfly_cntx->transaction = nullptr;
  }
}

class ReplyGuard {
 public:
  ReplyGuard(std::string_view cid_name, SinkReplyBuilder* builder, ConnectionContext* cntx) {
    const bool is_script = bool(cntx->conn_state.script_info);
    const bool is_one_of =
        absl::flat_hash_set<std::string_view>({"REPLCONF", "DFLY"}).contains(cid_name);
    bool is_mcache = builder->GetProtocol() == Protocol::MEMCACHE;
    const bool is_no_reply_memcache =
        is_mcache && (static_cast<MCReplyBuilder*>(builder)->NoReply() || cid_name == "QUIT");
    const bool should_dcheck = !is_one_of && !is_script && !is_no_reply_memcache;
    if (should_dcheck) {
      builder_ = builder;
      replies_recorded_ = builder_->RepliesRecorded();
    }
  }

  ~ReplyGuard() {
    if (builder_) {
      DCHECK_GT(builder_->RepliesRecorded(), replies_recorded_)
          << cid_name_ << " " << typeid(*builder_).name();
    }
  }

 private:
  size_t replies_recorded_ = 0;
  std::string_view cid_name_;
  SinkReplyBuilder* builder_ = nullptr;
};

OpResult<void> OpTrackKeys(const OpArgs slice_args, const facade::Connection::WeakRef& conn_ref,
                           const ShardArgs& args) {
  if (conn_ref.IsExpired()) {
    DVLOG(2) << "Connection expired, exiting TrackKey function.";
    return OpStatus::OK;
  }

  DVLOG(2) << "Start tracking keys for client ID: " << conn_ref.GetClientId()
           << " with thread ID: " << conn_ref.Thread();

  auto& db_slice = slice_args.GetDbSlice();
  // TODO: There is a bug here that we track all arguments instead of tracking only keys.
  for (auto key : args) {
    DVLOG(2) << "Inserting client ID " << conn_ref.GetClientId()
             << " into the tracking client set of key " << key;
    db_slice.TrackKey(conn_ref, key);
  }

  return OpStatus::OK;
}

bool Service::InvokeCmd(const CommandId* cid, CmdArgList tail_args,
                        const CommandContext& cmd_cntx) {
  DCHECK(cid);
  DCHECK(!cid->Validate(tail_args));

  ConnectionContext* cntx = cmd_cntx.conn_cntx;
  auto* builder = cmd_cntx.rb;
  DCHECK(builder);
  DCHECK(cntx);

  if (auto err = VerifyCommandExecution(cid, cntx, tail_args); err) {
    // We need to skip this because ACK's should not be replied to
    // Bonus points because this allows to continue replication with ACL users who got
    // their access revoked and reinstated
    if (cid->name() == "REPLCONF" && absl::EqualsIgnoreCase(ArgS(tail_args, 0), "ACK")) {
      return true;
    }
    builder->SendError(std::move(*err));
    builder->ConsumeLastError();
    return true;  // return false only for internal error aborts
  }

  // We are not sending any admin command in the monitor, and we do not want to
  // do any processing if we don't have any waiting connections with monitor
  // enabled on them - see https://redis.io/commands/monitor/
  if (!ServerState::tlocal()->Monitors().Empty() && (cid->opt_mask() & CO::ADMIN) == 0) {
    DispatchMonitor(cntx, cid, tail_args);
  }

  ServerState::tlocal()->RecordCmd(cntx->has_main_or_memcache_listener);
  auto& info = cntx->conn_state.tracking_info_;
  const bool is_read_only = cid->opt_mask() & CO::READONLY;
  Transaction* tx = cmd_cntx.tx;
  if (tx) {
    // Reset it, because in multi/exec the transaction pointer is the same and
    // we will end up triggerring the callback on the following commands. To avoid this
    // we reset it.
    tx->SetTrackingCallback({});
    if (is_read_only && info.ShouldTrackKeys()) {
      auto conn = cntx->conn()->Borrow();
      tx->SetTrackingCallback([conn](Transaction* trans) {
        auto* shard = EngineShard::tlocal();
        OpTrackKeys(trans->GetOpArgs(shard), conn, trans->GetShardArgs(shard->shard_id()));
      });
    }
  }

#ifndef NDEBUG
  // Verifies that we reply to the client when needed.
  ReplyGuard reply_guard(cid->name(), builder, cntx);
#endif
  uint64_t invoke_time_usec = 0;
  auto last_error = builder->ConsumeLastError();
  DCHECK(last_error.empty());
  try {
    invoke_time_usec = cid->Invoke(tail_args, cmd_cntx);
  } catch (std::exception& e) {
    LOG(ERROR) << "Internal error, system probably unstable " << e.what();
    return false;
  }

  if (std::string reason = builder->ConsumeLastError(); !reason.empty()) {
    VLOG(2) << FailedCommandToString(cid->name(), tail_args, reason);
    LOG_EVERY_T(WARNING, 1) << FailedCommandToString(cid->name(), tail_args, reason);
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

  // TODO: we should probably discard more commands here,
  // not just the blocking ones
  const auto* conn = cntx->conn();
  if (cntx->conn_state.squashing_info) {
    conn = cntx->conn_state.squashing_info->owner->conn();
  }

  if (!(cid->opt_mask() & CO::BLOCKING) && conn != nullptr &&
      // Use SafeTLocal() to avoid accessing the wrong thread local instance
      ServerState::SafeTLocal()->ShouldLogSlowCmd(invoke_time_usec)) {
    vector<string> aux_params;
    CmdArgVec aux_slices;

    if (tail_args.empty() && cid->name() == "EXEC") {
      // abuse tail_args to pass more information about the slow EXEC.
      aux_params.emplace_back(StrCat("CMDCOUNT/", cntx->last_command_debug.exec_body_len));
      aux_slices.emplace_back(aux_params.back());
      tail_args = absl::MakeSpan(aux_slices);
    }
    ServerState::SafeTLocal()->GetSlowLog().Add(cid->name(), tail_args, conn->GetName(),
                                                conn->RemoteEndpointStr(), invoke_time_usec,
                                                absl::GetCurrentTimeNanos() / 1000);
  }

  if (tx && !cntx->conn_state.exec_info.IsRunning() && cntx->conn_state.script_info == nullptr) {
    cntx->last_command_debug.clock = tx->txid();
  }

  return true;
}

size_t Service::DispatchManyCommands(absl::Span<CmdArgList> args_list, SinkReplyBuilder* builder,
                                     facade::ConnectionContext* cntx) {
  ConnectionContext* dfly_cntx = static_cast<ConnectionContext*>(cntx);
  DCHECK(!dfly_cntx->conn_state.exec_info.IsRunning());
  DCHECK_EQ(builder->GetProtocol(), Protocol::REDIS);

  auto* ss = dfly::ServerState::tlocal();
  // Don't even start when paused. We can only continue if DispatchTracker is aware of us running.
  if (ss->IsPaused())
    return 0;

  vector<StoredCmd> stored_cmds;
  intrusive_ptr<Transaction> dist_trans;
  size_t dispatched = 0;

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

    size_t squashed_num = MultiCommandSquasher::Execute(absl::MakeSpan(stored_cmds),
                                                        static_cast<RedisReplyBuilder*>(builder),
                                                        dfly_cntx, this, opts);
    dfly_cntx->transaction = nullptr;

    dispatched += stored_cmds.size();
    ss->stats.squashed_commands += squashed_num;
    stored_cmds.clear();
  };

  for (auto args : args_list) {
    string cmd = absl::AsciiStrToUpper(ArgS(args, 0));
    const auto [cid, tail_args] = registry_.FindExtended(cmd, args.subspan(1));

    // MULTI...EXEC commands need to be collected into a single context, so squashing is not
    // possible
    const bool is_multi = dfly_cntx->conn_state.exec_info.IsCollecting() || CO::IsTransKind(cmd);

    // Generally, executing any multi-transactions (including eval) is not possible because they
    // might request a stricter multi mode than non-atomic which is used for squashing.
    // TODO: By allowing promoting non-atomic multit transactions to lock-ahead for specific command
    // invocations, we can potentially execute multiple eval in parallel, which is very powerful
    // paired with shardlocal eval
    const bool is_eval = CO::IsEvalKind(cmd);

    const bool is_blocking = cid != nullptr && cid->IsBlocking();

    if (!is_multi && !is_eval && !is_blocking && cid != nullptr) {
      stored_cmds.reserve(args_list.size());
      stored_cmds.emplace_back(cid, false /* do not deep-copy commands*/, tail_args);
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

  return dispatched;
}

void Service::DispatchMC(const MemcacheParser::Command& cmd, std::string_view value,
                         MCReplyBuilder* mc_builder, facade::ConnectionContext* cntx) {
  absl::InlinedVector<MutableSlice, 8> args;
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
      server_family_.StatsMC(cmd.key, mc_builder);
      return;
    case MemcacheParser::VERSION:
      mc_builder->SendSimpleString("VERSION 1.6.0 DF");
      return;
    default:
      mc_builder->SendClientError("bad command line format");
      return;
  }

  args.emplace_back(cmd_name, strlen(cmd_name));

  if (!cmd.key.empty()) {
    char* key = const_cast<char*>(cmd.key.data());
    args.emplace_back(key, cmd.key.size());
  }

  ConnectionContext* dfly_cntx = static_cast<ConnectionContext*>(cntx);

  if (MemcacheParser::IsStoreCmd(cmd.type)) {
    char* v = const_cast<char*>(value.data());
    args.emplace_back(v, value.size());

    if (store_opt[0]) {
      args.emplace_back(store_opt, strlen(store_opt));
    }

    // if expire_ts is greater than month it's a unix timestamp
    // https://github.com/memcached/memcached/blob/master/doc/protocol.txt#L139
    constexpr uint32_t kExpireLimit = 60 * 60 * 24 * 30;
    const uint64_t expire_ts = cmd.expire_ts && cmd.expire_ts <= kExpireLimit
                                   ? cmd.expire_ts + time(nullptr)
                                   : cmd.expire_ts;
    if (expire_ts && memcmp(cmd_name, "SET", 3) == 0) {
      char* next = absl::numbers_internal::FastIntToBuffer(expire_ts, ttl);
      args.emplace_back(ttl_op, 4);
      args.emplace_back(ttl, next - ttl);
    }
    dfly_cntx->conn_state.memcache_flag = cmd.flags;
  } else if (cmd.type < MemcacheParser::QUIT) {  // read commands
    for (auto s : cmd.keys_ext) {
      char* key = const_cast<char*>(s.data());
      args.emplace_back(key, s.size());
    }
    if (cmd.type == MemcacheParser::GETS) {
      dfly_cntx->conn_state.memcache_flag |= ConnectionState::FETCH_CAS_VER;
    }
  } else {  // write commands.
    if (store_opt[0]) {
      args.emplace_back(store_opt, strlen(store_opt));
    }
  }

  DispatchCommand(CmdArgList{args}, mc_builder, cntx);

  // Reset back.
  dfly_cntx->conn_state.memcache_flag = 0;
}

ErrorReply Service::ReportUnknownCmd(string_view cmd_name) {
  lock_guard lk(mu_);
  if (unknown_cmds_.size() < 1024)
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

void Service::Quit(CmdArgList args, const CommandContext& cmd_cntx) {
  if (cmd_cntx.rb->GetProtocol() == Protocol::REDIS)
    cmd_cntx.rb->SendOk();

  cmd_cntx.rb->CloseConnection();

  DeactivateMonitoring(cmd_cntx.conn_cntx);
  cmd_cntx.conn_cntx->conn()->ShutdownSelf();
}

void Service::Multi(CmdArgList args, const CommandContext& cmd_cntx) {
  if (cmd_cntx.conn_cntx->conn_state.exec_info.IsCollecting()) {
    return cmd_cntx.rb->SendError("MULTI calls can not be nested");
  }
  cmd_cntx.conn_cntx->conn_state.exec_info.state = ConnectionState::ExecInfo::EXEC_COLLECT;
  // TODO: to protect against huge exec transactions.
  return cmd_cntx.rb->SendOk();
}

void Service::Watch(CmdArgList args, const CommandContext& cmd_cntx) {
  auto& exec_info = cmd_cntx.conn_cntx->conn_state.exec_info;

  // Skip if EXEC will already fail due previous WATCH.
  if (exec_info.watched_dirty.load(memory_order_relaxed)) {
    return cmd_cntx.rb->SendOk();
  }

  atomic_uint32_t keys_existed = 0;
  auto cb = [&](Transaction* t, EngineShard* shard) {
    ShardId shard_id = shard->shard_id();
    ShardArgs largs = t->GetShardArgs(shard_id);
    for (auto k : largs) {
      t->GetDbSlice(shard_id).RegisterWatchedKey(cmd_cntx.conn_cntx->db_index(), k, &exec_info);
    }

    auto res = GenericFamily::OpExists(t->GetOpArgs(shard), largs);
    keys_existed.fetch_add(res.value_or(0), memory_order_relaxed);
    return OpStatus::OK;
  };
  cmd_cntx.tx->ScheduleSingleHop(std::move(cb));

  // Duplicate keys are stored to keep correct count.
  exec_info.watched_existed += keys_existed.load(memory_order_relaxed);
  for (string_view key : args) {
    exec_info.watched_keys.emplace_back(cmd_cntx.conn_cntx->db_index(), key);
  }

  return cmd_cntx.rb->SendOk();
}

void Service::Unwatch(CmdArgList args, const CommandContext& cmd_cntx) {
  UnwatchAllKeys(cmd_cntx.conn_cntx->ns, &cmd_cntx.conn_cntx->conn_state.exec_info);
  return cmd_cntx.rb->SendOk();
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

void Service::CallFromScript(ConnectionContext* cntx, Interpreter::CallArgs& ca) {
  auto* tx = cntx->transaction;
  DCHECK(tx);
  DVLOG(2) << "CallFromScript " << ca.args[0];

  InterpreterReplier replier(ca.translator);
  optional<ErrorReply> findcmd_err;

  if (ca.async) {
    auto& info = cntx->conn_state.script_info;
    string cmd = absl::AsciiStrToUpper(ca.args[0]);

    // Full command verification happens during squashed execution
    if (auto* cid = registry_.Find(cmd); cid != nullptr) {
      auto replies = ca.error_abort ? ReplyMode::ONLY_ERR : ReplyMode::NONE;
      info->async_cmds.emplace_back(std::move(*ca.buffer), cid, ca.args.subspan(1), replies);
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
    replier.RedisReplyBuilder::SendError(std::move(*findcmd_err));
    *ca.requested_abort |= ca.error_abort;
  }

  if (ca.async)
    return;

  DispatchCommand(ca.args, &replier, cntx);
}

void Service::Eval(CmdArgList args, const CommandContext& cmd_cntx, bool read_only) {
  string_view body = ArgS(args, 0);

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  if (body.empty()) {
    return rb->SendNull();
  }

  BorrowedInterpreter interpreter{cmd_cntx.tx, &cmd_cntx.conn_cntx->conn_state};
  auto res = server_family_.script_mgr()->Insert(body, interpreter);
  if (!res)
    return cmd_cntx.rb->SendError(res.error().Format(), facade::kScriptErrType);

  string sha{std::move(res.value())};

  CallSHA(args, sha, interpreter, cmd_cntx.rb, cmd_cntx.conn_cntx, read_only);
}

void Service::EvalRo(CmdArgList args, const CommandContext& cmd_cntx) {
  Eval(args, cmd_cntx, true);
}

void Service::EvalSha(CmdArgList args, const CommandContext& cmd_cntx, bool read_only) {
  string sha = absl::AsciiStrToLower(ArgS(args, 0));

  BorrowedInterpreter interpreter{cmd_cntx.tx, &cmd_cntx.conn_cntx->conn_state};
  CallSHA(args, sha, interpreter, cmd_cntx.rb, cmd_cntx.conn_cntx, read_only);
}

void Service::EvalShaRo(CmdArgList args, const CommandContext& cmd_cntx) {
  EvalSha(args, cmd_cntx, true);
}

void Service::CallSHA(CmdArgList args, string_view sha, Interpreter* interpreter,
                      SinkReplyBuilder* builder, ConnectionContext* cntx, bool read_only) {
  uint32_t num_keys;
  CHECK(absl::SimpleAtoi(ArgS(args, 1), &num_keys));  // we already validated this

  EvalArgs ev_args;
  ev_args.sha = sha;
  ev_args.keys = args.subspan(2, num_keys);
  ev_args.args = args.subspan(2 + num_keys);

  uint64_t start = absl::GetCurrentTimeNanos();
  EvalInternal(args, ev_args, interpreter, builder, cntx, read_only);

  uint64_t end = absl::GetCurrentTimeNanos();
  ServerState::tlocal()->RecordCallLatency(sha, (end - start) / 1000);
}

optional<ScriptMgr::ScriptParams> LoadScript(string_view sha, ScriptMgr* script_mgr,
                                             Interpreter* interpreter) {
  auto ss = ServerState::tlocal();

  if (!interpreter->Exists(sha)) {
    auto script_data = script_mgr->Find(sha);
    if (!script_data)
      return std::nullopt;

    string err;
    Interpreter::AddResult add_res = interpreter->AddFunction(sha, script_data->body, &err);
    if (add_res != Interpreter::ADD_OK) {
      LOG(ERROR) << "Error adding " << sha << " to database, err " << err;
      return std::nullopt;
    }

    return script_data;
  }

  return ss->GetScriptParams(sha);
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

static bool CanRunSingleShardMulti(optional<ShardId> sid, const ScriptMgr::ScriptParams& params,
                                   const Transaction& tx) {
  if (!sid.has_value()) {
    return false;
  }

  if (DetermineMultiMode(params) != Transaction::LOCK_AHEAD) {
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
                           SinkReplyBuilder* builder, ConnectionContext* cntx, bool read_only) {
  DCHECK(!eval_args.sha.empty());

  // Sanitizing the input to avoid code injection.
  if (eval_args.sha.size() != 40 || !IsSHA(eval_args.sha)) {
    return builder->SendError(facade::kScriptNotFound);
  }

  auto params = LoadScript(eval_args.sha, server_family_.script_mgr(), interpreter);
  if (!params)
    return builder->SendError(facade::kScriptNotFound);

  string error;

  DCHECK(!cntx->conn_state.script_info);  // we should not call eval from the script.

  // TODO: to determine whether the script is RO by scanning all "redis.p?call" calls
  // and checking whether all invocations consist of RO commands.
  // we can do it once during script insertion into script mgr.
  auto& sinfo = cntx->conn_state.script_info;
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

  sinfo->async_cmds_heap_limit = absl::GetFlag(FLAGS_multi_eval_squash_buffer);
  Transaction* tx = cntx->transaction;
  CHECK(tx != nullptr);

  interpreter->SetGlobalArray("KEYS", eval_args.keys);
  interpreter->SetGlobalArray("ARGV", eval_args.args);

  absl::Cleanup clean = [interpreter, &sinfo]() {
    interpreter->ResetStack();
    sinfo.reset();
  };

  Interpreter::RunResult result;

  if (CanRunSingleShardMulti(sid, *params, *tx)) {
    // If script runs on a single shard, we run it remotely to save hops.
    interpreter->SetRedisFunc([cntx, this](Interpreter::CallArgs args) {
      // Disable squashing, as we're using the squashing mechanism to run remotely.
      args.async = false;
      CallFromScript(cntx, args);
    });

    ++ServerState::tlocal()->stats.eval_shardlocal_coordination_cnt;
    tx->PrepareMultiForScheduleSingleHop(cntx->ns, *sid, cntx->db_index(), args);
    tx->ScheduleSingleHop([&](Transaction*, EngineShard*) {
      boost::intrusive_ptr<Transaction> stub_tx =
          new Transaction{tx, *sid, slot_checker.GetUniqueSlotId()};
      cntx->transaction = stub_tx.get();

      result = interpreter->RunFunction(eval_args.sha, &error);

      cntx->transaction = tx;
      return OpStatus::OK;
    });

    if (*sid != ServerState::tlocal()->thread_index()) {
      VLOG(2) << "Migrating connection " << cntx->conn() << " from "
              << ProactorBase::me()->GetPoolIndex() << " to " << *sid;
      cntx->conn()->RequestAsyncMigration(shard_set->pool()->at(*sid));
    }
  } else {
    Transaction::MultiMode script_mode = DetermineMultiMode(*params);
    Transaction::MultiMode tx_mode = tx->GetMultiMode();
    bool scheduled = false;

    // Check if eval is already part of a running multi transaction
    if (tx_mode != Transaction::NOT_DETERMINED) {
      if (tx_mode > script_mode) {
        string err = StrCat(
            "Multi mode conflict when running eval in multi transaction. Multi mode is: ", tx_mode,
            " eval mode is: ", script_mode);
        return builder->SendError(err);
      }
    } else {
      scheduled = StartMulti(cntx, script_mode, eval_args.keys);
    }

    ++ServerState::tlocal()->stats.eval_io_coordination_cnt;
    interpreter->SetRedisFunc(
        [cntx, this](Interpreter::CallArgs args) { CallFromScript(cntx, args); });

    result = interpreter->RunFunction(eval_args.sha, &error);

    if (auto err = FlushEvalAsyncCmds(cntx, true); err) {
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
    return builder->SendError(resp, facade::kScriptErrType);
  }

  CHECK(result == Interpreter::RUN_OK);

  SinkReplyBuilder::ReplyAggregator agg(builder);
  EvalSerializer ser{static_cast<RedisReplyBuilder*>(builder)};
  if (!interpreter->IsResultSafe()) {
    builder->SendError("reached lua stack limit");
  } else {
    interpreter->SerializeResult(&ser);
  }
}

void Service::Discard(CmdArgList args, const CommandContext& cmd_cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);

  if (!cmd_cntx.conn_cntx->conn_state.exec_info.IsCollecting()) {
    return cmd_cntx.rb->SendError("DISCARD without MULTI");
  }

  MultiCleanup(cmd_cntx.conn_cntx);
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

void Service::Exec(CmdArgList args, const CommandContext& cmd_cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  auto& exec_info = cmd_cntx.conn_cntx->conn_state.exec_info;
  auto* cntx = cmd_cntx.conn_cntx;

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

  cntx->last_command_debug.exec_body_len = exec_info.body.size();

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
        BorrowedInterpreter(cmd_cntx.tx, &cntx->conn_state).Release();
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
    cmd_cntx.tx->UnlockMulti();
    return rb->SendNull();
  }

  exec_info.state = ConnectionState::ExecInfo::EXEC_RUNNING;

  VLOG(2) << "StartExec " << exec_info.body.size();

  // Make sure we flush whatever responses we aggregated in the reply builder.
  SinkReplyBuilder::ReplyAggregator agg(rb);
  rb->StartArray(exec_info.body.size());

  if (!exec_info.body.empty()) {
    string descr = CreateExecDescriptor(exec_info.body, cmd_cntx.tx->GetUniqueShardCnt());
    ServerState::tlocal()->exec_freq_count[descr]++;

    if (absl::GetFlag(FLAGS_multi_exec_squash) && state != ExecScriptUse::SCRIPT_RUN &&
        !cntx->conn_state.tracking_info_.IsTrackingOn()) {
      MultiCommandSquasher::Opts opts;
      opts.max_squash_size = ServerState::tlocal()->max_squash_cmd_num;
      MultiCommandSquasher::Execute(absl::MakeSpan(exec_info.body), rb, cntx, this, opts);
    } else {
      CmdArgVec arg_vec;
      for (const auto& scmd : exec_info.body) {
        VLOG(2) << "TX CMD " << scmd.Cid()->name() << " " << scmd.NumArgs();

        cntx->SwitchTxCmd(scmd.Cid());

        CmdArgList args = scmd.ArgList(&arg_vec);

        if (scmd.Cid()->IsTransactional()) {
          OpStatus st = cmd_cntx.tx->InitByArgs(cntx->ns, cntx->conn_state.db_index, args);
          if (st != OpStatus::OK) {
            rb->SendError(st);
            break;
          }
        }

        bool ok = InvokeCmd(scmd.Cid(), args, cmd_cntx);
        if (!ok || rb->GetError())  // checks for i/o error, not logical error.
          break;
      }
    }
  }

  if (scheduled) {
    VLOG(2) << "Exec unlocking " << exec_info.body.size() << " commands";
    cmd_cntx.tx->UnlockMulti();
  }

  cntx->cid = exec_cid_;
  VLOG(2) << "Exec completed";
}

namespace {
void PublishImpl(bool reject_cluster, CmdArgList args, const CommandContext& cmd_cntx) {
  if (reject_cluster && IsClusterEnabled()) {
    return cmd_cntx.rb->SendError("PUBLISH is not supported in cluster mode yet");
  }
  string_view channel = ArgS(args, 0);
  string_view messages[] = {ArgS(args, 1)};

  auto* cs = ServerState::tlocal()->channel_store();
  cmd_cntx.rb->SendLong(cs->SendMessages(channel, messages));
}

void SubscribeImpl(bool reject_cluster, CmdArgList args, const CommandContext& cmd_cntx) {
  if (reject_cluster && IsClusterEnabled()) {
    return cmd_cntx.rb->SendError("SUBSCRIBE is not supported in cluster mode yet");
  }
  cmd_cntx.conn_cntx->ChangeSubscription(true /*add*/, true /* reply*/, args,
                                         static_cast<RedisReplyBuilder*>(cmd_cntx.rb));
}

void UnSubscribeImpl(bool reject_cluster, CmdArgList args, const CommandContext& cmd_cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  if (reject_cluster && IsClusterEnabled()) {
    return cmd_cntx.rb->SendError("UNSUBSCRIBE is not supported in cluster mode yet");
  }

  if (args.size() == 0) {
    cmd_cntx.conn_cntx->UnsubscribeAll(true, rb);
  } else {
    cmd_cntx.conn_cntx->ChangeSubscription(false, true, args, rb);
  }
}

}  // namespace

void Service::Publish(CmdArgList args, const CommandContext& cmd_cntx) {
  PublishImpl(true, args, cmd_cntx);
}

void Service::SPublish(CmdArgList args, const CommandContext& cmd_cntx) {
  PublishImpl(false, args, cmd_cntx);
}

void Service::Subscribe(CmdArgList args, const CommandContext& cmd_cntx) {
  SubscribeImpl(true, args, cmd_cntx);
}

void Service::SSubscribe(CmdArgList args, const CommandContext& cmd_cntx) {
  SubscribeImpl(false, args, cmd_cntx);
}

void Service::Unsubscribe(CmdArgList args, const CommandContext& cmd_cntx) {
  UnSubscribeImpl(true, args, cmd_cntx);
}

void Service::SUnsubscribe(CmdArgList args, const CommandContext& cmd_cntx) {
  UnSubscribeImpl(false, args, cmd_cntx);
}

void Service::PSubscribe(CmdArgList args, const CommandContext& cmd_cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);

  if (IsClusterEnabled()) {
    return rb->SendError("PSUBSCRIBE is not supported in cluster mode yet");
  }
  cmd_cntx.conn_cntx->ChangePSubscription(true, true, args, rb);
}

void Service::PUnsubscribe(CmdArgList args, const CommandContext& cmd_cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);

  if (IsClusterEnabled()) {
    return rb->SendError("PUNSUBSCRIBE is not supported in cluster mode yet");
  }
  if (args.size() == 0) {
    cmd_cntx.conn_cntx->PUnsubscribeAll(true, rb);
  } else {
    cmd_cntx.conn_cntx->ChangePSubscription(false, true, args, rb);
  }
}

// Not a real implementation. Serves as a decorator to accept some function commands
// for testing.
void Service::Function(CmdArgList args, const CommandContext& cmd_cntx) {
  string sub_cmd = absl::AsciiStrToUpper(ArgS(args, 0));

  if (sub_cmd == "FLUSH") {
    return cmd_cntx.rb->SendOk();
  }

  string err = UnknownSubCmd(sub_cmd, "FUNCTION");
  return cmd_cntx.rb->SendError(err, kSyntaxErrType);
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

void Service::Monitor(CmdArgList args, const CommandContext& cmd_cntx) {
  VLOG(1) << "starting monitor on this connection: " << cmd_cntx.conn_cntx->conn()->GetClientId();
  // we are registering the current connection for all threads so they will be aware of
  // this connection, to send to it any command
  cmd_cntx.rb->SendOk();
  cmd_cntx.conn_cntx->ChangeMonitor(true /* start */);
}

void Service::Pubsub(CmdArgList args, const CommandContext& cmd_cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);

  if (args.size() < 1) {
    rb->SendError(WrongNumArgsError(cmd_cntx.conn_cntx->cid->name()));
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

void Service::Command(CmdArgList args, const CommandContext& cmd_cntx) {
  unsigned cmd_cnt = 0;
  registry_.Traverse([&](string_view name, const CommandId& cd) {
    if ((cd.opt_mask() & CO::HIDDEN) == 0) {
      ++cmd_cnt;
    }
  });

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  auto serialize_command = [rb, this](string_view name, const CommandId& cid) {
    rb->StartArray(7);
    rb->SendSimpleString(cid.name());
    rb->SendLong(cid.arity());
    rb->StartArray(CommandId::OptCount(cid.opt_mask()));

    for (uint32_t i = 0; i < 32; ++i) {
      unsigned obit = (1u << i);
      if (cid.opt_mask() & obit) {
        const char* name = CO::OptName(CO::CommandOpt{obit});
        rb->SendSimpleString(name);
      }
    }

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

  if (absl::GetFlag(FLAGS_expose_http_api)) {
    base->RegisterCb("/api",
                     [this](const http::QueryArgs& args, HttpRequest&& req, HttpContext* send) {
                       HttpAPI(args, std::move(req), this, send);
                     });
  }
}

void Service::OnConnectionClose(facade::ConnectionContext* cntx) {
  ConnectionContext* server_cntx = static_cast<ConnectionContext*>(cntx);
  ConnectionState& conn_state = server_cntx->conn_state;

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
  return {.db_index = server_cntx->db_index(),
          .async_dispatch = server_cntx->async_dispatch,
          .conn_closing = server_cntx->conn_closing,
          .subscribers = bool(server_cntx->conn_state.subscribe_info),
          .blocked = server_cntx->blocked};
}

#define HFUNC(x) SetHandler(&Service::x)
#define MFUNC(x) \
  SetHandler([this](CmdArgList sp, const CommandContext& cntx) { this->x(std::move(sp), cntx); })

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
      << CI{"SPUBLISH", CO::LOADING | CO::FAST, 3, 0, 0, acl::kPublish}.MFUNC(SPublish)
      << CI{"SUBSCRIBE", CO::NOSCRIPT | CO::LOADING, -2, 0, 0, acl::kSubscribe}.MFUNC(Subscribe)
      << CI{"SSUBSCRIBE", CO::NOSCRIPT | CO::LOADING, -2, 0, 0, acl::kSubscribe}.MFUNC(SSubscribe)
      << CI{"UNSUBSCRIBE", CO::NOSCRIPT | CO::LOADING, -1, 0, 0, acl::kUnsubscribe}.MFUNC(
             Unsubscribe)
      << CI{"SUNSUBSCRIBE", CO::NOSCRIPT | CO::LOADING, -1, 0, 0, acl::kUnsubscribe}.MFUNC(
             SUnsubscribe)
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
  StreamFamily::Register(&registry_);
  StringFamily::Register(&registry_);
  GenericFamily::Register(&registry_);
  ListFamily::Register(&registry_);
  SetFamily::Register(&registry_);
  HSetFamily::Register(&registry_);
  ZSetFamily::Register(&registry_);
  GeoFamily::Register(&registry_);
  JsonFamily::Register(&registry_);
  BitOpsFamily::Register(&registry_);
  HllFamily::Register(&registry_);
  SearchFamily::Register(&registry_);
  BloomFamily::Register(&registry_);
  server_family_.Register(&registry_);
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

void SetMaxMemoryFlag(uint64_t value) {
  absl::SetFlag(&FLAGS_maxmemory, {value});
}

uint64_t GetMaxMemoryFlag() {
  return absl::GetFlag(FLAGS_maxmemory).value;
}

}  // namespace dfly
