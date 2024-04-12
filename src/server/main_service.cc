// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/main_service.h"

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
#include "server/cluster/cluster_family.h"
#include "server/cluster/unique_slot_checker.h"
#include "server/conn_context.h"
#include "server/error.h"
#include "server/generic_family.h"
#include "server/hll_family.h"
#include "server/hset_family.h"
#include "server/http_api.h"
#include "server/json_family.h"
#include "server/list_family.h"
#include "server/multi_command_squasher.h"
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
using facade::ErrorReply;

ABSL_FLAG(int32_t, port, 6379,
          "Redis port. 0 disables the port, -1 will bind on a random available port.");

ABSL_FLAG(uint32_t, memcached_port, 0, "Memcached port");

ABSL_FLAG(uint32_t, num_shards, 0, "Number of database shards, 0 - to choose automatically");

ABSL_FLAG(uint32_t, multi_exec_mode, 2,
          "Set multi exec atomicity mode: 1 for global, 2 for locking ahead, 3 for non atomic");

ABSL_FLAG(bool, multi_exec_squash, true,
          "Whether multi exec will squash single shard commands to optimize performance");

ABSL_FLAG(bool, track_exec_frequencies, true, "Whether to track exec frequencies for multi exec");
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
ABSL_FLAG(double, oom_deny_ratio, 1.1,
          "commands with flag denyoom will return OOM when the ratio between maxmemory and used "
          "memory is above this value");

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
void UnwatchAllKeys(ConnectionState::ExecInfo* exec_info) {
  if (!exec_info->watched_keys.empty()) {
    auto cb = [&](EngineShard* shard) { shard->db_slice().UnregisterConnectionWatches(exec_info); };
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
  UnwatchAllKeys(&exec_info);
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
    VLOG(1) << "thread " << ProactorBase::me()->GetPoolIndex() << " sending monitor message '"
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

  VLOG(1) << "sending command '" << monitor_msg << "' to the clients that registered on it";

  shard_set->pool()->DispatchBrief(
      [msg = std::move(monitor_msg)](unsigned idx, util::ProactorBase*) { SendMonitor(msg); });
}

class InterpreterReplier : public RedisReplyBuilder {
 public:
  InterpreterReplier(ObjectExplorer* explr) : RedisReplyBuilder(nullptr), explr_(explr) {
  }

  void SendError(std::string_view str, std::string_view type = std::string_view{}) final;
  void SendStored() final;

  void SendSimpleString(std::string_view str) final;
  void SendMGetResponse(MGetResponse resp) final;
  void SendSimpleStrArr(StrSpan arr) final;
  void SendNullArray() final;

  void SendStringArr(StrSpan arr, CollectionType type) final;
  void SendNull() final;

  void SendLong(long val) final;
  void SendDouble(double val) final;

  void SendBulkString(std::string_view str) final;

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
  EvalSerializer(RedisReplyBuilder* rb) : rb_(rb) {
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
      long val = static_cast<long>(floor(d));
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

void InterpreterReplier::SendStored() {
  DCHECK(array_len_.empty());
  SendSimpleString("OK");
}

void InterpreterReplier::SendSimpleString(string_view str) {
  if (array_len_.empty())
    explr_->OnStatus(str);
  else
    explr_->OnString(str);
  PostItem();
}

void InterpreterReplier::SendMGetResponse(MGetResponse resp) {
  DCHECK(array_len_.empty());

  explr_->OnArrayStart(resp.resp_arr.size());
  for (uint32_t i = 0; i < resp.resp_arr.size(); ++i) {
    if (resp.resp_arr[i].has_value()) {
      explr_->OnString(resp.resp_arr[i]->value);
    } else {
      explr_->OnNil();
    }
  }
  explr_->OnArrayEnd();
}

void InterpreterReplier::SendSimpleStrArr(StrSpan arr) {
  WrappedStrSpan warr{arr};
  explr_->OnArrayStart(warr.Size());
  for (unsigned i = 0; i < warr.Size(); i++)
    explr_->OnString(warr[i]);
  explr_->OnArrayEnd();
}

void InterpreterReplier::SendNullArray() {
  SendSimpleStrArr({});
  PostItem();
}

void InterpreterReplier::SendStringArr(StrSpan arr, CollectionType) {
  WrappedStrSpan warr{arr};
  size_t size = warr.Size();
  explr_->OnArrayStart(size);
  for (size_t i = 0; i < size; i++)
    explr_->OnString(warr[i]);
  explr_->OnArrayEnd();
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

void InterpreterReplier::StartCollection(unsigned len, CollectionType) {
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
  for (auto c : str) {
    if (!absl::ascii_isxdigit(c))
      return false;
  }
  return true;
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

void Topkeys(const http::QueryArgs& args, HttpContext* send) {
  http::StringResponse resp = http::MakeStringResponse(h2::status::ok);
  resp.body() = "<h1>Detected top keys</h1>\n<pre>\n";

  std::atomic_bool is_enabled = false;
  if (shard_set) {
    vector<string> rows(shard_set->size());

    shard_set->RunBriefInParallel([&](EngineShard* shard) {
      for (const auto& db : shard->db_slice().databases()) {
        if (db->top_keys.IsEnabled()) {
          is_enabled = true;
          for (const auto& [key, count] : db->top_keys.GetTopKeys()) {
            absl::StrAppend(&resp.body(), key, ":\t", count, "\n");
          }
        }
      }
    });
  }

  resp.body() += "</pre>";

  if (!is_enabled) {
    resp.body() += "<i>TopKeys are disabled.</i>";
  }
  send->Invoke(std::move(resp));
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

void ClusterHtmlPage(const http::QueryArgs& args, HttpContext* send, ClusterFamily* cluster) {
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

  print_kv("Mode", ClusterConfig::IsEmulated()  ? "Emulated"
                   : ClusterConfig::IsEnabled() ? "Enabled"
                                                : "Disabled");

  if (ClusterConfig::IsEnabledOrEmulated()) {
    print_kb("Lock on hashtags", LockTagOptions::instance().enabled);
  }

  if (ClusterConfig::IsEnabled()) {
    if (cluster->cluster_config() == nullptr) {
      resp.body() += "<h2>Not yet configured.</h2>\n";
    } else {
      auto config = cluster->cluster_config()->GetConfig();
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

enum class ExecEvalState {
  NONE = 0,
  ALL = 1,
  SOME = 2,
};

ExecEvalState DetermineEvalPresense(const std::vector<StoredCmd>& body) {
  unsigned eval_cnt = 0;
  for (const auto& scmd : body) {
    if (CO::IsEvalKind(scmd.Cid()->name())) {
      eval_cnt++;
    }
  }

  if (eval_cnt == 0)
    return ExecEvalState::NONE;

  if (eval_cnt == body.size())
    return ExecEvalState::ALL;

  return ExecEvalState::SOME;
}

// Returns the multi mode for that transaction. Returns NOT_DETERMINED if no scheduling
// is required.
Transaction::MultiMode DeduceExecMode(ExecEvalState state,
                                      const ConnectionState::ExecInfo& exec_info,
                                      const ScriptMgr& script_mgr) {
  // Check if script most LIKELY has global eval transactions
  bool contains_global = false;
  Transaction::MultiMode multi_mode =
      static_cast<Transaction::MultiMode>(absl::GetFlag(FLAGS_multi_exec_mode));

  if (state != ExecEvalState::NONE) {
    contains_global = script_mgr.AreGlobalByDefault();
  }

  bool transactional = contains_global;
  if (!transactional) {
    for (const auto& scmd : exec_info.body) {
      transactional |= scmd.Cid()->IsTransactional();
      contains_global |= scmd.Cid()->opt_mask() & CO::GLOBAL_TRANS;

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

  // Atomic modes fall back to GLOBAL if they contain global commands.
  if (contains_global && multi_mode == Transaction::LOCK_AHEAD)
    multi_mode = Transaction::GLOBAL;

  return multi_mode;
}

string CreateExecDescriptor(const std::vector<StoredCmd>& stored_cmds, unsigned num_uniq_shards) {
  string result;
  result.reserve(stored_cmds.size() * 10);
  absl::StrAppend(&result, "EXEC/", num_uniq_shards, "\n");
  for (const auto& scmd : stored_cmds) {
    absl::StrAppend(&result, "  ", scmd.Cid()->name(), " ", scmd.NumArgs(), "\n");
  }
  return result;
}

// Ensures availability of an interpreter for EVAL-like commands and it's automatic release.
// If it's part of MULTI, the preborrowed interpreter is returned, otherwise a new is acquired.
struct BorrowedInterpreter {
  explicit BorrowedInterpreter(ConnectionContext* cntx) {
    // Ensure squashing ignores EVAL. We can't run on a stub context, because it doesn't have our
    // preborrowed interpreter (which can't be shared on multiple threads).
    CHECK(!cntx->conn_state.squashing_info);

    if (auto borrowed = cntx->conn_state.exec_info.preborrowed_interpreter; borrowed) {
      // Ensure a preborrowed interpreter is only set for an already running MULTI transaction.
      CHECK_EQ(cntx->conn_state.exec_info.state, ConnectionState::ExecInfo::EXEC_RUNNING);

      interpreter_ = borrowed;
    } else {
      // A scheduled transaction occupies a place in the transaction queue and holds locks,
      // preventing other transactions from progressing. Blocking below can deadlock!
      CHECK(!cntx->transaction->IsScheduled());

      interpreter_ = ServerState::tlocal()->BorrowInterpreter();
      owned_ = true;
    }
  }

  ~BorrowedInterpreter() {
    if (owned_)
      ServerState::tlocal()->ReturnInterpreter(interpreter_);
  }

  // Give up ownership of the interpreter, it must be returned manually.
  Interpreter* Release() && {
    DCHECK(owned_);
    owned_ = false;
    return interpreter_;
  }

  operator Interpreter*() {
    return interpreter_;
  }

 private:
  Interpreter* interpreter_ = nullptr;
  bool owned_ = false;
};

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
  pp_.GetNextProactor()->RegisterSignal({SIGUSR1}, [this](int signal) {
    LOG(INFO) << "Received " << strsignal(signal);
    util::fb2::Mutex m;
    pp_.AwaitFiberOnAll([&m](unsigned index, util::ProactorBase* base) {
      std::unique_lock lk(m);
      util::fb2::detail::FiberInterface::PrintAllFiberStackTraces();
    });
  });
#endif

  shard_set = new EngineShardSet(pp);

  // We support less than 1024 threads and we support less than 1024 shards.
  // For example, Scan uses 10 bits in cursor to encode shard id it currently traverses.
  CHECK_LT(pp->size(), kMaxThreadSize);
  RegisterCommands();

  exec_cid_ = FindCmd("EXEC");

  engine_varz.emplace("engine", [this] { return GetVarzStats(); });
}

Service::~Service() {
  delete shard_set;
  shard_set = nullptr;
}

void Service::Init(util::AcceptServer* acceptor, std::vector<facade::Listener*> listeners,
                   const InitOpts& opts) {
  InitRedisTables();

  config_registry.RegisterMutable("maxmemory", [](const absl::CommandLineFlag& flag) {
    auto res = flag.TryGet<MemoryBytesFlag>();
    if (!res)
      return false;

    max_memory_limit = res->value;
    return true;
  });

  config_registry.Register("dbnum");  // equivalent to databases in redis.
  config_registry.Register("dir");
  config_registry.RegisterMutable("masterauth");
  config_registry.RegisterMutable("masteruser");
  config_registry.RegisterMutable("tcp_keepalive");
  config_registry.RegisterMutable("replica_partial_sync");
  config_registry.RegisterMutable("max_eviction_per_heartbeat");
  config_registry.RegisterMutable("max_segment_to_consider");
  config_registry.RegisterMutable("enable_heartbeat_eviction");
  config_registry.RegisterMutable("dbfilename");
  config_registry.RegisterMutable("table_growth_margin");

  uint32_t shard_num = GetFlag(FLAGS_num_shards);
  if (shard_num == 0 || shard_num > pp_.size()) {
    LOG_IF(WARNING, shard_num > pp_.size())
        << "Requested num_shards (" << shard_num << ") is bigger than thread count (" << pp_.size()
        << "), using num_shards=" << pp_.size();
    shard_num = pp_.size();
  }

  // Must initialize before the shard_set because EngineShard::Init references ServerState.
  pp_.Await([&](uint32_t index, ProactorBase* pb) {
    tl_facade_stats = new FacadeStats;
    ServerState::Init(index, shard_num, &user_registry_);
  });

  shard_set->Init(shard_num, !opts.disable_time_update);
  const auto tcp_disabled = GetFlag(FLAGS_port) == 0u;
  // We assume that listeners.front() is the main_listener
  // see dfly_main RunEngine
  if (!tcp_disabled && !listeners.empty()) {
    acl_family_.Init(listeners.front(), &user_registry_);
  }

  StringFamily::Init(&pp_);
  GenericFamily::Init(&pp_);
  server_family_.Init(acceptor, std::move(listeners));

  ChannelStore* cs = new ChannelStore{};
  pp_.Await(
      [cs](uint32_t index, ProactorBase* pb) { ServerState::tlocal()->UpdateChannelStore(cs); });
}

void Service::Shutdown() {
  VLOG(1) << "Service::Shutdown";

  // We mark that we are shutting down. After this incoming requests will be
  // rejected
  pp_.AwaitFiberOnAll([](ProactorBase* pb) {
    ServerState::tlocal()->EnterLameDuck();
    facade::Connection::ShutdownThreadLocal();
  });

  config_registry.Reset();

  // to shutdown all the runtime components that depend on EngineShard.
  server_family_.Shutdown();
  StringFamily::Shutdown();
  GenericFamily::Shutdown();

  engine_varz.reset();

  ChannelStore::Destroy();

  shard_set->Shutdown();
  pp_.Await([](ProactorBase* pb) { ServerState::tlocal()->Destroy(); });

  // wait for all the pending callbacks to stop.
  ThisFiber::SleepFor(10ms);
}

optional<ErrorReply> Service::CheckKeysOwnership(const CommandId* cid, CmdArgList args,
                                                 const ConnectionContext& dfly_cntx) {
  if (dfly_cntx.is_replicating) {
    // Always allow commands on the replication port, as it might be for future-owned keys.
    return nullopt;
  }

  if (cid->first_key_pos() == 0) {
    return nullopt;  // No key command.
  }

  OpResult<KeyIndex> key_index_res = DetermineKeys(cid, args);
  if (!key_index_res) {
    return ErrorReply{key_index_res.status()};
  }

  const auto& key_index = *key_index_res;
  optional<SlotId> keys_slot;
  bool cross_slot = false;
  // Iterate keys and check to which slot they belong.
  for (unsigned i = key_index.start; i < key_index.end; i += key_index.step) {
    string_view key = ArgS(args, i);
    SlotId slot = ClusterConfig::KeySlot(key);
    if (keys_slot && slot != *keys_slot) {
      cross_slot = true;  // keys belong to different slots
      break;
    } else {
      keys_slot = slot;
    }
  }

  if (cross_slot) {
    return ErrorReply{"-CROSSSLOT Keys in request don't hash to the same slot"};
  }

  // Check keys slot is in my ownership
  const ClusterConfig* cluster_config = cluster_family_.cluster_config();
  if (cluster_config == nullptr) {
    return ErrorReply{kClusterNotConfigured};
  }

  if (keys_slot.has_value() && !cluster_config->IsMySlot(*keys_slot)) {
    // See more details here: https://redis.io/docs/reference/cluster-spec/#moved-redirection
    ClusterNodeInfo master = cluster_config->GetMasterNodeForSlot(*keys_slot);
    return ErrorReply{absl::StrCat("-MOVED ", *keys_slot, " ", master.ip, ":", master.port)};
  }

  return nullopt;
}

// Return OK if all keys are allowed to be accessed: either declared in EVAL or
// transaction is running in global or non-atomic mode.
OpStatus CheckKeysDeclared(const ConnectionState::ScriptInfo& eval_info, const CommandId* cid,
                           CmdArgList args, Transaction* trans) {
  Transaction::MultiMode multi_mode = trans->GetMultiMode();

  // We either scheduled on all shards or re-schedule for each operation,
  // so we are not restricted to any keys.
  if (multi_mode == Transaction::GLOBAL || multi_mode == Transaction::NON_ATOMIC)
    return OpStatus::OK;

  OpResult<KeyIndex> key_index_res = DetermineKeys(cid, args);
  if (!key_index_res)
    return key_index_res.status();

  // TODO: Switch to transaction internal locked keys once single hop multi transactions are merged
  // const auto& locked_keys = trans->GetMultiKeys();
  const auto& locked_tags = eval_info.lock_tags;

  const auto& key_index = *key_index_res;
  for (unsigned i = key_index.start; i < key_index.end; ++i) {
    LockTag tag{ArgS(args, i)};
    if (!locked_tags.contains(tag)) {
      VLOG(1) << "Key " << string_view(tag) << " is not declared for command " << cid->name();
      return OpStatus::KEY_NOTFOUND;
    }
  }

  if (key_index.bonus && !locked_tags.contains(LockTag{ArgS(args, *key_index.bonus)}))
    return OpStatus::KEY_NOTFOUND;

  return OpStatus::OK;
}

static optional<ErrorReply> VerifyConnectionAclStatus(const CommandId* cid,
                                                      const ConnectionContext* cntx,
                                                      string_view error_msg, CmdArgList tail_args) {
  // If we are on a squashed context we need to use the owner, because the
  // context we are operating on is a stub and the acl username is not copied
  // See: MultiCommandSquasher::SquashedHopCb
  if (cntx->conn_state.squashing_info)
    cntx = cntx->conn_state.squashing_info->owner;

  if (!acl::IsUserAllowedToInvokeCommand(*cntx, *cid, tail_args)) {
    return ErrorReply(absl::StrCat("NOPERM: ", cntx->authed_username, " ", error_msg));
  }
  return nullopt;
}

optional<ErrorReply> Service::VerifyCommandExecution(const CommandId* cid,
                                                     const ConnectionContext* cntx,
                                                     CmdArgList tail_args) {
  ServerState& etl = *ServerState::tlocal();

  if ((cid->opt_mask() & CO::DENYOOM) && etl.is_master) {
    uint64_t start_ns = absl::GetCurrentTimeNanos();

    uint64_t used_memory = etl.GetUsedMemory(start_ns);
    double oom_deny_ratio = GetFlag(FLAGS_oom_deny_ratio);
    if (used_memory > (max_memory_limit * oom_deny_ratio)) {
      etl.stats.oom_error_cmd_cnt++;
      return facade::ErrorReply{kOutOfMemory};
    }
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
    return ErrorReply{"Cannot execute restricted command (admin only)"};
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
      allowed_by_state = !cid->IsWriteOnly();
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
    if (cmd_name == "SELECT" || absl::EndsWith(cmd_name, "SUBSCRIBE"))
      return ErrorReply{absl::StrCat("Can not call ", cmd_name, " within a transaction")};

    if (cmd_name == "WATCH" || cmd_name == "FLUSHALL" || cmd_name == "FLUSHDB")
      return ErrorReply{absl::StrCat("'", cmd_name, "' inside MULTI is not allowed")};
  }

  if (ClusterConfig::IsEnabled()) {
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
  }

  if (under_script && cid->IsTransactional()) {
    OpStatus status =
        CheckKeysDeclared(*dfly_cntx.conn_state.script_info, cid, tail_args, dfly_cntx.transaction);

    if (status == OpStatus::KEY_NOTFOUND)
      return ErrorReply(kUndeclaredKeyErr);

    if (status != OpStatus::OK)
      return ErrorReply{status};
  }

  return VerifyConnectionAclStatus(cid, &dfly_cntx, "has no ACL permissions", tail_args);
}

OpResult<void> OpTrackKeys(const OpArgs& op_args, ConnectionContext* cntx, const ArgSlice& keys) {
  auto& db_slice = op_args.shard->db_slice();
  db_slice.TrackKeys(cntx->conn()->Borrow(), keys);
  return OpStatus::OK;
}

void Service::DispatchCommand(CmdArgList args, facade::ConnectionContext* cntx) {
  CHECK(!args.empty());
  DCHECK_NE(0u, shard_set->size()) << "Init was not called";

  ServerState& etl = *ServerState::tlocal();

  ToUpper(&args[0]);
  const auto [cid, args_no_cmd] = FindCmd(args);

  if (cid == nullptr) {
    return cntx->SendError(ReportUnknownCmd(ArgS(args, 0)));
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
  if (!dispatching_in_multi && (!cntx->conn() || !cntx->conn()->IsPrivileged())) {
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
      LOG(ERROR) << "Tried to reply to REPLCONF";
      return;
    }
    dfly_cntx->SendError(std::move(*err));
    return;
  }

  bool is_trans_cmd = CO::IsTransKind(cid->name());
  if (dfly_cntx->conn_state.exec_info.IsCollecting() && !is_trans_cmd) {
    // TODO: protect against aggregating huge transactions.
    StoredCmd stored_cmd{cid, args_no_cmd};
    dfly_cntx->conn_state.exec_info.body.push_back(std::move(stored_cmd));
    if (stored_cmd.Cid()->IsWriteOnly()) {
      dfly_cntx->conn_state.exec_info.is_write = true;
    }
    return cntx->SendSimpleString("QUEUED");
  }

  // Create command transaction
  intrusive_ptr<Transaction> dist_trans;

  if (dispatching_in_multi) {
    DCHECK(dfly_cntx->transaction);
    if (cid->IsTransactional()) {
      dfly_cntx->transaction->MultiSwitchCmd(cid);
      OpStatus status =
          dfly_cntx->transaction->InitByArgs(dfly_cntx->conn_state.db_index, args_no_cmd);

      if (status != OpStatus::OK)
        return cntx->SendError(status);
    }
  } else {
    DCHECK(dfly_cntx->transaction == nullptr);

    if (cid->IsTransactional()) {
      dist_trans.reset(new Transaction{cid});

      if (!dist_trans->IsMulti()) {  // Multi command initialize themself based on their mode.
        if (auto st = dist_trans->InitByArgs(dfly_cntx->conn_state.db_index, args_no_cmd);
            st != OpStatus::OK)
          return cntx->SendError(st);
      }

      dfly_cntx->transaction = dist_trans.get();
      dfly_cntx->last_command_debug.shards_count = dfly_cntx->transaction->GetUniqueShardCnt();
    } else {
      dfly_cntx->transaction = nullptr;
    }
  }

  dfly_cntx->cid = cid;

  if (!InvokeCmd(cid, args_no_cmd, dfly_cntx)) {
    dfly_cntx->reply_builder()->SendError("Internal Error");
    dfly_cntx->reply_builder()->CloseConnection();
  }

  // if this is a read command, and client tracking has enabled,
  // start tracking all the updates to the keys in this read command
  if ((cid->opt_mask() & CO::READONLY) && dfly_cntx->conn()->IsTrackingOn() &&
      cid->IsTransactional()) {
    auto cb = [&](Transaction* t, EngineShard* shard) {
      auto keys = t->GetShardArgs(shard->shard_id());
      return OpTrackKeys(t->GetOpArgs(shard), dfly_cntx, keys);
    };
    dfly_cntx->transaction->Refurbish();
    dfly_cntx->transaction->ScheduleSingleHopT(cb);
  }

  if (!dispatching_in_multi) {
    dfly_cntx->transaction = nullptr;
  }
}

class ReplyGuard {
 public:
  ReplyGuard(ConnectionContext* cntx, std::string_view cid_name) {
    const bool is_script = bool(cntx->conn_state.script_info);
    const bool is_one_of =
        absl::flat_hash_set<std::string_view>({"REPLCONF", "DFLY"}).contains(cid_name);
    auto* maybe_mcache = dynamic_cast<MCReplyBuilder*>(cntx->reply_builder());
    const bool is_no_reply_memcache =
        maybe_mcache && (maybe_mcache->NoReply() || cid_name == "QUIT");
    const bool should_dcheck = !is_one_of && !is_script && !is_no_reply_memcache;
    if (should_dcheck) {
      builder_ = cntx->reply_builder();
      builder_->ExpectReply();
    }
  }

  ~ReplyGuard() {
    if (builder_) {
      DCHECK(builder_->HasReplied());
    }
  }

 private:
  SinkReplyBuilder* builder_ = nullptr;
};

bool Service::InvokeCmd(const CommandId* cid, CmdArgList tail_args, ConnectionContext* cntx) {
  DCHECK(cid);
  DCHECK(!cid->Validate(tail_args));

  if (auto err = VerifyCommandExecution(cid, cntx, tail_args); err) {
    // We need to skip this because ACK's should not be replied to
    // Bonus points because this allows to continue replication with ACL users who got
    // their access revoked and reinstated
    if (cid->name() == "REPLCONF" && absl::EqualsIgnoreCase(ArgS(tail_args, 0), "ACK")) {
      return true;
    }
    cntx->SendError(std::move(*err));
    return true;  // return false only for internal error aborts
  }

  // We are not sending any admin command in the monitor, and we do not want to
  // do any processing if we don't have any waiting connections with monitor
  // enabled on them - see https://redis.io/commands/monitor/
  if (!ServerState::tlocal()->Monitors().Empty() && (cid->opt_mask() & CO::ADMIN) == 0) {
    DispatchMonitor(cntx, cid, tail_args);
  }

  ServerState::tlocal()->RecordCmd();

#ifndef NDEBUG
  // Verifies that we reply to the client when needed.
  ReplyGuard reply_guard(cntx, cid->name());
#endif
  uint64_t invoke_time_usec = 0;
  try {
    invoke_time_usec = cid->Invoke(tail_args, cntx);
  } catch (std::exception& e) {
    LOG(ERROR) << "Internal error, system probably unstable " << e.what();
    return false;
  }

  // TODO: we should probably discard more commands here,
  // not just the blocking ones
  const auto* conn = cntx->conn();
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

  if (cntx->transaction && !cntx->conn_state.exec_info.IsRunning() &&
      cntx->conn_state.script_info == nullptr) {
    cntx->last_command_debug.clock = cntx->transaction->txid();
  }

  return true;
}

size_t Service::DispatchManyCommands(absl::Span<CmdArgList> args_list,
                                     facade::ConnectionContext* cntx) {
  ConnectionContext* dfly_cntx = static_cast<ConnectionContext*>(cntx);
  DCHECK(!dfly_cntx->conn_state.exec_info.IsRunning());

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
    MultiCommandSquasher::Execute(absl::MakeSpan(stored_cmds), dfly_cntx, this, true, false);
    dfly_cntx->transaction = nullptr;

    dispatched += stored_cmds.size();
    stored_cmds.clear();
  };

  // Don't even start when paused. We can only continue if DispatchTracker is aware of us running.
  if (dfly::ServerState::tlocal()->IsPaused())
    return 0;

  for (auto args : args_list) {
    ToUpper(&args[0]);
    const auto [cid, tail_args] = FindCmd(args);

    // MULTI...EXEC commands need to be collected into a single context, so squashing is not
    // possible
    const bool is_multi =
        dfly_cntx->conn_state.exec_info.IsCollecting() || CO::IsTransKind(ArgS(args, 0));

    // Generally, executing any multi-transactions (including eval) is not possible because they
    // might request a stricter multi mode than non-atomic which is used for squashing.
    // TODO: By allowing promoting non-atomic multit transactions to lock-ahead for specific command
    // invocations, we can potentially execute multiple eval in parallel, which is very powerful
    // paired with shardlocal eval
    const bool is_eval = CO::IsEvalKind(ArgS(args, 0));

    const bool is_blocking = cid != nullptr && cid->IsBlocking();

    if (!is_multi && !is_eval && !is_blocking && cid != nullptr) {
      stored_cmds.reserve(args_list.size());
      stored_cmds.emplace_back(cid, tail_args);
      continue;
    }

    // Squash accumulated commands
    perform_squash();

    // Stop accumulating when a pause is requested, fall back to regular dispatch
    if (dfly::ServerState::tlocal()->IsPaused())
      break;

    // Dispatch non squashed command only after all squshed commands were executed and replied
    DispatchCommand(args, cntx);
    dispatched++;
  }

  perform_squash();

  if (dist_trans)
    dist_trans->UnlockMulti();

  return dispatched;
}

void Service::DispatchMC(const MemcacheParser::Command& cmd, std::string_view value,
                         facade::ConnectionContext* cntx) {
  absl::InlinedVector<MutableSlice, 8> args;
  char cmd_name[16];
  char ttl[16];
  char store_opt[32] = {0};
  char ttl_op[] = "EX";

  MCReplyBuilder* mc_builder = static_cast<MCReplyBuilder*>(cntx->reply_builder());
  mc_builder->SetNoreply(cmd.no_reply);

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
      strcpy(cmd_name, "MGET");
      break;
    case MemcacheParser::FLUSHALL:
      strcpy(cmd_name, "FLUSHDB");
      break;
    case MemcacheParser::QUIT:
      strcpy(cmd_name, "QUIT");
      break;
    case MemcacheParser::STATS:
      server_family_.StatsMC(cmd.key, cntx);
      return;
    case MemcacheParser::VERSION:
      mc_builder->SendSimpleString("VERSION 1.5.0 DF");
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

    if (cmd.expire_ts && memcmp(cmd_name, "SET", 3) == 0) {
      char* next = absl::numbers_internal::FastIntToBuffer(cmd.expire_ts, ttl);
      args.emplace_back(ttl_op, 2);
      args.emplace_back(ttl, next - ttl);
    }
    dfly_cntx->conn_state.memcache_flag = cmd.flags;
  } else if (cmd.type < MemcacheParser::QUIT) {  // read commands
    for (auto s : cmd.keys_ext) {
      char* key = const_cast<char*>(s.data());
      args.emplace_back(key, s.size());
    }
  } else {  // write commands.
    if (store_opt[0]) {
      args.emplace_back(store_opt, strlen(store_opt));
    }
  }

  DispatchCommand(CmdArgList{args}, cntx);

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

facade::ConnectionContext* Service::CreateContext(util::FiberSocketBase* peer,
                                                  facade::Connection* owner) {
  ConnectionContext* res = new ConnectionContext{peer, owner};

  if (peer->IsUDS()) {
    res->req_auth = false;
  } else if (owner->IsPrivileged() && RequirePrivilegedAuth()) {
    res->req_auth = !GetPassword().empty();
  } else if (!owner->IsPrivileged()) {
    res->req_auth = !user_registry_.AuthUser("default", "");
  }

  // a bit of a hack. I set up breaker callback here for the owner.
  // Should work though it's confusing to have it here.
  owner->RegisterBreakHook([res, this](uint32_t) {
    if (res->transaction)
      res->transaction->CancelBlocking(nullptr);
    this->server_family().BreakOnShutdown();
  });

  return res;
}

const CommandId* Service::FindCmd(std::string_view cmd) const {
  return registry_.Find(registry_.RenamedOrOriginal(cmd));
}

bool Service::IsLocked(DbIndex db_index, std::string_view key) const {
  ShardId sid = Shard(key, shard_count());
  bool is_open = pp_.at(sid)->AwaitBrief([db_index, key] {
    return EngineShard::tlocal()->db_slice().CheckLock(IntentLock::EXCLUSIVE, db_index, key);
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

void Service::Quit(CmdArgList args, ConnectionContext* cntx) {
  if (cntx->protocol() == facade::Protocol::REDIS)
    cntx->SendOk();
  using facade::SinkReplyBuilder;
  SinkReplyBuilder* builder = cntx->reply_builder();
  builder->CloseConnection();

  DeactivateMonitoring(static_cast<ConnectionContext*>(cntx));
  cntx->conn()->ShutdownSelf();
}

void Service::Multi(CmdArgList args, ConnectionContext* cntx) {
  if (cntx->conn_state.exec_info.IsCollecting()) {
    return cntx->SendError("MULTI calls can not be nested");
  }
  cntx->conn_state.exec_info.state = ConnectionState::ExecInfo::EXEC_COLLECT;
  // TODO: to protect against huge exec transactions.
  return cntx->SendOk();
}

void Service::Watch(CmdArgList args, ConnectionContext* cntx) {
  auto& exec_info = cntx->conn_state.exec_info;

  // Skip if EXEC will already fail due previous WATCH.
  if (exec_info.watched_dirty.load(memory_order_relaxed)) {
    return cntx->SendOk();
  }

  atomic_uint32_t keys_existed = 0;
  auto cb = [&](Transaction* t, EngineShard* shard) {
    ArgSlice largs = t->GetShardArgs(shard->shard_id());
    for (auto k : largs) {
      shard->db_slice().RegisterWatchedKey(cntx->db_index(), k, &exec_info);
    }

    auto res = GenericFamily::OpExists(t->GetOpArgs(shard), largs);
    keys_existed.fetch_add(res.value_or(0), memory_order_relaxed);
    return OpStatus::OK;
  };
  cntx->transaction->ScheduleSingleHop(std::move(cb));

  // Duplicate keys are stored to keep correct count.
  exec_info.watched_existed += keys_existed.load(memory_order_relaxed);
  for (size_t i = 0; i < args.size(); i++) {
    exec_info.watched_keys.emplace_back(cntx->db_index(), ArgS(args, i));
  }

  return cntx->SendOk();
}

void Service::Unwatch(CmdArgList args, ConnectionContext* cntx) {
  UnwatchAllKeys(&cntx->conn_state.exec_info);
  return cntx->SendOk();
}

template <typename F> void WithReplies(CapturingReplyBuilder* crb, ConnectionContext* cntx, F&& f) {
  SinkReplyBuilder* old_rrb = nullptr;
  old_rrb = cntx->Inject(crb);
  f();
  cntx->Inject(old_rrb);
}

optional<CapturingReplyBuilder::Payload> Service::FlushEvalAsyncCmds(ConnectionContext* cntx,
                                                                     bool force) {
  auto& info = cntx->conn_state.script_info;

  size_t used_mem = info->async_cmds_heap_mem + info->async_cmds.size() * sizeof(StoredCmd);
  if ((info->async_cmds.empty() || !force) && used_mem < info->async_cmds_heap_limit)
    return nullopt;

  ++ServerState::tlocal()->stats.eval_squashed_flushes;

  auto* eval_cid = registry_.Find("EVAL");
  DCHECK(eval_cid);
  cntx->transaction->MultiSwitchCmd(eval_cid);

  CapturingReplyBuilder crb{ReplyMode::ONLY_ERR};
  WithReplies(&crb, cntx, [&] {
    MultiCommandSquasher::Execute(absl::MakeSpan(info->async_cmds), cntx, this, true, true);
  });

  info->async_cmds_heap_mem = 0;
  info->async_cmds.clear();

  auto reply = crb.Take();
  return CapturingReplyBuilder::GetError(reply) ? make_optional(std::move(reply)) : nullopt;
}

void Service::CallFromScript(ConnectionContext* cntx, Interpreter::CallArgs& ca) {
  DCHECK(cntx->transaction);
  DVLOG(2) << "CallFromScript " << ArgS(ca.args, 0);

  InterpreterReplier replier(ca.translator);
  facade::SinkReplyBuilder* orig = cntx->Inject(&replier);
  absl::Cleanup clean = [orig, cntx] { cntx->Inject(orig); };

  optional<ErrorReply> findcmd_err;

  if (ca.async) {
    auto& info = cntx->conn_state.script_info;
    ToUpper(&ca.args[0]);

    // Full command verification happens during squashed execution
    if (auto* cid = registry_.Find(ArgS(ca.args, 0)); cid != nullptr) {
      auto replies = ca.error_abort ? ReplyMode::ONLY_ERR : ReplyMode::NONE;
      info->async_cmds.emplace_back(std::move(*ca.buffer), cid, ca.args.subspan(1), replies);
      info->async_cmds_heap_mem += info->async_cmds.back().UsedMemory();
    } else if (ca.error_abort) {  // If we don't abort on errors, we can ignore it completely
      findcmd_err = ReportUnknownCmd(ArgS(ca.args, 0));
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

  DispatchCommand(ca.args, cntx);
}

void Service::Eval(CmdArgList args, ConnectionContext* cntx) {
  string_view body = ArgS(args, 0);

  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  if (body.empty()) {
    return rb->SendNull();
  }

  BorrowedInterpreter interpreter{cntx};
  auto res = server_family_.script_mgr()->Insert(body, interpreter);
  if (!res)
    return rb->SendError(res.error().Format(), facade::kScriptErrType);

  string sha{std::move(res.value())};

  CallSHA(args, sha, interpreter, cntx);
}

void Service::EvalSha(CmdArgList args, ConnectionContext* cntx) {
  ToLower(&args[0]);
  string_view sha = ArgS(args, 0);

  BorrowedInterpreter interpreter{cntx};
  CallSHA(args, sha, interpreter, cntx);
}

void Service::CallSHA(CmdArgList args, string_view sha, Interpreter* interpreter,
                      ConnectionContext* cntx) {
  uint32_t num_keys;
  CHECK(absl::SimpleAtoi(ArgS(args, 1), &num_keys));  // we already validated this

  EvalArgs ev_args;
  ev_args.sha = sha;
  ev_args.keys = args.subspan(2, num_keys);
  ev_args.args = args.subspan(2 + num_keys);

  uint64_t start = absl::GetCurrentTimeNanos();
  EvalInternal(args, ev_args, interpreter, cntx);

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
    CHECK_EQ(Interpreter::ADD_OK, interpreter->AddFunction(sha, script_data->body, &err));
    CHECK(err.empty()) << err;

    return script_data;
  }

  auto params = ss->GetScriptParams(sha);
  CHECK(params);  // We update all caches from script manager
  return params;
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

// Start multi transaction for eval. Returns true if transaction was scheduled.
// Skips scheduling if multi mode requires declaring keys, but no keys were declared.
// Return nullopt if eval runs inside multi and conflicts with multi mode
optional<bool> StartMultiEval(DbIndex dbid, CmdArgList keys, ScriptMgr::ScriptParams params,
                              ConnectionContext* cntx) {
  Transaction* trans = cntx->transaction;
  Transaction::MultiMode script_mode = DetermineMultiMode(params);
  Transaction::MultiMode multi_mode = trans->GetMultiMode();
  // Check if eval is already part of a running multi transaction
  if (multi_mode != Transaction::NOT_DETERMINED) {
    if (multi_mode > script_mode) {
      string err = StrCat(
          "Multi mode conflict when running eval in multi transaction. Multi mode is: ", multi_mode,
          " eval mode is: ", script_mode);
      cntx->SendError(err);
      return nullopt;
    }
    return false;
  }

  if (keys.empty() && script_mode == Transaction::LOCK_AHEAD)
    return false;

  switch (script_mode) {
    case Transaction::GLOBAL:
      trans->StartMultiGlobal(dbid);
      return true;
    case Transaction::LOCK_AHEAD:
      trans->StartMultiLockedAhead(dbid, CmdArgVec{keys.begin(), keys.end()});
      return true;
    case Transaction::NON_ATOMIC:
      trans->StartMultiNonAtomic();
      return true;
    default:
      CHECK(false) << "Invalid mode";
  };

  return false;
}

static std::string FullAclCommandFromArgs(CmdArgList args, std::string_view name) {
  ToUpper(&args[1]);
  auto res = absl::StrCat(name, " ", ArgS(args, 1));
  return res;
}

std::pair<const CommandId*, CmdArgList> Service::FindCmd(CmdArgList args) const {
  const std::string_view command = facade::ToSV(args[0]);
  std::string_view acl = "ACL";
  if (command == registry_.RenamedOrOriginal(acl)) {
    if (args.size() == 1) {
      return {registry_.Find(ArgS(args, 0)), args};
    }
    return {registry_.Find(FullAclCommandFromArgs(args, command)), args.subspan(2)};
  }

  const CommandId* res = registry_.Find(ArgS(args, 0));
  if (!res)
    return {nullptr, args};

  // A workaround for XGROUP HELP that does not fit our static taxonomy of commands.
  if (args.size() == 2 && res->name() == "XGROUP") {
    if (absl::EqualsIgnoreCase(ArgS(args, 1), "HELP")) {
      res = registry_.Find("_XGROUP_HELP");
    }
  }
  return {res, args.subspan(1)};
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
                           ConnectionContext* cntx) {
  DCHECK(!eval_args.sha.empty());

  // Sanitizing the input to avoid code injection.
  if (eval_args.sha.size() != 40 || !IsSHA(eval_args.sha)) {
    return cntx->SendError(facade::kScriptNotFound);
  }

  auto params = LoadScript(eval_args.sha, server_family_.script_mgr(), interpreter);
  if (!params)
    return cntx->SendError(facade::kScriptNotFound);

  string error;

  DCHECK(!cntx->conn_state.script_info);  // we should not call eval from the script.

  // TODO: to determine whether the script is RO by scanning all "redis.p?call" calls
  // and checking whether all invocations consist of RO commands.
  // we can do it once during script insertion into script mgr.
  auto& sinfo = cntx->conn_state.script_info;
  sinfo = make_unique<ConnectionState::ScriptInfo>();
  sinfo->lock_tags.reserve(eval_args.keys.size());

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
    tx->PrepareMultiForScheduleSingleHop(*sid, tx->GetDbIndex(), args);
    tx->ScheduleSingleHop([&](Transaction*, EngineShard*) {
      boost::intrusive_ptr<Transaction> stub_tx =
          new Transaction{tx, *sid, slot_checker.GetUniqueSlotId()};
      cntx->transaction = stub_tx.get();

      result = interpreter->RunFunction(eval_args.sha, &error);
      cntx->transaction->FIX_ConcludeJournalExec();  // flush journal

      cntx->transaction = tx;
      return OpStatus::OK;
    });

    if (*sid != ServerState::tlocal()->thread_index()) {
      VLOG(1) << "Migrating connection " << cntx->conn() << " from "
              << ProactorBase::me()->GetPoolIndex() << " to " << *sid;
      cntx->conn()->RequestAsyncMigration(shard_set->pool()->at(*sid));
    }
  } else {
    optional<bool> scheduled = StartMultiEval(cntx->db_index(), eval_args.keys, *params, cntx);
    if (!scheduled) {
      return;
    }

    ++ServerState::tlocal()->stats.eval_io_coordination_cnt;
    interpreter->SetRedisFunc(
        [cntx, this](Interpreter::CallArgs args) { CallFromScript(cntx, args); });

    result = interpreter->RunFunction(eval_args.sha, &error);

    if (auto err = FlushEvalAsyncCmds(cntx, true); err) {
      auto err_ref = CapturingReplyBuilder::GetError(*err);
      result = Interpreter::RUN_ERR;
      error = absl::StrCat(err_ref->first);
    }

    // Conclude the transaction.
    if (*scheduled)
      cntx->transaction->UnlockMulti();
  }

  if (result == Interpreter::RUN_ERR) {
    string resp = StrCat("Error running script (call to ", eval_args.sha, "): ", error);
    server_family_.script_mgr()->OnScriptError(eval_args.sha, error);
    return cntx->SendError(resp, facade::kScriptErrType);
  }

  CHECK(result == Interpreter::RUN_OK);

  SinkReplyBuilder::ReplyAggregator agg(cntx->reply_builder());
  EvalSerializer ser{static_cast<RedisReplyBuilder*>(cntx->reply_builder())};
  if (!interpreter->IsResultSafe()) {
    cntx->SendError("reached lua stack limit");
  } else {
    interpreter->SerializeResult(&ser);
  }
}

void Service::Discard(CmdArgList args, ConnectionContext* cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());

  if (!cntx->conn_state.exec_info.IsCollecting()) {
    return rb->SendError("DISCARD without MULTI");
  }

  MultiCleanup(cntx);
  rb->SendOk();
}

// Return true if non of the connections watched keys expired.
bool CheckWatchedKeyExpiry(ConnectionContext* cntx, const CommandRegistry& registry) {
  static char EXISTS[] = "EXISTS";
  auto& exec_info = cntx->conn_state.exec_info;

  CmdArgVec str_list(exec_info.watched_keys.size());
  for (size_t i = 0; i < str_list.size(); i++) {
    auto& [db, s] = exec_info.watched_keys[i];
    str_list[i] = MutableSlice{s.data(), s.size()};
  }

  atomic_uint32_t watch_exist_count{0};
  auto cb = [&watch_exist_count](Transaction* t, EngineShard* shard) {
    ArgSlice args = t->GetShardArgs(shard->shard_id());
    auto res = GenericFamily::OpExists(t->GetOpArgs(shard), args);
    watch_exist_count.fetch_add(res.value_or(0), memory_order_relaxed);

    return OpStatus::OK;
  };

  cntx->transaction->MultiSwitchCmd(registry.Find(EXISTS));
  cntx->transaction->InitByArgs(cntx->conn_state.db_index, CmdArgList{str_list});
  OpStatus status = cntx->transaction->ScheduleSingleHop(std::move(cb));
  CHECK_EQ(OpStatus::OK, status);

  // The comparison can still be true even if a key expired due to another one being created.
  // So we have to check the watched_dirty flag, which is set if a key expired.
  return watch_exist_count.load() == exec_info.watched_existed &&
         !exec_info.watched_dirty.load(memory_order_relaxed);
}

// Check if exec_info watches keys on dbs other than db_indx.
bool IsWatchingOtherDbs(DbIndex db_indx, const ConnectionState::ExecInfo& exec_info) {
  for (const auto& [key_db, _] : exec_info.watched_keys) {
    if (key_db != db_indx) {
      return true;
    }
  }
  return false;
}

template <typename F> void IterateAllKeys(ConnectionState::ExecInfo* exec_info, F&& f) {
  for (auto& [dbid, key] : exec_info->watched_keys)
    f(MutableSlice{key.data(), key.size()});

  CmdArgVec arg_vec{};

  for (auto& scmd : exec_info->body) {
    if (!scmd.Cid()->IsTransactional())
      continue;

    scmd.Fill(&arg_vec);

    auto key_res = DetermineKeys(scmd.Cid(), absl::MakeSpan(arg_vec));
    if (!key_res.ok())
      continue;

    auto key_index = key_res.value();

    for (unsigned i = key_index.start; i < key_index.end; i += key_index.step)
      f(arg_vec[i]);

    if (key_index.bonus)
      f(arg_vec[*key_index.bonus]);
  }
}

CmdArgVec CollectAllKeys(ConnectionState::ExecInfo* exec_info) {
  CmdArgVec out;
  out.reserve(exec_info->watched_keys.size() + exec_info->body.size());

  IterateAllKeys(exec_info, [&out](MutableSlice key) { out.push_back(key); });

  return out;
}

// Return true if transaction was scheduled, false if scheduling was not required.
void StartMultiExec(DbIndex dbid, Transaction* trans, ConnectionState::ExecInfo* exec_info,
                    Transaction::MultiMode multi_mode) {
  switch (multi_mode) {
    case Transaction::GLOBAL:
      trans->StartMultiGlobal(dbid);
      break;
    case Transaction::LOCK_AHEAD:
      trans->StartMultiLockedAhead(dbid, CollectAllKeys(exec_info));
      break;
    case Transaction::NON_ATOMIC:
      trans->StartMultiNonAtomic();
      break;
    case Transaction::NOT_DETERMINED:
      LOG(FATAL) << "should not reach";
  };
}

void Service::Exec(CmdArgList args, ConnectionContext* cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  auto& exec_info = cntx->conn_state.exec_info;

  // Clean the context no matter the outcome
  absl::Cleanup exec_clear = [&cntx] { MultiCleanup(cntx); };

  // Check basic invariants
  if (!exec_info.IsCollecting()) {
    return rb->SendError("EXEC without MULTI");
  }

  if (IsWatchingOtherDbs(cntx->db_index(), exec_info)) {
    return rb->SendError("Dragonfly does not allow WATCH and EXEC on different databases");
  }

  if (exec_info.state == ConnectionState::ExecInfo::EXEC_ERROR) {
    return rb->SendError("-EXECABORT Transaction discarded because of previous errors");
  }

  if (exec_info.watched_dirty.load(memory_order_relaxed)) {
    return rb->SendNull();
  }

  cntx->last_command_debug.exec_body_len = exec_info.body.size();

  // The transaction can contain scripts, determine their presence ahead to customize logic below.
  ExecEvalState state = DetermineEvalPresense(exec_info.body);

  // We borrow a single interpreter for all the EVALs inside. Returned by MultiCleanup
  if (state != ExecEvalState::NONE) {
    exec_info.preborrowed_interpreter = BorrowedInterpreter(cntx).Release();
  }

  // Determine according multi mode, not only only flag, but based on presence of global commands
  // and scripts
  Transaction::MultiMode multi_mode = DeduceExecMode(state, exec_info, *script_mgr());

  bool scheduled = false;
  if (multi_mode != Transaction::NOT_DETERMINED) {
    StartMultiExec(cntx->db_index(), cntx->transaction, &exec_info, multi_mode);
    scheduled = true;
  }

  // EXEC should not run if any of the watched keys expired.
  if (!exec_info.watched_keys.empty() && !CheckWatchedKeyExpiry(cntx, registry_)) {
    cntx->transaction->UnlockMulti();
    return rb->SendNull();
  }

  exec_info.state = ConnectionState::ExecInfo::EXEC_RUNNING;

  VLOG(1) << "StartExec " << exec_info.body.size();

  // Make sure we flush whatever responses we aggregated in the reply builder.
  SinkReplyBuilder::ReplyAggregator agg(rb);
  rb->StartArray(exec_info.body.size());

  if (!exec_info.body.empty()) {
    if (GetFlag(FLAGS_track_exec_frequencies)) {
      string descr = CreateExecDescriptor(exec_info.body, cntx->transaction->GetUniqueShardCnt());
      ServerState::tlocal()->exec_freq_count[descr]++;
    }

    if (absl::GetFlag(FLAGS_multi_exec_squash) && state == ExecEvalState::NONE) {
      MultiCommandSquasher::Execute(absl::MakeSpan(exec_info.body), cntx, this);
    } else {
      CmdArgVec arg_vec;
      for (auto& scmd : exec_info.body) {
        VLOG(2) << "TX CMD " << scmd.Cid()->name() << " " << scmd.NumArgs();

        cntx->transaction->MultiSwitchCmd(scmd.Cid());
        cntx->cid = scmd.Cid();

        arg_vec.resize(scmd.NumArgs());
        scmd.Fill(&arg_vec);

        CmdArgList args = absl::MakeSpan(arg_vec);

        if (scmd.Cid()->IsTransactional()) {
          OpStatus st = cntx->transaction->InitByArgs(cntx->conn_state.db_index, args);
          if (st != OpStatus::OK) {
            cntx->SendError(st);
            break;
          }
        }

        bool ok = InvokeCmd(scmd.Cid(), args, cntx);
        if (!ok || rb->GetError())  // checks for i/o error, not logical error.
          break;
      }
    }
  }

  if (scheduled) {
    VLOG(1) << "Exec unlocking " << exec_info.body.size() << " commands";
    cntx->transaction->UnlockMulti();
  }

  cntx->cid = exec_cid_;
  VLOG(1) << "Exec completed";
}

void Service::Publish(CmdArgList args, ConnectionContext* cntx) {
  string_view channel = ArgS(args, 0);
  string_view msg = ArgS(args, 1);

  auto* cs = ServerState::tlocal()->channel_store();
  vector<ChannelStore::Subscriber> subscribers = cs->FetchSubscribers(channel);
  int num_published = subscribers.size();

  if (!subscribers.empty()) {
    // Make sure neither of the threads limits is reached.
    // This check actually doesn't reserve any memory ahead and doesn't prevent the buffer
    // from eventually filling up, especially if multiple clients are unblocked simultaneously,
    // but is generally good enough to limit too fast producers.
    // Most importantly, this approach allows not blocking and not awaiting in the dispatch below,
    // thus not adding any overhead to backpressure checks.
    optional<uint32_t> last_thread;
    for (auto& sub : subscribers) {
      DCHECK_LE(last_thread.value_or(0), sub.Thread());
      if (last_thread && *last_thread == sub.Thread())  // skip same thread
        continue;

      if (sub.EnsureMemoryBudget())  // Invalid pointers are skipped
        last_thread = sub.Thread();
    }

    auto subscribers_ptr = make_shared<decltype(subscribers)>(std::move(subscribers));
    auto buf = shared_ptr<char[]>{new char[channel.size() + msg.size()]};
    memcpy(buf.get(), channel.data(), channel.size());
    memcpy(buf.get() + channel.size(), msg.data(), msg.size());

    auto cb = [subscribers_ptr, buf, channel, msg](unsigned idx, util::ProactorBase*) {
      auto it = lower_bound(subscribers_ptr->begin(), subscribers_ptr->end(), idx,
                            ChannelStore::Subscriber::ByThreadId);

      while (it != subscribers_ptr->end() && it->Thread() == idx) {
        if (auto* ptr = it->Get(); ptr) {
          ptr->SendPubMessageAsync(
              {std::move(it->pattern), std::move(buf), channel.size(), msg.size()});
        }
        it++;
      }
    };
    shard_set->pool()->DispatchBrief(std::move(cb));
  }

  cntx->SendLong(num_published);
}

void Service::Subscribe(CmdArgList args, ConnectionContext* cntx) {
  cntx->ChangeSubscription(true /*add*/, true /* reply*/, std::move(args));
}

void Service::Unsubscribe(CmdArgList args, ConnectionContext* cntx) {
  if (args.size() == 0) {
    cntx->UnsubscribeAll(true);
  } else {
    cntx->ChangeSubscription(false, true, args);
  }
}

void Service::PSubscribe(CmdArgList args, ConnectionContext* cntx) {
  cntx->ChangePSubscription(true, true, args);
}

void Service::PUnsubscribe(CmdArgList args, ConnectionContext* cntx) {
  if (args.size() == 0) {
    cntx->PUnsubscribeAll(true);
  } else {
    cntx->ChangePSubscription(false, true, args);
  }
}

// Not a real implementation. Serves as a decorator to accept some function commands
// for testing.
void Service::Function(CmdArgList args, ConnectionContext* cntx) {
  ToUpper(&args[0]);
  string_view sub_cmd = ArgS(args, 0);

  if (sub_cmd == "FLUSH") {
    return cntx->SendOk();
  }

  string err = UnknownSubCmd(sub_cmd, "FUNCTION");
  return cntx->SendError(err, kSyntaxErrType);
}

void Service::PubsubChannels(string_view pattern, ConnectionContext* cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  rb->SendStringArr(ServerState::tlocal()->channel_store()->ListChannels(pattern));
}

void Service::PubsubPatterns(ConnectionContext* cntx) {
  size_t pattern_count = ServerState::tlocal()->channel_store()->PatternCount();

  cntx->SendLong(pattern_count);
}

void Service::PubsubNumSub(CmdArgList args, ConnectionContext* cntx) {
  int channels_size = args.size();
  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  rb->StartArray(channels_size * 2);

  for (auto i = 0; i < channels_size; i++) {
    auto channel = ArgS(args, i);
    rb->SendBulkString(channel);
    rb->SendLong(ServerState::tlocal()->channel_store()->FetchSubscribers(channel).size());
  }
}

void Service::Monitor(CmdArgList args, ConnectionContext* cntx) {
  VLOG(1) << "starting monitor on this connection: " << cntx->conn()->GetClientId();
  // we are registering the current connection for all threads so they will be aware of
  // this connection, to send to it any command
  cntx->SendOk();
  cntx->ChangeMonitor(true /* start */);
}

void Service::Pubsub(CmdArgList args, ConnectionContext* cntx) {
  if (args.size() < 1) {
    cntx->SendError(WrongNumArgsError(cntx->cid->name()));
    return;
  }

  ToUpper(&args[0]);
  string_view subcmd = ArgS(args, 0);

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
        "HELP",
        "\tPrints this help."};

    auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
    rb->SendSimpleStrArr(help_arr);
    return;
  }

  if (subcmd == "CHANNELS") {
    string_view pattern;
    if (args.size() > 1) {
      pattern = ArgS(args, 1);
    }

    PubsubChannels(pattern, cntx);
  } else if (subcmd == "NUMPAT") {
    PubsubPatterns(cntx);
  } else if (subcmd == "NUMSUB") {
    args.remove_prefix(1);
    PubsubNumSub(args, cntx);
  } else {
    cntx->SendError(UnknownSubCmd(subcmd, "PUBSUB"));
  }
}

void Service::Command(CmdArgList args, ConnectionContext* cntx) {
  unsigned cmd_cnt = 0;
  registry_.Traverse([&](string_view name, const CommandId& cd) {
    if ((cd.opt_mask() & CO::HIDDEN) == 0) {
      ++cmd_cnt;
    }
  });

  auto* rb = static_cast<RedisReplyBuilder*>(cntx->reply_builder());
  auto serialize_command = [&rb](string_view name, const CommandId& cid) {
    rb->StartArray(6);
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

  ToUpper(&args[0]);
  string_view subcmd = ArgS(args, 0);

  // COUNT
  if (subcmd == "COUNT") {
    return cntx->SendLong(cmd_cnt);
  }

  // INFO [cmd]
  if (subcmd == "INFO" && args.size() == 2) {
    ToUpper(&args[1]);
    string_view cmd = ArgS(args, 1);

    if (const auto* cid = registry_.Find(cmd); cid) {
      rb->StartArray(1);
      serialize_command(cmd, *cid);
    } else {
      rb->SendNull();
    }

    return;
  }

  return cntx->SendError(kSyntaxErr, kSyntaxErrType);
}

VarzValue::Map Service::GetVarzStats() {
  VarzValue::Map res;

  Metrics m = server_family_.GetMetrics();
  DbStats db_stats;
  for (const auto& s : m.db_stats) {
    db_stats += s;
  }

  res.emplace_back("keys", VarzValue::FromInt(db_stats.key_count));
  res.emplace_back("obj_mem_usage", VarzValue::FromInt(db_stats.obj_memory_usage));
  double load = double(db_stats.key_count) / (1 + db_stats.bucket_count);
  res.emplace_back("table_load_factor", VarzValue::FromDouble(load));

  return res;
}

GlobalState Service::SwitchState(GlobalState from, GlobalState to) {
  lock_guard lk(mu_);
  if (global_state_ != from) {
    return global_state_;
  }

  VLOG(1) << "Switching state from " << from << " to " << to;
  global_state_ = to;

  pp_.Await([&](ProactorBase*) { ServerState::tlocal()->set_gstate(to); });
  return to;
}

void Service::RequestLoadingState() {
  unique_lock lk(mu_);
  ++loading_state_counter_;
  if (global_state_ != GlobalState::LOADING) {
    DCHECK_EQ(global_state_, GlobalState::ACTIVE);
    lk.unlock();
    SwitchState(GlobalState::ACTIVE, GlobalState::LOADING);
  }
}

void Service::RemoveLoadingState() {
  unique_lock lk(mu_);
  DCHECK_EQ(global_state_, GlobalState::LOADING);
  DCHECK_GT(loading_state_counter_, 0u);
  --loading_state_counter_;
  if (loading_state_counter_ == 0) {
    lk.unlock();
    SwitchState(GlobalState::LOADING, GlobalState::ACTIVE);
  }
}

GlobalState Service::GetGlobalState() const {
  lock_guard lk(mu_);
  return global_state_;
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
  base->RegisterCb("/topkeys", Topkeys);
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

void Service::OnClose(facade::ConnectionContext* cntx) {
  ConnectionContext* server_cntx = static_cast<ConnectionContext*>(cntx);
  ConnectionState& conn_state = server_cntx->conn_state;

  if (conn_state.subscribe_info) {  // Clean-ups related to PUBSUB
    if (!conn_state.subscribe_info->channels.empty()) {
      server_cntx->UnsubscribeAll(false);
    }

    if (conn_state.subscribe_info) {
      DCHECK(!conn_state.subscribe_info->patterns.empty());
      server_cntx->PUnsubscribeAll(false);
    }

    DCHECK(!conn_state.subscribe_info);
  }

  UnwatchAllKeys(&conn_state.exec_info);

  DeactivateMonitoring(server_cntx);

  server_family_.OnClose(server_cntx);

  cntx->conn()->SetClientTrackingSwitch(false);
}

string Service::GetContextInfo(facade::ConnectionContext* cntx) {
  char buf[16] = {0};
  unsigned index = 0;
  ConnectionContext* server_cntx = static_cast<ConnectionContext*>(cntx);

  string res = absl::StrCat("db=", server_cntx->db_index());

  if (server_cntx->async_dispatch)
    buf[index++] = 'a';

  if (server_cntx->conn_closing)
    buf[index++] = 't';

  if (server_cntx->conn_state.subscribe_info)
    buf[index++] = 'P';

  if (server_cntx->blocked)
    buf[index++] = 'b';

  if (index) {
    absl::StrAppend(&res, " flags=", buf);
  }
  return res;
}

using ServiceFunc = void (Service::*)(CmdArgList, ConnectionContext* cntx);

#define HFUNC(x) SetHandler(&Service::x)
#define MFUNC(x) \
  SetHandler([this](CmdArgList sp, ConnectionContext* cntx) { this->x(std::move(sp), cntx); })

namespace acl {
constexpr uint32_t kQuit = FAST | CONNECTION;
constexpr uint32_t kMulti = FAST | TRANSACTION;
constexpr uint32_t kWatch = FAST | TRANSACTION;
constexpr uint32_t kUnwatch = FAST | TRANSACTION;
constexpr uint32_t kDiscard = FAST | TRANSACTION;
constexpr uint32_t kEval = SLOW | SCRIPTING;
constexpr uint32_t kEvalSha = SLOW | SCRIPTING;
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
      << CI{"EVALSHA", CO::NOSCRIPT | CO::VARIADIC_KEYS, -3, 3, 3, acl::kEvalSha}
             .MFUNC(EvalSha)
             .SetValidator(&EvalValidator)
      << CI{"EXEC", CO::LOADING | CO::NOSCRIPT, 1, 0, 0, acl::kExec}.MFUNC(Exec)
      << CI{"PUBLISH", CO::LOADING | CO::FAST, 3, 0, 0, acl::kPublish}.MFUNC(Publish)
      << CI{"SUBSCRIBE", CO::NOSCRIPT | CO::LOADING, -2, 0, 0, acl::kSubscribe}.MFUNC(Subscribe)
      << CI{"UNSUBSCRIBE", CO::NOSCRIPT | CO::LOADING, -1, 0, 0, acl::kUnsubscribe}.MFUNC(
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
  StreamFamily::Register(&registry_);
  StringFamily::Register(&registry_);
  GenericFamily::Register(&registry_);
  ListFamily::Register(&registry_);
  SetFamily::Register(&registry_);
  HSetFamily::Register(&registry_);
  ZSetFamily::Register(&registry_);
  JsonFamily::Register(&registry_);
  BitOpsFamily::Register(&registry_);
  HllFamily::Register(&registry_);
  SearchFamily::Register(&registry_);
  BloomFamily::Register(&registry_);
  server_family_.Register(&registry_);
  cluster_family_.Register(&registry_);

  acl_family_.Register(&registry_);
  acl::BuildIndexers(registry_.GetFamilies());

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

void Service::TestInit() {
  acl_family_.Init(nullptr, &user_registry_);
}

void SetMaxMemoryFlag(uint64_t value) {
  absl::SetFlag(&FLAGS_maxmemory, {value});
}

uint64_t GetMaxMemoryFlag() {
  return absl::GetFlag(FLAGS_maxmemory).value;
}

}  // namespace dfly
