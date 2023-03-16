// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/main_service.h"

extern "C" {
#include "redis/redis_aux.h"
}

#include <absl/cleanup/cleanup.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_format.h>
#include <xxhash.h>

#include <boost/fiber/operations.hpp>
#include <filesystem>

#include "base/flags.h"
#include "base/logging.h"
#include "facade/dragonfly_connection.h"
#include "facade/error.h"
#include "server/bitops_family.h"
#include "server/conn_context.h"
#include "server/error.h"
#include "server/generic_family.h"
#include "server/hset_family.h"
#include "server/json_family.h"
#include "server/list_family.h"
#include "server/script_mgr.h"
#include "server/server_state.h"
#include "server/set_family.h"
#include "server/stream_family.h"
#include "server/string_family.h"
#include "server/transaction.h"
#include "server/version.h"
#include "server/zset_family.h"
#include "util/html/sorted_table.h"
#include "util/varz.h"

using namespace std;

ABSL_FLAG(uint32_t, port, 6379, "Redis port");
ABSL_FLAG(uint32_t, memcache_port, 0, "Memcached port");

ABSL_FLAG(int, multi_exec_mode, 1,
          "Set multi exec atomicity mode: 1 for global, 2 for locking ahead, 3 for locking "
          "incrementally, 4 for non atomic");
ABSL_FLAG(int, multi_eval_mode, 1,
          "Set EVAL atomicity mode: 1 for global, 2 for locking ahead, 3 for locking "
          "incrementally, 4 for non atomic");

namespace dfly {

#if __GLIBC__ == 2 && __GLIBC_MINOR__ < 30
#include <sys/syscall.h>
#define gettid() syscall(SYS_gettid)
#endif

using namespace util;
using base::VarzValue;
using ::boost::intrusive_ptr;
namespace fibers = ::boost::fibers;
using absl::GetFlag;
using absl::StrCat;
using namespace facade;
namespace h2 = boost::beast::http;

namespace {

DEFINE_VARZ(VarzMapAverage, request_latency_usec);

std::optional<VarzFunction> engine_varz;

constexpr size_t kMaxThreadSize = 1024;

// Unwatch all keys for a connection and unregister from DbSlices.
// Used by UNWATCH, DICARD and EXEC.
void UnwatchAllKeys(ConnectionContext* cntx) {
  auto& exec_info = cntx->conn_state.exec_info;
  if (!exec_info.watched_keys.empty()) {
    auto cb = [&](EngineShard* shard) {
      shard->db_slice().UnregisterConnectionWatches(&exec_info);
    };
    shard_set->RunBriefInParallel(std::move(cb));
  }
  exec_info.ClearWatched();
}

void MultiCleanup(ConnectionContext* cntx) {
  UnwatchAllKeys(cntx);
  cntx->conn_state.exec_info.Clear();
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
          absl::StrAppend(&result, "\\x", absl::Hex((unsigned char)c, absl::kZeroPad2));
        }
        break;
    }
  }
  absl::StrAppend(&result, "\"");
  return result;
}

std::string MakeMonitorMessage(const ConnectionState& conn_state,
                               const facade::Connection* connection, CmdArgList args) {
  std::string message = absl::StrCat(CreateMonitorTimestamp(), " [", conn_state.db_index);

  if (conn_state.script_info.has_value()) {
    absl::StrAppend(&message, " lua] ");
  } else {
    auto endpoint = connection == nullptr ? "REPLICATION:0" : connection->RemoteEndpointStr();
    absl::StrAppend(&message, " ", endpoint, "] ");
  }
  if (args.empty()) {
    absl::StrAppend(&message, "error - empty cmd list!");
  } else if (auto cmd_name = std::string_view(args[0].data(), args[0].size());
             cmd_name == "AUTH") {  // we cannot just send auth details in this case
    absl::StrAppend(&message, "\"", cmd_name, "\"");
  } else {
    message = std::accumulate(args.begin(), args.end(), message, [](auto str, const auto& cmd) {
      absl::StrAppend(&str, " ", CmdEntryToMonitorFormat(std::string_view(cmd.data(), cmd.size())));
      return str;
    });
  }
  return message;
}

void SendMonitor(const std::string& msg) {
  const auto& monitor_repo = ServerState::tlocal()->Monitors();
  const auto& monitors = monitor_repo.monitors();
  if (!monitors.empty()) {
    VLOG(1) << "thread " << util::ProactorBase::GetIndex() << " sending monitor message '" << msg
            << "' for " << monitors.size();

    for (auto monitor_conn : monitors) {
      // never preempts, so we can iterate safely.
      monitor_conn->SendMonitorMsg(msg);
    }
  }
}

void DispatchMonitorIfNeeded(bool admin_cmd, ConnectionContext* connection, CmdArgList args) {
  // We are not sending any admin command in the monitor, and we do not want to
  // do any processing if we don't have any waiting connections with monitor
  // enabled on them - see https://redis.io/commands/monitor/
  const auto& my_monitors = ServerState::tlocal()->Monitors();
  if (!(my_monitors.Empty() || admin_cmd)) {
    //  We have connections waiting to get the info on the last command, send it to them
    auto monitor_msg = MakeMonitorMessage(connection->conn_state, connection->owner(), args);

    VLOG(1) << "sending command '" << monitor_msg << "' to the clients that registered on it";

    shard_set->pool()->DispatchBrief(
        [msg = std::move(monitor_msg)](unsigned idx, util::ProactorBase*) { SendMonitor(msg); });
  }
}

class InterpreterReplier : public RedisReplyBuilder {
 public:
  InterpreterReplier(ObjectExplorer* explr) : RedisReplyBuilder(nullptr), explr_(explr) {
  }

  void SendError(std::string_view str, std::string_view type = std::string_view{}) override;
  void SendStored() override;

  void SendSimpleString(std::string_view str) final;
  void SendMGetResponse(const OptResp* resp, uint32_t count) final;
  void SendSimpleStrArr(const string_view* arr, uint32_t count) final;
  void SendNullArray() final;

  void SendStringArr(absl::Span<const string_view> arr, Resp3Type type) final;
  void SendStringArr(absl::Span<const string> arr, Resp3Type type) final;
  void SendNull() final;

  void SendLong(long val) final;
  void SendDouble(double val) final;

  void SendBulkString(std::string_view str) final;

  void StartArray(unsigned len) final;

 private:
  void PostItem();

  ObjectExplorer* explr_;
  vector<pair<unsigned, unsigned>> array_len_;
  unsigned num_elems_ = 0;
};

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
    rb_->SendDouble(d);
  }

  void OnInt(int64_t val) final {
    rb_->SendLong(val);
  }

  void OnArrayStart(unsigned len) final {
    rb_->StartArray(len);
  }

  void OnArrayEnd() final {
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

void InterpreterReplier::SendMGetResponse(const OptResp* resp, uint32_t count) {
  DCHECK(array_len_.empty());

  explr_->OnArrayStart(count);
  for (uint32_t i = 0; i < count; ++i) {
    if (resp[i].has_value()) {
      explr_->OnString(resp[i]->value);
    } else {
      explr_->OnNil();
    }
  }
  explr_->OnArrayEnd();
}

void InterpreterReplier::SendSimpleStrArr(const string_view* arr, uint32_t count) {
  explr_->OnArrayStart(count);
  for (uint32_t i = 0; i < count; ++i) {
    explr_->OnString(arr[i]);
  }
  explr_->OnArrayEnd();
}

void InterpreterReplier::SendNullArray() {
  SendSimpleStrArr(nullptr, 0);
  PostItem();
}

void InterpreterReplier::SendStringArr(absl::Span<const string_view> arr, Resp3Type type) {
  SendSimpleStrArr(arr.data(), arr.size());
  PostItem();
}

void InterpreterReplier::SendStringArr(absl::Span<const string> arr, Resp3Type type) {
  explr_->OnArrayStart(arr.size());
  for (uint32_t i = 0; i < arr.size(); ++i) {
    explr_->OnString(arr[i]);
  }
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

void InterpreterReplier::StartArray(unsigned len) {
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

bool IsTransactional(const CommandId* cid) {
  if (cid->first_key_pos() > 0 || (cid->opt_mask() & CO::GLOBAL_TRANS))
    return true;

  string_view name{cid->name()};

  if (name == "EVAL" || name == "EVALSHA" || name == "EXEC")
    return true;

  return false;
}

bool EvalValidator(CmdArgList args, ConnectionContext* cntx) {
  string_view num_keys_str = ArgS(args, 2);
  int32_t num_keys;

  if (!absl::SimpleAtoi(num_keys_str, &num_keys) || num_keys < 0) {
    (*cntx)->SendError(facade::kInvalidIntErr);
    return false;
  }

  if (unsigned(num_keys) > args.size() - 3) {
    (*cntx)->SendError("Number of keys can't be greater than number of args", kSyntaxErrType);
    return false;
  }

  return true;
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
          absl::AlphaNum an3(trx->IsArmedInShard(sid));
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

}  // namespace

Service::Service(ProactorPool* pp) : pp_(*pp), server_family_(this) {
  CHECK(pp);
  CHECK(shard_set == NULL);

  shard_set = new EngineShardSet(pp);

  // We support less than 1024 threads and we support less than 1024 shards.
  // For example, Scan uses 10 bits in cursor to encode shard id it currently traverses.
  CHECK_LT(pp->size(), kMaxThreadSize);
  RegisterCommands();

  engine_varz.emplace("engine", [this] { return GetVarzStats(); });
}

Service::~Service() {
  delete shard_set;
  shard_set = nullptr;
}

void Service::Init(util::AcceptServer* acceptor, util::ListenerInterface* main_interface,
                   const InitOpts& opts) {
  InitRedisTables();

  pp_.Await([](uint32_t index, ProactorBase* pb) { ServerState::Init(index); });

  uint32_t shard_num = pp_.size() > 1 ? pp_.size() - 1 : pp_.size();
  shard_set->Init(shard_num, !opts.disable_time_update);

  request_latency_usec.Init(&pp_);
  StringFamily::Init(&pp_);
  GenericFamily::Init(&pp_);
  server_family_.Init(acceptor, main_interface);
}

void Service::Shutdown() {
  VLOG(1) << "Service::Shutdown";

  // We mark that we are shutting down. After this incoming requests will be
  // rejected
  pp_.AwaitFiberOnAll([](ProactorBase* pb) {
    ServerState::tlocal()->EnterLameDuck();
    facade::Connection::ShutdownThreadLocal();
  });

  // to shutdown all the runtime components that depend on EngineShard.
  server_family_.Shutdown();
  StringFamily::Shutdown();
  GenericFamily::Shutdown();

  engine_varz.reset();
  request_latency_usec.Shutdown();

  shard_set->Shutdown();
  pp_.Await([](ProactorBase* pb) { ServerState::tlocal()->Destroy(); });

  // wait for all the pending callbacks to stop.
  fibers_ext::SleepFor(10ms);
}

static void MultiSetError(ConnectionContext* cntx) {
  if (cntx->conn_state.exec_info.IsActive()) {
    cntx->conn_state.exec_info.state = ConnectionState::ExecInfo::EXEC_ERROR;
  }
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

  const auto& key_index = *key_index_res;
  for (unsigned i = key_index.start; i < key_index.end; ++i) {
    if (!eval_info.keys.contains(ArgS(args, i))) {
      return OpStatus::KEY_NOTFOUND;
    }
  }

  if (unsigned i = key_index.bonus; i && !eval_info.keys.contains(ArgS(args, i)))
    return OpStatus::KEY_NOTFOUND;

  return OpStatus::OK;
}

void Service::DispatchCommand(CmdArgList args, facade::ConnectionContext* cntx) {
  CHECK(!args.empty());
  DCHECK_NE(0u, shard_set->size()) << "Init was not called";

  ToUpper(&args[0]);

  ConnectionContext* dfly_cntx = static_cast<ConnectionContext*>(cntx);
  bool under_script = dfly_cntx->conn_state.script_info.has_value();

  if (VLOG_IS_ON(2) &&
      cntx->owner()) {  // owner may not exists in case of this being called from replica context
    const char* lua = under_script ? "LUA " : "";
    LOG(INFO) << "Got (" << cntx->owner()->GetClientId() << "): " << lua << args;
  }

  string_view cmd_str = ArgS(args, 0);
  bool is_trans_cmd = (cmd_str == "EXEC" || cmd_str == "MULTI" || cmd_str == "DISCARD");
  const CommandId* cid = registry_.Find(cmd_str);
  ServerState& etl = *ServerState::tlocal();

  etl.RecordCmd();

  absl::Cleanup multi_error([dfly_cntx] { MultiSetError(dfly_cntx); });

  if (cid == nullptr) {
    (*cntx)->SendError(StrCat("unknown command `", cmd_str, "`"), "unknown_cmd");

    lock_guard lk(mu_);
    if (unknown_cmds_.size() < 1024)
      unknown_cmds_[cmd_str]++;
    return;
  }

  bool blocked_by_loading = !cntx->journal_emulated && etl.gstate() == GlobalState::LOADING &&
                            (cid->opt_mask() & CO::LOADING) == 0;
  if (blocked_by_loading || etl.gstate() == GlobalState::SHUTTING_DOWN) {
    string err = StrCat("Can not execute during ", GlobalStateName(etl.gstate()));
    (*cntx)->SendError(err);
    return;
  }

  string_view cmd_name{cid->name()};

  if (cntx->req_auth && !cntx->authenticated) {
    if (cmd_name != "AUTH" && cmd_name != "QUIT") {
      return (*cntx)->SendError("-NOAUTH Authentication required.");
    }
  }

  // only reset and quit are allow if this connection is used for monitoring
  if (dfly_cntx->monitor && (cmd_name != "RESET" && cmd_name != "QUIT")) {
    return (*cntx)->SendError("Replica can't interact with the keyspace");
  }

  if (under_script && (cid->opt_mask() & CO::NOSCRIPT)) {
    return (*cntx)->SendError("This Redis command is not allowed from script");
  }

  bool is_write_cmd = (cid->opt_mask() & CO::WRITE) ||
                      (under_script && dfly_cntx->conn_state.script_info->is_write);
  bool under_multi = dfly_cntx->conn_state.exec_info.IsActive() && !is_trans_cmd;

  if (!etl.is_master && is_write_cmd && !dfly_cntx->is_replicating) {
    (*cntx)->SendError("-READONLY You can't write against a read only replica.");
    return;
  }

  if ((cid->arity() > 0 && args.size() != size_t(cid->arity())) ||
      (cid->arity() < 0 && args.size() < size_t(-cid->arity()))) {
    return (*cntx)->SendError(facade::WrongNumArgsError(cmd_str), kSyntaxErrType);
  }

  if (cid->key_arg_step() == 2 && (args.size() % 2) == 0) {
    return (*cntx)->SendError(facade::WrongNumArgsError(cmd_str), kSyntaxErrType);
  }

  // Validate more complicated cases with custom validators.
  if (!cid->Validate(args, dfly_cntx)) {
    return;
  }

  if (under_multi) {
    if (cmd_name == "SELECT") {
      (*cntx)->SendError("Can not call SELECT within a transaction");
      return;
    }

    if (cmd_name == "WATCH" || cmd_name == "FLUSHALL" || cmd_name == "FLUSHDB") {
      auto error = absl::StrCat("'", cmd_name, "' inside MULTI is not allowed");
      return (*cntx)->SendError(error);
    }
  }

  std::move(multi_error).Cancel();

  etl.connection_stats.cmd_count_map[cmd_name]++;

  if (dfly_cntx->conn_state.exec_info.IsActive() && !is_trans_cmd) {
    auto cmd_name = ArgS(args, 0);
    if (cmd_name == "EVAL" || cmd_name == "EVALSHA") {
      auto error =
          absl::StrCat("'", cmd_name,
                       "' Dragonfly does not allow execution of a server-side Lua script inside "
                       "transaction block");
      MultiSetError(dfly_cntx);
      return (*cntx)->SendError(error);
    }
    // TODO: protect against aggregating huge transactions.
    StoredCmd stored_cmd{cid, args};
    dfly_cntx->conn_state.exec_info.body.push_back(std::move(stored_cmd));

    return (*cntx)->SendSimpleString("QUEUED");
  }

  uint64_t start_usec = ProactorBase::GetMonotonicTimeNs(), end_usec;

  DispatchMonitorIfNeeded(cid->opt_mask() & CO::ADMIN, dfly_cntx, args);
  // Create command transaction
  intrusive_ptr<Transaction> dist_trans;

  if (under_script) {
    DCHECK(dfly_cntx->transaction);
    if (IsTransactional(cid)) {
      OpStatus status =
          CheckKeysDeclared(*dfly_cntx->conn_state.script_info, cid, args, dfly_cntx->transaction);

      if (status == OpStatus::KEY_NOTFOUND)
        return (*cntx)->SendError("script tried accessing undeclared key");

      if (status != OpStatus::OK)
        return (*cntx)->SendError(status);

      dfly_cntx->transaction->MultiSwitchCmd(cid);
      status = dfly_cntx->transaction->InitByArgs(dfly_cntx->conn_state.db_index, args);

      if (status != OpStatus::OK)
        return (*cntx)->SendError(status);
    }
  } else {
    DCHECK(dfly_cntx->transaction == nullptr);

    if (IsTransactional(cid)) {
      dist_trans.reset(new Transaction{cid, etl.thread_index()});

      if (!dist_trans->IsMulti()) {  // Multi command initialize themself based on their mode.
        if (auto st = dist_trans->InitByArgs(dfly_cntx->conn_state.db_index, args);
            st != OpStatus::OK)
          return (*cntx)->SendError(st);
      }

      dfly_cntx->transaction = dist_trans.get();
      dfly_cntx->last_command_debug.shards_count = dfly_cntx->transaction->GetUniqueShardCnt();
    } else {
      dfly_cntx->transaction = nullptr;
    }
  }

  dfly_cntx->cid = cid;

  // Collect stats for all regular transactions and all multi transactions from scripts, except EVAL
  // itself. EXEC does not use DispatchCommand for dispatching.
  bool collect_stats =
      dfly_cntx->transaction && (!dfly_cntx->transaction->IsMulti() || under_script);
  if (!InvokeCmd(args, cid, dfly_cntx, collect_stats)) {
    dfly_cntx->reply_builder()->SendError("Internal Error");
    dfly_cntx->reply_builder()->CloseConnection();
  }

  end_usec = ProactorBase::GetMonotonicTimeNs();
  request_latency_usec.IncBy(cmd_str, (end_usec - start_usec) / 1000);

  if (!under_script) {
    dfly_cntx->transaction = nullptr;
  }
}

bool Service::InvokeCmd(CmdArgList args, const CommandId* cid, ConnectionContext* cntx,
                        bool record_stats) {
  try {
    cid->Invoke(args, cntx);
  } catch (std::exception& e) {
    LOG(ERROR) << "Internal error, system probably unstable " << e.what();
    return false;
  }

  if (record_stats) {
    DCHECK(cntx->transaction);
    ServerState& etl = *ServerState::tlocal();
    bool is_ooo = cntx->transaction->IsOOO();

    cntx->last_command_debug.clock = cntx->transaction->txid();
    cntx->last_command_debug.is_ooo = is_ooo;
    etl.stats.ooo_tx_cnt += is_ooo;
  }

  return true;
}

void Service::DispatchMC(const MemcacheParser::Command& cmd, std::string_view value,
                         facade::ConnectionContext* cntx) {
  absl::InlinedVector<MutableSlice, 8> args;
  char cmd_name[16];
  char ttl[16];
  char store_opt[32] = {0};
  char ttl_op[] = "EX";

  MCReplyBuilder* mc_builder = static_cast<MCReplyBuilder*>(cntx->reply_builder());

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
      mc_builder->SendSimpleString(StrCat("VERSION ", kGitTag));
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

facade::ConnectionContext* Service::CreateContext(util::FiberSocketBase* peer,
                                                  facade::Connection* owner) {
  ConnectionContext* res = new ConnectionContext{peer, owner};
  res->req_auth = !GetPassword().empty();

  // a bit of a hack. I set up breaker callback here for the owner.
  // Should work though it's confusing to have it here.
  owner->RegisterOnBreak([res, this](uint32_t) {
    if (res->transaction) {
      res->transaction->BreakOnShutdown();
    }
    this->server_family().BreakOnShutdown();
  });

  return res;
}

facade::ConnectionStats* Service::GetThreadLocalConnectionStats() {
  return ServerState::tl_connection_stats();
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
    (*cntx)->SendOk();
  using facade::SinkReplyBuilder;

  SinkReplyBuilder* builder = cntx->reply_builder();
  builder->CloseConnection();

  DeactivateMonitoring(static_cast<ConnectionContext*>(cntx));
  cntx->owner()->ShutdownSelf();
}

void Service::Multi(CmdArgList args, ConnectionContext* cntx) {
  if (cntx->conn_state.exec_info.IsActive()) {
    return (*cntx)->SendError("MULTI calls can not be nested");
  }
  cntx->conn_state.exec_info.state = ConnectionState::ExecInfo::EXEC_COLLECT;
  // TODO: to protect against huge exec transactions.
  return (*cntx)->SendOk();
}

void Service::Watch(CmdArgList args, ConnectionContext* cntx) {
  auto& exec_info = cntx->conn_state.exec_info;

  // Skip if EXEC will already fail due previous WATCH.
  if (exec_info.watched_dirty.load(memory_order_relaxed)) {
    return (*cntx)->SendOk();
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
  for (size_t i = 1; i < args.size(); i++) {
    exec_info.watched_keys.emplace_back(cntx->db_index(), ArgS(args, i));
  }

  return (*cntx)->SendOk();
}

void Service::Unwatch(CmdArgList args, ConnectionContext* cntx) {
  UnwatchAllKeys(cntx);
  return (*cntx)->SendOk();
}

void Service::CallFromScript(CmdArgList args, ObjectExplorer* reply, ConnectionContext* cntx) {
  DCHECK(cntx->transaction);
  DVLOG(1) << "CallFromScript " << cntx->transaction->DebugId() << " " << ArgS(args, 0);

  InterpreterReplier replier(reply);
  facade::SinkReplyBuilder* orig = cntx->Inject(&replier);

  DispatchCommand(std::move(args), cntx);

  cntx->Inject(orig);
}

void Service::Eval(CmdArgList args, ConnectionContext* cntx) {
  uint32_t num_keys;

  CHECK(absl::SimpleAtoi(ArgS(args, 2), &num_keys));  // we already validated this

  string_view body = ArgS(args, 1);
  // body = absl::StripAsciiWhitespace(body);

  if (body.empty()) {
    return (*cntx)->SendNull();
  }

  ServerState* ss = ServerState::tlocal();
  auto interpreter = ss->BorrowInterpreter();
  absl::Cleanup clean = [ss, interpreter]() { ss->ReturnInterpreter(interpreter); };

  string result;
  Interpreter::AddResult add_result = interpreter->AddFunction(body, &result);
  if (add_result == Interpreter::COMPILE_ERR) {
    return (*cntx)->SendError(result, facade::kScriptErrType);
  }

  if (add_result == Interpreter::ADD_OK) {
    if (auto err = server_family_.script_mgr()->Insert(result, body); err)
      return (*cntx)->SendError(err.Format(), facade::kScriptErrType);
  }

  EvalArgs eval_args;
  eval_args.sha = result;
  eval_args.keys = args.subspan(3, num_keys);
  eval_args.args = args.subspan(3 + num_keys);

  uint64_t start = absl::GetCurrentTimeNanos();
  EvalInternal(eval_args, interpreter, cntx);

  uint64_t end = absl::GetCurrentTimeNanos();
  ss->RecordCallLatency(result, (end - start) / 1000);
}

void Service::EvalSha(CmdArgList args, ConnectionContext* cntx) {
  string_view num_keys_str = ArgS(args, 2);
  uint32_t num_keys;

  CHECK(absl::SimpleAtoi(num_keys_str, &num_keys));

  ToLower(&args[1]);

  string_view sha = ArgS(args, 1);
  ServerState* ss = ServerState::tlocal();
  auto interpreter = ss->BorrowInterpreter();
  absl::Cleanup clean = [ss, interpreter]() { ss->ReturnInterpreter(interpreter); };

  EvalArgs ev_args;
  ev_args.sha = sha;
  ev_args.keys = args.subspan(3, num_keys);
  ev_args.args = args.subspan(3 + num_keys);

  uint64_t start = absl::GetCurrentTimeNanos();
  EvalInternal(ev_args, interpreter, cntx);

  uint64_t end = absl::GetCurrentTimeNanos();
  ss->RecordCallLatency(sha, (end - start) / 1000);
}

vector<bool> DetermineKeyShards(CmdArgList keys) {
  vector<bool> out(shard_set->size());
  for (auto k : keys)
    out[Shard(facade::ToSV(k), out.size())] = true;
  return out;
}

optional<ScriptMgr::ScriptParams> LoadScipt(string_view sha, ScriptMgr* script_mgr,
                                            Interpreter* interpreter) {
  auto ss = ServerState::tlocal();

  if (!interpreter->Exists(sha)) {
    auto script_data = script_mgr->Find(sha);
    if (!script_data)
      return std::nullopt;

    string res;
    CHECK_EQ(Interpreter::ADD_OK, interpreter->AddFunction(script_data->body, &res));
    CHECK_EQ(res, sha);

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
// Skips scheduling if multi mode requies declaring keys, but no keys were declared.
bool StartMultiEval(DbIndex dbid, CmdArgList keys, ScriptMgr::ScriptParams params,
                    Transaction* trans) {
  Transaction::MultiMode multi_mode = DetermineMultiMode(params);

  if (keys.empty() &&
      (multi_mode == Transaction::LOCK_AHEAD || multi_mode == Transaction::LOCK_INCREMENTAL))
    return false;

  switch (multi_mode) {
    case Transaction::GLOBAL:
      trans->StartMultiGlobal(dbid);
      return true;
    case Transaction::LOCK_AHEAD:
      trans->StartMultiLockedAhead(dbid, keys);
      return true;
    case Transaction::LOCK_INCREMENTAL:
      trans->StartMultiLockedIncr(dbid, DetermineKeyShards(keys));
      return true;
    case Transaction::NON_ATOMIC:
      trans->StartMultiNonAtomic();
      return true;
    default:
      CHECK(false) << "Invalid mode";
  };

  return false;
}

void Service::EvalInternal(const EvalArgs& eval_args, Interpreter* interpreter,
                           ConnectionContext* cntx) {
  DCHECK(!eval_args.sha.empty());

  // Sanitizing the input to avoid code injection.
  if (eval_args.sha.size() != 40 || !IsSHA(eval_args.sha)) {
    return (*cntx)->SendError(facade::kScriptNotFound);
  }

  auto params = LoadScipt(eval_args.sha, server_family_.script_mgr(), interpreter);
  if (!params)
    return (*cntx)->SendError(facade::kScriptNotFound);

  string error;

  DCHECK(!cntx->conn_state.script_info);  // we should not call eval from the script.

  // TODO: to determine whether the script is RO by scanning all "redis.p?call" calls
  // and checking whether all invocations consist of RO commands.
  // we can do it once during script insertion into script mgr.
  cntx->conn_state.script_info.emplace(ConnectionState::ScriptInfo{});
  for (size_t i = 0; i < eval_args.keys.size(); ++i) {
    cntx->conn_state.script_info->keys.insert(ArgS(eval_args.keys, i));
  }
  DCHECK(cntx->transaction);

  bool scheduled = StartMultiEval(cntx->db_index(), eval_args.keys, *params, cntx->transaction);

  interpreter->SetGlobalArray("KEYS", eval_args.keys);
  interpreter->SetGlobalArray("ARGV", eval_args.args);
  interpreter->SetRedisFunc(
      [cntx, this](CmdArgList args, ObjectExplorer* reply) { CallFromScript(args, reply, cntx); });

  Interpreter::RunResult result = interpreter->RunFunction(eval_args.sha, &error);
  absl::Cleanup clean = [interpreter]() { interpreter->ResetStack(); };

  cntx->conn_state.script_info.reset();  // reset script_info

  // Conclude the transaction.
  if (scheduled)
    cntx->transaction->UnlockMulti();

  if (result == Interpreter::RUN_ERR) {
    string resp = StrCat("Error running script (call to ", eval_args.sha, "): ", error);
    return (*cntx)->SendError(resp, facade::kScriptErrType);
  }

  CHECK(result == Interpreter::RUN_OK);

  EvalSerializer ser{static_cast<RedisReplyBuilder*>(cntx->reply_builder())};
  if (!interpreter->IsResultSafe()) {
    (*cntx)->SendError("reached lua stack limit");
  } else {
    interpreter->SerializeResult(&ser);
  }
}

void Service::Discard(CmdArgList args, ConnectionContext* cntx) {
  RedisReplyBuilder* rb = (*cntx).operator->();

  if (!cntx->conn_state.exec_info.IsActive()) {
    return rb->SendError("DISCARD without MULTI");
  }

  MultiCleanup(cntx);
  rb->SendOk();
}

// Return true if non of the connections watched keys expired.
bool CheckWatchedKeyExpiry(ConnectionContext* cntx, const CommandRegistry& registry) {
  static char EXISTS[] = "EXISTS";
  auto& exec_info = cntx->conn_state.exec_info;

  CmdArgVec str_list(exec_info.watched_keys.size() + 1);
  str_list[0] = MutableSlice{EXISTS, strlen(EXISTS)};
  for (size_t i = 1; i < str_list.size(); i++) {
    auto& [db, s] = exec_info.watched_keys[i - 1];
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
  cntx->transaction->InitByArgs(cntx->conn_state.db_index,
                                CmdArgList{str_list.data(), str_list.size()});
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

  for (const auto& scmd : exec_info->body) {
    if (!IsTransactional(scmd.descr))
      continue;

    auto args = scmd.ArgList();

    auto key_res = DetermineKeys(scmd.descr, args);
    if (!key_res.ok())
      continue;

    auto key_index = key_res.value();

    for (unsigned i = key_index.start; i < key_index.end; i += key_index.step)
      f(args[i]);

    if (key_index.bonus)
      f(args[key_index.bonus]);
  }
}

CmdArgVec CollectAllKeys(ConnectionState::ExecInfo* exec_info) {
  CmdArgVec out;
  out.reserve(exec_info->watched_keys.size() + exec_info->body.size());

  IterateAllKeys(exec_info, [&out](MutableSlice key) { out.push_back(key); });

  return out;
}

vector<bool> DetermineKeyShards(ConnectionState::ExecInfo* exec_info) {
  vector<bool> out(shard_set->size());

  IterateAllKeys(exec_info, [&out](MutableSlice key) {
    ShardId sid = Shard(facade::ToSV(key), shard_set->size());
    out[sid] = true;
  });
  return out;
}

// Return true if transaction was scheduled, false if scheduling was not required.
bool StartMultiExec(DbIndex dbid, Transaction* trans, ConnectionState::ExecInfo* exec_info,
                    CmdArgVec* tmp_keys) {
  bool global = false;
  bool transactional = false;
  for (const auto& scmd : exec_info->body) {
    transactional |= IsTransactional(scmd.descr);
    global |= scmd.descr->opt_mask() & CO::GLOBAL_TRANS;
    if (global)
      break;
  }

  if (!transactional && exec_info->watched_keys.empty())
    return false;

  int multi_mode = absl::GetFlag(FLAGS_multi_exec_mode);
  DCHECK(multi_mode >= Transaction::GLOBAL && multi_mode <= Transaction::NON_ATOMIC);

  // Atomic modes fall back to GLOBAL if they contain global commands.
  if (global &&
      (multi_mode == Transaction::LOCK_AHEAD || multi_mode == Transaction::LOCK_INCREMENTAL))
    multi_mode = Transaction::GLOBAL;

  switch ((Transaction::MultiMode)multi_mode) {
    case Transaction::GLOBAL:
      trans->StartMultiGlobal(dbid);
      break;
    case Transaction::LOCK_AHEAD:
      *tmp_keys = CollectAllKeys(exec_info);
      trans->StartMultiLockedAhead(dbid, CmdArgList{*tmp_keys});
      break;
    case Transaction::LOCK_INCREMENTAL:
      trans->StartMultiLockedIncr(dbid, DetermineKeyShards(exec_info));
      break;
    case Transaction::NON_ATOMIC:
      trans->StartMultiNonAtomic();
      break;
    case Transaction::NOT_DETERMINED:
      DCHECK(false);
  };
  return true;
}

void Service::Exec(CmdArgList args, ConnectionContext* cntx) {
  RedisReplyBuilder* rb = (*cntx).operator->();

  if (!cntx->conn_state.exec_info.IsActive()) {
    return rb->SendError("EXEC without MULTI");
  }

  auto& exec_info = cntx->conn_state.exec_info;
  absl::Cleanup exec_clear = [&cntx] { MultiCleanup(cntx); };

  if (IsWatchingOtherDbs(cntx->db_index(), exec_info)) {
    return rb->SendError("Dragonfly does not allow WATCH and EXEC on different databases");
  }

  if (exec_info.state == ConnectionState::ExecInfo::EXEC_ERROR) {
    return rb->SendError("-EXECABORT Transaction discarded because of previous errors");
  }

  if (exec_info.watched_dirty.load(memory_order_relaxed)) {
    return rb->SendNull();
  }

  CmdArgVec tmp_keys;
  bool scheduled = StartMultiExec(cntx->db_index(), cntx->transaction, &exec_info, &tmp_keys);

  // EXEC should not run if any of the watched keys expired.
  if (!exec_info.watched_keys.empty() && !CheckWatchedKeyExpiry(cntx, registry_)) {
    cntx->transaction->UnlockMulti();
    return rb->SendNull();
  }

  VLOG(1) << "StartExec " << exec_info.body.size();
  rb->StartArray(exec_info.body.size());

  if (!exec_info.body.empty()) {
    CmdArgVec str_list;

    for (auto& scmd : exec_info.body) {
      cntx->transaction->MultiSwitchCmd(scmd.descr);
      if (IsTransactional(scmd.descr)) {
        OpStatus st = cntx->transaction->InitByArgs(cntx->conn_state.db_index, scmd.ArgList());
        if (st != OpStatus::OK) {
          (*cntx)->SendError(st);
          break;
        }
      }
      bool ok = InvokeCmd(scmd.ArgList(), scmd.descr, cntx, true);
      if (!ok || rb->GetError())  // checks for i/o error, not logical error.
        break;
    }
  }

  if (scheduled) {
    VLOG(1) << "Exec unlocking " << exec_info.body.size() << " commands";
    cntx->transaction->UnlockMulti();
  }

  VLOG(1) << "Exec completed";
}

void Service::Publish(CmdArgList args, ConnectionContext* cntx) {
  string_view channel = ArgS(args, 1);

  ShardId sid = Shard(channel, shard_count());

  auto cb = [&] { return EngineShard::tlocal()->channel_slice().FetchSubscribers(channel); };

  // How do we know that subscribers did not disappear after we fetched them?
  // Each subscriber object hold a borrow_token.
  // OnClose does not reset subscribe_info before all tokens are returned.
  vector<ChannelSlice::Subscriber> subscriber_arr = shard_set->Await(sid, std::move(cb));
  atomic_uint32_t published{0};

  if (!subscriber_arr.empty()) {
    sort(subscriber_arr.begin(), subscriber_arr.end(),
         [](const auto& left, const auto& right) { return left.thread_id < right.thread_id; });

    vector<unsigned> slices(shard_set->pool()->size(), UINT_MAX);
    for (size_t i = 0; i < subscriber_arr.size(); ++i) {
      if (slices[subscriber_arr[i].thread_id] > i) {
        slices[subscriber_arr[i].thread_id] = i;
      }
    }

    // shared_ptr ensures that the message lives until it's been sent to all subscribers and handled
    // by DispatchOperations.
    shared_ptr<string> msg_ptr = make_shared<string>(ArgS(args, 2));
    shared_ptr<string> channel_ptr = make_shared<string>(channel);
    using PubMessage = facade::Connection::PubMessage;

    // We run publish_cb in each subscriber's thread.
    auto publish_cb = [&](unsigned idx, util::ProactorBase*) mutable {
      unsigned start = slices[idx];

      for (unsigned i = start; i < subscriber_arr.size(); ++i) {
        ChannelSlice::Subscriber& subscriber = subscriber_arr[i];
        if (subscriber.thread_id != idx)
          break;

        published.fetch_add(1, memory_order_relaxed);

        facade::Connection* conn = subscriber_arr[i].conn_cntx->owner();
        DCHECK(conn);

        PubMessage pmsg;
        pmsg.channel = channel_ptr;
        pmsg.message = msg_ptr;
        pmsg.pattern = move(subscriber.pattern);
        pmsg.type = PubMessage::kPublish;

        conn->SendMsgVecAsync(move(pmsg));
      }
    };

    shard_set->pool()->Await(publish_cb);
  }

  // If subscriber connections are closing they will wait
  // for the tokens to be reclaimed in OnClose(). This guarantees that subscribers we gathered
  // still exist till we finish publishing.
  for (auto& s : subscriber_arr) {
    s.borrow_token.Dec();
  }

  (*cntx)->SendLong(published.load(memory_order_relaxed));
}

void Service::Subscribe(CmdArgList args, ConnectionContext* cntx) {
  args.remove_prefix(1);

  cntx->ChangeSubscription(true /*add*/, true /* reply*/, std::move(args));
}

void Service::Unsubscribe(CmdArgList args, ConnectionContext* cntx) {
  args.remove_prefix(1);

  if (args.size() == 0) {
    cntx->UnsubscribeAll(true);
  } else {
    cntx->ChangeSubscription(false, true, std::move(args));
  }
}

void Service::PSubscribe(CmdArgList args, ConnectionContext* cntx) {
  args.remove_prefix(1);
  cntx->ChangePSub(true, true, args);
}

void Service::PUnsubscribe(CmdArgList args, ConnectionContext* cntx) {
  args.remove_prefix(1);

  if (args.size() == 0) {
    cntx->PUnsubscribeAll(true);
  } else {
    cntx->ChangePSub(false, true, args);
  }
}

// Not a real implementation. Serves as a decorator to accept some function commands
// for testing.
void Service::Function(CmdArgList args, ConnectionContext* cntx) {
  ToUpper(&args[1]);
  string_view sub_cmd = ArgS(args, 1);

  if (sub_cmd == "FLUSH") {
    return (*cntx)->SendOk();
  }

  string err = UnknownSubCmd(sub_cmd, "FUNCTION");
  return (*cntx)->SendError(err, kSyntaxErrType);
}

void Service::PubsubChannels(string_view pattern, ConnectionContext* cntx) {
  vector<vector<string>> result_set(shard_set->size());

  shard_set->RunBriefInParallel([&](EngineShard* shard) {
    result_set[shard->shard_id()] = shard->channel_slice().ListChannels(pattern);
  });

  vector<string> union_set;
  for (auto&& v : result_set) {
    union_set.insert(union_set.end(), v.begin(), v.end());
  }

  (*cntx)->SendStringArr(union_set, Resp3Type::ARRAY);
}

void Service::PubsubPatterns(ConnectionContext* cntx) {
  size_t pattern_count =
      shard_set->Await(0, [&] { return EngineShard::tlocal()->channel_slice().PatternCount(); });

  (*cntx)->SendLong(pattern_count);
}

void Service::Monitor(CmdArgList args, ConnectionContext* cntx) {
  VLOG(1) << "starting monitor on this connection: " << cntx->owner()->GetClientInfo();
  // we are registering the current connection for all threads so they will be aware of
  // this connection, to send to it any command
  (*cntx)->SendOk();
  cntx->ChangeMonitor(true /* start */);
}

void Service::Pubsub(CmdArgList args, ConnectionContext* cntx) {
  if (args.size() < 2) {
    (*cntx)->SendError(WrongNumArgsError(ArgS(args, 0)));
    return;
  }

  ToUpper(&args[1]);
  string_view subcmd = ArgS(args, 1);

  if (subcmd == "HELP") {
    string_view help_arr[] = {
        "PUBSUB <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        "CHANNELS [<pattern>]",
        "\tReturn the currently active channels matching a <pattern> (default: '*').",
        "NUMPAT",
        "\tReturn number of subscriptions to patterns.",
        "HELP",
        "\tPrints this help."};

    (*cntx)->SendSimpleStrArr(help_arr, ABSL_ARRAYSIZE(help_arr));
    return;
  }

  if (subcmd == "CHANNELS") {
    string_view pattern;
    if (args.size() > 2) {
      pattern = ArgS(args, 2);
    }

    PubsubChannels(pattern, cntx);
  } else if (subcmd == "NUMPAT") {
    PubsubPatterns(cntx);
  } else {
    (*cntx)->SendError(UnknownSubCmd(subcmd, "PUBSUB"));
  }
}

VarzValue::Map Service::GetVarzStats() {
  VarzValue::Map res;

  Metrics m = server_family_.GetMetrics();
  DbStats db_stats;
  for (const auto& s : m.db) {
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
  if (global_state_ != from)
    return global_state_;
  global_state_ = to;

  pp_.Await([&](ProactorBase*) { ServerState::tlocal()->set_gstate(to); });
  return to;
}

void Service::ConfigureHttpHandlers(util::HttpListenerBase* base) {
  server_family_.ConfigureMetrics(base);
  base->RegisterCb("/txz", TxTable);
}

void Service::OnClose(facade::ConnectionContext* cntx) {
  ConnectionContext* server_cntx = static_cast<ConnectionContext*>(cntx);
  ConnectionState& conn_state = server_cntx->conn_state;

  if (conn_state.subscribe_info) {  // Clean-ups related to PUBSUB
    if (!conn_state.subscribe_info->channels.empty()) {
      auto token = conn_state.subscribe_info->borrow_token;
      server_cntx->UnsubscribeAll(false);

      // Check that all borrowers finished processing.
      // token is increased in channel_slice (the publisher side).
      token.Wait();
    }

    if (conn_state.subscribe_info) {
      DCHECK(!conn_state.subscribe_info->patterns.empty());
      auto token = conn_state.subscribe_info->borrow_token;
      server_cntx->PUnsubscribeAll(false);
      // Check that all borrowers finished processing
      token.Wait();
      DCHECK(!conn_state.subscribe_info);
    }
  }

  DeactivateMonitoring(server_cntx);

  server_family_.OnClose(server_cntx);
}

string Service::GetContextInfo(facade::ConnectionContext* cntx) {
  char buf[16] = {0};
  unsigned index = 0;
  if (cntx->async_dispatch)
    buf[index++] = 'a';

  if (cntx->conn_closing)
    buf[index++] = 't';

  return index ? absl::StrCat("flags:", buf) : string();
}

using ServiceFunc = void (Service::*)(CmdArgList, ConnectionContext* cntx);

#define HFUNC(x) SetHandler(&Service::x)
#define MFUNC(x) \
  SetHandler([this](CmdArgList sp, ConnectionContext* cntx) { this->x(std::move(sp), cntx); })

void Service::RegisterCommands() {
  using CI = CommandId;

  registry_
      << CI{"QUIT", CO::READONLY | CO::FAST, 1, 0, 0, 0}.HFUNC(Quit)
      << CI{"MULTI", CO::NOSCRIPT | CO::FAST | CO::LOADING, 1, 0, 0, 0}.HFUNC(Multi)
      << CI{"WATCH", CO::LOADING, -2, 1, -1, 1}.HFUNC(Watch)
      << CI{"UNWATCH", CO::LOADING, 1, 0, 0, 0}.HFUNC(Unwatch)
      << CI{"DISCARD", CO::NOSCRIPT | CO::FAST | CO::LOADING, 1, 0, 0, 0}.MFUNC(Discard)
      << CI{"EVAL", CO::NOSCRIPT | CO::VARIADIC_KEYS, -3, 3, 3, 1}.MFUNC(Eval).SetValidator(
             &EvalValidator)
      << CI{"EVALSHA", CO::NOSCRIPT | CO::VARIADIC_KEYS, -3, 3, 3, 1}.MFUNC(EvalSha).SetValidator(
             &EvalValidator)
      << CI{"EXEC", CO::LOADING | CO::NOSCRIPT, 1, 0, 0, 1}.MFUNC(Exec)
      << CI{"PUBLISH", CO::LOADING | CO::FAST, 3, 0, 0, 0}.MFUNC(Publish)
      << CI{"SUBSCRIBE", CO::NOSCRIPT | CO::LOADING, -2, 0, 0, 0}.MFUNC(Subscribe)
      << CI{"UNSUBSCRIBE", CO::NOSCRIPT | CO::LOADING, -1, 0, 0, 0}.MFUNC(Unsubscribe)
      << CI{"PSUBSCRIBE", CO::NOSCRIPT | CO::LOADING, -2, 0, 0, 0}.MFUNC(PSubscribe)
      << CI{"PUNSUBSCRIBE", CO::NOSCRIPT | CO::LOADING, -1, 0, 0, 0}.MFUNC(PUnsubscribe)
      << CI{"FUNCTION", CO::NOSCRIPT, 2, 0, 0, 0}.MFUNC(Function)
      << CI{"MONITOR", CO::ADMIN, 1, 0, 0, 0}.MFUNC(Monitor)
      << CI{"PUBSUB", CO::LOADING | CO::FAST, -1, 0, 0, 0}.MFUNC(Pubsub);

  StreamFamily::Register(&registry_);
  StringFamily::Register(&registry_);
  GenericFamily::Register(&registry_);
  ListFamily::Register(&registry_);
  SetFamily::Register(&registry_);
  HSetFamily::Register(&registry_);
  ZSetFamily::Register(&registry_);
  JsonFamily::Register(&registry_);
  BitOpsFamily::Register(&registry_);

  server_family_.Register(&registry_);

  if (VLOG_IS_ON(1)) {
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
      if (!IsTransactional(&cid)) {
        LOG(INFO) << "    " << name;
      }
    });
  }
}

}  // namespace dfly
