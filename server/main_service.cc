// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/main_service.h"

extern "C" {
#include "redis/redis_aux.h"
}

#include <absl/cleanup/cleanup.h>
#include <absl/strings/ascii.h>
#include <xxhash.h>

#include <boost/fiber/operations.hpp>
#include <filesystem>

#include "base/logging.h"
#include "server/conn_context.h"
#include "server/error.h"
#include "server/generic_family.h"
#include "server/list_family.h"
#include "server/server_state.h"
#include "server/string_family.h"
#include "server/transaction.h"
#include "util/metrics/metrics.h"
#include "util/uring/uring_fiber_algo.h"
#include "util/varz.h"

DEFINE_uint32(port, 6380, "Redis port");
DEFINE_uint32(memcache_port, 0, "Memcached port");

namespace dfly {

using namespace std;
using namespace util;
using base::VarzValue;
using ::boost::intrusive_ptr;
namespace fibers = ::boost::fibers;
namespace this_fiber = ::boost::this_fiber;

namespace {

DEFINE_VARZ(VarzMapAverage, request_latency_usec);
DEFINE_VARZ(VarzQps, ping_qps);

std::optional<VarzFunction> engine_varz;
metrics::CounterFamily cmd_req("requests_total", "Number of served redis requests");

constexpr size_t kMaxThreadSize = 1024;

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

  void OnString(std::string_view str) final {
    rb_->SendBulkString(str);
  }

  void OnDouble(double d) final {
    rb_->SendDouble(d);
  }

  void OnInt(int64_t val) final {
    rb_->SendLong(val);
  }

  void OnArrayStart(unsigned len) final {
    LOG(FATAL) << "TBD";
  }

  void OnArrayEnd() final {
    LOG(FATAL) << "TBD";
  }

  void OnNil() final {
    rb_->SendNull();
  }

  void OnStatus(std::string_view str) {
    rb_->SendSimpleString(str);
  }

  void OnError(std::string_view str) {
    rb_->SendError(str);
  }

 private:
  RedisReplyBuilder* rb_;
};

}  // namespace

Service::Service(ProactorPool* pp) : pp_(*pp), shard_set_(pp), server_family_(this) {
  CHECK(pp);

  // We support less than 1024 threads and we support less than 1024 shards.
  // For example, Scan uses 10 bits in cursor to encode shard id it currently traverses.
  CHECK_LT(pp->size(), kMaxThreadSize);
  RegisterCommands();

  engine_varz.emplace("engine", [this] { return GetVarzStats(); });
}

Service::~Service() {
}

void Service::Init(util::AcceptServer* acceptor, const InitOpts& opts) {
  InitRedisTables();

  uint32_t shard_num = pp_.size() > 1 ? pp_.size() - 1 : pp_.size();
  shard_set_.Init(shard_num);

  pp_.Await([&](uint32_t index, ProactorBase* pb) {
    ServerState::tlocal()->Init();

    if (index < shard_count()) {
      shard_set_.InitThreadLocal(pb, !opts.disable_time_update);
    }
  });

  request_latency_usec.Init(&pp_);
  ping_qps.Init(&pp_);
  StringFamily::Init(&pp_);
  GenericFamily::Init(&pp_);
  cmd_req.Init(&pp_, {"type"});
}

void Service::Shutdown() {
  VLOG(1) << "Service::Shutdown";

  auto [current, switched] = server_family_.global_state()->Next(GlobalState::SHUTTING_DOWN);

  // TODO: to introduce BlockingNext that waits until the state is switched to idle.
  CHECK(switched) << "TBD " << GlobalState::Name(current);

  engine_varz.reset();
  request_latency_usec.Shutdown();
  ping_qps.Shutdown();

  pp_.AwaitFiberOnAll([](ProactorBase* pb) { ServerState::tlocal()->Shutdown(); });

  // to shutdown all the runtime components that depend on EngineShard.
  server_family_.Shutdown();
  StringFamily::Shutdown();
  GenericFamily::Shutdown();

  cmd_req.Shutdown();
  shard_set_.RunBlockingInParallel([&](EngineShard*) { EngineShard::DestroyThreadLocal(); });
}

void Service::DispatchCommand(CmdArgList args, ConnectionContext* cntx) {
  CHECK(!args.empty());
  DCHECK_NE(0u, shard_set_.size()) << "Init was not called";

  ToUpper(&args[0]);

  VLOG(2) << "Got: " << args;

  string_view cmd_str = ArgS(args, 0);
  bool is_trans_cmd = (cmd_str == "EXEC" || cmd_str == "MULTI");
  const CommandId* cid = registry_.Find(cmd_str);
  ServerState& etl = *ServerState::tlocal();
  ++etl.connection_stats.command_cnt;

  absl::Cleanup multi_error = [cntx] {
    if (cntx->conn_state.exec_state != ConnectionState::EXEC_INACTIVE) {
      cntx->conn_state.exec_state = ConnectionState::EXEC_ERROR;
    }
  };

  if (cid == nullptr) {
    return (*cntx)->SendError(absl::StrCat("unknown command `", cmd_str, "`"));
  }

  if (etl.gstate() == GlobalState::LOADING || etl.gstate() == GlobalState::SHUTTING_DOWN) {
    string err = absl::StrCat("Can not execute during ", GlobalState::Name(etl.gstate()));
    (*cntx)->SendError(err);
    return;
  }

  bool is_write_cmd = cid->opt_mask() & CO::WRITE;
  bool under_multi = cntx->conn_state.exec_state != ConnectionState::EXEC_INACTIVE && !is_trans_cmd;

  if (!etl.is_master && is_write_cmd) {
    (*cntx)->SendError("-READONLY You can't write against a read only replica.");
    return;
  }

  if ((cid->arity() > 0 && args.size() != size_t(cid->arity())) ||
      (cid->arity() < 0 && args.size() < size_t(-cid->arity()))) {
    return (*cntx)->SendError(WrongNumArgsError(cmd_str));
  }

  if (cid->key_arg_step() == 2 && (args.size() % 2) == 0) {
    return (*cntx)->SendError(WrongNumArgsError(cmd_str));
  }

  if (under_multi && (cid->opt_mask() & CO::ADMIN)) {
    (*cntx)->SendError("Can not run admin commands under multi-transactions");
    return;
  }

  std::move(multi_error).Cancel();

  if (cntx->conn_state.exec_state != ConnectionState::EXEC_INACTIVE && !is_trans_cmd) {
    // TODO: protect against aggregating huge transactions.
    StoredCmd stored_cmd{cid};
    stored_cmd.cmd.reserve(args.size());
    for (size_t i = 0; i < args.size(); ++i) {
      stored_cmd.cmd.emplace_back(ArgS(args, i));
    }
    cntx->conn_state.exec_body.push_back(std::move(stored_cmd));

    return (*cntx)->SendSimpleString("QUEUED");
  }

  uint64_t start_usec = ProactorBase::GetMonotonicTimeNs(), end_usec;

  // Create command transaction
  intrusive_ptr<Transaction> dist_trans;

  if (cid->first_key_pos() > 0 || (cid->opt_mask() & CO::GLOBAL_TRANS)) {
    dist_trans.reset(new Transaction{cid, &shard_set_});
    cntx->transaction = dist_trans.get();

    if (cid->first_key_pos() > 0) {
      dist_trans->InitByArgs(cntx->conn_state.db_index, args);
      cntx->last_command_debug.shards_count = cntx->transaction->unique_shard_cnt();
    }
  } else {
    cntx->transaction = nullptr;
  }

  cntx->cid = cid;
  cmd_req.Inc({cid->name()});
  cid->Invoke(args, cntx);
  end_usec = ProactorBase::GetMonotonicTimeNs();

  request_latency_usec.IncBy(cmd_str, (end_usec - start_usec) / 1000);
  if (dist_trans) {
    cntx->last_command_debug.clock = dist_trans->txid();
    cntx->last_command_debug.is_ooo = dist_trans->IsOOO();
  }
  cntx->transaction = nullptr;
}

void Service::DispatchMC(const MemcacheParser::Command& cmd, std::string_view value,
                         ConnectionContext* cntx) {
  absl::InlinedVector<MutableStrSpan, 8> args;
  char cmd_name[16];
  char set_opt[4] = {0};
  MCReplyBuilder* mc_builder = static_cast<MCReplyBuilder*>(cntx->reply_builder());
  switch (cmd.type) {
    case MemcacheParser::REPLACE:
      strcpy(cmd_name, "SET");
      strcpy(set_opt, "XX");
      break;
    case MemcacheParser::SET:
      strcpy(cmd_name, "SET");
      break;
    case MemcacheParser::ADD:
      strcpy(cmd_name, "SET");
      strcpy(set_opt, "NX");
      break;
    case MemcacheParser::GET:
      strcpy(cmd_name, "GET");
      break;
    default:
      mc_builder->SendClientError("bad command line format");
      return;
  }

  args.emplace_back(cmd_name, strlen(cmd_name));
  char* key = const_cast<char*>(cmd.key.data());
  args.emplace_back(key, cmd.key.size());

  if (MemcacheParser::IsStoreCmd(cmd.type)) {
    char* v = const_cast<char*>(value.data());
    args.emplace_back(v, value.size());

    if (set_opt[0]) {
      args.emplace_back(set_opt, strlen(set_opt));
    }
  }

  CmdArgList arg_list{args.data(), args.size()};
  DispatchCommand(arg_list, cntx);
}

bool Service::IsLocked(DbIndex db_index, std::string_view key) const {
  ShardId sid = Shard(key, shard_count());
  KeyLockArgs args;
  args.db_index = db_index;
  args.args = ArgSlice{&key, 1};
  args.key_step = 1;
  bool is_open = pp_.at(sid)->AwaitBrief(
      [args] { return EngineShard::tlocal()->db_slice().CheckLock(IntentLock::EXCLUSIVE, args); });
  return !is_open;
}

bool Service::IsShardSetLocked() const {
  std::atomic_uint res{0};

  shard_set_.RunBriefInParallel([&](EngineShard* shard) {
    bool unlocked = shard->shard_lock()->Check(IntentLock::SHARED);
    res.fetch_add(!unlocked, memory_order_relaxed);
  });

  return res.load() != 0;
}

void Service::RegisterHttp(HttpListenerBase* listener) {
  CHECK_NOTNULL(listener);
  http_listener_ = listener;
}

void Service::Quit(CmdArgList args, ConnectionContext* cntx) {
  (*cntx)->SendOk();
  (*cntx)->CloseConnection();
}

void Service::Multi(CmdArgList args, ConnectionContext* cntx) {
  if (cntx->conn_state.exec_state != ConnectionState::EXEC_INACTIVE) {
    return (*cntx)->SendError("MULTI calls can not be nested");
  }
  cntx->conn_state.exec_state = ConnectionState::EXEC_COLLECT;
  // TODO: to protect against huge exec transactions.
  return (*cntx)->SendOk();
}

void Service::Eval(CmdArgList args, ConnectionContext* cntx) {
  string_view body = ArgS(args, 1);
  string_view num_keys_str = ArgS(args, 2);
  int32_t num_keys;

  if (!absl::SimpleAtoi(num_keys_str, &num_keys) || num_keys < 0) {
    return (*cntx)->SendError(kInvalidIntErr);
  }

  if (unsigned(num_keys) > args.size() - 3) {
    return (*cntx)->SendError("Number of keys can't be greater than number of args");
  }

  ServerState* ss = ServerState::tlocal();
  lock_guard lk(ss->interpreter_mutex);
  Interpreter& script = ss->GetInterpreter();
  string error;
  char f_id[48];
  bool success = script.Execute(body, f_id, &error);
  if (success) {
    EvalSerializer ser{static_cast<RedisReplyBuilder*>(cntx->reply_builder())};
    string error;

    if (!script.Serialize(&ser, &error)) {
      (*cntx)->SendError(error);
    }
  } else {
    string resp = absl::StrCat("Error running script (call to ", f_id, "): ", error);
    return (*cntx)->SendError(resp);
  }
}

void Service::Exec(CmdArgList args, ConnectionContext* cntx) {
  if (cntx->conn_state.exec_state == ConnectionState::EXEC_INACTIVE) {
    return (*cntx)->SendError("EXEC without MULTI");
  }

  if (cntx->conn_state.exec_state == ConnectionState::EXEC_ERROR) {
    cntx->conn_state.exec_state = ConnectionState::EXEC_INACTIVE;
    cntx->conn_state.exec_body.clear();
    return (*cntx)->SendError("-EXECABORT Transaction discarded because of previous errors");
  }

  (*cntx)->SendRespBlob(absl::StrCat("*", cntx->conn_state.exec_body.size(), "\r\n"));

  if (!(*cntx)->GetError() && !cntx->conn_state.exec_body.empty()) {
    CmdArgVec str_list;

    for (auto& scmd : cntx->conn_state.exec_body) {
      str_list.resize(scmd.cmd.size());
      for (size_t i = 0; i < scmd.cmd.size(); ++i) {
        string& s = scmd.cmd[i];
        str_list[i] = MutableStrSpan{s.data(), s.size()};
      }

      cntx->transaction->SetExecCmd(scmd.descr);
      CmdArgList cmd_arg_list{str_list.data(), str_list.size()};
      cntx->transaction->InitByArgs(cntx->conn_state.db_index, cmd_arg_list);
      scmd.descr->Invoke(cmd_arg_list, cntx);
      if ((*cntx)->GetError())
        break;
    }

    VLOG(1) << "Exec unlocking " << cntx->conn_state.exec_body.size() << " commands";
    cntx->transaction->UnlockMulti();
  }

  cntx->conn_state.exec_state = ConnectionState::EXEC_INACTIVE;
  cntx->conn_state.exec_body.clear();
  VLOG(1) << "Exec completed";
}

VarzValue::Map Service::GetVarzStats() {
  VarzValue::Map res;

  Metrics m = server_family_.GetMetrics();

  res.emplace_back("keys", VarzValue::FromInt(m.db.key_count));
  res.emplace_back("obj_mem_usage", VarzValue::FromInt(m.db.obj_memory_usage));
  double load = double(m.db.key_count) / (1 + m.db.bucket_count);
  res.emplace_back("table_load_factor", VarzValue::FromDouble(load));

  return res;
}

using ServiceFunc = void (Service::*)(CmdArgList, ConnectionContext* cntx);

#define HFUNC(x) SetHandler(&Service::x)

void Service::RegisterCommands() {
  using CI = CommandId;

  constexpr auto kExecMask = CO::LOADING | CO::NOSCRIPT | CO::GLOBAL_TRANS;

  auto cb_exec = [this](CmdArgList sp, ConnectionContext* cntx) {
    this->Exec(std::move(sp), cntx);
  };

  registry_ << CI{"QUIT", CO::READONLY | CO::FAST, 1, 0, 0, 0}.HFUNC(Quit)
            << CI{"MULTI", CO::NOSCRIPT | CO::FAST | CO::LOADING | CO::STALE, 1, 0, 0, 0}.HFUNC(
                   Multi)
            << CI{"EVAL", CO::NOSCRIPT, -3, 0, 0, 0}.HFUNC(Eval)
            << CI{"EXEC", kExecMask, 1, 0, 0, 0}.SetHandler(cb_exec);

  StringFamily::Register(&registry_);
  GenericFamily::Register(&registry_);
  ListFamily::Register(&registry_);
  server_family_.Register(&registry_);

  LOG(INFO) << "Multi-key commands are: ";

  registry_.Traverse([](std::string_view key, const CI& cid) {
    if (cid.is_multi_key()) {
      string key_len;
      if (cid.last_key_pos() < 0)
        key_len = "unlimited";
      else
        key_len = absl::StrCat(cid.last_key_pos() - cid.first_key_pos() + 1);
      LOG(INFO) << "    " << key << ": with " << key_len << " keys";
    }
  });
}

}  // namespace dfly
