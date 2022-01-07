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

}  // namespace

Service::Service(ProactorPool* pp) : shard_set_(pp), pp_(*pp), server_family_(this)  {
  CHECK(pp);

  // We support less than 1024 threads.
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

  pp_.AwaitOnAll([&](uint32_t index, ProactorBase* pb) {
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

  engine_varz.reset();
  request_latency_usec.Shutdown();
  ping_qps.Shutdown();

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

  absl::Cleanup multi_error = [cntx] {
    if (cntx->conn_state.exec_state != ConnectionState::EXEC_INACTIVE) {
      cntx->conn_state.exec_state = ConnectionState::EXEC_ERROR;
    }
  };

  if (cid == nullptr) {
    return cntx->SendError(absl::StrCat("unknown command `", cmd_str, "`"));
  }

  bool under_multi = cntx->conn_state.exec_state != ConnectionState::EXEC_INACTIVE && !is_trans_cmd;
  if ((cid->arity() > 0 && args.size() != size_t(cid->arity())) ||
      (cid->arity() < 0 && args.size() < size_t(-cid->arity()))) {
    return cntx->SendError(WrongNumArgsError(cmd_str));
  }

  if (cid->key_arg_step() == 2 && (args.size() % 2) == 0) {
    return cntx->SendError(WrongNumArgsError(cmd_str));
  }

  if (under_multi && (cid->opt_mask() & CO::ADMIN)) {
    cntx->SendError("Can not run admin commands under multi-transactions");
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

    return cntx->SendSimpleRespString("QUEUED");
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
  }
}

void Service::DispatchMC(const MemcacheParser::Command& cmd, std::string_view value,
                         ConnectionContext* cntx) {
  absl::InlinedVector<MutableStrSpan, 8> args;
  char cmd_name[16];
  char set_opt[4] = {0};

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
      cntx->SendMCClientError("bad command line format");
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

void Service::RegisterHttp(HttpListenerBase* listener) {
  CHECK_NOTNULL(listener);
}

void Service::Quit(CmdArgList args, ConnectionContext* cntx) {
  cntx->SendOk();
  cntx->CloseConnection();
}

void Service::Exec(CmdArgList args, ConnectionContext* cntx) {
  if (cntx->conn_state.exec_state == ConnectionState::EXEC_INACTIVE) {
    return cntx->SendError("EXEC without MULTI");
  }

  if (cntx->conn_state.exec_state == ConnectionState::EXEC_ERROR) {
    cntx->conn_state.exec_state = ConnectionState::EXEC_INACTIVE;
    cntx->conn_state.exec_body.clear();
    return cntx->SendError("-EXECABORT Transaction discarded because of previous errors");
  }

  cntx->SendRespBlob(absl::StrCat("*", cntx->conn_state.exec_body.size(), "\r\n"));

  if (!cntx->ec() && !cntx->conn_state.exec_body.empty()) {
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
      if (cntx->ec())
        break;
    }

    VLOG(1) << "Exec unlocking " << cntx->conn_state.exec_body.size() << " commands";
    cntx->transaction->UnlockMulti();
  }

  cntx->conn_state.exec_state = ConnectionState::EXEC_INACTIVE;
  cntx->conn_state.exec_body.clear();
  VLOG(1) << "Exec completed";
}

void Service::Multi(CmdArgList args, ConnectionContext* cntx) {
  if (cntx->conn_state.exec_state != ConnectionState::EXEC_INACTIVE) {
    return cntx->SendError("MULTI calls can not be nested");
  }
  cntx->conn_state.exec_state = ConnectionState::EXEC_COLLECT;
  // TODO: to protect against huge exec transactions.
  return cntx->SendOk();
}

VarzValue::Map Service::GetVarzStats() {
  VarzValue::Map res;

  atomic_ulong num_keys{0};
  shard_set_.RunBriefInParallel([&](EngineShard* es) { num_keys += es->db_slice().DbSize(0); });
  res.emplace_back("keys", VarzValue::FromInt(num_keys.load()));

  return res;
}

using ServiceFunc = void (Service::*)(CmdArgList, ConnectionContext* cntx);

#define HFUNC(x) SetHandler(&Service::x)

void Service::RegisterCommands() {
  using CI = CommandId;

  constexpr auto kExecMask =
      CO::LOADING | CO::NOSCRIPT | CO::GLOBAL_TRANS;

  auto cb_exec = [this](CmdArgList sp, ConnectionContext* cntx) {
    this->Exec(std::move(sp), cntx);
  };

  registry_ << CI{"QUIT", CO::READONLY | CO::FAST, 1, 0, 0, 0}.HFUNC(Quit)
            << CI{"MULTI", CO::NOSCRIPT | CO::FAST | CO::LOADING | CO::STALE, 1, 0, 0, 0}.HFUNC(
                   Multi)
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
