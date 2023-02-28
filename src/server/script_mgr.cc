// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/script_mgr.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>

#include "base/logging.h"
#include "core/interpreter.h"
#include "facade/error.h"
#include "server/engine_shard_set.h"
#include "server/server_state.h"

namespace dfly {

using namespace std;
using namespace facade;

ScriptMgr::ScriptMgr() {
}

void ScriptMgr::Run(CmdArgList args, ConnectionContext* cntx) {
  string_view subcmd = ArgS(args, 0);

  if (subcmd == "HELP") {
    string_view kHelp[] = {
        "SCRIPT <subcommand> [<arg> [value] [opt] ...]",
        "Subcommands are:",
        "EXISTS <sha1> [<sha1> ...]",
        "   Return information about the existence of the scripts in the script cache.",
        "LOAD <script>",
        "   Load a script into the scripts cache without executing it.",
        "LIST",
        "   Lists loaded scripts.",
        "LATENCY",
        "   Prints latency histograms in usec for every called function.",
        "HELP"
        "   Prints this help."};
    return (*cntx)->SendSimpleStrArr(kHelp, ABSL_ARRAYSIZE(kHelp));
  }

  if (subcmd == "EXISTS" && args.size() > 1)
    return ExistsCmd(args, cntx);

  if (subcmd == "LIST")
    return ListCmd(cntx);

  if (subcmd == "LATENCY")
    return LatencyCmd(cntx);

  if (subcmd == "LOAD" && args.size() == 2)
    return LoadCmd(args, cntx);

  string err = absl::StrCat("Unknown subcommand or wrong number of arguments for '", subcmd,
                            "'. Try SCRIPT HELP.");
  cntx->reply_builder()->SendError(err, kSyntaxErrType);
}

void ScriptMgr::ExistsCmd(CmdArgList args, ConnectionContext* cntx) const {
  vector<uint8_t> res(args.size() - 1, 0);
  for (size_t i = 1; i < args.size(); ++i) {
    if (string_view sha = ArgS(args, i); Find(sha)) {
      res[i - 1] = 1;
    }
  }

  (*cntx)->StartArray(res.size());
  for (uint8_t v : res) {
    (*cntx)->SendLong(v);
  }
  return;
}

void ScriptMgr::LoadCmd(CmdArgList args, ConnectionContext* cntx) {
  string_view body = ArgS(args, 1);

  if (body.empty()) {
    char sha[41];
    Interpreter::FuncSha1(body, sha);
    return (*cntx)->SendBulkString(sha);
  }

  ServerState* ss = ServerState::tlocal();
  auto interpreter = ss->BorrowInterpreter();
  absl::Cleanup clean = [ss, interpreter]() { ss->ReturnInterpreter(interpreter); };

  // no need to lock the interpreter since we do not mess the stack.
  string error_or_id;
  Interpreter::AddResult add_result = interpreter->AddFunction(body, &error_or_id);

  if (add_result == Interpreter::ALREADY_EXISTS) {
    return (*cntx)->SendBulkString(error_or_id);
  }
  if (add_result == Interpreter::COMPILE_ERR) {
    return (*cntx)->SendError(error_or_id);
  }

  Insert(error_or_id, body);

  return (*cntx)->SendBulkString(error_or_id);
}

void ScriptMgr::ListCmd(ConnectionContext* cntx) const {
  vector<pair<string, string>> scripts = GetAll();
  (*cntx)->StartArray(scripts.size());
  for (const auto& k_v : scripts) {
    (*cntx)->StartArray(2);
    (*cntx)->SendBulkString(k_v.first);
    (*cntx)->SendBulkString(k_v.second);
  }
}

void ScriptMgr::LatencyCmd(ConnectionContext* cntx) const {
  absl::flat_hash_map<std::string, base::Histogram> result;
  boost::fibers::mutex mu;

  shard_set->pool()->AwaitFiberOnAll([&](auto* pb) {
    auto* ss = ServerState::tlocal();
    mu.lock();
    for (const auto& k_v : ss->call_latency_histos()) {
      result[k_v.first].Merge(k_v.second);
    }
    mu.unlock();
  });

  (*cntx)->StartArray(result.size());
  for (const auto& k_v : result) {
    (*cntx)->StartArray(2);
    (*cntx)->SendBulkString(k_v.first);
    (*cntx)->SendBulkString(k_v.second.ToString());
  }
}

bool ScriptMgr::Insert(std::string_view id, std::string_view body) {
  ScriptKey key;
  CHECK_EQ(key.size(), id.size());
  memcpy(key.data(), id.data(), key.size());

  lock_guard lk(mu_);
  auto [it, inserted] = db_.emplace(key, nullptr);
  if (inserted) {
    it->second.reset(new char[body.size() + 1]);
    memcpy(it->second.get(), body.data(), body.size());
    it->second[body.size()] = '\0';
  }
  return inserted;
}

const char* ScriptMgr::Find(std::string_view sha) const {
  if (sha.size() != 40)
    return nullptr;

  ScriptKey key;
  memcpy(key.data(), sha.data(), key.size());

  lock_guard lk(mu_);
  auto it = db_.find(key);
  if (it == db_.end())
    return nullptr;

  return it->second.get();
}

vector<pair<string, string>> ScriptMgr::GetAll() const {
  vector<pair<string, string>> res;

  lock_guard lk(mu_);
  res.reserve(db_.size());
  for (const auto& k_v : db_) {
    string key(k_v.first.data(), k_v.first.size());

    res.emplace_back(move(key), k_v.second.get());
  }

  return res;
}

}  // namespace dfly
