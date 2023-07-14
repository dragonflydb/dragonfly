// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/script_mgr.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>
#include <bits/utility.h>

#include <algorithm>
#include <boost/context/detail/exception.hpp>
#include <cstdint>
#include <cstring>
#include <iosfwd>
#include <mutex>
#include <regex>
#include <string>
#include <type_traits>

#include "absl/container/flat_hash_map.h"
#include "absl/flags/flag.h"
#include "absl/hash/hash.h"
#include "absl/meta/type_traits.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "base/expected.hpp"
#include "base/histogram.h"
#include "core/interpreter.h"
#include "facade/conn_context.h"
#include "facade/error.h"
#include "facade/op_status.h"
#include "facade/reply_builder.h"
#include "glog/logging.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"
#include "server/server_state.h"
#include "server/transaction.h"
#include "util/fibers/detail/wait_queue.h"
#include "util/proactor_pool.h"

ABSL_FLAG(std::string, default_lua_flags, "",
          "Configure default flags for running Lua scripts: \n - Use 'allow-undeclared-keys' to "
          "allow accessing undeclared keys, \n - Use 'disable-atomicity' to allow "
          "running scripts non-atomically. \nSpecify multiple values "
          "separated by space, for example 'allow-undeclared-keys disable-atomicity' runs scripts "
          "non-atomically and allows accessing undeclared keys");

ABSL_FLAG(
    bool, lua_auto_async, false,
    "If enabled, call/pcall with discarded values are automatically replaced with acall/apcall.");

namespace dfly {

using namespace std;
using namespace facade;
using namespace util;

ScriptMgr::ScriptMgr() {
  // Build default script flags
  string flags = absl::GetFlag(FLAGS_default_lua_flags);

  static_assert(ScriptParams{}.atomic && !ScriptParams{}.undeclared_keys);

  auto err = ScriptParams::ApplyFlags(flags, &default_params_);
  CHECK(!err) << err.Format();
}

ScriptMgr::ScriptKey::ScriptKey(string_view sha) : array{} {
  DCHECK_EQ(sha.size(), size());
  memcpy(data(), sha.data(), size());
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
        "FLAGS <sha> [flags ...]",
        "   Set specific flags for script. Can be called before the sript is loaded."
        "   The following flags are possible: ",
        "      - Use 'allow-undeclared-keys' to allow accessing undeclared keys",
        "      - Use 'disable-atomicity' to allow running scripts non-atomically",
        "LIST",
        "   Lists loaded scripts.",
        "LATENCY",
        "   Prints latency histograms in usec for every called function.",
        "HELP"
        "   Prints this help."};
    return (*cntx)->SendSimpleStrArr(kHelp);
  }

  if (subcmd == "EXISTS" && args.size() > 1)
    return ExistsCmd(args, cntx);

  if (subcmd == "LIST")
    return ListCmd(cntx);

  if (subcmd == "LATENCY")
    return LatencyCmd(cntx);

  if (subcmd == "LOAD" && args.size() == 2)
    return LoadCmd(args, cntx);

  if (subcmd == "FLAGS" && args.size() > 2)
    return ConfigCmd(args, cntx);

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

  auto res = Insert(body, interpreter);
  if (!res)
    return (*cntx)->SendError(res.error().Format());

  // Schedule empty callback inorder to journal command via transaction framework.
  auto cb = [&](Transaction* t, EngineShard* shard) { return OpStatus::OK; };

  cntx->transaction->ScheduleSingleHop(std::move(cb));
  return (*cntx)->SendBulkString(res.value());
}

void ScriptMgr::ConfigCmd(CmdArgList args, ConnectionContext* cntx) {
  lock_guard lk{mu_};
  ScriptKey key{ArgS(args, 1)};
  auto& data = db_[key];

  for (auto flag : args.subspan(2)) {
    if (auto err = ScriptParams::ApplyFlags(facade::ToSV(flag), &data); err)
      return (*cntx)->SendError("Invalid config format: " + err.Format());
  }

  UpdateScriptCaches(key, data);

  return (*cntx)->SendOk();
}

void ScriptMgr::ListCmd(ConnectionContext* cntx) const {
  vector<pair<string, ScriptData>> scripts = GetAll();
  (*cntx)->StartArray(scripts.size());
  for (const auto& [sha, data] : scripts) {
    (*cntx)->StartArray(data.orig_body.empty() ? 2 : 3);
    (*cntx)->SendBulkString(sha);
    (*cntx)->SendBulkString(data.body);
    if (!data.orig_body.empty())
      (*cntx)->SendBulkString(data.orig_body);
  }
}

void ScriptMgr::LatencyCmd(ConnectionContext* cntx) const {
  absl::flat_hash_map<std::string, base::Histogram> result;
  Mutex mu;

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

// Check if script starts with shebang (#!lua). If present, look for flags parameter and truncate
// it.
io::Result<optional<ScriptMgr::ScriptParams>, GenericError> DeduceParams(string_view* body) {
  static const regex kRegex{"^\\s*?#!lua.*?flags=([^\\s\\n\\r]*).*[\\s\\r\\n]"};
  cmatch matches;

  if (!regex_search(body->data(), matches, kRegex))
    return nullopt;

  ScriptMgr::ScriptParams params;
  if (auto err = ScriptMgr::ScriptParams::ApplyFlags(matches.str(1), &params); err)
    return nonstd::make_unexpected(err);

  *body = body->substr(matches[0].length());
  return params;
}

unique_ptr<char[]> CharBufFromSV(string_view sv) {
  auto ptr = make_unique<char[]>(sv.size() + 1);
  memcpy(ptr.get(), sv.data(), sv.size());
  ptr[sv.size()] = '\0';
  return ptr;
}

io::Result<string, GenericError> ScriptMgr::Insert(string_view body, Interpreter* interpreter) {
  // Calculate hash before removing shebang (#!lua).
  char sha_buf[64];
  Interpreter::FuncSha1(body, sha_buf);
  string_view sha{sha_buf, std::strlen(sha_buf)};

  if (interpreter->Exists(sha)) {
    return string{sha};
  }

  string_view orig_body = body;

  auto params_opt = DeduceParams(&body);
  if (!params_opt)
    return params_opt.get_unexpected();
  auto params = params_opt->value_or(default_params_);

  // If the script is atomic, check for possible squashing optimizations.
  // For non atomic modes, squashing increases the time locks are held, which
  // can decrease throughput with frequently accessed keys.
  optional<string> async_body;
  if (params.atomic && absl::GetFlag(FLAGS_lua_auto_async)) {
    if (async_body = Interpreter::DetectPossibleAsyncCalls(body); async_body)
      body = *async_body;
  }

  string result;
  Interpreter::AddResult add_result = interpreter->AddFunction(sha, body, &result);
  if (add_result == Interpreter::COMPILE_ERR)
    return nonstd::make_unexpected(GenericError{move(result)});

  lock_guard lk{mu_};
  auto [it, _] = db_.emplace(sha, InternalScriptData{params, nullptr});

  if (!it->second.body) {
    it->second.body = CharBufFromSV(body);
    if (body != orig_body)
      it->second.orig_body = CharBufFromSV(orig_body);
  }

  UpdateScriptCaches(sha, it->second);

  return string{sha};
}

optional<ScriptMgr::ScriptData> ScriptMgr::Find(std::string_view sha) const {
  if (sha.size() != ScriptKey{}.size())
    return std::nullopt;

  lock_guard lk{mu_};
  if (auto it = db_.find(sha); it != db_.end() && it->second.body)
    return ScriptData{it->second, it->second.body.get(), {}};

  return std::nullopt;
}

vector<pair<string, ScriptMgr::ScriptData>> ScriptMgr::GetAll() const {
  vector<pair<string, ScriptData>> res;

  lock_guard lk{mu_};
  res.reserve(db_.size());
  for (const auto& [sha, data] : db_) {
    string body = data.body ? string{data.body.get()} : string{};
    string orig_body = data.orig_body ? string{data.orig_body.get()} : string{};
    res.emplace_back(string{sha.data(), sha.size()}, ScriptData{data, move(body), move(orig_body)});
  }

  return res;
}

void ScriptMgr::UpdateScriptCaches(ScriptKey sha, ScriptParams params) const {
  shard_set->pool()->Await([&sha, &params](auto index, auto* pb) {
    ServerState::tlocal()->SetScriptParams(sha, params);
  });
}

bool ScriptMgr::AreGlobalByDefault() const {
  return default_params_.undeclared_keys && default_params_.atomic;
}

GenericError ScriptMgr::ScriptParams::ApplyFlags(string_view config, ScriptParams* params) {
  auto parts = absl::StrSplit(config, absl::ByAnyChar(",; "), absl::SkipEmpty());
  for (auto flag : parts) {
    if (flag == "disable-atomicity") {
      params->atomic = false;
      continue;
    }

    if (flag == "allow-undeclared-keys") {
      params->undeclared_keys = true;
      continue;
    }

    if (flag == "no-writes") {  // Used by Redis.
      // TODO: lock read-only.
      continue;
    }

    return GenericError{"Invalid flag: "s + string{flag}};
  }

  return {};
}

}  // namespace dfly
