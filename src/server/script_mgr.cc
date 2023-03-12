// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/script_mgr.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>

#include <regex>
#include <string>

#include "base/flags.h"
#include "base/logging.h"
#include "core/interpreter.h"
#include "facade/error.h"
#include "server/engine_shard_set.h"
#include "server/server_state.h"

ABSL_FLAG(std::string, default_lua_config, "",
          "Configure default mode for running Lua scripts: \n - Use 'allow-undeclared-keys' to "
          "allow accessing undeclared keys, \n - Use 'disable-atomicity' to allow "
          "running scripts non-atomically. \nSpecify multiple values "
          "separated by space, for example 'allow-undeclared-keys disable-atomicity' runs scripts "
          "non-atomically and allows accessing undeclared keys");

namespace dfly {

using namespace std;
using namespace facade;

ScriptMgr::ScriptMgr() {
  // Build default script config
  std::string config = absl::GetFlag(FLAGS_default_lua_config);

  static_assert(ScriptParams{}.atomic && !ScriptParams{}.undeclared_keys);

  auto err = ScriptParams::ApplyPragmas(config, &default_params_);
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
        "CONFIGURE <sha> [options ...]",
        "   The following options are possible: ",
        "      - Use 'allow-undeclared-keys' to allow accessing undeclared keys",
        "      - Use 'disable-atomicity' to allow running scripts non-atomically to improve "
        "performance",
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

  if (subcmd == "CONFIG" && args.size() > 2)
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

  // no need to lock the interpreter since we do not mess the stack.
  string error_or_id;
  Interpreter::AddResult add_result = interpreter->AddFunction(body, &error_or_id);

  if (add_result == Interpreter::ALREADY_EXISTS) {
    return (*cntx)->SendBulkString(error_or_id);
  }
  if (add_result == Interpreter::COMPILE_ERR) {
    return (*cntx)->SendError(error_or_id);
  }

  if (auto err = Insert(error_or_id, body); err) {
    return (*cntx)->SendError(err.Format());
  }

  return (*cntx)->SendBulkString(error_or_id);
}

void ScriptMgr::ConfigCmd(CmdArgList args, ConnectionContext* cntx) {
  lock_guard lk{mu_};
  ScriptKey key{ArgS(args, 1)};
  auto& data = db_[key];

  for (auto pragma : args.subspan(2)) {
    if (auto err = ScriptParams::ApplyPragmas(facade::ToSV(pragma), &data); err)
      return (*cntx)->SendError("Invalid config format: " + err.Format());
  }

  UpdateScriptCaches(key, data);

  return (*cntx)->SendOk();
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

// Search for pragmas in script body
io::Result<optional<ScriptMgr::ScriptParams>, GenericError> DeduceParams(string_view body) {
  static const regex kRegex{"^\\s*?--\\s*?pragma\\s*?:(.*)"};
  cmatch matches;

  if (!regex_search(body.data(), matches, kRegex))
    return nullopt;

  ScriptMgr::ScriptParams params;
  if (auto err = ScriptMgr::ScriptParams::ApplyPragmas(matches.str(1), &params); err)
    return nonstd::make_unexpected(err);

  return params;
}

GenericError ScriptMgr::Insert(string_view id, string_view body) {
  auto params = DeduceParams(body);
  if (!params)
    return params.get_unexpected().value();

  lock_guard lk{mu_};
  auto [it, _] = db_.emplace(id, InternalScriptData{params->value_or(default_params_), nullptr});

  if (auto& body_ptr = it->second.body; !body_ptr) {
    body_ptr.reset(new char[body.size() + 1]);
    memcpy(body_ptr.get(), body.data(), body.size());
    body_ptr[body.size()] = '\0';
  }

  UpdateScriptCaches(id, it->second);

  return {};
}

optional<ScriptMgr::ScriptData> ScriptMgr::Find(std::string_view sha) const {
  if (sha.size() != ScriptKey{}.size())
    return std::nullopt;

  lock_guard lk{mu_};
  if (auto it = db_.find(sha); it != db_.end() && it->second.body)
    return ScriptData{it->second, it->second.body.get()};

  return std::nullopt;
}

vector<pair<string, string>> ScriptMgr::GetAll() const {
  vector<pair<string, string>> res;

  lock_guard lk{mu_};
  res.reserve(db_.size());
  for (const auto& [sha, data] : db_) {
    res.emplace_back(string{sha.data(), sha.size()}, data.body.get());
  }

  return res;
}

void ScriptMgr::UpdateScriptCaches(ScriptKey sha, ScriptParams params) const {
  shard_set->pool()->Await([&sha, &params](auto index, auto* pb) {
    ServerState::tlocal()->SetScriptParams(sha, params);
  });
}

GenericError ScriptMgr::ScriptParams::ApplyPragmas(string_view config, ScriptParams* params) {
  auto parts = absl::StrSplit(config, absl::ByAnyChar(",; "), absl::SkipEmpty());
  for (auto pragma : parts) {
    if (pragma == "disable-atomicity") {
      params->atomic = false;
      continue;
    }

    if (pragma == "allow-undeclared-keys") {
      params->undeclared_keys = true;
      continue;
    }

    return GenericError{make_error_code(errc::invalid_argument),
                        "Invalid pragma: "s + string{pragma}};
  }

  return {};
}

}  // namespace dfly
