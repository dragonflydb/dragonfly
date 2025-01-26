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
#include "server/transaction.h"

ABSL_FLAG(std::string, default_lua_flags, "",
          "Configure default flags for running Lua scripts: \n - Use 'allow-undeclared-keys' to "
          "allow accessing undeclared keys, \n - Use 'disable-atomicity' to allow "
          "running scripts non-atomically. \nSpecify multiple values "
          "separated by space, for example 'allow-undeclared-keys disable-atomicity' runs scripts "
          "non-atomically and allows accessing undeclared keys");

ABSL_FLAG(
    bool, lua_auto_async, false,
    "If enabled, call/pcall with discarded values are automatically replaced with acall/apcall.");

ABSL_FLAG(bool, lua_allow_undeclared_auto_correct, false,
          "If enabled, when a script that is not allowed to run with undeclared keys is trying to "
          "access undeclared keys, automaticaly set the script flag to be able to run with "
          "undeclared key.");

ABSL_FLAG(
    std::vector<std::string>, lua_undeclared_keys_shas,
    std::vector<std::string>({
        "351130589c64523cb98978dc32c64173a31244f3",  // Sidekiq, see #2442
        "6ae15ef4678593dc61f991c9953722d67d822776",  // Sidekiq, see #2442
        "34b1048274c8e50a0cc587a3ed9c383a82bb78c5"   // Sidekiq
    }),
    "Comma-separated list of Lua script SHAs which are allowed to access undeclared keys. SHAs are "
    "only looked at when loading the script, and new values do not affect already-loaded script.");

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

void ScriptMgr::Run(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder,
                    ConnectionContext* cntx) {
  string subcmd = absl::AsciiStrToUpper(ArgS(args, 0));

  if (subcmd == "HELP") {
    string_view kHelp[] = {
        "SCRIPT <subcommand> [<arg> [value] [opt] ...]",
        "Subcommands are:",
        "EXISTS <sha1> [<sha1> ...]",
        "   Return information about the existence of the scripts in the script cache.",
        "FLUSH",
        "   Flush the Lua scripts cache. Very dangerous on replicas.",
        "LOAD <script>",
        "   Load a script into the scripts cache without executing it.",
        "FLAGS <sha> [flags ...]",
        "   Set specific flags for script. Can be called before the sript is loaded.",
        "   The following flags are possible: ",
        "      - Use 'allow-undeclared-keys' to allow accessing undeclared keys",
        "      - Use 'disable-atomicity' to allow running scripts non-atomically",
        "LIST",
        "   Lists loaded scripts.",
        "LATENCY",
        "   Prints latency histograms in usec for every called function.",
        "GC",
        "   Invokes garbage collection on all unused interpreter instances.",
        "HELP",
        "   Prints this help."};
    auto rb = static_cast<RedisReplyBuilder*>(builder);
    return rb->SendSimpleStrArr(kHelp);
  }

  if (subcmd == "EXISTS" && args.size() > 1)
    return ExistsCmd(args, tx, builder);

  if (subcmd == "FLUSH")
    return FlushCmd(args, tx, builder);

  if (subcmd == "LIST")
    return ListCmd(tx, builder);

  if (subcmd == "LATENCY")
    return LatencyCmd(tx, builder);

  if (subcmd == "LOAD" && args.size() == 2)
    return LoadCmd(args, tx, builder, cntx);

  if (subcmd == "FLAGS" && args.size() > 2)
    return ConfigCmd(args, tx, builder);

  if (subcmd == "GC")
    return GCCmd(tx, builder);

  string err = absl::StrCat("Unknown subcommand or wrong number of arguments for '", subcmd,
                            "'. Try SCRIPT HELP.");
  builder->SendError(err, kSyntaxErrType);
}

void ScriptMgr::ExistsCmd(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder) const {
  vector<uint8_t> res(args.size() - 1, 0);
  for (size_t i = 1; i < args.size(); ++i) {
    if (string_view sha = ArgS(args, i); Find(sha)) {
      res[i - 1] = 1;
    }
  }

  auto rb = static_cast<RedisReplyBuilder*>(builder);
  rb->StartArray(res.size());
  for (uint8_t v : res) {
    rb->SendLong(v);
  }
}

void ScriptMgr::FlushCmd(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder) {
  FlushAllScript();

  return builder->SendOk();
}

void ScriptMgr::LoadCmd(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder,
                        ConnectionContext* cntx) {
  string_view body = ArgS(args, 1);
  auto rb = static_cast<RedisReplyBuilder*>(builder);
  if (body.empty()) {
    char sha[41];
    Interpreter::FuncSha1(body, sha);
    return rb->SendBulkString(sha);
  }

  BorrowedInterpreter interpreter{tx, &cntx->conn_state};

  auto res = Insert(body, interpreter);
  if (!res)
    return builder->SendError(res.error().Format());

  // Schedule empty callback inorder to journal command via transaction framework.
  tx->ScheduleSingleHop([](auto* t, auto* shard) { return OpStatus::OK; });

  return rb->SendBulkString(res.value());
}

void ScriptMgr::ConfigCmd(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder) {
  lock_guard lk{mu_};
  ScriptKey key{ArgS(args, 1)};
  auto& data = db_[key];

  for (auto flag : args.subspan(2)) {
    if (auto err = ScriptParams::ApplyFlags(facade::ToSV(flag), &data); err)
      return builder->SendError("Invalid config format: " + err.Format());
  }

  UpdateScriptCaches(key, data);

  // Schedule empty callback inorder to journal command via transaction framework.
  tx->ScheduleSingleHop([](auto* t, auto* shard) { return OpStatus::OK; });

  return builder->SendOk();
}

void ScriptMgr::ListCmd(Transaction* tx, SinkReplyBuilder* builder) const {
  vector<pair<string, ScriptData>> scripts = GetAll();
  auto rb = static_cast<RedisReplyBuilder*>(builder);
  rb->StartArray(scripts.size());
  for (const auto& [sha, data] : scripts) {
    rb->StartArray(2);
    rb->SendBulkString(sha);
    rb->SendBulkString(data.body);
  }
}

void ScriptMgr::LatencyCmd(Transaction* tx, SinkReplyBuilder* builder) const {
  absl::flat_hash_map<std::string, base::Histogram> result;
  fb2::Mutex mu;

  shard_set->pool()->AwaitFiberOnAll([&](auto* pb) {
    auto* ss = ServerState::tlocal();
    mu.lock();
    for (const auto& k_v : ss->call_latency_histos()) {
      result[k_v.first].Merge(k_v.second);
    }
    mu.unlock();
  });

  auto rb = static_cast<RedisReplyBuilder*>(builder);
  rb->StartArray(result.size());
  for (const auto& k_v : result) {
    rb->StartArray(2);
    rb->SendBulkString(k_v.first);
    rb->SendVerbatimString(k_v.second.ToString());
  }
}

void ScriptMgr::GCCmd(Transaction* tx, SinkReplyBuilder* builder) const {
  auto cb = [](Interpreter* ir) {
    ir->RunGC();
    ThisFiber::Yield();
  };
  shard_set->pool()->AwaitFiberOnAll(
      [cb](auto* pb) { ServerState::tlocal()->AlterInterpreters(cb); });
  return builder->SendOk();
}

// Check if script starts with lua flags instructions (--df flags=...).
io::Result<optional<ScriptMgr::ScriptParams>, GenericError> DeduceParams(string_view body) {
  static const regex kRegex{R"(^\s*?--!df flags=([^\s\n\r]*)[\s\n\r])"};
  cmatch matches;

  if (!regex_search(body.data(), matches, kRegex))
    return nullopt;

  ScriptMgr::ScriptParams params;
  if (auto err = ScriptMgr::ScriptParams::ApplyFlags(matches.str(1), &params); err)
    return nonstd::make_unexpected(err);

  return params;
}

unique_ptr<char[]> CharBufFromSV(string_view sv) {
  auto ptr = make_unique<char[]>(sv.size() + 1);
  memcpy(ptr.get(), sv.data(), sv.size());
  ptr[sv.size()] = '\0';
  return ptr;
}

io::Result<string, GenericError> ScriptMgr::Insert(string_view body, Interpreter* interpreter) {
  char sha_buf[64];
  Interpreter::FuncSha1(body, sha_buf);
  string_view sha{sha_buf, std::strlen(sha_buf)};

  if (interpreter->Exists(sha)) {
    return string{sha};
  }

  auto params_opt = DeduceParams(body);
  if (!params_opt)
    return params_opt.get_unexpected();
  auto params = params_opt->value_or(default_params_);

  auto undeclared_shas = absl::GetFlag(FLAGS_lua_undeclared_keys_shas);
  if (find(undeclared_shas.begin(), undeclared_shas.end(), sha) != undeclared_shas.end()) {
    params.undeclared_keys = true;
  }

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
    return nonstd::make_unexpected(GenericError{std::move(result)});

  lock_guard lk{mu_};
  auto [it, _] = db_.emplace(sha, InternalScriptData{params, nullptr});

  if (!it->second.body) {
    it->second.body = CharBufFromSV(body);
  }

  UpdateScriptCaches(sha, it->second);

  return string{sha};
}

optional<ScriptMgr::ScriptData> ScriptMgr::Find(std::string_view sha) const {
  if (sha.size() != ScriptKey{}.size())
    return std::nullopt;

  lock_guard lk{mu_};
  if (auto it = db_.find(sha); it != db_.end() && it->second.body)
    return ScriptData{it->second, it->second.body.get()};

  return std::nullopt;
}

void ScriptMgr::OnScriptError(std::string_view sha, std::string_view error) {
  ++tl_facade_stats->reply_stats.script_error_count;

  // Log script errors at most 5 times a second.
  LOG_EVERY_T(WARNING, 0.2) << "Error running script (call to " << sha << "): " << error;

  // If script has undeclared_keys and was not flaged to run in this mode we will change the
  // script flag - this will make script next run to not fail but run as global.
  if (absl::GetFlag(FLAGS_lua_allow_undeclared_auto_correct)) {
    size_t pos = error.rfind(kUndeclaredKeyErr);
    lock_guard lk{mu_};
    auto it = db_.find(sha);
    if (it == db_.end()) {
      return;
    }

    if (pos != string::npos) {
      it->second.undeclared_keys = true;
      LOG(WARNING) << "Setting undeclared_keys flag for script with sha : (" << sha << ")";
      UpdateScriptCaches(sha, it->second);
    }
  }
}

void ScriptMgr::FlushAllScript() {
  lock_guard lk{mu_};
  db_.clear();

  shard_set->pool()->AwaitFiberOnAll([](auto* pb) {
    ServerState* ss = ServerState::tlocal();
    ss->ResetInterpreter();
  });
}

vector<pair<string, ScriptMgr::ScriptData>> ScriptMgr::GetAll() const {
  vector<pair<string, ScriptData>> res;

  lock_guard lk{mu_};
  res.reserve(db_.size());
  for (const auto& [sha, data] : db_) {
    string body = data.body ? string{data.body.get()} : string{};
    res.emplace_back(string{sha.data(), sha.size()}, ScriptData{data, std::move(body)});
  }

  return res;
}

void ScriptMgr::UpdateScriptCaches(ScriptKey sha, ScriptParams params) const {
  shard_set->pool()->AwaitBrief([&sha, &params](auto index, auto* pb) {
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
