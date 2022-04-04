// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/script_mgr.h"

#include <absl/strings/str_cat.h>

#include "base/logging.h"
#include "core/interpreter.h"
#include "facade/error.h"
#include "server/server_state.h"

namespace dfly {

using namespace std;
using namespace facade;

ScriptMgr::ScriptMgr(EngineShardSet* ess) : ess_(ess) {
}

void ScriptMgr::Run(CmdArgList args, ConnectionContext* cntx) {
  string_view subcmd = ArgS(args, 0);

  if (args.size() == 1 && subcmd == "HELP") {
    string_view kHelp[] = {
        "SCRIPT <subcommand> [<arg> [value] [opt] ...]",
        "Subcommands are:",
        "EXISTS <sha1> [<sha1> ...]",
        "   Return information about the existence of the scripts in the script cache.",
        "LOAD <script>",
        "   Load a script into the scripts cache without executing it.",
        "HELP"
        "   Prints this help."};
    return (*cntx)->SendSimpleStrArr(kHelp, ABSL_ARRAYSIZE(kHelp));
  }

  if (args.size() == 2 && subcmd == "LOAD") {
    string_view body = ArgS(args, 1);
    body = absl::StripAsciiWhitespace(body);

    if (body.empty())
      return (*cntx)->SendError("Refuse to load empty script");

    Interpreter& interpreter = ServerState::tlocal()->GetInterpreter();
    // no need to lock the interpreter since we do not mess the stack.
    string error_or_id;
    Interpreter::AddResult add_result = interpreter.AddFunction(body, &error_or_id);
    if (add_result == Interpreter::ALREADY_EXISTS) {
      return (*cntx)->SendBulkString(error_or_id);
    }
    if (add_result == Interpreter::COMPILE_ERR) {
      return (*cntx)->SendError(error_or_id);
    }

    InsertFunction(error_or_id, body);
    return (*cntx)->SendBulkString(error_or_id);
  }

  string err = absl::StrCat("Unknown subcommand or wrong number of arguments for '", subcmd,
                            "'. Try SCRIPT HELP.");
  cntx->reply_builder()->SendError(err, kSyntaxErr);
}

bool ScriptMgr::InsertFunction(std::string_view id, std::string_view body) {
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
  ScriptKey key;
  CHECK_EQ(key.size(), sha.size());
  memcpy(key.data(), sha.data(), key.size());

  lock_guard lk(mu_);
  auto it = db_.find(key);
  if (it == db_.end())
    return nullptr;

  return it->second.get();
}

}  // namespace dfly
