// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/script_mgr.h"

#include <absl/strings/str_cat.h>

#include "base/logging.h"
#include "core/interpreter.h"
#include "server/server_state.h"

namespace dfly {

using namespace std;

ScriptMgr::ScriptMgr(EngineShardSet* ess) : ess_(ess) {
}

void ScriptMgr::Run(CmdArgList args, ConnectionContext* cntx) {
  string_view subcmd = ArgS(args, 0);

  if (args.size() == 1 && subcmd == "HELP") {
    string_view kHelp[] = {
        "SCRIPT <subcommand> [<arg> [value] [opt] ...]",
        "Subcommands are:",
        "EXISTS <sha1> [<s  ha1> ...]",
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
    string error_or_id;
    Interpreter::AddResult add_result = interpreter.AddFunction(body, &error_or_id);
    if (add_result == Interpreter::ALREADY_EXISTS) {
      return (*cntx)->SendBulkString(error_or_id);
    }
    if (add_result == Interpreter::COMPILE_ERR) {
      return (*cntx)->SendError(error_or_id);
    }

    ScriptKey sha1;
    CHECK_EQ(sha1.size(), error_or_id.size());
    memcpy(sha1.data(), error_or_id.data(), sha1.size());

    lock_guard lk(mu_);
    auto [it, inserted] = db_.emplace(sha1, nullptr);
    if (inserted) {
      it->second.reset(new char[body.size() + 1]);
      memcpy(it->second.get(), body.data(), body.size());
      it->second[body.size()] = '\0';
    }
    return (*cntx)->SendBulkString(error_or_id);
  }

  cntx->reply_builder()->SendError(absl::StrCat(
      "Unknown subcommand or wrong number of arguments for '", subcmd, "'. Try SCRIPT HELP."));
}  // namespace dfly

}  // namespace dfly
