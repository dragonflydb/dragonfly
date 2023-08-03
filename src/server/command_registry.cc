// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/command_registry.h"

#include <absl/strings/str_split.h>
#include <absl/time/clock.h>

#include "absl/strings/str_cat.h"
#include "base/bits.h"
#include "base/flags.h"
#include "base/logging.h"
#include "facade/error.h"
#include "server/conn_context.h"
#include "server/server_state.h"

using namespace std;
ABSL_FLAG(vector<string>, rename_command, {},
          "Change the name of commands, format is: <cmd1_name>=<cmd1_new_name>, "
          "<cmd2_name>=<cmd2_new_name>");

namespace dfly {

using namespace facade;

using absl::AsciiStrToUpper;
using absl::GetFlag;
using absl::StrAppend;
using absl::StrCat;
using absl::StrSplit;

CommandId::CommandId(const char* name, uint32_t mask, int8_t arity, int8_t first_key,
                     int8_t last_key, int8_t step)
    : facade::CommandId(name, mask, arity, first_key, last_key, step) {
  if (mask & CO::ADMIN)
    opt_mask_ |= CO::NOSCRIPT;

  if (mask & CO::BLOCKING)
    opt_mask_ |= CO::REVERSE_MAPPING;
}

bool CommandId::IsTransactional() const {
  if (first_key_ > 0 || (opt_mask_ & CO::GLOBAL_TRANS) || (opt_mask_ & CO::NO_KEY_JOURNAL))
    return true;

  if (name_ == "EVAL" || name_ == "EVALSHA" || name_ == "EXEC")
    return true;

  return false;
}

void CommandId::Invoke(CmdArgList args, ConnectionContext* cntx) const {
  uint64_t before = absl::GetCurrentTimeNanos();
  handler_(std::move(args), cntx);
  uint64_t after = absl::GetCurrentTimeNanos();

  auto& ent = ServerState::tlocal()->cmd_stats_map[cntx->cid->name().data()];
  ++ent.first;
  ent.second += (after - before) / 1000;
}

optional<facade::ErrorReply> CommandId::Validate(CmdArgList args) const {
  if ((arity() > 0 && args.size() != size_t(arity())) ||
      (arity() < 0 && args.size() < size_t(-arity()))) {
    return facade::ErrorReply{facade::WrongNumArgsError(name()), kSyntaxErrType};
  }

  if (key_arg_step() == 2 && (args.size() % 2) == 0) {
    return facade::ErrorReply{facade::WrongNumArgsError(name()), kSyntaxErrType};
  }

  if (validator_)
    return validator_(args.subspan(1));
  return nullopt;
}

CommandRegistry::CommandRegistry() {
  vector<string> rename_command = GetFlag(FLAGS_rename_command);

  for (string command_data : rename_command) {
    pair<string_view, string_view> kv = StrSplit(command_data, '=');
    auto [_, inserted] =
        cmd_rename_map_.emplace(AsciiStrToUpper(kv.first), AsciiStrToUpper(kv.second));
    if (!inserted) {
      LOG(ERROR) << "Invalid rename_command flag, trying to give 2 names to a command";
      exit(1);
    }
  }
}

CommandRegistry& CommandRegistry::operator<<(CommandId cmd) {
  string_view k = cmd.name();
  auto it = cmd_rename_map_.find(k);
  if (it != cmd_rename_map_.end()) {
    if (it->second.empty()) {
      return *this;  // Incase of empty string we want to remove the command from registry.
    }
    k = it->second;
  }
  CHECK(cmd_map_.emplace(k, std::move(cmd)).second) << k;

  return *this;
}

namespace CO {

const char* OptName(CO::CommandOpt fl) {
  using namespace CO;

  switch (fl) {
    case WRITE:
      return "write";
    case READONLY:
      return "readonly";
    case DENYOOM:
      return "denyoom";
    case REVERSE_MAPPING:
      return "reverse-mapping";
    case FAST:
      return "fast";
    case LOADING:
      return "loading";
    case ADMIN:
      return "admin";
    case NOSCRIPT:
      return "noscript";
    case BLOCKING:
      return "blocking";
    case HIDDEN:
      return "hidden";
    case GLOBAL_TRANS:
      return "global-trans";
    case VARIADIC_KEYS:
      return "variadic-keys";
    case NO_AUTOJOURNAL:
      return "custom-journal";
    case NO_KEY_JOURNAL:
      return "no-key-journal";
  }
  return "unknown";
}

}  // namespace CO

}  // namespace dfly
