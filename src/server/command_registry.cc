// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/command_registry.h"

#include <absl/strings/str_split.h>

#include "absl/strings/str_cat.h"
#include "base/bits.h"
#include "base/flags.h"
#include "base/logging.h"
#include "facade/error.h"
#include "server/conn_context.h"

using namespace std;
ABSL_FLAG(string, rename_command, "",
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

CommandRegistry::CommandRegistry() {
  string rename_command = GetFlag(FLAGS_rename_command);

  for (std::string_view p : StrSplit(rename_command, ",", absl::SkipEmpty())) {
    pair<string_view, string_view> kv = StrSplit(p, '=');
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
