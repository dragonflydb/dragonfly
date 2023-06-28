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

using absl::GetFlag;
using absl::StrAppend;
using absl::StrCat;

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
  vector<std::string_view> parts = absl::StrSplit(rename_command, ",", absl::SkipEmpty());
  for (std::string_view p : parts) {
    size_t sep = p.find('=');

    string cmd_name = string(p.substr(0, sep));
    transform(cmd_name.begin(), cmd_name.end(), cmd_name.begin(), ::toupper);
    string cmd_new_name = string(p.substr(sep + 1));
    transform(cmd_new_name.begin(), cmd_new_name.end(), cmd_new_name.begin(), ::toupper);
    cmd_rename_map_.emplace(cmd_name, cmd_new_name);
  }
}

CommandRegistry& CommandRegistry::operator<<(CommandId cmd) {
  string_view k = cmd.name();
  auto it = cmd_rename_map_.find(k);
  if (it != cmd_rename_map_.end()) {
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
