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
#include "server/acl/acl_commands_def.h"
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
                     int8_t last_key, int8_t step, uint32_t acl_categories)
    : facade::CommandId(name, mask, arity, first_key, last_key, step, acl_categories) {
  if (mask & CO::ADMIN)
    opt_mask_ |= CO::NOSCRIPT;

  if (mask & CO::BLOCKING)
    opt_mask_ |= CO::REVERSE_MAPPING;
}

bool CommandId::IsTransactional() const {
  if (first_key_ > 0 || (opt_mask_ & CO::GLOBAL_TRANS) || (opt_mask_ & CO::NO_KEY_TRANSACTIONAL))
    return true;

  if (name_ == "EVAL" || name_ == "EVALSHA" || name_ == "EXEC")
    return true;

  return false;
}

void CommandId::Invoke(CmdArgList args, ConnectionContext* cntx) const {
  uint64_t before = absl::GetCurrentTimeNanos();
  handler_(std::move(args), cntx);
  uint64_t after = absl::GetCurrentTimeNanos();
  auto& ent = command_stats_[ServerState::tlocal()->thread_index()];
  ++ent.first;
  ent.second += (after - before) / 1000;
}

optional<facade::ErrorReply> CommandId::Validate(CmdArgList tail_args) const {
  if ((arity() > 0 && tail_args.size() + 1 != size_t(arity())) ||
      (arity() < 0 && tail_args.size() + 1 < size_t(-arity()))) {
    return facade::ErrorReply{facade::WrongNumArgsError(name()), kSyntaxErrType};
  }

  if (key_arg_step() == 2 && (tail_args.size() % 2) != 0) {
    return facade::ErrorReply{facade::WrongNumArgsError(name()), kSyntaxErrType};
  }

  if (validator_)
    return validator_(tail_args);
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

void CommandRegistry::Init(unsigned int thread_count) {
  for (auto& [_, cmd] : cmd_map_) {
    cmd.Init(thread_count);
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

  family_of_commands_.back().push_back(std::string(k));
  cmd.SetFamily(family_of_commands_.size() - 1);
  cmd.SetBitIndex(1ULL << bit_index_++);
  CHECK(cmd_map_.emplace(k, std::move(cmd)).second) << k;

  return *this;
}

void CommandRegistry::StartFamily() {
  family_of_commands_.push_back({});
  bit_index_ = 0;
}

CommandRegistry::FamiliesVec CommandRegistry::GetFamilies() {
  return std::move(family_of_commands_);
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
    case NO_KEY_TRANSACTIONAL:
      return "no-key-transactional";
  }
  return "unknown";
}

}  // namespace CO

}  // namespace dfly
