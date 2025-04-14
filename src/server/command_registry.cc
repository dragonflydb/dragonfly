// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/command_registry.h"

#include <absl/strings/str_split.h>
#include <absl/time/clock.h>

#include "absl/container/inlined_vector.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "base/bits.h"
#include "base/flags.h"
#include "base/logging.h"
#include "facade/dragonfly_connection.h"
#include "facade/error.h"
#include "server/acl/acl_commands_def.h"
#include "server/server_state.h"

using namespace std;
ABSL_FLAG(vector<string>, rename_command, {},
          "Change the name of commands, format is: <cmd1_name>=<cmd1_new_name>, "
          "<cmd2_name>=<cmd2_new_name>");
ABSL_FLAG(vector<string>, restricted_commands, {},
          "Commands restricted to connections on the admin port");

ABSL_FLAG(vector<string>, oom_deny_commands, {},
          "Additinal commands that will be marked as denyoom");

ABSL_FLAG(vector<string>, command_alias, {},
          "Add an alias for given command(s), format is: <alias>=<original>, <alias>=<original>");

namespace dfly {

using namespace facade;

using absl::AsciiStrToUpper;
using absl::GetFlag;
using absl::StrAppend;
using absl::StrCat;
using absl::StrSplit;

namespace {

uint32_t ImplicitCategories(uint32_t mask) {
  if (mask & CO::ADMIN)
    mask |= CO::NOSCRIPT;
  return mask;
}

uint32_t ImplicitAclCategories(uint32_t mask) {
  mask = ImplicitCategories(mask);
  uint32_t out = 0;

  if (mask & CO::WRITE)
    out |= acl::WRITE;

  if ((mask & CO::READONLY) && ((mask & CO::NOSCRIPT) == 0))
    out |= acl::READ;

  if (mask & CO::ADMIN)
    out |= acl::ADMIN | acl::DANGEROUS;

  // todo pubsub

  if (mask & CO::FAST)
    out |= acl::FAST;

  if (mask & CO::BLOCKING)
    out |= acl::BLOCKING;

  if ((out & acl::FAST) == 0)
    out |= acl::SLOW;

  return out;
}

absl::flat_hash_map<std::string, std::string> ParseCmdlineArgMap(
    const absl::Flag<std::vector<std::string>>& flag, const bool allow_duplicates = false) {
  const auto& mappings = absl::GetFlag(flag);
  absl::flat_hash_map<std::string, std::string> parsed_mappings;
  parsed_mappings.reserve(mappings.size());

  for (const std::string& mapping : mappings) {
    absl::InlinedVector<std::string_view, 2> kv = absl::StrSplit(mapping, '=');
    if (kv.size() != 2) {
      LOG(ERROR) << "Malformed command '" << mapping << "' for " << flag.Name()
                 << ", expected key=value";
      exit(1);
    }

    std::string key = absl::AsciiStrToUpper(kv[0]);
    std::string value = absl::AsciiStrToUpper(kv[1]);

    if (key == value) {
      LOG(ERROR) << "Invalid attempt to map " << key << " to itself in " << flag.Name();
      exit(1);
    }

    const bool inserted = parsed_mappings.emplace(std::move(key), std::move(value)).second;
    if (!allow_duplicates && !inserted) {
      LOG(ERROR) << "Duplicate insert to " << flag.Name() << " not allowed";
      exit(1);
    }
  }
  return parsed_mappings;
}

}  // namespace

CommandId::CommandId(const char* name, uint32_t mask, int8_t arity, int8_t first_key,
                     int8_t last_key, std::optional<uint32_t> acl_categories)
    : facade::CommandId(name, ImplicitCategories(mask), arity, first_key, last_key,
                        acl_categories.value_or(ImplicitAclCategories(mask))) {
  implicit_acl_ = !acl_categories.has_value();
}

CommandId::CommandId(const char* name, uint32_t mask, int8_t arity, int8_t first_key,
                     int8_t last_key, uint32_t acl_categories, bool implicit_acl)
    : facade::CommandId(name, mask, arity, first_key, last_key, acl_categories),
      implicit_acl_(implicit_acl) {
}

CommandId CommandId::Clone(const std::string_view name) const {
  CommandId cloned = CommandId{name.data(), opt_mask_ | CO::HIDDEN, arity_,       first_key_,
                               last_key_,   acl_categories_,        implicit_acl_};
  cloned.handler_ = handler_;
  return cloned;
}

bool CommandId::IsTransactional() const {
  if (first_key_ > 0 || (opt_mask_ & CO::GLOBAL_TRANS) || (opt_mask_ & CO::NO_KEY_TRANSACTIONAL))
    return true;

  if (name_ == "EVAL" || name_ == "EVALSHA" || name_ == "EVAL_RO" || name_ == "EVALSHA_RO" ||
      name_ == "EXEC")
    return true;

  return false;
}

bool CommandId::IsMultiTransactional() const {
  return CO::IsTransKind(name()) || CO::IsEvalKind(name());
}

uint64_t CommandId::Invoke(CmdArgList args, const CommandContext& cmd_cntx) const {
  int64_t before = absl::GetCurrentTimeNanos();
  handler_(args, cmd_cntx);
  int64_t after = absl::GetCurrentTimeNanos();

  ServerState* ss = ServerState::tlocal();  // Might have migrated thread, read after invocation
  int64_t execution_time_usec = (after - before) / 1000;

  auto& ent = command_stats_[ss->thread_index()];

  ++ent.first;
  ent.second += execution_time_usec;

  return execution_time_usec;
}

optional<facade::ErrorReply> CommandId::Validate(CmdArgList tail_args) const {
  if ((arity() > 0 && tail_args.size() + 1 != size_t(arity())) ||
      (arity() < 0 && tail_args.size() + 1 < size_t(-arity()))) {
    string prefix;
    if (name() == "EXEC")
      prefix = "-EXECABORT Transaction discarded because of: ";
    return facade::ErrorReply{prefix + facade::WrongNumArgsError(name()), kSyntaxErrType};
  }

  if ((opt_mask() & CO::INTERLEAVED_KEYS)) {
    if ((name() == "JSON.MSET" && tail_args.size() % 3 != 0) ||
        (name() == "MSET" && tail_args.size() % 2 != 0))
      return facade::ErrorReply{facade::WrongNumArgsError(name()), kSyntaxErrType};
  }

  if (validator_)
    return validator_(tail_args);
  return nullopt;
}

CommandRegistry::CommandRegistry() {
  cmd_rename_map_ = ParseCmdlineArgMap(FLAGS_rename_command);
  alias_map_ = ParseCmdlineArgMap(FLAGS_command_alias, true);

  for (string name : GetFlag(FLAGS_restricted_commands)) {
    restricted_cmds_.emplace(AsciiStrToUpper(name));
  }

  for (string name : GetFlag(FLAGS_oom_deny_commands)) {
    oomdeny_cmds_.emplace(AsciiStrToUpper(name));
  }
}

void CommandRegistry::Init(unsigned int thread_count) {
  for (auto& [_, cmd] : cmd_map_) {
    cmd.Init(thread_count);
  }
}

CommandRegistry& CommandRegistry::operator<<(CommandId cmd) {
  string k = string(cmd.name());

  absl::InlinedVector<std::string_view, 2> maybe_subcommand = StrSplit(cmd.name(), " ");
  const bool is_sub_command = maybe_subcommand.size() == 2;
  auto it = cmd_rename_map_.find(maybe_subcommand.front());
  if (it != cmd_rename_map_.end()) {
    if (it->second.empty()) {
      return *this;  // Incase of empty string we want to remove the command from registry.
    }
    k = is_sub_command ? absl::StrCat(it->second, " ", maybe_subcommand[1]) : it->second;
  }

  std::optional<CommandId> cloned = std::nullopt;
  for (const auto& [alias, target] : alias_map_) {
    if (target == k) {
      cloned.emplace(cmd.Clone(alias));
      alias_map_.erase(alias);
      break;
    }
  }

  if (restricted_cmds_.find(k) != restricted_cmds_.end()) {
    cmd.SetRestricted(true);
    if (cloned.has_value()) {
      cloned->SetRestricted(true);
    }
  }

  if (oomdeny_cmds_.find(k) != oomdeny_cmds_.end()) {
    cmd.SetFlag(CO::DENYOOM);
    if (cloned.has_value()) {
      cloned->SetFlag(CO::DENYOOM);
    }
  }

  cmd.SetFamily(family_of_commands_.size() - 1);
  if (cloned.has_value()) {
    cloned->SetFamily(cmd.GetFamily());
  }
  if (acl_category_) {
    cmd.SetAclCategory(*acl_category_);
    if (cloned.has_value()) {
      cloned->SetAclCategory(*acl_category_);
    }
  }

  if (!is_sub_command || absl::StartsWith(cmd.name(), "ACL")) {
    cmd.SetBitIndex(1ULL << bit_index_);
    family_of_commands_.back().emplace_back(k);
    ++bit_index_;

    if (cloned) {
      cloned->SetBitIndex(1ULL << bit_index_);
      family_of_commands_.back().emplace_back(cloned->name());
      ++bit_index_;
    }
  } else {
    DCHECK(absl::StartsWith(k, family_of_commands_.back().back()));
    cmd.SetBitIndex(1ULL << (bit_index_ - 1));
  }
  CHECK(cmd_map_.emplace(k, std::move(cmd)).second) << k;
  if (cloned.has_value()) {
    CHECK(cmd_map_.emplace(cloned->name(), std::move(cloned.value())).second) << cloned->name();
  }
  return *this;
}

void CommandRegistry::StartFamily(std::optional<uint32_t> acl_category) {
  family_of_commands_.emplace_back();
  bit_index_ = 0;
  acl_category_ = acl_category;
}

std::string_view CommandRegistry::RenamedOrOriginal(std::string_view orig) const {
  if (!cmd_rename_map_.empty() && cmd_rename_map_.contains(orig)) {
    return cmd_rename_map_.find(orig)->second;
  }
  return orig;
}

CommandRegistry::FamiliesVec CommandRegistry::GetFamilies() {
  return std::move(family_of_commands_);
}

std::pair<const CommandId*, ArgSlice> CommandRegistry::FindExtended(string_view cmd,
                                                                    ArgSlice tail_args) const {
  if (cmd == RenamedOrOriginal("ACL"sv)) {
    if (tail_args.empty()) {
      return {Find(cmd), {}};
    }

    auto second_cmd = absl::AsciiStrToUpper(ArgS(tail_args, 0));
    string full_cmd = absl::StrCat(cmd, " ", second_cmd);

    return {Find(full_cmd), tail_args.subspan(1)};
  }

  const CommandId* res = Find(cmd);
  if (!res)
    return {nullptr, {}};

  // A workaround for XGROUP HELP that does not fit our static taxonomy of commands.
  if (tail_args.size() == 1 && res->name() == "XGROUP") {
    if (absl::EqualsIgnoreCase(ArgS(tail_args, 0), "HELP")) {
      res = Find("_XGROUP_HELP");
    }
  }
  return {res, tail_args};
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
    case FAST:
      return "fast";
    case LOADING:
      return "loading";
    case DANGEROUS:
      return "dangerous";
    case ADMIN:
      return "admin";
    case NOSCRIPT:
      return "noscript";
    case BLOCKING:
      return "blocking";
    case HIDDEN:
      return "hidden";
    case INTERLEAVED_KEYS:
      return "interleaved-keys";
    case GLOBAL_TRANS:
      return "global-trans";
    case STORE_LAST_KEY:
      return "store-last-key";
    case VARIADIC_KEYS:
      return "variadic-keys";
    case NO_AUTOJOURNAL:
      return "custom-journal";
    case NO_KEY_TRANSACTIONAL:
      return "no-key-transactional";
    case NO_KEY_TX_SPAN_ALL:
      return "no-key-tx-span-all";
    case IDEMPOTENT:
      return "idempotent";
    case SLOW:
      return "slow";
  }
  return "unknown";
}

}  // namespace CO

}  // namespace dfly
