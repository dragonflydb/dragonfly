// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/acl/validator.h"

#include <absl/algorithm/container.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>

#include "base/logging.h"
#include "core/glob_matcher.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/server_state.h"
#include "server/transaction.h"

namespace dfly::acl {

namespace {

using facade::ParsedArgs;
using std::string_view;

bool Matches(string_view pattern, string_view target) {
  return GlobMatcher{pattern, true}.Matches(target);
}

bool IsCommandAllowed(const std::vector<uint64_t>& commands, const CommandId& id,
                      const ParsedArgs& tail_args) {
  const size_t family = id.GetFamily();
  DCHECK_LT(family, commands.size());
  return (commands[family] & id.AclTarget(tail_args).second) != 0;
}

// Restrictions for users that may access a single database.
bool IsDbAllowed(size_t acl_db, size_t db_index, const CommandId& id, const ParsedArgs& tail_args) {
  if (acl_db == kAllDbs)
    return true;

  // Once we support db ranges this must change.
  if (id.name() == "MOVE")
    return false;

  if (acl_db != db_index && id.IsTransactional())
    return false;

  size_t target_db = 0;
  return !(tail_args.size() == 1 && id.name() == "SELECT" &&
           absl::SimpleAtoi(tail_args[0], &target_db) && target_db != acl_db);
}

// PSUBSCRIBE patterns are compared literally against the user's channel globs.
bool IsChannelAllowed(const AclPubSub& pub_sub, string_view channel, bool literal) {
  return absl::c_any_of(pub_sub.globs, [&](const GlobTypePubSub& glob) {
    return literal ? glob.first == channel : Matches(glob.first, channel);
  });
}

bool AreChannelsAllowed(const AclPubSub& pub_sub, const CommandId& id,
                        const ParsedArgs& tail_args) {
  if (pub_sub.all_channels)
    return true;

  // Valkey never gates unsubscribe on channel ACLs (only PUBLISH/SUBSCRIBE flags are checked).
  string_view name = id.name();
  if (name == "UNSUBSCRIBE" || name == "PUNSUBSCRIBE" || name == "SUNSUBSCRIBE")
    return true;

  const bool literal = id.IsPatternPubSub();
  if (id.IsPublish() || id.IsSPublish()) {
    // tail_args can be empty for ACL DRYRUN's simulated commands, which skip arity validation.
    // Treat a missing channel as permissive, like DRYRUN skips key checks for incomplete args.
    return tail_args.empty() || IsChannelAllowed(pub_sub, tail_args[0], literal);
  }

  for (string_view channel : tail_args) {
    if (!IsChannelAllowed(pub_sub, channel, literal))
      return false;
  }
  return true;
}

struct KeyPermissions {
  bool read = false;
  bool write = false;
};

// Per-key ACL requirements for commands that read one key and write another (e.g. COPY).
KeyPermissions RequiredKeyPermissions(const CommandId& id, unsigned key_offset) {
  if (id.opt_mask() & CO::WRITE_KEY_OFFSET_0)
    return {key_offset != 0, key_offset == 0};
  if (id.opt_mask() & CO::WRITE_KEY_OFFSET_1)
    return {key_offset != 1, key_offset == 1};

  if (id.opt_mask() & CO::STORE_LAST_KEY)
    return {true, false};

  return {id.IsReadOnly(), id.IsJournaled()};
}

bool IsKeyAllowed(const AclKeys& keys, string_view key, KeyPermissions perms) {
  return absl::c_any_of(keys.key_globs, [&](const GlobType& glob) {
    const auto& [pattern, op] = glob;
    const bool grants = (perms.read && op != KeyOp::WRITE) || (perms.write && op != KeyOp::READ);
    return grants && Matches(pattern, key);
  });
}

// This command has no key position because a subcommand comes first; the key follows it.
bool ReadsKeyAfterSubcommand(string_view cmd, string_view sub) {
  return cmd == "MEMORY" && absl::EqualsIgnoreCase(sub, "USAGE");
}

// RM deletes whatever its MATCH selects, so it needs write access to every key.
bool CanWriteAllKeys(const AclKeys& keys) {
  return absl::c_any_of(keys.key_globs, [](const GlobType& glob) {
    return glob.first == "*" && glob.second != KeyOp::READ;
  });
}

bool AreKeysAllowed(const AclKeys& keys, const CommandId& id, const ParsedArgs& tail_args) {
  if (keys.all_keys)
    return true;

  if (id.first_key_pos() != 0 && (id.IsReadOnly() || id.IsJournaled())) {
    auto key_index = DetermineKeys(&id, tail_args);

    // Commands with content-dependent key positions (e.g. ZUNION's numkeys) can fail to parse
    // arbitrary/malformed tail_args; that's a normal rejection, so fail closed.
    if (!key_index)
      return false;

    // The bonus key (dest of Z*STORE/CMS.MERGE/GEORADIUS.../SORT STORE) is the sole write target.
    const bool has_bonus_dest = key_index->bonus.has_value();
    for (unsigned idx = key_index->start; idx < key_index->end; idx += key_index->step) {
      const KeyPermissions perms = has_bonus_dest
                                       ? KeyPermissions{true, false}
                                       : RequiredKeyPermissions(id, idx - key_index->start);
      if (!IsKeyAllowed(keys, tail_args[idx], perms))
        return false;
    }
    return !has_bonus_dest || IsKeyAllowed(keys, tail_args[*key_index->bonus], {false, true});
  }

  if (tail_args.size() >= 2 && ReadsKeyAfterSubcommand(id.name(), tail_args[0]))
    return IsKeyAllowed(keys, tail_args[1], {true, false});

  if (id.name() == "RM")
    return CanWriteAllKeys(keys);

  return true;
}

}  // namespace

std::optional<AclLog::Reason> CheckCommand(const AclRulesView& rules, size_t db_index,
                                           const CommandId& id, const ParsedArgs& tail_args) {
  using Reason = AclLog::Reason;

  if (!id.IsPubSub() && !IsDbAllowed(rules.db, db_index, id, tail_args))
    return Reason::AUTH;

  if (!IsCommandAllowed(rules.commands, id, tail_args))
    return Reason::COMMAND;

  if (id.IsPubSub()) {
    if (!AreChannelsAllowed(rules.pub_sub, id, tail_args))
      return Reason::PUB_SUB;
  } else if (!AreKeysAllowed(rules.keys, id, tail_args)) {
    return Reason::KEY;
  }
  return std::nullopt;
}

bool IsUserAllowedToInvokeCommand(const ConnectionContext& cntx, const CommandId& id,
                                  const ParsedArgs& tail_args) {
  if (cntx.skip_acl_validation)
    return true;

  if (id.IsAlias())
    return false;

  // While a script is running, redis.call() must be checked against the
  // ACL rules that were in effect when the script started.
  const auto* sinfo = cntx.conn_state.script_info.get();
  const AclRulesView rules =
      sinfo ? AclRulesView{sinfo->acl_commands, sinfo->acl_keys, sinfo->acl_pub_sub,
                           sinfo->acl_db_idx}
            : AclRulesView{cntx.acl_commands, cntx.keys, cntx.pub_sub, cntx.acl_db_idx};

  const auto denial = CheckCommand(rules, cntx.db_index(), id, tail_args);
  if (!denial)
    return true;

  ServerState::tlocal()->acl_log.Add(cntx, std::string(id.AclTarget(tail_args).first), *denial);
  return false;
}

}  // namespace dfly::acl
