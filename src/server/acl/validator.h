// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/types/span.h>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "facade/facade_types.h"
#include "server/acl/acl_commands_def.h"
#include "server/acl/acl_log.h"

namespace dfly {

class CommandId;
class ConnectionContext;

namespace acl {

// The ACL rules a command is checked against. A non-owning view: the rules must outlive it.
struct AclRulesView {
  const std::vector<uint64_t>& commands;  // Per-family bitmasks of the allowed commands.
  const AclKeys& keys;
  const AclPubSub& pub_sub;
  size_t db;  // The only database the user may access, or kAllDbs.
};

// Checks whether `rules` allow running `id` with `tail_args` while `db_index` is selected.
// Returns the reason of a denial or nullopt if the command is allowed. Has no side effects and
// needs no connection, so it can also simulate commands (ACL DRYRUN).
std::optional<AclLog::Reason> CheckCommand(const AclRulesView& rules, size_t db_index,
                                           const CommandId& id,
                                           const facade::ParsedArgs& tail_args);

// Checks the command against the connection's ACL rules, or against the rules snapshotted when a
// running script started. Records denials in the ACL log.
[[nodiscard]] bool IsUserAllowedToInvokeCommand(const ConnectionContext& cntx, const CommandId& id,
                                                const facade::ParsedArgs& tail_args);

// Whether the same rules allow op on every key of database 0 under prefixes; "" or none: all keys.
[[nodiscard]] bool ArePrefixesAllowed(const ConnectionContext& cntx,
                                      absl::Span<const std::string> prefixes, KeyOp op);

}  // namespace acl
}  // namespace dfly
