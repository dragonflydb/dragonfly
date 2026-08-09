// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <utility>
#include <vector>

#include "facade/facade_types.h"
#include "server/acl/acl_log.h"
#include "server/command_registry.h"

namespace dfly::acl {

struct AclKeys;
struct AclPubSub;

std::pair<bool, AclLog::Reason> IsUserAllowedToInvokeCommandGeneric(
    const ConnectionContext& cntx, const CommandId& id, const facade::ParsedArgs& tail_args);

bool IsUserAllowedToInvokeCommand(const ConnectionContext& cntx, const CommandId& id,
                                  const facade::ParsedArgs& tail_args);

// Exposed so callers without a live Connection (e.g. ACL DRYRUN's stub context) can run the
// pub/sub channel check directly, bypassing IsUserAllowedToInvokeCommand's AclLog::Add, which
// requires a real connection.
std::pair<bool, AclLog::Reason> IsPubSubCommandAuthorized(bool literal_match,
                                                          const std::vector<uint64_t>& acl_commands,
                                                          const AclPubSub& pub_sub,
                                                          const facade::ParsedArgs& tail_args,
                                                          const CommandId& id);

}  // namespace dfly::acl
