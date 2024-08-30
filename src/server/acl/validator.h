// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <utility>

#include "facade/facade_types.h"
#include "server/acl/acl_log.h"
#include "server/command_registry.h"

namespace dfly::acl {

struct AclKeys;
struct AclPubSub;

std::pair<bool, AclLog::Reason> IsUserAllowedToInvokeCommandGeneric(
    const std::vector<uint64_t>& acl_commands, const AclKeys& keys, facade::CmdArgList tail_args,
    const CommandId& id);

bool IsUserAllowedToInvokeCommand(const ConnectionContext& cntx, const CommandId& id,
                                  facade::CmdArgList tail_args);

std::pair<bool, AclLog::Reason> IsPubSubCommandAuthorized(bool literal_match,
                                                          const std::vector<uint64_t>& acl_commands,
                                                          const AclPubSub& pub_sub,
                                                          facade::CmdArgList tail_args,
                                                          const CommandId& id);

}  // namespace dfly::acl
