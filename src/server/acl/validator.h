// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <utility>

#include "server/acl/acl_log.h"
#include "server/command_registry.h"
#include "server/conn_context.h"

namespace dfly::acl {

std::pair<bool, AclLog::Reason> IsUserAllowedToInvokeCommandGeneric(
    const std::vector<uint64_t>& acl_commands, const AclKeys& keys, CmdArgList tail_args,
    const CommandId& id);

bool IsUserAllowedToInvokeCommand(const ConnectionContext& cntx, const CommandId& id,
                                  CmdArgList tail_args);

}  // namespace dfly::acl
