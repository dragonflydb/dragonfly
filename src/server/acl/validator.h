// Copyright 2025, DragonflyDB authors.  All rights reserved.
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

std::pair<bool, AclLog::Reason> IsUserAllowedToInvokeCommandGeneric(const ConnectionContext& cntx,
                                                                    const CommandId& id,
                                                                    facade::CmdArgList tail_args);

bool IsUserAllowedToInvokeCommand(const ConnectionContext& cntx, const CommandId& id,
                                  facade::CmdArgList tail_args);
}  // namespace dfly::acl
