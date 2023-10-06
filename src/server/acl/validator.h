// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "facade/command_id.h"
#include "server/conn_context.h"

namespace dfly::acl {

bool IsUserAllowedToInvokeCommandGeneric(uint32_t acl_cat,
                                         const std::vector<uint64_t>& acl_commands,
                                         const ConnectionContext& cntx,
                                         const facade::CommandId& id);

bool IsUserAllowedToInvokeCommand(const ConnectionContext& cntx, const facade::CommandId& id);

}  // namespace dfly::acl
