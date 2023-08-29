// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string_view>

#include "facade/command_id.h"
#include "server/conn_context.h"

namespace dfly::acl {

bool IsUserAllowedToInvokeCommand(const ConnectionContext& cntx, const facade::CommandId& id);

}
