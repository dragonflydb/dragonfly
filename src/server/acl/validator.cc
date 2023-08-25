// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/acl/validator.h"

#include "server/server_state.h"

namespace dfly::acl {

[[nodiscard]] bool IsUserAllowedToInvokeCommand(const ConnectionContext& cntx,
                                                const facade::CommandId& id) {
  auto& registry = *ServerState::tlocal()->user_registry;
  auto credentials = registry.GetCredentials(cntx.authed_username);
  auto command_credentials = id.acl_categories();
  return (credentials.acl_categories & command_credentials) != 0;
}

}  // namespace dfly::acl
