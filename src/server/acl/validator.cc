// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/acl/validator.h"

#include "base/logging.h"
#include "facade/dragonfly_connection.h"
#include "server/acl/acl_commands_def.h"
#include "server/server_state.h"

namespace dfly::acl {

[[nodiscard]] bool IsUserAllowedToInvokeCommand(const ConnectionContext& cntx,
                                                const facade::CommandId& id) {
  const auto cat_credentials = id.acl_categories();

  const size_t index = id.GetFamily();
  const uint64_t command_mask = id.GetBitIndex();
  DCHECK_LT(index, cntx.acl_commands.size());
  const bool is_authed = (cntx.acl_categories & cat_credentials) != 0 ||
                         (cntx.acl_commands[index] & command_mask) != 0;

  if (!is_authed) {
    auto& log = ServerState::tlocal()->log;
    using Reason = acl::AclLog::Reason;
    log.Add(cntx.authed_username, cntx.owner()->GetClientInfo(), std::string(id.name()),
            Reason::COMMAND);
  }

  return is_authed;
}

}  // namespace dfly::acl
