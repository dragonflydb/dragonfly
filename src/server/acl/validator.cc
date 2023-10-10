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
  if (cntx.skip_acl_validation) {
    return true;
  }

  const bool is_authed =
      IsUserAllowedToInvokeCommandGeneric(cntx.acl_categories, cntx.acl_commands, id);

  if (!is_authed) {
    auto& log = ServerState::tlocal()->acl_log;
    using Reason = acl::AclLog::Reason;
    log.Add(cntx, std::string(id.name()), Reason::COMMAND);
  }

  return is_authed;
}

[[nodiscard]] bool IsUserAllowedToInvokeCommandGeneric(uint32_t acl_cat,
                                                       const std::vector<uint64_t>& acl_commands,
                                                       const facade::CommandId& id) {
  const auto cat_credentials = id.acl_categories();
  const size_t index = id.GetFamily();
  const uint64_t command_mask = id.GetBitIndex();
  DCHECK_LT(index, acl_commands.size());
  return (acl_cat & cat_credentials) != 0 || (acl_commands[index] & command_mask) != 0;
}

}  // namespace dfly::acl
