// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/acl/validator.h"

#include "base/logging.h"
#include "server/acl/acl_commands_def.h"
#include "server/server_state.h"

namespace dfly::acl {

[[nodiscard]] bool IsUserAllowedToInvokeCommand(const ConnectionContext& cntx,
                                                const facade::CommandId& id) {
  const auto cat_credentials = id.acl_categories();

  const size_t index = id.GetFamily();
  const uint64_t command_mask = id.GetBitIndex();
  DCHECK_LT(index, cntx.acl_commands.size());

  return (cntx.acl_categories & cat_credentials) != 0 ||
         (cntx.acl_commands[index] & command_mask) != 0;
}

}  // namespace dfly::acl
