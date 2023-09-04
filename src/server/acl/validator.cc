// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/acl/validator.h"

namespace dfly::acl {

[[nodiscard]] bool IsUserAllowedToInvokeCommand(const ConnectionContext& cntx,
                                                const facade::CommandId& id) {
  auto command_credentials = id.acl_categories();
  return (cntx.acl_categories & command_credentials) != 0;
}

}  // namespace dfly::acl
