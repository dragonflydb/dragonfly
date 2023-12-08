// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/acl/validator.h"

#include "base/logging.h"
#include "facade/dragonfly_connection.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_registry.h"
#include "server/server_state.h"
#include "server/transaction.h"
// we need this because of stringmatchlen
extern "C" {
#include "redis/util.h"
}

namespace dfly::acl {

[[nodiscard]] bool IsUserAllowedToInvokeCommand(const ConnectionContext& cntx, const CommandId& id,
                                                CmdArgList tail_args) {
  if (cntx.skip_acl_validation) {
    return true;
  }

  const auto is_authed = IsUserAllowedToInvokeCommandGeneric(cntx.acl_categories, cntx.acl_commands,
                                                             cntx.keys, tail_args, id);

  if (!is_authed) {
    auto& log = ServerState::tlocal()->acl_log;
    using Reason = acl::AclLog::Reason;
    log.Add(cntx, std::string(id.name()), Reason::COMMAND);
  }

  return is_authed;
}

[[nodiscard]] bool IsUserAllowedToInvokeCommandGeneric(uint32_t acl_cat,
                                                       const std::vector<uint64_t>& acl_commands,
                                                       const AclKeys& keys, CmdArgList tail_args,
                                                       const CommandId& id) {
  const auto cat_credentials = id.acl_categories();
  const size_t index = id.GetFamily();
  const uint64_t command_mask = id.GetBitIndex();
  DCHECK_LT(index, acl_commands.size());

  const bool command =
      (acl_cat & cat_credentials) != 0 || (acl_commands[index] & command_mask) != 0;

  if (!command) {
    return false;
  }

  auto match = [](const auto& pattern, const auto& target) {
    return stringmatchlen(pattern.data(), pattern.size(), target.data(), target.size(), 0);
  };

  const bool is_read_command = id.IsReadOnly();
  const bool is_write_command = id.IsWriteOnly();

  auto iterate_globs = [&](auto target) {
    for (auto& [elem, op] : keys.key_globs) {
      if (match(elem, target)) {
        if (is_read_command && (op == KeyOp::READ || op == KeyOp::READ_WRITE)) {
          return true;
        }
        if (is_write_command && (op == KeyOp::WRITE || op == KeyOp::READ_WRITE)) {
          return true;
        }
      }
    }
    return false;
  };

  bool keys_allowed = true;
  if (!keys.all_keys && id.first_key_pos() != 0 && (is_read_command || is_write_command)) {
    const auto& keys_index = DetermineKeys(&id, tail_args).value();
    const size_t end = keys_index.end;
    if (keys_index.bonus) {
      auto target = facade::ToSV(tail_args[*keys_index.bonus]);
      if (!iterate_globs(target)) {
        keys_allowed = false;
      }
    }
    if (keys_allowed) {
      for (size_t i = keys_index.start; i < end; i += keys_index.step) {
        auto target = facade::ToSV(tail_args[i]);
        if (!iterate_globs(target)) {
          keys_allowed = false;
          break;
        }
      }
    }
  }
  return keys_allowed;
}

}  // namespace dfly::acl
