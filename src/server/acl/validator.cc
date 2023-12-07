// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/acl/validator.h"

#include "base/logging.h"
#include "facade/dragonfly_connection.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_registry.h"
#include "server/server_state.h"
extern "C" {
#include "redis/util.h"
}

namespace dfly::acl {

[[nodiscard]] bool IsUserAllowedToInvokeCommand(const ConnectionContext& cntx,
                                                const facade::CommandId& id, CmdArgList tail_args) {
  if (cntx.skip_acl_validation) {
    return true;
  }

  const auto [is_authed, reason] = IsUserAllowedToInvokeCommandGeneric(
      cntx.acl_categories, cntx.acl_commands, cntx.keys, tail_args, id);

  if (!is_authed) {
    auto& log = ServerState::tlocal()->acl_log;
    log.Add(cntx, std::string(id.name()), reason);
  }

  return is_authed;
}

[[nodiscard]] std::pair<bool, AclLog::Reason> IsUserAllowedToInvokeCommandGeneric(
    uint32_t acl_cat, const std::vector<uint64_t>& acl_commands, const AclKeys& keys,
    CmdArgList tail_args, const facade::CommandId& id) {
  const auto cat_credentials = id.acl_categories();
  const size_t index = id.GetFamily();
  const uint64_t command_mask = id.GetBitIndex();
  DCHECK_LT(index, acl_commands.size());

  const bool is_read_command = id.opt_mask() & CO::CommandOpt::READONLY;
  const bool is_write_command = id.opt_mask() & CO::CommandOpt::WRITE;
  const size_t step = (id.opt_mask() & CO::CommandOpt::INTERLEAVED_KEYS) ? 2 : 1;
  const bool var_keys = (id.opt_mask() & CO::CommandOpt::VARIADIC_KEYS);

  auto match = [](const auto& pattern, const auto& target) {
    return stringmatchlen(pattern.data(), pattern.size(), target.data(), target.size(), 0);
  };

  bool key_allowed = true;
  if (!keys.all_keys && id.first_key_pos() != 0 && (is_read_command || is_write_command)) {
    const size_t end = var_keys ? tail_args.size() : id.last_key_pos() + 1;
    for (size_t i = id.first_key_pos(); i < end; i += step) {
      auto target = facade::ToSV(tail_args[i - 1]);
      bool allowed = false;
      for (auto& [elem, op] : keys.key_globs) {
        if (match(elem, target)) {
          if (is_read_command && (op == KeyOp::READ || op == KeyOp::READ_WRITE)) {
            allowed = true;
            break;
          }
          if (is_write_command && (op == KeyOp::WRITE || op == KeyOp::READ_WRITE)) {
            allowed = true;
            break;
          }
        }
      }
      if (!allowed) {
        key_allowed = false;
        break;
      }
    }
  }

  const bool command =
      (acl_cat & cat_credentials) != 0 || (acl_commands[index] & command_mask) != 0;

  return {command && key_allowed, !command ? AclLog::Reason::COMMAND : AclLog::Reason::KEY};
}

}  // namespace dfly::acl
