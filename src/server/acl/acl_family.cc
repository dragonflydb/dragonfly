// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/acl/acl_family.h"

#include "absl/strings/str_cat.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/server_state.h"

namespace dfly::acl {

constexpr uint32_t kList = acl::ADMIN | acl::SLOW | acl::DANGEROUS;

static std::string AclToString(uint32_t acl_category) {
  std::string tmp;

  if (acl_category == acl::ALL) {
    return "+@all";
  }

  if (acl_category == acl::NONE) {
    return "+@none";
  }

  const std::string prefix = "+@";
  const std::string postfix = " ";

  for (uint32_t step = 0, cat = 0; cat != JSON; cat = 1ULL << ++step) {
    if (acl_category & cat) {
      absl::StrAppend(&tmp, prefix, REVERSE_CATEGORY_INDEX_TABLE[step], postfix);
    }
  }
  tmp.erase(tmp.size());

  return tmp;
}

void AclFamily::Acl(CmdArgList args, ConnectionContext* cntx) {
  (*cntx)->SendError("Wrong number of arguments for acl command");
}

void AclFamily::List(CmdArgList args, ConnectionContext* cntx) {
  const auto registry_with_lock = ServerState::tlocal()->user_registry->GetRegistryWithLock();
  const auto& registry = registry_with_lock.registry;

  (*cntx)->StartArray(registry.size());
  for (const auto& [username, user] : registry) {
    std::string buffer = "user ";
    const std::string_view pass = user.Password();
    const std::string password = pass == "nopass" ? "nopass" : std::string(pass.substr(0, 15));
    const std::string acl_cat = AclToString(user.AclCategory());

    using namespace std::string_view_literals;

    absl::StrAppend(&buffer, username, " ", user.IsActive() ? "on "sv : "off "sv, password, " ",
                    acl_cat);

    (*cntx)->SendSimpleString(buffer);
  }
}

using CI = dfly::CommandId;

#define HFUNC(x) SetHandler(&AclFamily::x)

// We can't implement the ACL commands and its respective subcommands LIST, CAT, etc
// the usual way, (that is, one command called ACL which then dispatches to the subcommand
// based on the secocond argument) because each of the subcommands has different ACL
// categories. Therefore, to keep it compatible with the CommandId, I need to treat them
// as separate commands in the registry. This is the least intrusive change because it's very
// easy to handle that case explicitly in `DispatchCommand`.

void AclFamily::Register(dfly::CommandRegistry* registry) {
  *registry << CI{"ACL", CO::NOSCRIPT | CO::LOADING, 0, 0, 0, 0, acl::kList}.HFUNC(Acl);
  *registry << CI{"ACL LIST", CO::ADMIN | CO::NOSCRIPT | CO::LOADING, 1, 0, 0, 0, acl::kList}.HFUNC(
      List);
}

#undef HFUNC

}  // namespace dfly::acl
