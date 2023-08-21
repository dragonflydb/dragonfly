// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/acl/acl_family.h"

#include "server/acl/acl_commands_def.h"
#include "server/command_registry.h"
#include "server/conn_context.h"

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
      tmp += prefix;
      tmp += REVERSE_CATEGORY_INDEX_TABLE[step];
      tmp += postfix;
    }
  }
  tmp.erase(tmp.size());

  return tmp;
}

void AclFamily::List(CmdArgList args, ConnectionContext* cntx) {
  auto registry_with_lock = cntx->user_registry->GetRegistryWithLock();
  const auto& registry = registry_with_lock.registry;

  std::string buffer(255, ' ');
  buffer.replace(0, 5, "user ");

  auto resize_if_exceeds_size = [](std::string& buffer, size_t size) {
    if (buffer.size() < size) {
      buffer.resize(size);
    }
  };

  auto step = [&resize_if_exceeds_size](std::string& buffer, size_t& size, size_t increment) {
    size += increment;
    resize_if_exceeds_size(buffer, size);
  };

  (*cntx)->StartArray(registry.size());
  for (const auto& [username, user] : registry) {
    size_t size = 5;
    buffer.replace(size, username.size(), username);
    step(buffer, size, username.size());

    buffer.replace(size, 1, " ");
    step(buffer, size, 1);

    const bool is_active = user.IsActive();
    buffer.replace(size, is_active ? 3 : 4, is_active ? "on " : "off ");
    step(buffer, size, is_active ? 3 : 4);

    if (user.Password() == "nopass") {
      buffer.replace(size, 6, "nopass");
      step(buffer, size, 6);
    } else {
      buffer.replace(size, 15, user.Password().substr(0, 15));
      step(buffer, size, 15);
    }

    buffer.replace(size, 1, " ");
    step(buffer, size, 1);

    std::string acl_cat = AclToString(user.AclCategory());
    buffer.replace(size, acl_cat.size(), acl_cat);
    step(buffer, size, acl_cat.size());

    (*cntx)->SendSimpleString(std::string_view{buffer.data(), size});
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
  *registry << CI{"ACL LIST", CO::ADMIN | CO::NOSCRIPT | CO::LOADING, 0, 0, 0, 0, acl::kList}.HFUNC(
      List);
}

#undef HFUNC

}  // namespace dfly::acl
