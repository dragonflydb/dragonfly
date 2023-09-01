// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/acl/acl_family.h"

#include <glog/logging.h>

#include <cctype>
#include <optional>
#include <variant>

#include "absl/strings/ascii.h"
#include "absl/strings/escaping.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "core/overloaded.h"
#include "facade/dragonfly_connection.h"
#include "facade/facade_types.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/server_state.h"

namespace dfly::acl {

static std::string AclToString(uint32_t acl_category) {
  std::string tmp;

  if (acl_category == acl::ALL) {
    return "+@ALL";
  }

  if (acl_category == acl::NONE) {
    return "+@NONE";
  }

  const std::string prefix = "+@";
  const std::string postfix = " ";

  for (uint32_t i = 0; i < 32; i++) {
    uint32_t cat_bit = 1ULL << i;
    if (acl_category & cat_bit) {
      absl::StrAppend(&tmp, prefix, REVERSE_CATEGORY_INDEX_TABLE[i], postfix);
    }
  }

  tmp.pop_back();

  return tmp;
}

void AclFamily::Acl(CmdArgList args, ConnectionContext* cntx) {
  (*cntx)->SendError("Wrong number of arguments for acl command");
}

void AclFamily::List(CmdArgList args, ConnectionContext* cntx) {
  const auto registry_with_lock = ServerState::tlocal()->user_registry->GetRegistryWithLock();
  const auto& registry = registry_with_lock.registry;
  (*cntx)->StartArray(registry.size());

  auto pretty_print_sha = [](std::string_view pass) {
    return absl::BytesToHexString(pass.substr(15)).substr(15);
  };

  for (const auto& [username, user] : registry) {
    std::string buffer = "user ";
    const std::string_view pass = user.Password();
    const std::string password = pass == "nopass" ? "nopass" : pretty_print_sha(pass);
    const std::string acl_cat = AclToString(user.AclCategory());

    using namespace std::string_view_literals;

    absl::StrAppend(&buffer, username, " ", user.IsActive() ? "on "sv : "off "sv, password, " ",
                    acl_cat);

    (*cntx)->SendSimpleString(buffer);
  }
}

namespace {

std::optional<std::string> MaybeParsePassword(std::string_view command) {
  if (command[0] != '>') {
    return {};
  }

  return {std::string(command.substr(1))};
}

std::optional<bool> MaybeParseStatus(std::string_view command) {
  if (command == "ON") {
    return true;
  }
  if (command == "OFF") {
    return false;
  }
  return {};
}

using OptCat = std::optional<uint32_t>;

// bool == true if +
// bool == false if -
std::pair<OptCat, bool> MaybeParseAclCategory(std::string_view command) {
  if (absl::StartsWith(command, "+@")) {
    auto res = CATEGORY_INDEX_TABLE.find(command.substr(2));
    if (res == CATEGORY_INDEX_TABLE.end()) {
      return {};
    }
    return {res->second, true};
  }

  if (absl::StartsWith(command, "-@")) {
    auto res = CATEGORY_INDEX_TABLE.find(command.substr(2));
    if (res == CATEGORY_INDEX_TABLE.end()) {
      return {};
    }
    return {res->second, false};
  }

  return {};
}

using facade::ErrorReply;

std::variant<User::UpdateRequest, ErrorReply> ParseAclSetUser(CmdArgList args) {
  User::UpdateRequest req;

  for (auto arg : args) {
    if (auto pass = MaybeParsePassword(facade::ToSV(arg)); pass) {
      if (req.password) {
        return ErrorReply("Only one password is allowed");
      }
      req.password = std::move(pass);
      continue;
    }

    ToUpper(&arg);
    const auto command = facade::ToSV(arg);

    if (auto status = MaybeParseStatus(command); status) {
      if (req.is_active) {
        return ErrorReply("Multiple ON/OFF are not allowed");
      }
      req.is_active = *status;
      continue;
    }

    auto [cat, add] = MaybeParseAclCategory(command);
    if (!cat) {
      return ErrorReply(absl::StrCat("Unrecognized parameter", command));
    }

    auto* acl_field = add ? &req.plus_acl_categories : &req.minus_acl_categories;
    *acl_field = acl_field->value_or(0) | *cat;
  }

  return req;
}

}  // namespace

void AclFamily::StreamUpdatesToAllProactorConnections(std::string_view user, uint32_t update_cat) {
  auto update_cb = [user, update_cat]([[maybe_unused]] size_t id, util::Connection* conn) {
    DCHECK(conn);
    auto connection = static_cast<facade::Connection*>(conn);
    connection->SendAclUpdateAsync(facade::Connection::AclUpdateMessage{user, update_cat});
  };

  if (main_listener_) {
    main_listener_->TraverseConnections(update_cb);
  }
}

void AclFamily::SetUser(CmdArgList args, ConnectionContext* cntx) {
  std::string_view username = facade::ToSV(args[0]);
  auto req = ParseAclSetUser(args.subspan(1));
  auto error_case = [cntx](ErrorReply&& error) { (*cntx)->SendError(error); };
  auto update_case = [username, cntx, this](User::UpdateRequest&& req) {
    auto& registry = ServerState::tlocal()->user_registry;
    auto user_with_lock = registry->MaybeAddAndUpdateWithLock(username, std::move(req));
    if (user_with_lock.exists) {
      StreamUpdatesToAllProactorConnections(username, user_with_lock.user.AclCategory());
    }
    (*cntx)->SendOk();
  };

  std::visit(Overloaded{error_case, update_case}, std::move(req));
}

using CI = dfly::CommandId;

using MemberFunc = void (AclFamily::*)(CmdArgList args, ConnectionContext* cntx);

inline CommandId::Handler HandlerFunc(AclFamily* acl, MemberFunc f) {
  return [=](CmdArgList args, ConnectionContext* cntx) { return (acl->*f)(args, cntx); };
}

#define HFUNC(x) SetHandler(HandlerFunc(this, &AclFamily::x))

constexpr uint32_t kAcl = acl::CONNECTION;
constexpr uint32_t kList = acl::ADMIN | acl::SLOW | acl::DANGEROUS;
constexpr uint32_t kSetUser = acl::ADMIN | acl::SLOW | acl::DANGEROUS;

// We can't implement the ACL commands and its respective subcommands LIST, CAT, etc
// the usual way, (that is, one command called ACL which then dispatches to the subcommand
// based on the secocond argument) because each of the subcommands has different ACL
// categories. Therefore, to keep it compatible with the CommandId, I need to treat them
// as separate commands in the registry. This is the least intrusive change because it's very
// easy to handle that case explicitly in `DispatchCommand`.

void AclFamily::Register(dfly::CommandRegistry* registry) {
  *registry << CI{"ACL", CO::NOSCRIPT | CO::LOADING, 0, 0, 0, 0, acl::kAcl}.HFUNC(Acl);
  *registry << CI{"ACL LIST", CO::ADMIN | CO::NOSCRIPT | CO::LOADING, 1, 0, 0, 0, acl::kList}.HFUNC(
      List);
  *registry << CI{"ACL SETUSER", CO::ADMIN | CO::NOSCRIPT | CO::LOADING, -2, 0, 0, 0, acl::kSetUser}
                   .HFUNC(SetUser);
}

#undef HFUNC

void AclFamily::Init(facade::Listener* main_listener) {
  main_listener_ = main_listener;
}

}  // namespace dfly::acl
