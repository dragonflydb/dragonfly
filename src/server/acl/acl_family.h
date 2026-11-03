// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "absl/functional/function_ref.h"
#include "facade/facade_types.h"
#include "helio/util/proactor_pool.h"
#include "server/acl/acl_commands_def.h"
#include "server/acl/user_registry.h"
#include "server/command_registry.h"
#include "server/execution_state.h"

namespace facade {
class Listener;
}  // namespace facade

namespace dfly {

using facade::CmdArgList;
using facade::CmdArgParser;

namespace acl {

class AclFamily final {
 public:
  explicit AclFamily(UserRegistry* registry, util::ProactorPool* pool);

  void Register(CommandRegistry* registry);
  void Init(facade::Listener* listener);

 private:
  void Acl(CmdArgParser parser, CommandContext* cmd_cntx);
  void List(CmdArgParser parser, CommandContext* cmd_cntx);
  void SetUser(CmdArgParser parser, CommandContext* cmd_cntx);
  void DelUser(CmdArgParser parser, CommandContext* cmd_cntx);
  void WhoAmI(CmdArgParser parser, CommandContext* cmd_cntx);
  void Save(CmdArgParser parser, CommandContext* cmd_cntx);
  void Load(CmdArgParser parser, CommandContext* cmd_cntx);
  void Log(CmdArgParser parser, CommandContext* cmd_cntx);
  void Users(CmdArgParser parser, CommandContext* cmd_cntx);
  void Cat(CmdArgParser parser, CommandContext* cmd_cntx);
  void GetUser(CmdArgParser parser, CommandContext* cmd_cntx);
  void DryRun(CmdArgParser parser, CommandContext* cmd_cntx);
  void GenPass(CmdArgParser parser, CommandContext* cmd_cntx);
  void Help(CmdArgParser parser, CommandContext* cmd_cntx);

  // Pushes the user's updated ACL rules to all its open connections.
  void StreamUpdatesToConnections(std::string_view username, const User& user);

  // Closes all open connections whose authenticated user matches `pred`.
  void EvictConnections(absl::FunctionRef<bool(std::string_view username)> pred);

  using UserRequests = std::vector<std::pair<std::string, User::UpdateRequest>>;

  // Reads and parses the ACL file into one update request per user.
  GenericError ParseAclFile(std::string_view path, UserRequests* requests) const;

  // Adds the users of an ACL file to a registry that holds the write lock, and a default user if
  // the file has none.
  void AddUsers(UserRequests requests, UserRegistry::RegistryType* registry);

  // Creates or updates a user in a registry that holds the write lock.
  User& UpsertUser(std::string_view username, User::UpdateRequest req,
                   UserRegistry::RegistryType* registry);

  // `has_all_keys` is whether the updated user can already access all keys.
  std::variant<User::UpdateRequest, facade::ErrorReply> ParseAclSetUser(facade::ParsedArgs args,
                                                                        bool has_all_keys) const;

  void BuildIndexers(RevCommandsIndexStore families);

  facade::Listener* main_listener_{nullptr};
  UserRegistry* registry_;
  CommandRegistry* cmd_registry_{nullptr};
  util::ProactorPool* pool_;
  uint32_t dbnum_;

  // Command names by family and bit position, to print command rules.
  RevCommandsIndexStore family_commands_;

  // The bits of each category's commands: category_commands_[bit position][family].
  CategoryCommandsIndex category_commands_;

 public:
  // Only for tests.
  const RevCommandsIndexStore& GetCommandsRevIndexer() const {
    return family_commands_;
  }
};

}  // namespace acl
}  // namespace dfly
