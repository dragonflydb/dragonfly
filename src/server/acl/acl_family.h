// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <array>
#include <cstdint>
#include <optional>
#include <string_view>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "facade/facade_types.h"
#include "helio/util/proactor_pool.h"
#include "server/acl/acl_commands_def.h"
#include "server/acl/user_registry.h"
#include "server/command_registry.h"
#include "server/execution_state.h"

namespace facade {
class SinkReplyBuilder;
class Listener;
}  // namespace facade

namespace dfly {

using facade::CmdArgList;
using facade::CmdArgParser;

class ConnectionContext;
namespace acl {

class AclFamily final {
 public:
  explicit AclFamily(UserRegistry* registry, util::ProactorPool* pool);

  void Register(CommandRegistry* registry);
  void Init(facade::Listener* listener);

 private:
  using SinkReplyBuilder = facade::SinkReplyBuilder;

  void Acl(CmdArgParser parser, CommandContext* cmd_cntx);
  void List(CmdArgParser parser, CommandContext* cmd_cntx);
  void SetUser(CmdArgParser parser, CommandContext* cmd_cntx);
  void DelUser(CmdArgParser parser, CommandContext* cmd_cntx);
  void WhoAmI(CmdArgParser parser, CommandContext* cmd_cntx);
  void Save(CmdArgParser parser, CommandContext* cmd_cntx);
  void Load(CmdArgParser parser, CommandContext* cmd_cntx);
  // Helper function for bootstrap
  bool Load();
  void Log(CmdArgParser parser, CommandContext* cmd_cntx);
  void Users(CmdArgParser parser, CommandContext* cmd_cntx);
  void Cat(CmdArgParser parser, CommandContext* cmd_cntx);
  void GetUser(CmdArgParser parser, CommandContext* cmd_cntx);
  void DryRun(CmdArgParser parser, CommandContext* cmd_cntx);
  void GenPass(CmdArgParser parser, CommandContext* cmd_cntx);
  void Help(CmdArgParser parser, CommandContext* cmd_cntx);

  // Helper function that updates all open connections and their
  // respective ACL fields on all the available proactor threads
  using Commands = std::vector<uint64_t>;
  void StreamUpdatesToAllProactorConnections(const std::string& user,
                                             const Commands& update_commands,
                                             const AclKeys& update_keys,
                                             const AclPubSub& update_pub_sub, size_t db);

  // Helper function that closes all open connection from the deleted user
  void EvictOpenConnectionsOnAllProactors(const absl::flat_hash_set<std::string_view>& user);

  // Helper function that closes all open connections for users in the registry
  void EvictOpenConnectionsOnAllProactorsWithRegistry(const UserRegistry::RegistryType& registry);

  // Helper function that loads the acl state of an acl file into the user registry
  GenericError LoadToRegistryFromFile(std::string_view full_path, SinkReplyBuilder* builder);

  // Serializes the whole registry into a string
  std::string RegistryToString() const;

  std::string AclCatToString(uint32_t acl_category, User::Sign sign) const;

  std::string AclCommandToString(size_t family, uint64_t mask, User::Sign sign) const;

  // Serializes category and command to string
  std::string AclCatAndCommandToString(const User::CategoryChanges& cat,
                                       const User::CommandChanges& cmds) const;

  using OptCat = std::optional<uint32_t>;
  std::pair<OptCat, bool> MaybeParseAclCategory(std::string_view command) const;

  using OptCommand = std::optional<std::pair<size_t, uint64_t>>;
  std::pair<OptCommand, bool> MaybeParseAclCommand(std::string_view command) const;

  std::optional<std::string> MaybeParseNamespace(std::string_view command) const;

  User::UpdateRequest::CategoryValueType CategoryRule(User::Sign sign, uint32_t category) const;

  std::variant<User::UpdateRequest, facade::ErrorReply> ParseAclSetUser(
      facade::ParsedArgs args, bool hashed = false, bool has_all_keys = false,
      bool has_all_channels = false) const;

  void BuildIndexers(RevCommandsIndexStore families);

  // Data members

  facade::Listener* main_listener_{nullptr};
  UserRegistry* registry_;
  CommandRegistry* cmd_registry_;
  util::ProactorPool* pool_;

  // Command names by family and bit position, to print command rules.
  RevCommandsIndexStore family_commands_;

  // The bits of each category's commands: category_commands_[bit position][family].
  std::array<std::vector<uint64_t>, kCategoryNames.size()> category_commands_;

  size_t dbnum_ = 0;

 public:
  // Only for tests.
  const RevCommandsIndexStore& GetCommandsRevIndexer() const {
    return family_commands_;
  }
};

}  // namespace acl
}  // namespace dfly
