// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <optional>
#include <string_view>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "facade/dragonfly_listener.h"
#include "facade/facade_types.h"
#include "helio/util/proactor_pool.h"
#include "server/acl/acl_commands_def.h"
#include "server/acl/user_registry.h"
#include "server/command_registry.h"
#include "server/common.h"

namespace facade {
class SinkReplyBuilder;
}  // namespace facade

namespace dfly {

class ConnectionContext;
namespace acl {

class AclFamily final {
 public:
  explicit AclFamily(UserRegistry* registry, util::ProactorPool* pool);

  void Register(CommandRegistry* registry);
  void Init(facade::Listener* listener, UserRegistry* registry);

 private:
  using SinkReplyBuilder = facade::SinkReplyBuilder;

  void Acl(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  void List(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  void SetUser(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  void DelUser(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  void WhoAmI(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder, ConnectionContext* cntx);
  void Save(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  void Load(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  // Helper function for bootstrap
  bool Load();
  void Log(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  void Users(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  void Cat(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  void GetUser(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  void DryRun(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);
  void GenPass(CmdArgList args, Transaction* tx, SinkReplyBuilder* builder);

  // Helper function that updates all open connections and their
  // respective ACL fields on all the available proactor threads
  using Commands = std::vector<uint64_t>;
  void StreamUpdatesToAllProactorConnections(const std::string& user,
                                             const Commands& update_commands,
                                             const AclKeys& update_keys,
                                             const AclPubSub& update_pub_sub);

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

  std::variant<User::UpdateRequest, facade::ErrorReply> ParseAclSetUser(
      const facade::ArgRange& args, bool hashed = false, bool has_all_keys = false,
      bool has_all_channels = false) const;

  void BuildIndexers(RevCommandsIndexStore families);

  // Data members

  facade::Listener* main_listener_{nullptr};
  UserRegistry* registry_;
  CommandRegistry* cmd_registry_;
  util::ProactorPool* pool_;

  // Indexes

  // See definitions for NONE and ALL in facade/acl_commands_def.h
  const CategoryIndexTable cat_table_{{"KEYSPACE", KEYSPACE},
                                      {"READ", READ},
                                      {"WRITE", WRITE},
                                      {"SET", SET},
                                      {"SORTEDSET", SORTEDSET},
                                      {"LIST", LIST},
                                      {"HASH", HASH},
                                      {"STRING", STRING},
                                      {"BITMAP", BITMAP},
                                      {"HYPERLOG", HYPERLOGLOG},
                                      {"GEO", GEO},
                                      {"STREAM", STREAM},
                                      {"PUBSUB", PUBSUB},
                                      {"ADMIN", ADMIN},
                                      {"FAST", FAST},
                                      {"SLOW", SLOW},
                                      {"BLOCKING", BLOCKING},
                                      {"DANGEROUS", DANGEROUS},
                                      {"CONNECTION", CONNECTION},
                                      {"TRANSACTION", TRANSACTION},
                                      {"SCRIPTING", SCRIPTING},
                                      {"BLOOM", BLOOM},
                                      {"FT_SEARCH", FT_SEARCH},
                                      {"THROTTLE", THROTTLE},
                                      {"JSON", JSON},
                                      {"ALL", ALL}};

  // bit 0 at index 0
  // bit 1 at index 1
  // bit n at index n
  const ReverseCategoryIndexTable reverse_cat_table_{
      "KEYSPACE",  "READ",      "WRITE",     "SET",       "SORTEDSET",  "LIST",        "HASH",
      "STRING",    "BITMAP",    "HYPERLOG",  "GEO",       "STREAM",     "PUBSUB",      "ADMIN",
      "FAST",      "SLOW",      "BLOCKING",  "DANGEROUS", "CONNECTION", "TRANSACTION", "SCRIPTING",
      "_RESERVED", "_RESERVED", "_RESERVED", "_RESERVED", "_RESERVED",  "_RESERVED",   "_RESERVED",
      "BLOOM",     "FT_SEARCH", "THROTTLE",  "JSON"};

  // We need this to act as a const member, since the initialization of const data members
  // must be done on the constructor. However, these are initialized a little later, when
  // we Register the commands
  const CategoryToIdxStore& CategoryToIdx(CategoryToIdxStore store = {}) const {
    static CategoryToIdxStore cat_idx = std::move(store);
    return cat_idx;
  }

  const RevCommandsIndexStore& CommandsRevIndexer(RevCommandsIndexStore store = {}) const {
    static RevCommandsIndexStore rev_index_store = std::move(store);
    return rev_index_store;
  }

  const CategoryToCommandsIndexStore& CategoryToCommandsIndex(
      CategoryToCommandsIndexStore store = {}) const {
    static CategoryToCommandsIndexStore index = std::move(store);
    return index;
  }

  // Only for testing interface
 public:
  // Helper accessors for tests. Do not use them directly.
  const ReverseCategoryIndexTable& GetRevTable() const {
    return reverse_cat_table_;
  }

  // We could make CommandsRevIndexer public, but I want this to be
  // clear that this is for TESTING so do not use this in the codebase
  const RevCommandsIndexStore& GetCommandsRevIndexer() const {
    return CommandsRevIndexer();
  }
};

}  // namespace acl
}  // namespace dfly
