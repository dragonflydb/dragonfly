// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <variant>

#include "absl/container/flat_hash_set.h"
#include "facade/facade_types.h"
#include "server/acl/acl_log.h"
#include "server/acl/user.h"
#include "server/command_registry.h"

namespace dfly::acl {

std::string AclCatAndCommandToString(const User::CategoryChanges& cat,
                                     const User::CommandChanges& cmds);

std::string PrettyPrintSha(std::string_view pass, bool all = false);

// When hashed is true, we allow passwords that start with both # and >
std::optional<User::UpdatePass> MaybeParsePassword(std::string_view command, bool hashed = false);

std::optional<bool> MaybeParseStatus(std::string_view command);

using OptCat = std::optional<uint32_t>;
std::pair<OptCat, bool> MaybeParseAclCategory(std::string_view command);

bool IsIndexAllCommandsFlag(size_t index);

using OptCommand = std::optional<std::pair<size_t, uint64_t>>;
std::pair<OptCommand, bool> MaybeParseAclCommand(std::string_view command,
                                                 const CommandRegistry& registry);

std::variant<User::UpdateRequest, facade::ErrorReply> ParseAclSetUser(
    facade::ArgRange args, const CommandRegistry& registry, bool hashed = false,
    bool has_all_keys = false);

using MaterializedContents = std::optional<std::vector<std::vector<std::string_view>>>;

MaterializedContents MaterializeFileContents(std::vector<std::string>* usernames,
                                             std::string_view file_contents);

struct ParseKeyResult {
  std::string glob;
  KeyOp op;
  bool all_keys{false};
  bool reset_keys{false};
};

std::optional<ParseKeyResult> MaybeParseAclKey(std::string_view command);

std::string AclKeysToString(const AclKeys& keys);

std::string PasswordsToString(const absl::flat_hash_set<std::string>& passwords, bool nopass,
                              bool full_sha);

}  // namespace dfly::acl
