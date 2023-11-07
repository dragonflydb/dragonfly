// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <variant>

#include "facade/facade_types.h"
#include "server/acl/acl_log.h"
#include "server/acl/user.h"
#include "server/command_registry.h"

namespace dfly::acl {

std::string AclCatToString(uint32_t acl_category);

std::string AclCommandToString(const std::vector<uint64_t>& acl_category);

std::string PrettyPrintSha(std::string_view pass, bool all = false);

std::optional<std::string> MaybeParsePassword(std::string_view command, bool hashed = false);

std::optional<bool> MaybeParseStatus(std::string_view command);

using OptCat = std::optional<uint32_t>;
std::pair<OptCat, bool> MaybeParseAclCategory(std::string_view command);

bool IsIndexAllCommandsFlag(size_t index);

using OptCommand = std::optional<std::pair<size_t, uint64_t>>;
std::pair<OptCommand, bool> MaybeParseAclCommand(std::string_view command,
                                                 const CommandRegistry& registry);

template <typename T>
std::variant<User::UpdateRequest, facade::ErrorReply> ParseAclSetUser(
    T args, const CommandRegistry& registry, bool hashed = false);

using MaterializedContents = std::optional<std::vector<std::vector<std::string_view>>>;

MaterializedContents MaterializeFileContents(std::vector<std::string>* usernames,
                                             std::string_view file_contents);

}  // namespace dfly::acl
