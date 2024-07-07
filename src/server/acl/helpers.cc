// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/acl/helpers.h"

#include <limits>
#include <vector>

#include "absl/strings/ascii.h"
#include "absl/strings/escaping.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "core/overloaded.h"
#include "facade/acl_commands_def.h"
#include "server/acl/acl_commands_def.h"
#include "server/acl/user.h"
#include "server/common.h"

namespace dfly::acl {

namespace {

std::string AclCatToString(uint32_t acl_category, User::Sign sign) {
  std::string res = sign == User::Sign::PLUS ? "+@" : "-@";
  if (acl_category == acl::ALL) {
    absl::StrAppend(&res, "all");
    return res;
  }

  const auto& index = CategoryToIdx().at(acl_category);
  absl::StrAppend(&res, absl::AsciiStrToLower(REVERSE_CATEGORY_INDEX_TABLE[index]));
  return res;
}

std::string AclCommandToString(size_t family, uint64_t mask, User::Sign sign) {
  // This is constant but can be optimized with an indexer
  const auto& rev_index = CommandsRevIndexer();
  std::string res;
  std::string prefix = (sign == User::Sign::PLUS) ? "+" : "-";
  if (mask == ALL_COMMANDS) {
    for (const auto& cmd : rev_index[family]) {
      absl::StrAppend(&res, prefix, absl::AsciiStrToLower(cmd), " ");
    }
    res.pop_back();
    return res;
  }

  size_t pos = 0;
  while (mask != 0) {
    ++pos;
    mask = mask >> 1;
  }
  --pos;
  absl::StrAppend(&res, prefix, absl::AsciiStrToLower(rev_index[family][pos]));
  return res;
}

struct CategoryAndMetadata {
  User::CategoryChange change;
  User::ChangeMetadata metadata;
};

struct CommandAndMetadata {
  User::CommandChange change;
  User::ChangeMetadata metadata;
};

using MergeResult = std::vector<std::variant<CategoryAndMetadata, CommandAndMetadata>>;

}  // namespace

// Merge Category and Command changes and sort them by global order seq_no
MergeResult MergeTables(const User::CategoryChanges& categories,
                        const User::CommandChanges& commands) {
  MergeResult result;
  for (auto [cat, meta] : categories) {
    result.push_back(CategoryAndMetadata{cat, meta});
  }

  for (auto [cmd, meta] : commands) {
    result.push_back(CommandAndMetadata{cmd, meta});
  }

  std::sort(result.begin(), result.end(), [](const auto& l, const auto& r) {
    auto fetch = [](const auto& l) { return l.metadata.seq_no; };
    return std::visit(fetch, l) < std::visit(fetch, r);
  });

  return result;
}

std::string AclCatAndCommandToString(const User::CategoryChanges& cat,
                                     const User::CommandChanges& cmds) {
  std::string result;

  auto tables = MergeTables(cat, cmds);

  auto cat_visitor = [&result](const CategoryAndMetadata& val) {
    const auto& [change, meta] = val;
    absl::StrAppend(&result, AclCatToString(change, meta.sign), " ");
  };

  auto cmd_visitor = [&result](const CommandAndMetadata& val) {
    const auto& [change, meta] = val;
    const auto [family, bit_index] = change;
    absl::StrAppend(&result, AclCommandToString(family, bit_index, meta.sign), " ");
  };

  Overloaded visitor{cat_visitor, cmd_visitor};

  for (auto change : tables) {
    std::visit(visitor, change);
  }

  if (!result.empty()) {
    result.pop_back();
  }

  return result;
}

std::string PrettyPrintSha(std::string_view pass, bool all) {
  if (all) {
    return absl::BytesToHexString(pass);
  }
  return absl::BytesToHexString(pass.substr(0, 15)).substr(0, 15);
};

std::optional<ParseKeyResult> MaybeParseAclKey(std::string_view command) {
  if (absl::EqualsIgnoreCase(command, "ALLKEYS") || command == "~*") {
    return ParseKeyResult{"", {}, true};
  }

  if (absl::EqualsIgnoreCase(command, "RESETKEYS")) {
    return ParseKeyResult{"", {}, false, true};
  }

  auto op = KeyOp::READ_WRITE;

  if (absl::StartsWith(command, "%RW")) {
    command = command.substr(3);
  } else if (absl::StartsWith(command, "%R")) {
    op = KeyOp::READ;
    command = command.substr(2);
  } else if (absl::StartsWith(command, "%W")) {
    op = KeyOp::WRITE;
    command = command.substr(2);
  }

  if (!absl::StartsWith(command, "~")) {
    return {};
  }

  auto key = command.substr(1);
  if (key.empty()) {
    return {};
  }
  return ParseKeyResult{std::string(key), op};
}

std::optional<User::UpdatePass> MaybeParsePassword(std::string_view command, bool hashed) {
  using UpPass = User::UpdatePass;
  if (command == "nopass") {
    return UpPass{"", false, true};
  }

  if (command == "resetpass") {
    return UpPass{"", false, false, true};
  }

  if (command[0] == '>' || (hashed && command[0] == '#')) {
    return UpPass{std::string(command.substr(1))};
  }

  if (command[0] == '<') {
    return UpPass{std::string(command.substr(1)), true};
  }

  return {};
}

std::optional<std::string> MaybeParseNamespace(std::string_view command) {
  if (absl::StartsWith(command, "NAMESPACE:")) {
    return std::string(command.substr(7));
  }
  return {};
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

std::pair<OptCommand, bool> MaybeParseAclCommand(std::string_view command,
                                                 const CommandRegistry& registry) {
  if (absl::StartsWith(command, "+")) {
    auto res = registry.Find(command.substr(1));
    if (!res) {
      return {};
    }
    std::pair<size_t, uint64_t> cmd{res->GetFamily(), res->GetBitIndex()};
    return {cmd, true};
  }

  if (absl::StartsWith(command, "-")) {
    auto res = registry.Find(command.substr(1));
    if (!res) {
      return {};
    }
    std::pair<size_t, uint64_t> cmd{res->GetFamily(), res->GetBitIndex()};
    return {cmd, false};
  }

  return {};
}

MaterializedContents MaterializeFileContents(std::vector<std::string>* usernames,
                                             std::string_view file_contents) {
  // This is fine, a very large file will top at 1-2 mb. And that's for 5000+ users with 400
  // characters per line
  std::vector<std::string_view> commands = absl::StrSplit(file_contents, "\n");
  std::vector<std::vector<std::string_view>> materialized;
  materialized.reserve(commands.size());
  usernames->reserve(commands.size());
  for (auto& command : commands) {
    if (command.empty())
      continue;
    std::vector<std::string_view> cmds = absl::StrSplit(command, ' ');
    if (!absl::EqualsIgnoreCase(cmds[0], "USER") || cmds.size() < 4) {
      return {};
    }

    usernames->push_back(std::string(cmds[1]));
    cmds.erase(cmds.begin(), cmds.begin() + 2);
    materialized.push_back(cmds);
  }
  return materialized;
}

using facade::ErrorReply;

std::variant<User::UpdateRequest, ErrorReply> ParseAclSetUser(facade::ArgRange args,
                                                              const CommandRegistry& registry,
                                                              bool hashed, bool has_all_keys) {
  User::UpdateRequest req;

  for (std::string_view arg : args) {
    if (auto pass = MaybeParsePassword(facade::ToSV(arg), hashed); pass) {
      req.passwords.push_back(std::move(*pass));

      if (hashed && absl::StartsWith(facade::ToSV(arg), "#")) {
        req.is_hashed = hashed;
      }
      continue;
    }

    if (auto res = MaybeParseAclKey(facade::ToSV(arg)); res) {
      auto& [glob, op, all_keys, reset_keys] = *res;
      if ((has_all_keys && !all_keys && !reset_keys) ||
          (req.allow_all_keys && !all_keys && !reset_keys)) {
        return ErrorReply(
            "Error in ACL SETUSER modifier '~tmp': Adding a pattern after the * pattern (or the "
            "'allkeys' flag) is not valid and does not have any effect. Try 'resetkeys' to start "
            "with an empty list of patterns");
      }

      req.allow_all_keys = all_keys;
      req.reset_all_keys = reset_keys;
      if (reset_keys) {
        has_all_keys = false;
      }
      req.keys.push_back({std::move(glob), op, all_keys, reset_keys});
      continue;
    }

    std::string command = absl::AsciiStrToUpper(arg);

    if (auto status = MaybeParseStatus(command); status) {
      if (req.is_active) {
        return ErrorReply("Multiple ON/OFF are not allowed");
      }
      req.is_active = *status;
      continue;
    }

    if (auto ns = MaybeParseNamespace(command); ns) {
      req.ns = ns;
      continue;
    }

    auto [cat, add] = MaybeParseAclCategory(command);
    if (cat) {
      using Sign = User::Sign;
      using Val = std::pair<Sign, uint32_t>;
      auto val = add ? Val{Sign::PLUS, *cat} : Val{Sign::MINUS, *cat};
      req.updates.push_back(val);
      continue;
    }

    auto [cmd, sign] = MaybeParseAclCommand(command, registry);
    if (!cmd) {
      return ErrorReply(absl::StrCat("Unrecognized parameter ", command));
    }

    using Sign = User::Sign;
    using Val = User::UpdateRequest::CommandsValueType;
    auto [index, bit] = *cmd;
    auto val = sign ? Val{Sign::PLUS, index, bit} : Val{Sign::MINUS, index, bit};
    req.updates.push_back(val);
  }

  return req;
}

using facade::CmdArgList;

std::string AclKeysToString(const AclKeys& keys) {
  if (keys.all_keys) {
    return "~*";
  }
  std::string result;
  for (auto& [pattern, op] : keys.key_globs) {
    if (op == KeyOp::READ_WRITE) {
      absl::StrAppend(&result, "~", pattern, " ");
      continue;
    }
    std::string op_str = (op == KeyOp::READ) ? "R" : "W";
    absl::StrAppend(&result, "%", op_str, "~", pattern, " ");
  }

  if (!result.empty()) {
    result.pop_back();
  }
  return result;
}

std::string PasswordsToString(const absl::flat_hash_set<std::string>& passwords, bool nopass,
                              bool full_sha) {
  if (nopass) {
    return "nopass ";
  }
  std::string result;
  for (const auto& pass : passwords) {
    absl::StrAppend(&result, "#", PrettyPrintSha(pass, full_sha), " ");
  }

  return result;
}
}  // namespace dfly::acl
