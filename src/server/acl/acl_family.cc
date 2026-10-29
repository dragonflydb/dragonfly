// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/acl/acl_family.h"

#include <fcntl.h>
#include <libgen.h>
#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <variant>

#include "absl/container/flat_hash_set.h"
#include "absl/flags/commandlineflag.h"
#include "absl/numeric/bits.h"
#include "absl/strings/ascii.h"
#include "absl/strings/escaping.h"
#include "absl/strings/match.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "absl/strings/strip.h"
#include "base/flags.h"
#include "base/logging.h"
#include "core/overloaded.h"
#include "facade/dragonfly_connection.h"
#include "facade/dragonfly_listener.h"
#include "facade/facade_types.h"
#include "facade/reply_builder.h"
#include "io/file.h"
#include "io/file_util.h"
#include "server/acl/acl_commands_def.h"
#include "server/acl/acl_log.h"
#include "server/acl/validator.h"
#include "server/cluster_support.h"
#include "server/command_registry.h"
#include "server/common.h"
#include "server/config_registry.h"
#include "server/conn_context.h"
#include "server/error.h"
#include "server/server_state.h"
#include "util/proactor_pool.h"

namespace rng = std::ranges;

using namespace std;

ABSL_FLAG(string, aclfile, "", "Path and name to aclfile");
ABSL_DECLARE_FLAG(uint32_t, dbnum);
ABSL_DECLARE_FLAG(string, requirepass);

namespace dfly::acl {

namespace {

using MaterializedContents = variant<vector<vector<string_view>>, string>;

MaterializedContents MaterializeFileContents(vector<string>* usernames, string_view file_contents);

// A renamed file is durable only once its directory entry is.
bool SyncDir(string file_path) {
  const int fd = open(dirname(file_path.data()), O_RDONLY | O_DIRECTORY);
  const bool synced = fd != -1 && fsync(fd) == 0;
  if (fd != -1) {
    close(fd);
  }
  return synced;
}

void SendAclSecurityEvents(const AclLog::LogEntry& entry, facade::RedisReplyBuilder* rb);

template <typename P>
void TraverseEvictImpl(P predicate, facade::Listener* main_listener, util::ProactorPool* pool);

using Sign = User::Sign;

string_view SignPrefix(Sign sign) {
  return sign == Sign::PLUS ? "+" : "-";
}

// Returns the mask of an upper-case category name.
optional<uint32_t> ParseCategoryName(string_view name) {
  if (name == "ALL")
    return ALL;
  if (name == "SEARCH")  // Alias for FT_SEARCH.
    return FT_SEARCH;
  if (name == kReservedCategory)
    return nullopt;

  auto it = rng::find(kCategoryNames, name);
  if (it == kCategoryNames.end())
    return nullopt;
  return 1u << (it - kCategoryNames.begin());
}

// Parses ALLCOMMANDS, NOCOMMANDS, +@<category> and -@<category>, given in upper case.
optional<pair<Sign, uint32_t>> ParseCategoryRule(string_view rule) {
  if (rule == "ALLCOMMANDS")
    return pair{Sign::PLUS, ALL};
  if (rule == "NOCOMMANDS")
    return pair{Sign::MINUS, ALL};

  if (rule.size() < 2 || rule[1] != '@' || (rule[0] != '+' && rule[0] != '-'))
    return nullopt;

  auto category = ParseCategoryName(rule.substr(2));
  if (!category)
    return nullopt;
  return pair{rule[0] == '+' ? Sign::PLUS : Sign::MINUS, *category};
}

// #<hash> takes a hex SHA-256, the form the ACL file stores passwords in.
optional<User::UpdatePass> ParsePasswordRule(string_view rule) {
  if (rule == "nopass")
    return User::UpdatePass{.password = {}, .nopass = true};
  if (rule == "resetpass")
    return User::UpdatePass{.password = {}, .reset_password = true};

  if (absl::ConsumePrefix(&rule, ">"))
    return User::UpdatePass{.password = string(rule)};
  if (absl::ConsumePrefix(&rule, "#"))
    return User::UpdatePass{.password = string(rule), .is_hashed = true};
  if (absl::ConsumePrefix(&rule, "<"))
    return User::UpdatePass{.password = string(rule), .unset = true};

  return nullopt;
}

optional<User::UpdateKey> ParseKeyRule(string_view rule) {
  if (absl::EqualsIgnoreCase(rule, "ALLKEYS") || rule == "~*")
    return User::UpdateKey{.key = {}, .op = KeyOp::READ_WRITE, .all_keys = true};
  if (absl::EqualsIgnoreCase(rule, "RESETKEYS"))
    return User::UpdateKey{.key = {}, .op = KeyOp::READ_WRITE, .reset_keys = true};

  KeyOp op = KeyOp::READ_WRITE;
  if (absl::ConsumePrefix(&rule, "%RW")) {
    op = KeyOp::READ_WRITE;
  } else if (absl::ConsumePrefix(&rule, "%R")) {
    op = KeyOp::READ;
  } else if (absl::ConsumePrefix(&rule, "%W")) {
    op = KeyOp::WRITE;
  }

  if (!absl::ConsumePrefix(&rule, "~") || rule.empty())
    return nullopt;
  return User::UpdateKey{.key = string(rule), .op = op};
}

optional<User::UpdatePubSub> ParseChannelRule(string_view rule) {
  if (absl::EqualsIgnoreCase(rule, "ALLCHANNELS") || rule == "&*")
    return User::UpdatePubSub{.pattern = {}, .all_channels = true};
  if (absl::EqualsIgnoreCase(rule, "RESETCHANNELS"))
    return User::UpdatePubSub{.pattern = {}, .reset_channels = true};

  if (!absl::ConsumePrefix(&rule, "&") || rule.empty())
    return nullopt;
  return User::UpdatePubSub{.pattern = string(rule),
                            .has_asterisk = rule.find('*') != string_view::npos};
}

// Parses $<db> and $all.
optional<size_t> ParseDbRule(string_view rule, uint32_t dbnum) {
  if (!absl::ConsumePrefix(&rule, "$"))
    return nullopt;
  if (absl::EqualsIgnoreCase(rule, "ALL"))
    return kAllDbs;

  size_t db = 0;
  if (absl::SimpleAtoi(rule, &db) && db < dbnum)
    return db;
  return nullopt;
}

// Parses +<command> and -<command>, given in upper case.
optional<User::UpdateRequest::CommandsValueType> ParseCommandRule(string_view rule,
                                                                  const CommandRegistry& registry) {
  if (rule.empty() || (rule[0] != '+' && rule[0] != '-'))
    return nullopt;

  // "cmd|sub" names a subcommand, the form ACL SAVE writes.
  string name{rule.substr(1)};
  rng::replace(name, '|', ' ');
  const CommandId* cid = registry.Find(name);
  if (!cid || cid->IsAlias())
    return nullopt;

  const Sign sign = rule[0] == '+' ? Sign::PLUS : Sign::MINUS;
  return User::UpdateRequest::CommandsValueType{sign, cid->GetFamily(), cid->AclRuleMask()};
}

optional<bool> ParseStatusRule(string_view rule) {
  if (rule == "ON")
    return true;
  if (rule == "OFF")
    return false;
  return nullopt;
}

// Hashes are truncated unless `full_hash`, which the ACL file needs to restore the passwords.
string PasswordsToString(const User& user, bool full_hash) {
  if (user.HasNopass())
    return "nopass ";

  string result;
  for (const auto& hash : user.Passwords()) {
    const string hex = absl::BytesToHexString(hash);
    absl::StrAppend(&result, "#", full_hash ? hex : hex.substr(0, 15), " ");
  }
  return result;
}

string KeysToString(const AclKeys& keys) {
  if (keys.all_keys)
    return "~*";

  return absl::StrJoin(keys.key_globs, " ", [](string* out, const GlobType& glob) {
    const auto& [pattern, op] = glob;
    string_view prefix = op == KeyOp::READ_WRITE ? "" : (op == KeyOp::READ ? "%R" : "%W");
    absl::StrAppend(out, prefix, "~", pattern);
  });
}

string ChannelsToString(const AclPubSub& pub_sub) {
  if (pub_sub.all_channels)
    return "&*";

  string result = "resetchannels";
  for (const auto& [glob, _] : pub_sub.globs)
    absl::StrAppend(&result, " &", glob);
  return result;
}

string DbToString(size_t db) {
  return db == kAllDbs ? "all" : absl::StrCat(db);
}

string CategoryRuleToString(uint32_t category, Sign sign) {
  string_view name = category == ALL ? "all" : kCategoryNames[absl::countr_zero(category)];
  return absl::StrCat(SignPrefix(sign), "@", absl::AsciiStrToLower(name));
}

User::UpdateRequest::CategoryValueType CategoryRule(
    Sign sign, uint32_t category, const CategoryCommandsIndex& category_commands) {
  if (category == ALL)
    return {sign, ALL, vector<uint64_t>(NumberOfFamilies(), ALL_COMMANDS)};
  return {sign, category, category_commands[absl::countr_zero(category)]};
}

string CommandRuleToString(User::CommandChange cmd, Sign sign,
                           const RevCommandsIndexStore& family_commands) {
  const auto [family, mask] = cmd;

  // A +name rule also sets the bits of name's ACL-only subcommands, registered after name.
  string name = absl::AsciiStrToLower(family_commands[family][absl::countr_zero(mask)]);
  rng::replace(name, ' ', '|');  // One token in the ACL file.
  return absl::StrCat(SignPrefix(sign), name);
}

// Serializes the user's category and command rules in the order they were applied.
string CommandRulesToString(const User& user, const RevCommandsIndexStore& family_commands) {
  vector<pair<size_t, string>> rules;  // (seq_no, rule)
  rules.reserve(user.CatChanges().size() + user.CmdChanges().size());
  for (const auto& [category, meta] : user.CatChanges())
    rules.emplace_back(meta.seq_no, CategoryRuleToString(category, meta.sign));
  for (const auto& [command, meta] : user.CmdChanges())
    rules.emplace_back(meta.seq_no, CommandRuleToString(command, meta.sign, family_commands));

  rng::sort(rules);
  return absl::StrJoin(rules, " ", [](string* out, const auto& rule) { out->append(rule.second); });
}

// Serializes the user to a rule line. The ACL file format has upper-case keywords and full
// password hashes, the ACL LIST format lower-case keywords and truncated hashes.
string UserToString(string_view username, const User& user, bool file_format,
                    const RevCommandsIndexStore& family_commands) {
  string_view status =
      user.IsActive() ? (file_format ? "ON" : "on") : (file_format ? "OFF" : "off");
  const string keys = KeysToString(user.Keys());
  return absl::StrCat(file_format ? "USER " : "user ", username, " ", status, " ",
                      PasswordsToString(user, file_format), keys, keys.empty() ? "" : " ",
                      ChannelsToString(user.PubSub()), " ",
                      CommandRulesToString(user, family_commands), " $", DbToString(user.Db()));
}

}  // namespace

AclFamily::AclFamily(UserRegistry* registry, util::ProactorPool* pool)
    : registry_(registry), pool_(pool) {
  dbnum_ = absl::GetFlag(FLAGS_dbnum);
}

void AclFamily::Acl(CmdArgParser parser, CommandContext* cmd_cntx) {
  cmd_cntx->SendError("Wrong number of arguments for acl command");
}

void AclFamily::List(CmdArgParser parser, CommandContext* cmd_cntx) {
  const auto reg = registry_->GetRegistryWithLock();
  auto* rb = static_cast<facade::RedisReplyBuilder*>(cmd_cntx->rb());
  rb->StartArray(reg.registry.size());
  for (const auto& [username, user] : reg.registry)
    rb->SendSimpleString(UserToString(username, user, false, family_commands_));
}

void AclFamily::StreamUpdatesToAllProactorConnections(const std::string& user,
                                                      const Commands& update_commands,
                                                      const AclKeys& update_keys,
                                                      const AclPubSub& update_pub_sub, size_t db) {
  auto update_cb = [&]([[maybe_unused]] size_t id, util::Connection* conn) {
    DCHECK(conn);
    auto connection = static_cast<facade::Connection*>(conn);
    if (!connection->IsHttp() && connection->cntx()) {
      auto* cntx = static_cast<dfly::ConnectionContext*>(connection->cntx());
      if (user == cntx->authed_username) {
        cntx->SetAclCredentials({.acl_commands = update_commands,
                                 .keys = update_keys,
                                 .pub_sub = update_pub_sub,
                                 .ns = {},
                                 .db = db});
      }
    }
  };

  if (main_listener_ && main_listener_->protocol() == facade::Protocol::REDIS) {
    main_listener_->TraverseConnections(update_cb);
  }
}

using facade::ErrorReply;

void AclFamily::SetUser(CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view username = parser.Next();
  auto reg = registry_->GetRegistryWithWriteLock();
  const bool exists = reg.registry.contains(username);
  const bool has_all_keys = exists ? reg.registry.find(username)->second.Keys().all_keys : false;
  auto req = ParseAclSetUser(parser.RemainingRange(), has_all_keys);

  auto error_case = [cmd_cntx](ErrorReply&& error) { cmd_cntx->SendError(error); };

  auto update_case = [username, &reg, cmd_cntx, this, exists](User::UpdateRequest&& req) {
    auto& user = reg.registry[username];
    const User::MemoryUsage before = exists ? user.GetMemoryUsage() : User::MemoryUsage{};
    if (!exists) {
      User::UpdateRequest default_req;
      default_req.updates = {CategoryRule(User::Sign::MINUS, acl::ALL, category_commands_)};
      user.Update(std::move(default_req));
    }
    const bool reset_channels = req.reset_channels;
    user.Update(std::move(req));
    registry_->TrackUser(before, user.GetMemoryUsage(), !exists);
    // Send ok first because the connection might get evicted
    cmd_cntx->SendOk();
    if (exists) {
      if (!reset_channels) {
        StreamUpdatesToAllProactorConnections(string(username), user.AclCommands(), user.Keys(),
                                              user.PubSub(), user.Db());
      }
      // We evict connections that had their channels reseted
      else {
        EvictOpenConnectionsOnAllProactors({username});
      }
    }
  };

  std::visit(Overloaded{error_case, update_case}, std::move(req));
}

void AclFamily::EvictOpenConnectionsOnAllProactors(const absl::flat_hash_set<string_view>& users) {
  return TraverseEvictImpl(
      [&](auto* ctx) {
        auto* dfly_ctx = static_cast<dfly::ConnectionContext*>(ctx);
        return ctx && users.contains(dfly_ctx->authed_username);
      },
      main_listener_, pool_);
}

void AclFamily::EvictOpenConnectionsOnAllProactorsWithRegistry(
    const UserRegistry::RegistryType& registry) {
  return TraverseEvictImpl(
      [&](auto* ctx) {
        auto* dfly_ctx = static_cast<dfly::ConnectionContext*>(ctx);
        return ctx && dfly_ctx->authed_username != "default" &&
               registry.contains(dfly_ctx->authed_username);
      },
      main_listener_, pool_);
}

void AclFamily::DelUser(CmdArgParser parser, CommandContext* cmd_cntx) {
  auto& registry = *registry_;
  absl::flat_hash_set<string_view> users;

  for (string_view username : parser.RemainingRange()) {
    if (username == "default") {
      continue;
    }
    if (registry.RemoveUser(username)) {
      users.insert(username);
    }
  }

  if (users.empty()) {
    cmd_cntx->rb()->SendLong(0);
    return;
  }
  VLOG(1) << "Evicting open acl connections";
  EvictOpenConnectionsOnAllProactors(users);
  VLOG(1) << "Done evicting open acl connections";
  cmd_cntx->rb()->SendLong(users.size());
}

void AclFamily::WhoAmI(CmdArgParser parser, CommandContext* cmd_cntx) {
  auto* rb = static_cast<facade::RedisReplyBuilder*>(cmd_cntx->rb());
  rb->SendBulkString(absl::StrCat("User is ", cmd_cntx->server_conn_cntx()->authed_username));
}

string AclFamily::RegistryToString() const {
  const auto reg = registry_->GetRegistryWithLock();
  string result;
  for (const auto& [username, user] : reg.registry)
    absl::StrAppend(&result, UserToString(username, user, true, family_commands_), "\n");
  return result;
}

void AclFamily::Save(CmdArgParser parser, CommandContext* cmd_cntx) {
  auto acl_file_path = absl::GetFlag(FLAGS_aclfile);
  auto* builder = cmd_cntx->rb();
  if (acl_file_path.empty()) {
    builder->SendError("Dragonfly is not configured to use an ACL file.");
    return;
  }

  // Written next to the aclfile and renamed over it, so a reader never sees a partial file.
  std::string tmp = absl::StrCat(acl_file_path, ".tmpXXXXXX");
  int fd = mkstemp(tmp.data());
  auto fail = [&](std::string_view what) {
    std::error_code ec(errno, std::system_category());
    std::string error = absl::StrCat("Failed to ", what, ": ", ec.message());
    if (fd != -1) {
      close(fd);
      unlink(tmp.c_str());
    }
    LOG(ERROR) << error;
    builder->SendError(error);
  };
  if (fd == -1) {
    return fail("open the aclfile");
  }

  std::string output = RegistryToString();
  ssize_t n = 1;
  for (size_t off = 0; n > 0 && off < output.size(); off += n) {
    n = write(fd, output.data() + off, output.size() - off);
  }
  if (n <= 0 || fsync(fd) == -1) {
    return fail("write to the aclfile");
  }
  if (rename(tmp.c_str(), acl_file_path.c_str()) == -1) {
    return fail("rename the aclfile");
  }
  close(fd);
  fd = -1;
  if (!SyncDir(acl_file_path)) {
    return fail("sync the aclfile directory");
  }

  builder->SendOk();
}

GenericError AclFamily::LoadToRegistryFromFile(std::string_view full_path,
                                               SinkReplyBuilder* builder) {
  auto is_file_read = io::ReadFileToString(full_path);
  if (!is_file_read) {
    auto error = absl::StrCat("Dragonfly could not load ACL file ", full_path, " with error ",
                              is_file_read.error().message());

    LOG(WARNING) << error;
    return {std::move(error)};
  }

  auto file_contents = std::move(is_file_read.value());

  std::vector<std::string> usernames;
  auto materialized = MaterializeFileContents(&usernames, file_contents);

  if (auto* bad_line = std::get_if<std::string>(&materialized)) {
    std::string error = std::move(*bad_line);
    LOG(WARNING) << error;
    return {std::move(error)};
  }

  std::vector<User::UpdateRequest> requests;

  auto& lines = std::get<0>(materialized);
  for (size_t i = 0; i < lines.size(); ++i) {
    if (lines[i].empty())
      continue;
    auto req = ParseAclSetUser(facade::ParsedArgs{lines[i]}, false);
    if (std::holds_alternative<ErrorReply>(req)) {
      auto error = absl::StrCat("line ", i + 1, ": ", std::get<ErrorReply>(req).ToSv());
      LOG(WARNING) << "Error while parsing aclfile: " << error;
      return {std::move(error)};
    }
    requests.push_back(std::move(std::get<User::UpdateRequest>(req)));
  }

  auto registry_with_wlock = registry_->GetRegistryWithWriteLock();
  auto& registry = registry_with_wlock.registry;
  if (builder) {
    builder->SendOk();
    // Evict open connections for old users
    EvictOpenConnectionsOnAllProactorsWithRegistry(registry);
    registry.clear();
    registry_->ResetStats();
  }

  for (size_t i = 0; i < usernames.size(); ++i) {
    User::UpdateRequest default_req;
    default_req.updates = {CategoryRule(User::Sign::MINUS, acl::ALL, category_commands_)};
    auto& user = registry[usernames[i]];
    user.Update(std::move(default_req));
    user.Update(std::move(requests[i]));
    registry_->TrackUser({}, user.GetMemoryUsage(), /*is_new=*/true);
  }

  if (!registry.contains("default")) {
    // --requirepass applies when the file does not define default.
    auto req = registry_->DefaultUserUpdateRequest();
    if (auto pass = absl::GetFlag(FLAGS_requirepass); !pass.empty()) {
      req.passwords.front() = {std::move(pass)};
    }
    auto& user = registry["default"];
    user.Update(std::move(req));
    registry_->TrackUser({}, user.GetMemoryUsage(), /*is_new=*/true);
  }

  return {};
}

GenericError AclFamily::Load() {
  auto acl_file = absl::GetFlag(FLAGS_aclfile);
  return LoadToRegistryFromFile(acl_file, nullptr);
}

void AclFamily::Load(CmdArgParser parser, CommandContext* cmd_cntx) {
  auto acl_file = absl::GetFlag(FLAGS_aclfile);
  auto* rb = static_cast<facade::RedisReplyBuilder*>(cmd_cntx->rb());
  if (acl_file.empty()) {
    rb->SendError("Dragonfly is not configured to use an ACL file.");
    return;
  }

  const auto load_error = LoadToRegistryFromFile(acl_file, rb);

  if (load_error) {
    rb->SendError(absl::StrCat("Error loading: ", acl_file, " ", load_error.Format()));
  }
}

void AclFamily::Log(CmdArgParser parser, CommandContext* cmd_cntx) {
  auto args = parser.UnparsedArgs();
  auto* rb = static_cast<facade::RedisReplyBuilder*>(cmd_cntx->rb());
  if (args.size() > 1) {
    return rb->SendError(facade::OpStatus::OUT_OF_RANGE);
  }

  size_t max_output = 10;
  if (!args.empty()) {
    auto option = args[0];
    if (absl::EqualsIgnoreCase(option, "RESET")) {
      pool_->AwaitFiberOnAll(
          [](auto index, auto* context) { ServerState::tlocal()->acl_log.Reset(); });
      rb->SendOk();
      return;
    }

    if (!absl::SimpleAtoi(option, &max_output)) {
      rb->SendError("Invalid count");
      return;
    }
  }

  std::vector<AclLog::LogType> logs(pool_->size());
  pool_->AwaitFiberOnAll([&logs, max_output](auto index, auto* context) {
    logs[index] = ServerState::tlocal()->acl_log.GetLog(max_output);
  });

  size_t total_entries = 0;
  for (auto& log : logs) {
    total_entries += log.size();
  }

  if (total_entries == 0) {
    rb->SendEmptyArray();
    return;
  }

  auto n_way_minimum = [](const auto& logs) {
    size_t id = 0;
    AclLog::LogEntry limit;
    const AclLog::LogEntry* max = &limit;
    for (size_t i = 0; i < logs.size(); ++i) {
      if (!logs[i].empty() && logs[i].front() < *max) {
        id = i;
        max = &logs[i].front();
      }
    }

    return id;
  };

  rb->StartArray(total_entries);

  for (size_t i = 0; i < total_entries; ++i) {
    const auto min = n_way_minimum(logs);
    SendAclSecurityEvents(logs[min].front(), rb);
    logs[min].pop_front();
  }
}

void AclFamily::Users(CmdArgParser parser, CommandContext* cmd_cntx) {
  const auto registry_with_lock = registry_->GetRegistryWithLock();
  const auto& registry = registry_with_lock.registry;
  auto* rb = static_cast<facade::RedisReplyBuilder*>(cmd_cntx->rb());

  rb->StartArray(registry.size());
  for (const auto& [username, _] : registry) {
    rb->SendSimpleString(username);
  }
}

void AclFamily::Cat(CmdArgParser parser, CommandContext* cmd_cntx) {
  auto* rb = static_cast<facade::RedisReplyBuilder*>(cmd_cntx->rb());

  if (parser.HasNext()) {
    string category = absl::AsciiStrToUpper(parser.Next());
    if (!parser.Finalize())
      return rb->SendError(parser.TakeError().MakeReply());

    const auto cid_mask = ParseCategoryName(category);
    if (!cid_mask) {
      auto error = absl::StrCat("Unknown category: ", category);
      rb->SendError(error);
      return;
    }

    std::vector<std::string> results;
    // TODO replace this with indexer
    auto cb = [cid_mask, &results](auto name, auto& cid) {
      if (*cid_mask & cid.acl_categories()) {
        std::string& rule = results.emplace_back(name);
        std::replace(rule.begin(), rule.end(), ' ', '|');  // the form rules use
      }
    };

    cmd_registry_->Traverse(cb);
    rb->StartArray(results.size());
    for (const auto& command : results) {
      rb->SendSimpleString(command);
    }

    return;
  }

  vector<string_view> categories;
  rng::copy_if(kCategoryNames, back_inserter(categories),
               [](string_view name) { return name != kReservedCategory; });
  rb->SendSimpleStrArr(categories);
}

void AclFamily::GetUser(CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view username = parser.Next();
  const auto reg = registry_->GetRegistryWithLock();
  auto* rb = static_cast<facade::RedisReplyBuilder*>(cmd_cntx->rb());
  auto it = reg.registry.find(username);
  if (it == reg.registry.end())
    return rb->SendNull();

  const User& user = it->second;
  rb->StartArray(10);

  rb->SendSimpleString("flags");
  vector<string_view> flags = {user.IsActive() ? "on" : "off"};
  if (user.HasNopass())
    flags.push_back("nopass");
  rb->SendSimpleStrArr(flags);

  rb->SendSimpleString("passwords");
  if (user.HasNopass() || user.Passwords().empty()) {
    rb->SendEmptyArray();
  } else {
    string passwords = PasswordsToString(user, false);
    passwords.pop_back();
    rb->SendSimpleString(passwords);
  }

  rb->SendSimpleString("commands");
  rb->SendSimpleString(CommandRulesToString(user, family_commands_));

  rb->SendSimpleString("keys");
  if (string keys = KeysToString(user.Keys()); keys.empty())
    rb->SendEmptyArray();
  else
    rb->SendSimpleString(keys);

  rb->SendSimpleString("channels");
  rb->SendSimpleString(ChannelsToString(user.PubSub()));
}

void AclFamily::GenPass(CmdArgParser parser, CommandContext* cmd_cntx) {
  auto* builder = cmd_cntx->rb();

  using GenPassBits = facade::FInt<uint32_t{1}, uint32_t{4096}>;
  uint32_t random_bits = parser.NextOrDefault<GenPassBits>(GenPassBits{256});
  if (!parser.Finalize()) {
    if (parser.TakeError().type == facade::CmdArgParser::UNPROCESSED)
      return builder->SendError(facade::UnknownSubCmd("GENPASS", "ACL"));
    return builder->SendError(
        "ACL GENPASS argument must be the number of bits for the output password, a positive "
        "number up to 4096");
  }

  std::random_device urandom("/dev/urandom");
  const size_t result_length = (random_bits + 3) / 4;
  constexpr size_t step_size = sizeof(decltype(std::random_device::max()));
  std::string response;
  for (size_t bytes_written = 0; bytes_written < result_length; bytes_written += step_size) {
    absl::StrAppendFormat(&response, "%08x", urandom());
  }

  response.resize(result_length);

  builder->SendSimpleString(response);
}

void AclFamily::DryRun(CmdArgParser parser, CommandContext* cmd_cntx) {
  auto* rb = static_cast<facade::RedisReplyBuilder*>(cmd_cntx->rb());
  auto username = parser.Next();
  const auto registry_with_lock = registry_->GetRegistryWithLock();
  const auto& registry = registry_with_lock.registry;
  if (!registry.contains(username)) {
    auto error = absl::StrCat("User '", username, "' not found");
    rb->SendError(error);
    return;
  }

  const facade::ParsedArgs simulated_cmd = parser.UnparsedArgs();
  // Use the same lookup as real dispatch (FindExtended), not a plain single-token Find: multi-word
  // commands like "ACL SETUSER" otherwise resolve to the bare family command ("ACL") and get
  // checked against the wrong, less restrictive ACL category.
  auto [cid, simulated_args] = cmd_registry_->FindExtended(simulated_cmd);
  if (!cid || cid->IsAlias()) {
    // Mirror the lookup key FindExtended actually used: for ACL sub-commands that's "ACL
    // SUBCMD", not just "ACL", otherwise a bad sub-command misleadingly reports "ACL" itself
    // as not found.
    std::string looked_up = absl::AsciiStrToUpper(simulated_cmd.Front());
    if (looked_up == "ACL" && simulated_cmd.size() > 1) {
      absl::StrAppend(&looked_up, " ", absl::AsciiStrToUpper(simulated_cmd[1]));
    }
    auto error = absl::StrCat("Command '", looked_up, "' not found");
    rb->SendError(error);
    return;
  }

  // The regular command path validates arity before checking ACLs. DRYRUN historically accepts a
  // command without its arguments, so skip key-pattern validation for an incomplete simulation.
  const auto& user = registry.find(username)->second;
  const AclKeys all_keys{{}, true};
  const bool incomplete = cid->Validate(simulated_args).has_value();
  const AclRulesView rules{user.AclCommandsRef(), incomplete ? all_keys : user.Keys(),
                           user.PubSub(), user.Db()};
  const size_t db_index = user.Db() == kAllDbs ? 0 : user.Db();
  if (!CheckCommand(rules, db_index, *cid, simulated_args)) {
    rb->SendOk();
    return;
  }

  auto msg = absl::StrCat("This user has no permissions to run the '",
                          cid->AclTarget(simulated_args).first, "' command");

  rb->SendBulkString(msg);
}

void AclFamily::Init(facade::Listener* main_listener) {
  main_listener_ = main_listener;
  // Like Redis: replaces all of default's passwords, and an empty value means nopass.
  config_registry.RegisterMutable("requirepass", [this](const absl::CommandLineFlag& flag) {
    User::UpdateRequest rqst;
    string pass = flag.CurrentValue();
    if (pass.empty()) {
      rqst.passwords.push_back({.password = {}, .nopass = true});
    } else {
      rqst.passwords.push_back({.password = {}, .reset_password = true});
      rqst.passwords.push_back({std::move(pass)});
    }
    registry_->MaybeAddAndUpdate("default", std::move(rqst));
    return true;
  });
  auto acl_file = absl::GetFlag(FLAGS_aclfile);
  if (!acl_file.empty()) {
    // A file that does not load must not start an open server.
    if (auto error = Load(); error) {
      LOG(ERROR) << "Could not load the acl file " << acl_file << ": " << error.Format();
      exit(1);
    }
    return;
  }
  registry_->Init();
}

namespace {

using MaterializedContents = std::variant<std::vector<std::vector<std::string_view>>, std::string>;

MaterializedContents MaterializeFileContents(std::vector<std::string>* usernames,
                                             std::string_view file_contents) {
  // This is fine, a very large file will top at 1-2 mb. And that's for 5000+ users with 400
  // characters per line
  std::vector<std::string_view> commands = absl::StrSplit(file_contents, "\n");
  std::vector<std::vector<std::string_view>> materialized;
  materialized.reserve(commands.size());
  usernames->reserve(commands.size());
  for (auto& command : commands) {
    // Only a CRLF's '\r' is dropped: ACL SAVE writes values verbatim, so other whitespace is data
    // and ' ' alone separates rules. A skipped line keeps an empty entry: the index is the line.
    absl::ConsumeSuffix(&command, "\r");
    if (absl::StripAsciiWhitespace(command).empty()) {
      materialized.emplace_back();
      continue;
    }
    std::vector<std::string_view> cmds = absl::StrSplit(command, ' ', absl::SkipEmpty());
    if (!absl::EqualsIgnoreCase(cmds[0], "USER") || cmds.size() < 3) {
      return absl::StrCat("line ", materialized.size() + 1, ": expected USER <name> <rule>...");
    }

    usernames->push_back(std::string(cmds[1]));
    cmds.erase(cmds.begin(), cmds.begin() + 2);
    materialized.push_back(cmds);
  }
  return materialized;
}

void SendAclSecurityEvents(const AclLog::LogEntry& entry, facade::RedisReplyBuilder* rb) {
  rb->StartArray(12);
  rb->SendSimpleString("reason");
  using Reason = AclLog::Reason;
  std::string reason;
  if (entry.reason == Reason::COMMAND) {
    reason = "COMMAND";
  } else if (entry.reason == Reason::KEY) {
    reason = "KEY";
  } else if (entry.reason == Reason::PUB_SUB) {
    reason = "PUB_SUB";
  } else {
    reason = "AUTH";
  }

  rb->SendSimpleString(reason);
  rb->SendSimpleString("object");
  rb->SendSimpleString(entry.object);
  rb->SendSimpleString("username");
  rb->SendSimpleString(entry.username);
  rb->SendSimpleString("age-seconds");

  auto now_diff = std::chrono::system_clock::now() - entry.entry_creation;
  auto secs = std::chrono::duration_cast<std::chrono::seconds>(now_diff);
  auto left_over = now_diff - std::chrono::duration_cast<std::chrono::microseconds>(secs);
  auto age = absl::StrCat(secs.count(), ".", left_over.count());
  rb->SendSimpleString(absl::StrCat(age));
  rb->SendSimpleString("client-info");
  rb->SendSimpleString(entry.client_info);
  rb->SendSimpleString("timestamp-created");
  rb->SendLong(entry.entry_creation.time_since_epoch().count());
}

// Fetches the connections that predicate P evaluates to true and shuts them
// down gracefully.
template <typename P>
void TraverseEvictImpl(P predicate, facade::Listener* main_listener, util::ProactorPool* pool) {
  auto close_cb = [&](unsigned idx, util::ProactorBase* p) {
    std::vector<facade::Connection::WeakRef> connections;
    auto traverse_cb = [&](unsigned id, util::Connection* conn) {
      auto connection = static_cast<facade::Connection*>(conn);
      auto ctx = connection->cntx();
      if (predicate(ctx)) {
        connections.push_back(connection->Borrow());
      }
    };

    main_listener->TraverseConnectionsOnThread(traverse_cb, UINT32_MAX, nullptr);

    for (auto& tcon : connections) {
      facade::Connection* conn = tcon.Get();
      if (conn && conn->socket()->proactor()->GetPoolIndex() == p->GetPoolIndex()) {
        // preemptive for TlsSocket
        conn->ShutdownSelfBlocking();
      }
    }
  };

  pool->AwaitFiberOnAll(close_cb);
}

}  // namespace

variant<User::UpdateRequest, ErrorReply> AclFamily::ParseAclSetUser(facade::ParsedArgs args,
                                                                    bool has_all_keys) const {
  User::UpdateRequest req;

  for (string_view arg : args) {
    if (auto pass = ParsePasswordRule(arg); pass) {
      // Not a full SHA-256: it can never match and stays out of the error, it may be a plaintext.
      if (pass->is_hashed &&
          (pass->password.size() != 64 || !rng::all_of(pass->password, absl::ascii_isxdigit))) {
        return ErrorReply(
            "Error in ACL SETUSER modifier: The password hash must be exactly 64 hexadecimal "
            "characters");
      }
      req.passwords.push_back(std::move(*pass));
      continue;
    }

    if (auto key = ParseKeyRule(arg); key) {
      const bool is_pattern = !key->all_keys && !key->reset_keys;
      if (is_pattern && (has_all_keys || req.allow_all_keys)) {
        // The pattern may be a password given without '>'.
        return ErrorReply(
            "Error in ACL SETUSER modifier: Adding a pattern after the * pattern (or the "
            "'allkeys' flag) is not valid and does not have any effect. Try 'resetkeys' to start "
            "with an empty list of patterns");
      }

      req.allow_all_keys = key->all_keys;
      req.reset_all_keys = key->reset_keys;
      if (key->reset_keys)
        has_all_keys = false;
      req.keys.push_back(std::move(*key));
      continue;
    }

    if (auto channel = ParseChannelRule(arg); channel) {
      const bool is_pattern = !channel->all_channels && !channel->reset_channels;
      if (is_pattern && req.all_channels) {
        // The pattern may be a password given without '>'.
        return ErrorReply(
            "ERR Error in ACL SETUSER modifier: Adding a pattern after the * pattern (or the "
            "'allchannels' flag) is not valid and does not have any effect. Try 'resetchannels' to "
            "start with an empty list of channels");
      }

      req.all_channels = channel->all_channels;
      req.reset_channels = channel->reset_channels;
      req.pub_sub.push_back(std::move(*channel));
      continue;
    }

    if (auto db = ParseDbRule(arg, dbnum_); db) {
      if (req.select_db)
        return ErrorReply("ERR Error, select db $ was used twice");

      // AUTH switches the connection to this db, bypassing the SELECT restriction.
      if (IsClusterEnabled() && *db != 0 && *db != kAllDbs)
        return ErrorReply("ERR Error, select db $ is not allowed in cluster mode");
      req.select_db = db;
      continue;
    }

    string rule = absl::AsciiStrToUpper(arg);
    if (auto status = ParseStatusRule(rule); status) {
      if (req.is_active)
        return ErrorReply("Multiple ON/OFF are not allowed");
      req.is_active = *status;
      continue;
    }

    if (auto category = ParseCategoryRule(rule); category) {
      req.updates.push_back(CategoryRule(category->first, category->second, category_commands_));
      continue;
    }

    if (string_view ns = rule; absl::ConsumePrefix(&ns, "NAMESPACE:")) {
      req.ns = string(ns);
      continue;
    }

    auto command = ParseCommandRule(rule, *cmd_registry_);
    if (!command) {
      // The token may be a password given without '>'.
      return ErrorReply("Unrecognized parameter");
    }
    req.updates.push_back(*command);
  }

  return req;
}

void AclFamily::BuildIndexers(RevCommandsIndexStore families) {
  const size_t family_count = acl::NumberOfFamilies(families.size());
  family_commands_ = std::move(families);
  for (auto& commands : category_commands_)
    commands.assign(family_count, 0);

  cmd_registry_->Traverse([&](std::string_view, const CommandId& cid) {
    const size_t family = cid.GetFamily();
    DCHECK_LT(family, family_count);
    for (uint32_t categories = cid.acl_categories(); categories != 0;
         categories &= categories - 1) {
      category_commands_[absl::countr_zero(categories)][family] |= cid.GetBitIndex();
    }
  });
}

void AclFamily::Help(CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view help_arr[] = {
      "ACL <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
      "CAT [<category>]",
      "    List all commands that belong to <category>, or all command categories",
      "    when no category is specified.",
      "DELUSER <username> [<username> ...]",
      "    Delete a list of users.",
      "DRYRUN <username> <command> [<arg> ...]",
      "    Returns whether the user can execute the given command without executing the command.",
      "GETUSER <username>",
      "    Get the user's details.",
      "GENPASS [<bits>]",
      "    Generate a secure 256-bit user password. The optional `bits` argument can",
      "    be used to specify a different size.",
      "LIST",
      "    Show users details in config file format.",
      "LOAD",
      "    Reload users from the ACL file.",
      "LOG [<count> | RESET]",
      "    Show the ACL log entries.",
      "SAVE",
      "    Save the current config to the ACL file.",
      "SETUSER <username> <attribute> [<attribute> ...]",
      "    Create or modify a user with the specified attributes.",
      "USERS",
      "    List all the registered usernames.",
      "WHOAMI",
      "    Return the current connection username.",
      "HELP",
      "    Print this help."};
  auto* rb = static_cast<facade::RedisReplyBuilder*>(cmd_cntx->rb());
  return rb->SendSimpleStrArr(help_arr);
}

using MemberFunc = void (AclFamily::*)(CmdArgParser parser, CommandContext* cmd_cntx);

CommandId::Handler HandlerFunc(AclFamily* acl, MemberFunc f) {
  return [=](CmdArgParser parser, CommandContext* cmd_cntx) {
    return (acl->*f)(std::move(parser), cmd_cntx);
  };
}

#define HFUNC(x) SetHandler(HandlerFunc(this, &AclFamily::x))

constexpr uint32_t kAcl = acl::SLOW;
constexpr uint32_t kList = acl::ADMIN | acl::SLOW | acl::DANGEROUS;
constexpr uint32_t kSetUser = acl::ADMIN | acl::SLOW | acl::DANGEROUS;
constexpr uint32_t kDelUser = acl::ADMIN | acl::SLOW | acl::DANGEROUS;
constexpr uint32_t kWhoAmI = acl::SLOW;
constexpr uint32_t kSave = acl::ADMIN | acl::SLOW | acl::DANGEROUS;
constexpr uint32_t kLoad = acl::ADMIN | acl::SLOW | acl::DANGEROUS;
constexpr uint32_t kLog = acl::ADMIN | acl::SLOW | acl::DANGEROUS;
constexpr uint32_t kUsers = acl::ADMIN | acl::SLOW | acl::DANGEROUS;
constexpr uint32_t kCat = acl::SLOW;
constexpr uint32_t kGetUser = acl::ADMIN | acl::SLOW | acl::DANGEROUS;
constexpr uint32_t kDryRun = acl::ADMIN | acl::SLOW | acl::DANGEROUS;
constexpr uint32_t kGenPass = acl::SLOW;
constexpr uint32_t kHelp = acl::SLOW;

// We can't implement the ACL commands and its respective subcommands LIST, CAT, etc
// the usual way, (that is, one command called ACL which then dispatches to the subcommand
// based on the second argument) because each of the subcommands has different ACL
// categories. Therefore, to keep it compatible with the CommandId, I need to treat them
// as separate commands in the registry. This is the least intrusive change because it's very
// easy to handle that case explicitly in `DispatchCommand`.

void AclFamily::Register(dfly::CommandRegistry* registry) {
  using CI = dfly::CommandId;
  const uint32_t kAclMask = CO::ADMIN | CO::NOSCRIPT | CO::LOADING;
  registry->StartFamily();
  *registry << CI{"ACL", CO::NOSCRIPT | CO::LOADING, 0, 0, 0, acl::kAcl}.HFUNC(Acl);
  *registry << CI{"ACL LIST", kAclMask, 1, 0, 0, acl::kList}.HFUNC(List);
  *registry << CI{"ACL SETUSER", kAclMask, -2, 0, 0, acl::kSetUser}.HFUNC(SetUser);
  *registry << CI{"ACL DELUSER", kAclMask, -2, 0, 0, acl::kDelUser}.HFUNC(DelUser);
  *registry << CI{"ACL WHOAMI", CO::NOSCRIPT | CO::LOADING, 1, 0, 0, acl::kWhoAmI}.HFUNC(WhoAmI);
  *registry << CI{"ACL SAVE", kAclMask, 1, 0, 0, acl::kSave}.HFUNC(Save);
  *registry << CI{"ACL LOAD", kAclMask, 1, 0, 0, acl::kLoad}.HFUNC(Load);
  *registry << CI{"ACL LOG", kAclMask, 0, 0, 0, acl::kLog}.HFUNC(Log);
  *registry << CI{"ACL USERS", kAclMask, 1, 0, 0, acl::kUsers}.HFUNC(Users);
  *registry << CI{"ACL CAT", CO::NOSCRIPT | CO::LOADING, -1, 0, 0, acl::kCat}.HFUNC(Cat);
  *registry << CI{"ACL GETUSER", kAclMask, 2, 0, 0, acl::kGetUser}.HFUNC(GetUser);
  *registry << CI{"ACL DRYRUN", kAclMask, -3, 0, 0, acl::kDryRun}.HFUNC(DryRun);
  *registry << CI{"ACL GENPASS", CO::NOSCRIPT | CO::LOADING, -1, 0, 0, acl::kGenPass}.HFUNC(
      GenPass);
  *registry << CI{"ACL HELP", CO::NOSCRIPT | CO::LOADING, 0, 0, 0, acl::kHelp}.HFUNC(Help);
  cmd_registry_ = registry;

  // build indexers
  BuildIndexers(cmd_registry_->GetFamilies());
}

#undef HFUNC

}  // namespace dfly::acl
