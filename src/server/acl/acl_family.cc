// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/acl/acl_family.h"

#include <fcntl.h>
#include <libgen.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <iterator>
#include <memory>
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
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "absl/strings/strip.h"
#include "base/flags.h"
#include "base/logging.h"
#include "facade/dragonfly_connection.h"
#include "facade/dragonfly_listener.h"
#include "facade/error.h"
#include "facade/reply_builder.h"
#include "io/file_util.h"
#include "server/acl/acl_log.h"
#include "server/acl/validator.h"
#include "server/cluster_support.h"
#include "server/config_registry.h"
#include "server/conn_context.h"
#include "server/server_state.h"
#include "util/proactor_pool.h"

using namespace std;

ABSL_FLAG(string, aclfile, "", "Path and name to aclfile");
ABSL_DECLARE_FLAG(uint32_t, dbnum);
ABSL_DECLARE_FLAG(string, requirepass);

namespace dfly::acl {

namespace {

namespace rng = std::ranges;

using facade::ErrorReply;
using facade::RedisReplyBuilder;
using Sign = User::Sign;

constexpr string_view kNoAclFileErr = "Dragonfly is not configured to use an ACL file.";

// A renamed file is durable only once its directory entry is.
bool SyncDir(string file_path) {
  const int fd = open(dirname(file_path.data()), O_RDONLY | O_DIRECTORY);
  const bool synced = fd != -1 && fsync(fd) == 0;
  if (fd != -1) {
    close(fd);
  }
  return synced;
}

RedisReplyBuilder* GetRedisReplyBuilder(CommandContext* cmd_cntx) {
  return static_cast<RedisReplyBuilder*>(cmd_cntx->rb());
}

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

string_view ReasonName(AclLog::Reason reason) {
  switch (reason) {
    case AclLog::Reason::COMMAND:
      return "COMMAND";
    case AclLog::Reason::KEY:
      return "KEY";
    case AclLog::Reason::PUB_SUB:
      return "PUB_SUB";
    case AclLog::Reason::AUTH:
      return "AUTH";
  }
  return "AUTH";
}

void SendLogEntry(const AclLog::LogEntry& entry, RedisReplyBuilder* rb) {
  auto now_diff = chrono::system_clock::now() - entry.entry_creation;
  auto secs = chrono::duration_cast<chrono::seconds>(now_diff);
  auto left_over = now_diff - chrono::duration_cast<chrono::microseconds>(secs);

  rb->StartArray(12);
  rb->SendSimpleString("reason");
  rb->SendSimpleString(ReasonName(entry.reason));
  rb->SendSimpleString("object");
  rb->SendSimpleString(entry.object);
  rb->SendSimpleString("username");
  rb->SendSimpleString(entry.username);
  rb->SendSimpleString("age-seconds");
  rb->SendSimpleString(absl::StrCat(secs.count(), ".", left_over.count()));
  rb->SendSimpleString("client-info");
  rb->SendSimpleString(entry.client_info);
  rb->SendSimpleString("timestamp-created");
  rb->SendLong(entry.entry_creation.time_since_epoch().count());
}

}  // namespace

AclFamily::AclFamily(UserRegistry* registry, util::ProactorPool* pool)
    : registry_(registry), pool_(pool), dbnum_(absl::GetFlag(FLAGS_dbnum)) {
}

void AclFamily::Acl(CmdArgParser parser, CommandContext* cmd_cntx) {
  cmd_cntx->SendError("Wrong number of arguments for acl command");
}

void AclFamily::List(CmdArgParser parser, CommandContext* cmd_cntx) {
  const auto reg = registry_->GetRegistryWithLock();
  auto* rb = GetRedisReplyBuilder(cmd_cntx);
  rb->StartArray(reg.registry.size());
  for (const auto& [username, user] : reg.registry)
    rb->SendSimpleString(UserToString(username, user, false, family_commands_));
}

void AclFamily::StreamUpdatesToConnections(string_view username, const User& user) {
  if (!main_listener_ || main_listener_->protocol() != facade::Protocol::REDIS)
    return;

  main_listener_->TraverseConnections([&](unsigned, util::Connection* conn) {
    auto* connection = static_cast<facade::Connection*>(conn);
    if (connection->IsHttp() || !connection->cntx())
      return;

    auto* cntx = static_cast<ConnectionContext*>(connection->cntx());
    if (cntx->authed_username == username) {
      cntx->SetAclCredentials({.acl_commands = user.AclCommands(),
                               .keys = user.Keys(),
                               .pub_sub = user.PubSub(),
                               .ns = {},
                               .db = user.Db()});
    }
  });
}

void AclFamily::EvictConnections(absl::FunctionRef<bool(string_view username)> pred) {
  if (!main_listener_)
    return;

  pool_->AwaitFiberOnAll([&](unsigned, util::ProactorBase* pb) {
    vector<facade::Connection::WeakRef> connections;
    auto collect = [&](unsigned, util::Connection* conn) {
      auto* connection = static_cast<facade::Connection*>(conn);
      auto* cntx = static_cast<ConnectionContext*>(connection->cntx());
      if (cntx && pred(cntx->authed_username))
        connections.push_back(connection->Borrow());
    };
    main_listener_->TraverseConnectionsOnThread(collect, UINT32_MAX, nullptr);

    for (auto& weak_conn : connections) {
      facade::Connection* conn = weak_conn.Get();

      // Preemptive for TlsSocket
      if (conn && conn->socket()->proactor()->GetPoolIndex() == pb->GetPoolIndex())
        conn->ShutdownSelfBlocking();
    }
  });
}

User& AclFamily::UpsertUser(string_view username, User::UpdateRequest req,
                            UserRegistry::RegistryType* registry) {
  auto [it, is_new] = registry->try_emplace(username);
  User& user = it->second;
  const User::MemoryUsage before = is_new ? User::MemoryUsage{} : user.GetMemoryUsage();

  // New users start without permissions, which ACL LIST shows as a leading -@all.
  if (is_new)
    req.updates.insert(req.updates.begin(), CategoryRule(Sign::MINUS, ALL, category_commands_));

  user.Update(std::move(req));
  registry_->TrackUser(before, user.GetMemoryUsage(), is_new);
  return user;
}

void AclFamily::SetUser(CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view username = parser.Next();
  auto reg = registry_->GetRegistryWithWriteLock();
  auto it = reg.registry.find(username);
  const bool exists = it != reg.registry.end();
  auto req = ParseAclSetUser(parser.RemainingRange(), exists && it->second.Keys().all_keys);
  if (const auto* error = get_if<ErrorReply>(&req))
    return cmd_cntx->SendError(*error);

  auto& update = get<User::UpdateRequest>(req);
  const bool reset_channels = update.reset_channels;
  const User& user = UpsertUser(username, std::move(update), &reg.registry);

  // Reply first because the connection might get evicted.
  cmd_cntx->SendOk();
  if (!exists)
    return;

  if (reset_channels)
    EvictConnections([username](string_view name) { return name == username; });
  else
    StreamUpdatesToConnections(username, user);
}

void AclFamily::DelUser(CmdArgParser parser, CommandContext* cmd_cntx) {
  absl::flat_hash_set<string_view> users;
  for (string_view username : parser.RemainingRange()) {
    if (username != "default" && registry_->RemoveUser(username))
      users.insert(username);
  }

  if (!users.empty()) {
    VLOG(1) << "Evicting open acl connections";
    EvictConnections([&users](string_view name) { return users.contains(name); });
    VLOG(1) << "Done evicting open acl connections";
  }
  cmd_cntx->SendLong(users.size());
}

void AclFamily::WhoAmI(CmdArgParser parser, CommandContext* cmd_cntx) {
  GetRedisReplyBuilder(cmd_cntx)->SendBulkString(
      absl::StrCat("User is ", cmd_cntx->server_conn_cntx()->authed_username));
}

void AclFamily::Save(CmdArgParser parser, CommandContext* cmd_cntx) {
  const string acl_file = absl::GetFlag(FLAGS_aclfile);
  if (acl_file.empty())
    return cmd_cntx->SendError(kNoAclFileErr);

  string contents;
  {
    const auto reg = registry_->GetRegistryWithLock();
    for (const auto& [username, user] : reg.registry)
      absl::StrAppend(&contents, UserToString(username, user, true, family_commands_), "\n");
  }

  // Written next to the aclfile and renamed over it, so a reader never sees a partial file.
  string tmp = absl::StrCat(acl_file, ".tmpXXXXXX");
  int fd = mkstemp(tmp.data());
  auto fail = [&](string_view what) {
    error_code ec(errno, system_category());
    string error = absl::StrCat("Failed to ", what, ": ", ec.message());
    if (fd != -1) {
      close(fd);
      unlink(tmp.c_str());
    }
    LOG(ERROR) << error;
    cmd_cntx->SendError(error);
  };
  if (fd == -1)
    return fail("open the aclfile");

  ssize_t n = 1;
  for (size_t off = 0; n > 0 && off < contents.size(); off += n)
    n = write(fd, contents.data() + off, contents.size() - off);
  if (n <= 0 || fsync(fd) == -1)
    return fail("write to the aclfile");
  if (rename(tmp.c_str(), acl_file.c_str()) == -1)
    return fail("rename the aclfile");

  close(fd);
  fd = -1;
  if (!SyncDir(acl_file))
    return fail("sync the aclfile directory");

  cmd_cntx->SendOk();
}

GenericError AclFamily::ParseAclFile(string_view path, UserRequests* requests) const {
  auto contents = io::ReadFileToString(path);
  if (!contents) {
    string error = absl::StrCat("Dragonfly could not load ACL file ", path, " with error ",
                                contents.error().message());
    LOG(WARNING) << error;
    return GenericError{std::move(error)};
  }

  vector<string_view> lines = absl::StrSplit(*contents, '\n');
  for (size_t i = 0; i < lines.size(); ++i) {
    // Only a CRLF's '\r' is dropped: ACL SAVE writes values verbatim, so other whitespace is data
    // and ' ' alone separates rules.
    string_view line = lines[i];
    absl::ConsumeSuffix(&line, "\r");
    if (absl::StripAsciiWhitespace(line).empty())
      continue;

    vector<string_view> tokens = absl::StrSplit(line, ' ', absl::SkipEmpty());
    if (tokens.size() < 3 || !absl::EqualsIgnoreCase(tokens[0], "USER")) {
      string error = absl::StrCat("line ", i + 1, ": expected USER <name> <rule>...");
      LOG(WARNING) << error;
      return GenericError{std::move(error)};
    }

    auto req = ParseAclSetUser(facade::ArgSlice{tokens}.subspan(2), false);
    if (const auto* reply = get_if<ErrorReply>(&req)) {
      string error = absl::StrCat("line ", i + 1, ": ", reply->ToSv());
      LOG(WARNING) << "Error while parsing aclfile: " << error;
      return GenericError{std::move(error)};
    }
    requests->emplace_back(string(tokens[1]), std::move(get<User::UpdateRequest>(req)));
  }
  return {};
}

void AclFamily::AddUsers(UserRequests requests, UserRegistry::RegistryType* registry) {
  for (auto& [username, req] : requests) {
    // Each record starts without commands, also when the file repeats a username.
    if (registry->contains(username))
      req.updates.insert(req.updates.begin(), CategoryRule(Sign::MINUS, ALL, category_commands_));
    UpsertUser(username, std::move(req), registry);
  }

  if (!registry->contains("default")) {
    // --requirepass applies when the file does not define default.
    auto req = registry_->DefaultUserUpdateRequest();
    if (auto pass = absl::GetFlag(FLAGS_requirepass); !pass.empty())
      req.passwords.front() = {std::move(pass)};

    auto& user = (*registry)["default"];
    user.Update(std::move(req));
    registry_->TrackUser({}, user.GetMemoryUsage(), /*is_new=*/true);
  }
}

void AclFamily::Load(CmdArgParser parser, CommandContext* cmd_cntx) {
  const string acl_file = absl::GetFlag(FLAGS_aclfile);
  if (acl_file.empty())
    return cmd_cntx->SendError(kNoAclFileErr);

  UserRequests requests;
  if (auto error = ParseAclFile(acl_file, &requests); error)
    return cmd_cntx->SendError(absl::StrCat("Error loading: ", acl_file, " ", error.Format()));

  auto reg = registry_->GetRegistryWithWriteLock();

  // Reply first because the connection might get evicted.
  cmd_cntx->SendOk();
  EvictConnections(
      [&reg](string_view name) { return name != "default" && reg.registry.contains(name); });
  reg.registry.clear();
  registry_->ResetStats();
  AddUsers(std::move(requests), &reg.registry);
}

void AclFamily::Log(CmdArgParser parser, CommandContext* cmd_cntx) {
  auto args = parser.UnparsedArgs();
  if (args.size() > 1)
    return cmd_cntx->SendError(facade::OpStatus::OUT_OF_RANGE);

  size_t max_output = 10;
  if (!args.empty()) {
    if (absl::EqualsIgnoreCase(args[0], "RESET")) {
      pool_->AwaitFiberOnAll(
          [](unsigned, util::ProactorBase*) { ServerState::tlocal()->acl_log.Reset(); });
      return cmd_cntx->SendOk();
    }

    if (!absl::SimpleAtoi(args[0], &max_output))
      return cmd_cntx->SendError("Invalid count");
  }

  vector<AclLog::LogType> logs(pool_->size());
  pool_->AwaitFiberOnAll([&logs, max_output](unsigned index, util::ProactorBase*) {
    logs[index] = ServerState::tlocal()->acl_log.GetLog(max_output);
  });

  vector<AclLog::LogEntry> entries;
  for (auto& log : logs)
    rng::move(log, back_inserter(entries));

  // Newest first, like the log of each thread.
  rng::stable_sort(entries, [](const auto& l, const auto& r) { return r < l; });

  auto* rb = GetRedisReplyBuilder(cmd_cntx);
  rb->StartArray(entries.size());
  for (const auto& entry : entries)
    SendLogEntry(entry, rb);
}

void AclFamily::Users(CmdArgParser parser, CommandContext* cmd_cntx) {
  const auto reg = registry_->GetRegistryWithLock();
  auto* rb = GetRedisReplyBuilder(cmd_cntx);
  rb->StartArray(reg.registry.size());
  for (const auto& [username, _] : reg.registry)
    rb->SendSimpleString(username);
}

void AclFamily::Cat(CmdArgParser parser, CommandContext* cmd_cntx) {
  auto* rb = GetRedisReplyBuilder(cmd_cntx);
  if (!parser.HasNext()) {
    vector<string_view> categories;
    rng::copy_if(kCategoryNames, back_inserter(categories),
                 [](string_view name) { return name != kReservedCategory; });
    return rb->SendSimpleStrArr(categories);
  }

  const string category = absl::AsciiStrToUpper(parser.Next());
  if (!parser.Finalize())
    return rb->SendError(parser.TakeError().MakeReply());

  const auto mask = ParseCategoryName(category);
  if (!mask)
    return rb->SendError(absl::StrCat("Unknown category: ", category));

  vector<string> commands;
  cmd_registry_->Traverse([&](string_view name, const CommandId& cid) {
    if (cid.acl_categories() & *mask) {
      string& rule = commands.emplace_back(name);
      rng::replace(rule, ' ', '|');  // The form rules use.
    }
  });
  rb->SendSimpleStrArr(commands);
}

void AclFamily::GetUser(CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view username = parser.Next();
  const auto reg = registry_->GetRegistryWithLock();
  auto* rb = GetRedisReplyBuilder(cmd_cntx);
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
  using GenPassBits = facade::FInt<uint32_t{1}, uint32_t{4096}>;
  uint32_t random_bits = parser.NextOrDefault<GenPassBits>(GenPassBits{256});
  if (!parser.Finalize()) {
    if (parser.TakeError().type == facade::CmdArgParser::UNPROCESSED)
      return cmd_cntx->SendError(facade::UnknownSubCmd("GENPASS", "ACL"));
    return cmd_cntx->SendError(
        "ACL GENPASS argument must be the number of bits for the output password, a positive "
        "number up to 4096");
  }

  random_device urandom("/dev/urandom");
  const size_t result_length = (random_bits + 3) / 4;
  constexpr size_t step_size = sizeof(decltype(random_device::max()));
  string response;
  for (size_t bytes_written = 0; bytes_written < result_length; bytes_written += step_size) {
    absl::StrAppendFormat(&response, "%08x", urandom());
  }

  response.resize(result_length);
  cmd_cntx->SendSimpleString(response);
}

void AclFamily::DryRun(CmdArgParser parser, CommandContext* cmd_cntx) {
  auto* rb = GetRedisReplyBuilder(cmd_cntx);
  string_view username = parser.Next();
  const auto reg = registry_->GetRegistryWithLock();
  auto it = reg.registry.find(username);
  if (it == reg.registry.end())
    return rb->SendError(absl::StrCat("User '", username, "' not found"));

  const facade::ParsedArgs simulated_cmd = parser.UnparsedArgs();

  // Use the same lookup as real dispatch (FindExtended), not a plain single-token Find: multi-word
  // commands like "ACL SETUSER" otherwise resolve to the bare family command ("ACL") and get
  // checked against the wrong, less restrictive ACL category.
  auto [cid, simulated_args] = cmd_registry_->FindExtended(simulated_cmd);
  if (!cid || cid->IsAlias()) {
    // Mirror the lookup key FindExtended actually used: for ACL sub-commands that's "ACL
    // SUBCMD", not just "ACL", otherwise a bad sub-command misleadingly reports "ACL" itself
    // as not found.
    string looked_up = absl::AsciiStrToUpper(simulated_cmd.Front());
    if (looked_up == "ACL" && simulated_cmd.size() > 1)
      absl::StrAppend(&looked_up, " ", absl::AsciiStrToUpper(simulated_cmd[1]));
    return rb->SendError(absl::StrCat("Command '", looked_up, "' not found"));
  }

  // The regular command path validates arity before checking ACLs. DRYRUN historically accepts a
  // command without its arguments, so skip key-pattern validation for an incomplete simulation.
  const User& user = it->second;
  const AclKeys all_keys{{}, true};
  const bool incomplete = cid->Validate(simulated_args).has_value();
  const AclRulesView rules{user.AclCommandsRef(), incomplete ? all_keys : user.Keys(),
                           user.PubSub(), user.Db()};
  const size_t db_index = user.Db() == kAllDbs ? 0 : user.Db();
  if (!CheckCommand(rules, db_index, *cid, simulated_args))
    return rb->SendOk();

  rb->SendBulkString(absl::StrCat("This user has no permissions to run the '",
                                  cid->AclTarget(simulated_args).first, "' command"));
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

  const string acl_file = absl::GetFlag(FLAGS_aclfile);
  if (acl_file.empty()) {
    registry_->Init();
    return;
  }

  // A file that does not load must not start an open server.
  UserRequests requests;
  if (auto error = ParseAclFile(acl_file, &requests); error) {
    LOG(ERROR) << "Could not load the acl file " << acl_file << ": " << error.Format();
    exit(1);
  }

  auto reg = registry_->GetRegistryWithWriteLock();
  AddUsers(std::move(requests), &reg.registry);
}

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
  const size_t family_count = NumberOfFamilies(families.size());
  family_commands_ = std::move(families);
  for (auto& commands : category_commands_)
    commands.assign(family_count, 0);

  cmd_registry_->Traverse([&](string_view, const CommandId& cid) {
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
  GetRedisReplyBuilder(cmd_cntx)->SendSimpleStrArr(help_arr);
}

using MemberFunc = void (AclFamily::*)(CmdArgParser parser, CommandContext* cmd_cntx);

CommandId::Handler HandlerFunc(AclFamily* acl, MemberFunc f) {
  return [=](CmdArgParser parser, CommandContext* cmd_cntx) {
    return (acl->*f)(std::move(parser), cmd_cntx);
  };
}

#define HFUNC(x) SetHandler(HandlerFunc(this, &AclFamily::x))

// At namespace scope, so tools/verify_script_acl can resolve acl::kAdminCats.
constexpr uint32_t kOpts = CO::NOSCRIPT | CO::LOADING;
constexpr uint32_t kAdminOpts = CO::ADMIN | kOpts;
constexpr uint32_t kAdminCats = acl::ADMIN | acl::SLOW | acl::DANGEROUS;

// We can't implement the ACL commands and its respective subcommands LIST, CAT, etc
// the usual way, (that is, one command called ACL which then dispatches to the subcommand
// based on the second argument) because each of the subcommands has different ACL
// categories. Therefore, to keep it compatible with the CommandId, I need to treat them
// as separate commands in the registry. This is the least intrusive change because it's very
// easy to handle that case explicitly in `DispatchCommand`.

void AclFamily::Register(CommandRegistry* registry) {
  using CI = CommandId;
  registry->StartFamily();
  *registry << CI{"ACL", kOpts, 0, 0, 0, acl::SLOW}.HFUNC(Acl);
  *registry << CI{"ACL LIST", kAdminOpts, 1, 0, 0, acl::kAdminCats}.HFUNC(List);
  *registry << CI{"ACL SETUSER", kAdminOpts, -2, 0, 0, acl::kAdminCats}.HFUNC(SetUser);
  *registry << CI{"ACL DELUSER", kAdminOpts, -2, 0, 0, acl::kAdminCats}.HFUNC(DelUser);
  *registry << CI{"ACL WHOAMI", kOpts, 1, 0, 0, acl::SLOW}.HFUNC(WhoAmI);
  *registry << CI{"ACL SAVE", kAdminOpts, 1, 0, 0, acl::kAdminCats}.HFUNC(Save);
  *registry << CI{"ACL LOAD", kAdminOpts, 1, 0, 0, acl::kAdminCats}.HFUNC(Load);
  *registry << CI{"ACL LOG", kAdminOpts, 0, 0, 0, acl::kAdminCats}.HFUNC(Log);
  *registry << CI{"ACL USERS", kAdminOpts, 1, 0, 0, acl::kAdminCats}.HFUNC(Users);
  *registry << CI{"ACL CAT", kOpts, -1, 0, 0, acl::SLOW}.HFUNC(Cat);
  *registry << CI{"ACL GETUSER", kAdminOpts, 2, 0, 0, acl::kAdminCats}.HFUNC(GetUser);
  *registry << CI{"ACL DRYRUN", kAdminOpts, -3, 0, 0, acl::kAdminCats}.HFUNC(DryRun);
  *registry << CI{"ACL GENPASS", kOpts, -1, 0, 0, acl::SLOW}.HFUNC(GenPass);
  *registry << CI{"ACL HELP", kOpts, 0, 0, 0, acl::SLOW}.HFUNC(Help);
  cmd_registry_ = registry;

  BuildIndexers(cmd_registry_->GetFamilies());
}

#undef HFUNC

}  // namespace dfly::acl
