// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/acl/acl_family.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <deque>
#include <memory>
#include <numeric>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <utility>
#include <variant>

#include "absl/container/flat_hash_set.h"
#include "absl/flags/commandlineflag.h"
#include "absl/strings/escaping.h"
#include "absl/strings/match.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "base/flags.h"
#include "base/logging.h"
#include "core/overloaded.h"
#include "facade/dragonfly_connection.h"
#include "facade/dragonfly_listener.h"
#include "facade/facade_types.h"
#include "io/file.h"
#include "io/file_util.h"
#include "server/acl/acl_commands_def.h"
#include "server/acl/acl_log.h"
#include "server/acl/validator.h"
#include "server/command_registry.h"
#include "server/common.h"
#include "server/config_registry.h"
#include "server/conn_context.h"
#include "server/error.h"
#include "server/server_state.h"
#include "util/proactor_pool.h"

using namespace std;

ABSL_FLAG(string, aclfile, "", "Path and name to aclfile");

namespace dfly::acl {

namespace {

string PasswordsToString(const absl::flat_hash_set<string>& passwords, bool nopass, bool full_sha);
using MaterializedContents = optional<vector<vector<string_view>>>;

MaterializedContents MaterializeFileContents(vector<string>* usernames, string_view file_contents);

string AclKeysToString(const AclKeys& keys);

string AclPubSubToString(const AclPubSub& pub_sub);
}  // namespace

AclFamily::AclFamily(UserRegistry* registry, util::ProactorPool* pool)
    : registry_(registry), pool_(pool) {
}

void AclFamily::Acl(CmdArgList args, const CommandContext& cmd_cntx) {
  cmd_cntx.rb->SendError("Wrong number of arguments for acl command");
}

void AclFamily::List(CmdArgList args, const CommandContext& cmd_cntx) {
  const auto registry_with_lock = registry_->GetRegistryWithLock();
  const auto& registry = registry_with_lock.registry;
  auto* rb = static_cast<facade::RedisReplyBuilder*>(cmd_cntx.rb);
  rb->StartArray(registry.size());

  for (const auto& [username, user] : registry) {
    string buffer = "user ";
    const string password = PasswordsToString(user.Passwords(), user.HasNopass(), false);

    const string acl_keys = AclKeysToString(user.Keys());

    const string acl_pub_sub = AclPubSubToString(user.PubSub());

    const string maybe_space_com = acl_keys.empty() ? "" : " ";

    const string acl_cat_and_commands =
        AclCatAndCommandToString(user.CatChanges(), user.CmdChanges());

    using namespace string_view_literals;

    absl::StrAppend(&buffer, username, " ", user.IsActive() ? "on "sv : "off "sv, password,
                    acl_keys, maybe_space_com, acl_pub_sub, " ", acl_cat_and_commands);

    rb->SendSimpleString(buffer);
  }
}

void AclFamily::StreamUpdatesToAllProactorConnections(const std::string& user,
                                                      const Commands& update_commands,
                                                      const AclKeys& update_keys,
                                                      const AclPubSub& update_pub_sub) {
  auto update_cb = [&]([[maybe_unused]] size_t id, util::Connection* conn) {
    DCHECK(conn);
    auto connection = static_cast<facade::Connection*>(conn);
    if (!connection->IsHttp() && connection->cntx()) {
      connection->SendAclUpdateAsync(
          facade::Connection::AclUpdateMessage{user, update_commands, update_keys, update_pub_sub});
    }
  };

  if (main_listener_ && main_listener_->protocol() == facade::Protocol::REDIS) {
    main_listener_->TraverseConnections(update_cb);
  }
}

using facade::ErrorReply;

void AclFamily::SetUser(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view username = facade::ToSV(args[0]);
  auto reg = registry_->GetRegistryWithWriteLock();
  const bool exists = reg.registry.contains(username);
  const bool has_all_keys = exists ? reg.registry.find(username)->second.Keys().all_keys : false;
  auto* builder = cmd_cntx.rb;
  auto req = ParseAclSetUser(args.subspan(1), false, has_all_keys);

  auto error_case = [builder](ErrorReply&& error) { builder->SendError(error); };

  auto update_case = [username, &reg, builder, this, exists](User::UpdateRequest&& req) {
    auto& user = reg.registry[username];
    if (!exists) {
      User::UpdateRequest default_req;
      default_req.updates = {User::UpdateRequest::CategoryValueType{User::Sign::MINUS, acl::ALL}};
      user.Update(std::move(default_req), CategoryToIdx(), reverse_cat_table_,
                  CategoryToCommandsIndex());
    }
    const bool reset_channels = req.reset_channels;
    user.Update(std::move(req), CategoryToIdx(), reverse_cat_table_, CategoryToCommandsIndex());
    // Send ok first because the connection might get evicted
    builder->SendOk();
    if (exists) {
      if (!reset_channels) {
        StreamUpdatesToAllProactorConnections(string(username), user.AclCommands(), user.Keys(),
                                              user.PubSub());
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
  auto close_cb = [&users]([[maybe_unused]] size_t id, util::Connection* conn) {
    CHECK(conn);
    auto connection = static_cast<facade::Connection*>(conn);
    auto ctx = static_cast<ConnectionContext*>(connection->cntx());
    if (ctx && users.contains(ctx->authed_username)) {
      connection->ShutdownSelf();
    }
  };

  if (main_listener_) {
    main_listener_->TraverseConnections(close_cb);
  }
}

void AclFamily::EvictOpenConnectionsOnAllProactorsWithRegistry(
    const UserRegistry::RegistryType& registry) {
  auto close_cb = [&registry]([[maybe_unused]] size_t id, util::Connection* conn) {
    CHECK(conn);
    auto connection = static_cast<facade::Connection*>(conn);
    auto ctx = static_cast<ConnectionContext*>(connection->cntx());
    if (ctx && ctx->authed_username != "default" && registry.contains(ctx->authed_username)) {
      connection->ShutdownSelf();
    }
  };

  if (main_listener_) {
    main_listener_->TraverseConnections(close_cb);
  }
}

void AclFamily::DelUser(CmdArgList args, const CommandContext& cmd_cntx) {
  auto& registry = *registry_;
  absl::flat_hash_set<string_view> users;

  for (auto arg : args) {
    string_view username = facade::ToSV(arg);
    if (username == "default") {
      continue;
    }
    if (registry.RemoveUser(username)) {
      users.insert(username);
    }
  }

  if (users.empty()) {
    cmd_cntx.rb->SendLong(0);
    return;
  }
  VLOG(1) << "Evicting open acl connections";
  EvictOpenConnectionsOnAllProactors(users);
  VLOG(1) << "Done evicting open acl connections";
  cmd_cntx.rb->SendLong(users.size());
}

void AclFamily::WhoAmI(CmdArgList args, const CommandContext& cmd_cntx) {
  auto* rb = static_cast<facade::RedisReplyBuilder*>(cmd_cntx.rb);
  rb->SendBulkString(absl::StrCat("User is ", cmd_cntx.conn_cntx->authed_username));
}

string AclFamily::RegistryToString() const {
  auto registry_with_read_lock = registry_->GetRegistryWithLock();
  auto& registry = registry_with_read_lock.registry;
  string result;
  for (auto& [username, user] : registry) {
    string command = "USER ";
    const string password = PasswordsToString(user.Passwords(), user.HasNopass(), true);

    const string acl_keys = AclKeysToString(user.Keys());

    const string maybe_space = acl_keys.empty() ? "" : " ";

    const string acl_pub_sub = AclPubSubToString(user.PubSub());

    const string acl_cat_and_commands =
        AclCatAndCommandToString(user.CatChanges(), user.CmdChanges());

    using namespace string_view_literals;

    absl::StrAppend(&result, command, username, " ", user.IsActive() ? "ON "sv : "OFF "sv, password,
                    acl_keys, maybe_space, acl_pub_sub, " ", acl_cat_and_commands, "\n");
  }

  return result;
}

void AclFamily::Save(CmdArgList args, const CommandContext& cmd_cntx) {
  auto acl_file_path = absl::GetFlag(FLAGS_aclfile);
  auto* builder = cmd_cntx.rb;
  if (acl_file_path.empty()) {
    builder->SendError("Dragonfly is not configured to use an ACL file.");
    return;
  }

  auto res = io::OpenWrite(acl_file_path);
  if (!res) {
    std::string error = absl::StrCat("Failed to open the aclfile: ", res.error().message());
    LOG(ERROR) << error;
    builder->SendError(error);
    return;
  }

  std::unique_ptr<io::WriteFile> file(res.value());
  std::string output = RegistryToString();
  auto ec = file->Write(output);

  if (ec) {
    std::string error = absl::StrCat("Failed to write to the aclfile: ", ec.message());
    LOG(ERROR) << error;
    builder->SendError(error);
    return;
  }

  ec = file->Close();
  if (ec) {
    std::string error = absl::StrCat("Failed to close the aclfile ", ec.message());
    LOG(WARNING) << error;
    builder->SendError(error);
    return;
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

  if (file_contents.empty()) {
    return {"Empty file"};
  }

  std::vector<std::string> usernames;
  auto materialized = MaterializeFileContents(&usernames, file_contents);

  if (!materialized) {
    std::string error = "Error materializing acl file";
    LOG(WARNING) << error;
    return {std::move(error)};
  }

  std::vector<User::UpdateRequest> requests;

  for (auto& cmds : *materialized) {
    auto req = ParseAclSetUser(cmds, true);
    if (std::holds_alternative<ErrorReply>(req)) {
      auto error = std::move(std::get<ErrorReply>(req));
      LOG(WARNING) << "Error while parsing aclfile: " << error.ToSv();
      return {std::string(error.ToSv())};
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
  }

  for (size_t i = 0; i < usernames.size(); ++i) {
    User::UpdateRequest default_req;
    default_req.updates = {User::UpdateRequest::CategoryValueType{User::Sign::MINUS, acl::ALL}};
    auto& user = registry[usernames[i]];
    user.Update(std::move(default_req), CategoryToIdx(), reverse_cat_table_,
                CategoryToCommandsIndex());
    user.Update(std::move(requests[i]), CategoryToIdx(), reverse_cat_table_,
                CategoryToCommandsIndex());
  }

  if (!registry.contains("default")) {
    auto& user = registry["default"];
    user.Update(registry_->DefaultUserUpdateRequest(), CategoryToIdx(), reverse_cat_table_,
                CategoryToCommandsIndex());
  }

  return {};
}

bool AclFamily::Load() {
  auto acl_file = absl::GetFlag(FLAGS_aclfile);
  return !LoadToRegistryFromFile(acl_file, nullptr);
}

void AclFamily::Load(CmdArgList args, const CommandContext& cmd_cntx) {
  auto acl_file = absl::GetFlag(FLAGS_aclfile);
  if (acl_file.empty()) {
    cmd_cntx.rb->SendError("Dragonfly is not configured to use an ACL file.");
    return;
  }

  const auto load_error = LoadToRegistryFromFile(acl_file, cmd_cntx.rb);

  if (load_error) {
    cmd_cntx.rb->SendError(absl::StrCat("Error loading: ", acl_file, " ", load_error.Format()));
  }
}

void AclFamily::Log(CmdArgList args, const CommandContext& cmd_cntx) {
  auto* rb = static_cast<facade::RedisReplyBuilder*>(cmd_cntx.rb);
  if (args.size() > 1) {
    rb->SendError(facade::OpStatus::OUT_OF_RANGE);
  }

  size_t max_output = 10;
  if (args.size() == 1) {
    auto option = facade::ToSV(args[0]);
    if (absl::EqualsIgnoreCase(option, "RESET")) {
      pool_->AwaitFiberOnAll(
          [](auto index, auto* context) { ServerState::tlocal()->acl_log.Reset(); });
      rb->SendOk();
      return;
    }

    if (!absl::SimpleAtoi(facade::ToSV(args[0]), &max_output)) {
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

  rb->StartArray(total_entries);
  auto print_element = [rb](const auto& entry) {
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
  };

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

  for (size_t i = 0; i < total_entries; ++i) {
    auto min = n_way_minimum(logs);
    print_element(logs[min].front());
    logs[min].pop_front();
  }
}

void AclFamily::Users(CmdArgList args, const CommandContext& cmd_cntx) {
  const auto registry_with_lock = registry_->GetRegistryWithLock();
  const auto& registry = registry_with_lock.registry;
  auto* rb = static_cast<facade::RedisReplyBuilder*>(cmd_cntx.rb);

  rb->StartArray(registry.size());
  for (const auto& [username, _] : registry) {
    rb->SendSimpleString(username);
  }
}

void AclFamily::Cat(CmdArgList args, const CommandContext& cmd_cntx) {
  auto* rb = static_cast<facade::RedisReplyBuilder*>(cmd_cntx.rb);

  if (args.size() > 1) {
    rb->SendError(facade::OpStatus::SYNTAX_ERR);
    return;
  }

  if (args.size() == 1) {
    string category = absl::AsciiStrToUpper(ArgS(args, 0));

    if (!cat_table_.contains(category)) {
      auto error = absl::StrCat("Unkown category: ", category);
      rb->SendError(error);
      return;
    }

    const uint32_t cid_mask = cat_table_.find(category)->second;
    std::vector<std::string_view> results;
    // TODO replace this with indexer
    auto cb = [cid_mask, &results](auto name, auto& cid) {
      if (cid_mask & cid.acl_categories()) {
        results.push_back(name);
      }
    };

    cmd_registry_->Traverse(cb);
    rb->StartArray(results.size());
    for (const auto& command : results) {
      rb->SendSimpleString(command);
    }

    return;
  }

  size_t total_categories = 0;
  for (auto& elem : reverse_cat_table_) {
    if (elem != "_RESERVED") {
      ++total_categories;
    }
  }

  rb->StartArray(total_categories);
  for (auto& elem : reverse_cat_table_) {
    if (elem != "_RESERVED") {
      rb->SendSimpleString(elem);
    }
  }
}

void AclFamily::GetUser(CmdArgList args, const CommandContext& cmd_cntx) {
  auto username = facade::ToSV(args[0]);
  const auto registry_with_lock = registry_->GetRegistryWithLock();
  const auto& registry = registry_with_lock.registry;
  auto* rb = static_cast<facade::RedisReplyBuilder*>(cmd_cntx.rb);

  if (!registry.contains(username)) {
    rb->SendNull();
    return;
  }
  auto& user = registry.find(username)->second;
  std::string status = user.IsActive() ? "on" : "off";
  auto pass = PasswordsToString(user.Passwords(), user.HasNopass(), false);
  if (!pass.empty()) {
    pass.pop_back();
  }

  rb->StartArray(10);

  rb->SendSimpleString("flags");
  const size_t total_elements = (pass != "nopass") ? 1 : 2;
  rb->StartArray(total_elements);
  rb->SendSimpleString(status);
  if (total_elements == 2) {
    rb->SendSimpleString(pass);
  }

  rb->SendSimpleString("passwords");
  if (pass != "nopass" && !pass.empty()) {
    rb->SendSimpleString(pass);
  } else {
    rb->SendEmptyArray();
  }
  rb->SendSimpleString("commands");

  const std::string acl_cat_and_commands =
      AclCatAndCommandToString(user.CatChanges(), user.CmdChanges());

  rb->SendSimpleString(acl_cat_and_commands);

  rb->SendSimpleString("keys");
  std::string keys = AclKeysToString(user.Keys());
  if (!keys.empty()) {
    rb->SendSimpleString(keys);
  } else {
    rb->SendEmptyArray();
  }

  rb->SendSimpleString("channels");
  std::string pub_sub = AclPubSubToString(user.PubSub());
  rb->SendSimpleString(pub_sub);
}

void AclFamily::GenPass(CmdArgList args, const CommandContext& cmd_cntx) {
  auto* builder = cmd_cntx.rb;
  if (args.length() > 1) {
    builder->SendError(facade::UnknownSubCmd("GENPASS", "ACL"));
    return;
  }
  uint32_t random_bits = 256;
  if (args.length() == 1) {
    auto requested_bits = facade::ArgS(args, 0);

    if (!absl::SimpleAtoi(requested_bits, &random_bits) || random_bits == 0 || random_bits > 4096) {
      return builder->SendError(
          "ACL GENPASS argument must be the number of bits for the output password, a positive "
          "number up to 4096");
    }
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

void AclFamily::DryRun(CmdArgList args, const CommandContext& cmd_cntx) {
  auto* rb = static_cast<facade::RedisReplyBuilder*>(cmd_cntx.rb);
  auto username = facade::ArgS(args, 0);
  const auto registry_with_lock = registry_->GetRegistryWithLock();
  const auto& registry = registry_with_lock.registry;
  if (!registry.contains(username)) {
    auto error = absl::StrCat("User '", username, "' not found");
    rb->SendError(error);
    return;
  }

  string command = absl::AsciiStrToUpper(ArgS(args, 1));
  auto* cid = cmd_registry_->Find(command);
  if (!cid) {
    auto error = absl::StrCat("Command '", command, "' not found");
    rb->SendError(error);
    return;
  }

  const auto& user = registry.find(username)->second;
  const bool is_allowed =
      IsUserAllowedToInvokeCommandGeneric(user.AclCommandsRef(), {{}, true}, {}, *cid).first;
  if (is_allowed) {
    rb->SendOk();
    return;
  }

  auto msg = absl::StrCat("This user has no permissions to run the '", command, "' command");

  rb->SendBulkString(msg);
}

using MemberFunc = void (AclFamily::*)(CmdArgList args, const CommandContext& cmd_cntx);

CommandId::Handler3 HandlerFunc(AclFamily* acl, MemberFunc f) {
  return [=](CmdArgList args, const CommandContext& cmd_cntx) { return (acl->*f)(args, cmd_cntx); };
}

#define HFUNC(x) SetHandler(HandlerFunc(this, &AclFamily::x))

constexpr uint32_t kAcl = acl::CONNECTION;
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
  *registry << CI{"ACL WHOAMI", kAclMask, 1, 0, 0, acl::kWhoAmI}.HFUNC(WhoAmI);
  *registry << CI{"ACL SAVE", kAclMask, 1, 0, 0, acl::kSave}.HFUNC(Save);
  *registry << CI{"ACL LOAD", kAclMask, 1, 0, 0, acl::kLoad}.HFUNC(Load);
  *registry << CI{"ACL LOG", kAclMask, 0, 0, 0, acl::kLog}.HFUNC(Log);
  *registry << CI{"ACL USERS", kAclMask, 1, 0, 0, acl::kUsers}.HFUNC(Users);
  *registry << CI{"ACL CAT", kAclMask, -1, 0, 0, acl::kCat}.HFUNC(Cat);
  *registry << CI{"ACL GETUSER", kAclMask, 2, 0, 0, acl::kGetUser}.HFUNC(GetUser);
  *registry << CI{"ACL DRYRUN", kAclMask, 3, 0, 0, acl::kDryRun}.HFUNC(DryRun);
  *registry << CI{"ACL GENPASS", CO::NOSCRIPT | CO::LOADING, -1, 0, 0, acl::kGenPass}.HFUNC(
      GenPass);
  cmd_registry_ = registry;

  // build indexers
  BuildIndexers(cmd_registry_->GetFamilies());
}

#undef HFUNC

void AclFamily::Init(facade::Listener* main_listener, UserRegistry* registry) {
  main_listener_ = main_listener;
  registry_ = registry;
  config_registry.RegisterMutable("requirepass", [this](const absl::CommandLineFlag& flag) {
    User::UpdateRequest rqst;
    rqst.passwords.push_back({flag.CurrentValue()});
    registry_->MaybeAddAndUpdate("default", std::move(rqst));
    return true;
  });
  auto acl_file = absl::GetFlag(FLAGS_aclfile);
  if (!acl_file.empty() && Load()) {
    return;
  }
  registry_->Init(&CategoryToIdx(), &reverse_cat_table_, &CategoryToCommandsIndex());
}

std::string AclFamily::AclCatToString(uint32_t acl_category, User::Sign sign) const {
  std::string res = sign == User::Sign::PLUS ? "+@" : "-@";
  if (acl_category == acl::ALL) {
    absl::StrAppend(&res, "all");
    return res;
  }

  const auto& index = CategoryToIdx().at(acl_category);
  absl::StrAppend(&res, absl::AsciiStrToLower(reverse_cat_table_[index]));
  return res;
}

std::string AclFamily::AclCommandToString(size_t family, uint64_t mask, User::Sign sign) const {
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

namespace {
struct CategoryAndMetadata {
  User::CategoryChange change;
  User::ChangeMetadata metadata;
};

struct CommandAndMetadata {
  User::CommandChange change;
  User::ChangeMetadata metadata;
};

using MergeResult = std::vector<std::variant<CategoryAndMetadata, CommandAndMetadata>>;

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

using MaterializedContents = std::optional<std::vector<std::vector<std::string_view>>>;

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
    std::vector<std::string_view> cmds = absl::StrSplit(command, ' ', absl::SkipEmpty());
    if (!absl::EqualsIgnoreCase(cmds[0], "USER") || cmds.size() < 4) {
      return {};
    }

    usernames->push_back(std::string(cmds[1]));
    cmds.erase(cmds.begin(), cmds.begin() + 2);
    materialized.push_back(cmds);
  }
  return materialized;
}

struct ParseKeyResult {
  std::string glob;
  KeyOp op;
  bool all_keys{false};
  bool reset_keys{false};
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

struct ParsePubSubResult {
  std::string glob;
  bool has_asterisk{false};
  bool all_channels{false};
  bool reset_channels{false};
};

std::optional<ParsePubSubResult> MaybeParseAclPubSub(std::string_view command) {
  if (absl::EqualsIgnoreCase(command, "ALLCHANNELS") || command == "&*") {
    return ParsePubSubResult{"", false, true, false};
  }

  if (absl::EqualsIgnoreCase(command, "RESETCHANNELS")) {
    return ParsePubSubResult{"", false, false, true};
  }

  if (absl::StartsWith(command, "&") && command.size() >= 2) {
    const auto glob = command.substr(1);
    const bool has_asterisk = glob.find('*') != std::string_view::npos;
    return ParsePubSubResult{std::string(glob), has_asterisk};
  }

  return {};
}

std::string PrettyPrintSha(std::string_view pass, bool all) {
  if (all) {
    return absl::BytesToHexString(pass);
  }
  return absl::BytesToHexString(pass.substr(0, 15)).substr(0, 15);
};

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

std::optional<bool> MaybeParseStatus(std::string_view command) {
  if (command == "ON") {
    return true;
  }
  if (command == "OFF") {
    return false;
  }
  return {};
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

std::string AclPubSubToString(const AclPubSub& pub_sub) {
  if (pub_sub.all_channels) {
    return "&*";
  }

  std::string result = "resetchannels ";

  for (const auto& [glob, has_asterisk] : pub_sub.globs) {
    absl::StrAppend(&result, "&", glob, " ");
  }

  if (result.back() == ' ') {
    result.pop_back();
  }

  return result;
}

}  // namespace

std::string AclFamily::AclCatAndCommandToString(const User::CategoryChanges& cat,
                                                const User::CommandChanges& cmds) const {
  std::string result;

  auto tables = MergeTables(cat, cmds);

  auto cat_visitor = [&result, this](const CategoryAndMetadata& val) {
    const auto& [change, meta] = val;
    absl::StrAppend(&result, AclCatToString(change, meta.sign), " ");
  };

  auto cmd_visitor = [&result, this](const CommandAndMetadata& val) {
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

using OptCat = std::optional<uint32_t>;

// bool == true if +
// bool == false if -
std::pair<OptCat, bool> AclFamily::MaybeParseAclCategory(std::string_view command) const {
  if (absl::EqualsIgnoreCase(command, "ALLCOMMANDS")) {
    return {cat_table_.at("ALL"), true};
  }

  if (absl::EqualsIgnoreCase(command, "NOCOMMANDS")) {
    return {cat_table_.at("ALL"), false};
  }

  if (absl::StartsWith(command, "+@")) {
    auto res = cat_table_.find(command.substr(2));
    if (res == cat_table_.end()) {
      return {};
    }
    return {res->second, true};
  }

  if (absl::StartsWith(command, "-@")) {
    auto res = cat_table_.find(command.substr(2));
    if (res == cat_table_.end()) {
      return {};
    }
    return {res->second, false};
  }

  return {};
}

std::optional<std::string> AclFamily::MaybeParseNamespace(std::string_view command) const {
  constexpr std::string_view kPrefix = "NAMESPACE:";
  if (absl::StartsWith(command, kPrefix)) {
    return std::string(command.substr(kPrefix.size()));
  }
  return std::nullopt;
}

std::pair<AclFamily::OptCommand, bool> AclFamily::MaybeParseAclCommand(
    std::string_view command) const {
  if (absl::StartsWith(command, "+")) {
    auto res = cmd_registry_->Find(command.substr(1));
    if (!res) {
      return {};
    }
    std::pair<size_t, uint64_t> cmd{res->GetFamily(), res->GetBitIndex()};
    return {cmd, true};
  }

  if (absl::StartsWith(command, "-")) {
    auto res = cmd_registry_->Find(command.substr(1));
    if (!res) {
      return {};
    }
    std::pair<size_t, uint64_t> cmd{res->GetFamily(), res->GetBitIndex()};
    return {cmd, false};
  }

  return {};
}

using facade::ErrorReply;

std::variant<User::UpdateRequest, ErrorReply> AclFamily::ParseAclSetUser(
    const facade::ArgRange& args, bool hashed, bool has_all_keys, bool has_all_channels) const {
  User::UpdateRequest req;

  for (std::string_view arg : args) {
    if (auto pass = MaybeParsePassword(facade::ToSV(arg), hashed); pass) {
      req.passwords.push_back(std::move(*pass));

      if (hashed && absl::StartsWith(facade::ToSV(arg), "#")) {
        req.passwords.back().is_hashed = true;
      }
      continue;
    }

    if (auto res = MaybeParseAclKey(facade::ToSV(arg)); res) {
      auto& [glob, op, all_keys, reset_keys] = *res;
      if ((has_all_keys && !all_keys && !reset_keys) ||
          (req.allow_all_keys && !all_keys && !reset_keys)) {
        return ErrorReply(absl::StrCat(
            "Error in ACL SETUSER modifier \'", facade::ToSV(arg),
            "\': Adding a pattern after the * pattern (or the "
            "'allkeys' flag) is not valid and does not have any effect. Try 'resetkeys' to start "
            "with an empty list of patterns"));
      }

      req.allow_all_keys = all_keys;
      req.reset_all_keys = reset_keys;
      if (reset_keys) {
        has_all_keys = false;
      }
      req.keys.push_back({std::move(glob), op, all_keys, reset_keys});
      continue;
    }

    if (auto res = MaybeParseAclPubSub(facade::ToSV(arg)); res) {
      auto& [glob, has_asterisk, all_channels, reset_channels] = *res;
      if ((has_all_channels && !all_channels && !reset_channels) ||
          (req.all_channels && !all_channels && !reset_channels)) {
        return ErrorReply(
            absl::StrCat("ERR Error in ACL SETUSER modifier \'", facade::ToSV(arg),
                         "\': Adding a pattern after the * pattern (or the 'allchannels' flag) is "
                         "not valid and does not have any effect. Try 'resetchannels' to start "
                         "with an empty list of channels"));
      }

      req.all_channels = all_channels;
      req.reset_channels = reset_channels;
      if (reset_channels) {
        has_all_channels = false;
      }
      req.pub_sub.push_back({std::move(glob), has_asterisk, all_channels, reset_channels});
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

    auto [cat, add] = MaybeParseAclCategory(command);
    if (cat) {
      using Sign = User::Sign;
      using Val = std::pair<Sign, uint32_t>;
      auto val = add ? Val{Sign::PLUS, *cat} : Val{Sign::MINUS, *cat};
      req.updates.push_back(val);
      continue;
    }

    auto ns = MaybeParseNamespace(command);
    if (ns.has_value()) {
      req.ns = *ns;
      continue;
    }

    auto [cmd, sign] = MaybeParseAclCommand(command);
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

void AclFamily::BuildIndexers(RevCommandsIndexStore families) {
  acl::NumberOfFamilies(families.size());
  CommandsRevIndexer(std::move(families));
  CategoryToCommandsIndexStore index;
  cmd_registry_->Traverse([&](std::string_view name, auto& cid) {
    auto cat = cid.acl_categories();
    for (size_t i = 0; i < 32; ++i) {
      if (cat & (1 << i)) {
        std::string_view cat_name = reverse_cat_table_[i];
        if (index[cat_name].empty()) {
          index[cat_name].resize(CommandsRevIndexer().size());
        }
        auto family = cid.GetFamily();
        auto bit_index = cid.GetBitIndex();
        index[cat_name][family] |= bit_index;
      }
    }
  });

  CategoryToCommandsIndex(std::move(index));
  CategoryToIdxStore idx_store;
  for (size_t i = 0; i < 32; ++i) {
    idx_store[1 << i] = i;
  }
  CategoryToIdx(std::move(idx_store));
}

}  // namespace dfly::acl
