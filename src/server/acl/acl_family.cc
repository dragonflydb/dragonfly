// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "server/acl/acl_family.h"

#include <glog/logging.h>

#include <cctype>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <variant>

#include "absl/strings/ascii.h"
#include "absl/strings/escaping.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "absl/types/span.h"
#include "base/flags.h"
#include "base/logging.h"
#include "core/overloaded.h"
#include "facade/dragonfly_connection.h"
#include "facade/facade_types.h"
#include "io/file.h"
#include "io/file_util.h"
#include "io/io.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/server_state.h"

ABSL_FLAG(std::string, aclfile, "", "Path and name to aclfile");

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

static std::string PrettyPrintSha(std::string_view pass, bool all = false) {
  if (all) {
    return absl::BytesToHexString(pass);
  }
  return absl::BytesToHexString(pass.substr(0, 15)).substr(0, 15);
};

AclFamily::AclFamily(UserRegistry* registry) : registry_(registry) {
}

void AclFamily::Acl(CmdArgList args, ConnectionContext* cntx) {
  (*cntx)->SendError("Wrong number of arguments for acl command");
}

void AclFamily::List(CmdArgList args, ConnectionContext* cntx) {
  const auto registry_with_lock = registry_->GetRegistryWithLock();
  const auto& registry = registry_with_lock.registry;
  (*cntx)->StartArray(registry.size());

  for (const auto& [username, user] : registry) {
    std::string buffer = "user ";
    const std::string_view pass = user.Password();
    const std::string password = pass == "nopass" ? "nopass" : PrettyPrintSha(pass);
    const std::string acl_cat = AclToString(user.AclCategory());

    using namespace std::string_view_literals;

    absl::StrAppend(&buffer, username, " ", user.IsActive() ? "on "sv : "off "sv, password, " ",
                    acl_cat);

    (*cntx)->SendSimpleString(buffer);
  }
}

namespace {

std::optional<std::string> MaybeParsePassword(std::string_view command) {
  if (command == "nopass") {
    return std::string(command);
  }
  if (command[0] != '>') {
    return {};
  }

  return std::string(command.substr(1));
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

template <typename T>
std::variant<User::UpdateRequest, ErrorReply> ParseAclSetUser(T args, bool hashed = false) {
  User::UpdateRequest req;

  for (auto& arg : args) {
    if (auto pass = MaybeParsePassword(facade::ToSV(arg)); pass) {
      if (req.password) {
        return ErrorReply("Only one password is allowed");
      }
      req.password = std::move(pass);
      req.is_hashed = hashed;
      continue;
    }
    if constexpr (std::is_same_v<T, CmdArgList>) {
      ToUpper(&arg);
    }
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
      return ErrorReply(absl::StrCat("Unrecognized parameter ", command));
    }

    using Sign = User::Sign;
    using Val = std::pair<Sign, uint32_t>;
    auto val = add ? Val{Sign::PLUS, *cat} : Val{Sign::MINUS, *cat};
    req.categories.push_back(val);
  }

  return req;
}

using MaterializedContents = std::optional<std::vector<std::vector<std::string_view>>>;

MaterializedContents MaterializeFileContents(std::vector<std::string>* usernames,
                                             std::string_view file_contents) {
  // This is fine, a very large file will top at 1-2 mb. And that's for 5000+ users with 400
  // characters of ACL infor...
  std::vector<std::string_view> commands = absl::StrSplit(file_contents, "\n");
  std::vector<std::vector<std::string_view>> materialized;
  materialized.reserve(commands.size());
  usernames->reserve(commands.size());
  for (auto& command : commands) {
    if (command.empty())
      continue;
    std::vector<std::string_view> cmds = absl::StrSplit(command, ' ');
    if (cmds[0] != "ACL" || cmds[1] != "SETUSER" || cmds.size() < 3) {
      return {};
    }
    usernames->push_back(std::string(cmds[2]));
    cmds.erase(cmds.begin(), cmds.begin() + 3);
    materialized.push_back(cmds);
  }
  return materialized;
}

}  // namespace

void AclFamily::StreamUpdatesToAllProactorConnections(const std::vector<std::string>& user,
                                                      const std::vector<uint32_t>& update_cat) {
  auto update_cb = [&user, &update_cat]([[maybe_unused]] size_t id, util::Connection* conn) {
    DCHECK(conn);
    auto connection = static_cast<facade::Connection*>(conn);
    DCHECK(user.size() == update_cat.size());
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
    auto user_with_lock = registry_->MaybeAddAndUpdateWithLock(username, std::move(req));
    if (user_with_lock.exists) {
      StreamUpdatesToAllProactorConnections({std::string(username)},
                                            {user_with_lock.user.AclCategory()});
    }
    cntx->SendOk();
  };

  std::visit(Overloaded{error_case, update_case}, std::move(req));
}

void AclFamily::EvictOpenConnectionsOnAllProactors(std::string_view user) {
  auto close_cb = [user]([[maybe_unused]] size_t id, util::Connection* conn) {
    DCHECK(conn);
    auto connection = static_cast<facade::Connection*>(conn);
    auto ctx = static_cast<ConnectionContext*>(connection->cntx());
    if (ctx && ctx->authed_username == user) {
      connection->ShutdownSelf();
    }
  };

  if (main_listener_) {
    main_listener_->TraverseConnections(close_cb);
  }
}

void AclFamily::DelUser(CmdArgList args, ConnectionContext* cntx) {
  std::string_view username = facade::ToSV(args[0]);
  auto& registry = *registry_;
  if (!registry.RemoveUser(username)) {
    cntx->SendError(absl::StrCat("User ", username, " does not exist"));
    return;
  }

  EvictOpenConnectionsOnAllProactors(username);
  cntx->SendOk();
}

void AclFamily::WhoAmI(CmdArgList args, ConnectionContext* cntx) {
  cntx->SendSimpleString(absl::StrCat("User is ", cntx->authed_username));
}

std::string AclFamily::RegistryToString() const {
  auto registry_with_read_lock = registry_->GetRegistryWithLock();
  auto& registry = registry_with_read_lock.registry;
  std::string result;
  for (auto& [username, user] : registry) {
    std::string command = "ACL SETUSER ";
    const std::string_view pass = user.Password();
    const std::string password =
        pass == "nopass" ? "nopass " : absl::StrCat(">", PrettyPrintSha(pass, true), " ");
    const std::string acl_cat = AclToString(user.AclCategory());

    using namespace std::string_view_literals;

    absl::StrAppend(&result, command, username, " ", user.IsActive() ? "ON "sv : "OFF "sv, password,
                    acl_cat, "\n");
  }

  result.pop_back();

  return result;
}

void AclFamily::Save(CmdArgList args, ConnectionContext* cntx) {
  auto acl_file_path = absl::GetFlag(FLAGS_aclfile);
  if (acl_file_path.empty()) {
    cntx->SendError("Dragonfly is not configured to use an ACL file.");
    return;
  }

  auto res = io::OpenWrite(acl_file_path);
  if (!res) {
    std::string error = absl::StrCat("Failed to open the aclfile: ", res.error().message());
    LOG(ERROR) << error;
    cntx->SendError(error);
    return;
  }

  std::unique_ptr<io::WriteFile> file(res.value());
  std::string output = RegistryToString();
  auto ec = file->Write(output);

  if (ec) {
    std::string error = absl::StrCat("Failed to write to the aclfile: ", ec.message());
    LOG(ERROR) << error;
    cntx->SendError(error);
    return;
  }

  ec = file->Close();
  if (ec) {
    std::string error = absl::StrCat("Failed to close the aclfile ", ec.message());
    LOG(WARNING) << error;
    cntx->SendError(error);
    return;
  }

  cntx->SendOk();
}

std::optional<facade::ErrorReply> AclFamily::LoadToRegistryFromFile(std::string_view full_path,
                                                                    bool init) {
  auto is_file_read = io::ReadFileToString(full_path);
  if (!is_file_read) {
    auto error = absl::StrCat("Dragonfly could not load ACL file ", full_path, " with error ",
                              is_file_read.error().message());

    LOG(WARNING) << error;
    return {ErrorReply(std::move(error))};
  }

  auto file_contents = std::move(is_file_read.value());

  if (file_contents.empty()) {
    return {};
  }

  std::vector<std::string> usernames;
  auto materialized = MaterializeFileContents(&usernames, file_contents);

  if (!materialized) {
    std::string error = "Error materializing acl file";
    LOG(WARNING) << error;
    return {ErrorReply(std::move(error))};
  }

  std::vector<User::UpdateRequest> requests;

  for (auto& cmds : *materialized) {
    auto req = ParseAclSetUser<std::vector<std::string_view>&>(cmds, true);
    if (std::holds_alternative<ErrorReply>(req)) {
      auto error = std::move(std::get<ErrorReply>(req));
      LOG(WARNING) << "Error while parsing aclfile: " << error.ToSv();
      return {std::move(error)};
    }
    requests.push_back(std::move(std::get<User::UpdateRequest>(req)));
  }

  auto registry_with_wlock = registry_->GetRegistryWithWriteLock();
  auto& registry = registry_with_wlock.registry;
  // TODO(see what redis is doing here)
  if (!init) {
    registry.clear();
  }
  std::vector<uint32_t> categories;
  for (size_t i = 0; i < usernames.size(); ++i) {
    auto& user = registry[usernames[i]];
    user.Update(std::move(requests[i]));
    categories.push_back(user.AclCategory());
  }

  if (!init) {
    StreamUpdatesToAllProactorConnections(usernames, categories);
  }

  return {};
}

void AclFamily::Load() {
  auto acl_file = absl::GetFlag(FLAGS_aclfile);
  LoadToRegistryFromFile(acl_file, true);
}

void AclFamily::Load(CmdArgList args, ConnectionContext* cntx) {
  auto acl_file = absl::GetFlag(FLAGS_aclfile);
  if (acl_file.empty()) {
    cntx->SendError("Dragonfly is not configured to use an ACL file.");
    return;
  }

  const auto is_successfull = LoadToRegistryFromFile(acl_file, !cntx);

  if (is_successfull) {
    cntx->SendError(absl::StrCat("Error loading: ", acl_file, " ", is_successfull->ToSv()));
    return;
  }

  cntx->SendOk();
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
constexpr uint32_t kDelUser = acl::ADMIN | acl::SLOW | acl::DANGEROUS;
constexpr uint32_t kWhoAmI = acl::SLOW;
constexpr uint32_t kSave = acl::ADMIN | acl::SLOW | acl::DANGEROUS;
constexpr uint32_t kLoad = acl::ADMIN | acl::SLOW | acl::DANGEROUS;

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
  *registry << CI{"ACL DELUSER", CO::ADMIN | CO::NOSCRIPT | CO::LOADING, 2, 0, 0, 0, acl::kDelUser}
                   .HFUNC(DelUser);
  *registry << CI{"ACL WHOAMI", CO::ADMIN | CO::NOSCRIPT | CO::LOADING, 1, 0, 0, 0, acl::kWhoAmI}
                   .HFUNC(WhoAmI);
  *registry << CI{"ACL SAVE", CO::ADMIN | CO::NOSCRIPT | CO::LOADING, 1, 0, 0, 0, acl::kSave}.HFUNC(
      Save);
  *registry << CI{"ACL LOAD", CO::ADMIN | CO::NOSCRIPT | CO::LOADING, 1, 0, 0, 0, acl::kLoad}.HFUNC(
      Load);
}

#undef HFUNC

void AclFamily::Init(facade::Listener* main_listener, UserRegistry* registry) {
  main_listener_ = main_listener;
  registry_ = registry;
  auto acl_file = absl::GetFlag(FLAGS_aclfile);
  if (!acl_file.empty()) {
    Load();
  }
}

}  // namespace dfly::acl
