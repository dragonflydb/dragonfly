// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/command_registry.h"

#include <absl/container/inlined_vector.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>
#include <absl/time/clock.h>

#include "base/bits.h"
#include "base/flags.h"
#include "base/logging.h"
#include "base/stl_util.h"
#include "facade/dragonfly_connection.h"
#include "facade/error.h"
#include "server/acl/acl_commands_def.h"
#include "server/server_state.h"
#include "server/transaction.h"

using namespace std;
ABSL_FLAG(vector<string>, rename_command, {},
          "Change the name of commands, format is: <cmd1_name>=<cmd1_new_name>, "
          "<cmd2_name>=<cmd2_new_name>");
ABSL_FLAG(vector<string>, restricted_commands, {},
          "Commands restricted to connections on the admin port");

ABSL_FLAG(vector<string>, oom_deny_commands, {},
          "Additinal commands that will be marked as denyoom");

ABSL_FLAG(vector<string>, command_alias, {},
          "Add an alias for given command(s), format is: <alias>=<original>, <alias>=<original>. "
          "Aliases must be set identically on replicas, if applicable");

ABSL_FLAG(bool, latency_tracking, false, "If true, track latency for commands");

namespace dfly {

using namespace facade;

using absl::AsciiStrToUpper;
using absl::GetFlag;
using absl::StrCat;
using absl::StrSplit;

namespace {

uint32_t ImplicitCategories(uint32_t mask) {
  if (mask & CO::ADMIN)
    mask |= CO::NOSCRIPT;
  return mask;
}

uint32_t ImplicitAclCategories(uint32_t mask) {
  mask = ImplicitCategories(mask);
  uint32_t out = 0;

  if (mask & CO::JOURNALED)
    out |= acl::WRITE;

  if ((mask & CO::READONLY) && ((mask & CO::NOSCRIPT) == 0))
    out |= acl::READ;

  if (mask & CO::ADMIN)
    out |= acl::ADMIN | acl::DANGEROUS;

  // todo pubsub

  if (mask & CO::FAST)
    out |= acl::FAST;

  if (mask & CO::BLOCKING)
    out |= acl::BLOCKING;

  if ((out & acl::FAST) == 0)
    out |= acl::SLOW;

  return out;
}

using CmdLineMapping = absl::flat_hash_map<std::string, std::string>;

CmdLineMapping ParseCmdlineArgMap(const absl::Flag<std::vector<std::string>>& flag) {
  const auto& mappings = absl::GetFlag(flag);
  CmdLineMapping parsed_mappings;
  parsed_mappings.reserve(mappings.size());

  for (const std::string& mapping : mappings) {
    absl::InlinedVector<std::string_view, 2> kv = absl::StrSplit(mapping, '=');
    if (kv.size() != 2) {
      LOG(ERROR) << "Malformed command '" << mapping << "' for " << flag.Name()
                 << ", expected key=value";
      exit(1);
    }

    std::string key = absl::AsciiStrToUpper(kv[0]);
    std::string value = absl::AsciiStrToUpper(kv[1]);

    if (key == value) {
      LOG(ERROR) << "Invalid attempt to map " << key << " to itself in " << flag.Name();
      exit(1);
    }

    if (!parsed_mappings.emplace(std::move(key), std::move(value)).second) {
      LOG(ERROR) << "Duplicate insert to " << flag.Name() << " not allowed";
      exit(1);
    }
  }
  return parsed_mappings;
}

CmdLineMapping OriginalToAliasMap() {
  CmdLineMapping original_to_alias;
  CmdLineMapping alias_to_original = ParseCmdlineArgMap(FLAGS_command_alias);
  original_to_alias.reserve(alias_to_original.size());
  std::for_each(std::make_move_iterator(alias_to_original.begin()),
                std::make_move_iterator(alias_to_original.end()),
                [&original_to_alias](auto&& pair) {
                  original_to_alias.emplace(std::move(pair.second), std::move(pair.first));
                });

  return original_to_alias;
}

constexpr int64_t kLatencyHistogramMinValue = 1;        // Minimum value in usec
constexpr int64_t kLatencyHistogramMaxValue = 1000000;  // Maximum value in usec (1s)
constexpr int32_t kLatencyHistogramPrecision = 2;

}  // namespace

void CommandContext::RecordLatency(facade::ArgSlice tail_args) const {
  DCHECK_GT(start_time_ns, 0u);
  int64_t after = absl::GetCurrentTimeNanos();

  ServerState* ss = ServerState::tlocal();  // Might have migrated thread, read after invocation
  int64_t execution_time_usec = (after - start_time_ns) / 1000;

  cid->RecordLatency(ss->thread_index(), execution_time_usec);

  DCHECK(conn_cntx_ != nullptr);

  // TODO: we should probably discard more commands here,
  // not just the blocking ones
  const auto* conn = server_conn_cntx()->conn();
  auto& conn_state = server_conn_cntx()->conn_state;
  if (conn_state.squashing_info) {
    // We run from the squashed transaction.
    conn = conn_state.squashing_info->owner->conn();
  }

  if (!(cid->opt_mask() & CO::BLOCKING) && conn != nullptr &&
      // Use SafeTLocal() to avoid accessing the wrong thread local instance
      ServerState::SafeTLocal()->ShouldLogSlowCmd(execution_time_usec)) {
    vector<string> aux_params;
    CmdArgVec aux_slices;

    if (tail_args.empty() && cid->name() == "EXEC") {
      // abuse tail_args to pass more information about the slow EXEC.
      aux_params.emplace_back(StrCat("CMDCOUNT/", exec_body_len));
      aux_slices.emplace_back(aux_params.back());
      tail_args = absl::MakeSpan(aux_slices);
    }
    ServerState::SafeTLocal()->GetSlowLog().Add(cid->name(), tail_args, conn->GetName(),
                                                conn->RemoteEndpointStr(), execution_time_usec,
                                                absl::GetCurrentTimeNanos() / 1000);
  }
}

CommandId::CommandId(const char* name, uint32_t mask, int8_t arity, int8_t first_key,
                     int8_t last_key, std::optional<uint32_t> acl_categories)
    : facade::CommandId(name, ImplicitCategories(mask), arity, first_key, last_key,
                        acl_categories.value_or(ImplicitAclCategories(mask))) {
  implicit_acl_ = !acl_categories.has_value();
  bool is_latency_tracked = GetFlag(FLAGS_latency_tracking);
  if (is_latency_tracked) {
    hdr_histogram* hist = nullptr;
    const int init_result = hdr_init(kLatencyHistogramMinValue, kLatencyHistogramMaxValue,
                                     kLatencyHistogramPrecision, &hist);
    CHECK_EQ(init_result, 0) << "failed to initialize histogram for command " << name;
    latency_histogram_ = hist;
  }

  if (name_.rfind("EVAL", 0) == 0)
    kind_multi_ctr_ = CO::MultiControlKind::EVAL;
  else if (base::_in(name_, {"EXEC", "MULTI", "DISCARD"}))
    kind_multi_ctr_ = CO::MultiControlKind::EXEC;
  else if (base::_in(name_, {"PUBLISH", "SUBSCRIBE", "UNSUBSCRIBE"}))
    kind_pubsub_ = CO::PubSubKind::REGULAR;
  else if (base::_in(name_, {"PSUBSCRIBE", "PUNSUBSCRIBE"}))
    kind_pubsub_ = CO::PubSubKind::PATTERN;
  else if (base::_in(name_, {"SPUBLISH", "SSUBSCRIBE", "SUNSUBSCRIBE"}))
    kind_pubsub_ = CO::PubSubKind::SHARDED;
  can_be_monitored_ = (opt_mask_ & CO::ADMIN) == 0 && name_ != "EXEC";
}

CommandId::~CommandId() {
  // Aliases share the same latency histogram, so we only close it if this is not an alias.
  if (latency_histogram_ && !is_alias_) {
    hdr_close(latency_histogram_);
  }
}

CommandId CommandId::Clone(const std::string_view name) const {
  CommandId cloned =
      CommandId{name.data(), opt_mask_, arity_, first_key_, last_key_, acl_categories_};
  cloned.handler_ = handler_;
  cloned.opt_mask_ = opt_mask_ | CO::HIDDEN;
  cloned.acl_categories_ = acl_categories_;
  cloned.implicit_acl_ = implicit_acl_;
  cloned.is_alias_ = true;

  // explicit sharing of the object since it's an alias we can do that.
  // I am assuming that the source object lifetime is at least as of the cloned object.
  if (cloned.latency_histogram_) {
    hdr_close(cloned.latency_histogram_);  // Free the histogram in the cloned object.
    cloned.latency_histogram_ = static_cast<hdr_histogram*>(latency_histogram_);
  }
  return cloned;
}

bool CommandId::IsTransactional() const {
  if (first_key_ > 0 || (opt_mask_ & CO::GLOBAL_TRANS) || (opt_mask_ & CO::NO_KEY_TRANSACTIONAL))
    return true;

  if (name_ == "EVAL" || name_ == "EVALSHA" || name_ == "EVAL_RO" || name_ == "EVALSHA_RO" ||
      name_ == "EXEC")
    return true;

  return false;
}

bool CommandId::IsMultiTransactional() const {
  return kind_multi_ctr_.has_value();
}

optional<facade::ErrorReply> CommandId::Validate(CmdArgList tail_args) const {
  if ((arity() > 0 && tail_args.size() + 1 != size_t(arity())) ||
      (arity() < 0 && tail_args.size() + 1 < size_t(-arity()))) {
    string prefix;
    if (name() == "EXEC")
      prefix = "-EXECABORT Transaction discarded because of: ";
    return facade::ErrorReply{prefix + facade::WrongNumArgsError(name()), kSyntaxErrType};
  }

  if ((opt_mask() & CO::INTERLEAVED_KEYS)) {
    if ((name() == "JSON.MSET" && tail_args.size() % 3 != 0) ||
        (name() == "MSET" && tail_args.size() % 2 != 0))
      return facade::ErrorReply{facade::WrongNumArgsError(name()), kSyntaxErrType};
  }

  if (validator_)
    return validator_(tail_args);
  return nullopt;
}

void CommandId::ResetStats(unsigned thread_index) {
  command_stats_[thread_index] = {0, 0};
  if (hdr_histogram* h = latency_histogram_; h != nullptr) {
    hdr_reset(h);
  }
}

void CommandId::RecordLatency(unsigned tid, uint64_t latency_usec) const {
  auto& ent = command_stats_[tid];

  ++ent.first;
  ent.second += latency_usec;

  if (latency_histogram_) {
    hdr_record_value(latency_histogram_, latency_usec);
  }
}

void CommandId::AsyncToSync(AsyncHandlerReply (*f)(CmdArgList, CommandContext*)) {
  handler_ = [f](CmdArgList args, CommandContext* cntx) {
    auto reply = f(args, cntx);
    if (std::holds_alternative<facade::ErrorReply>(reply)) {
      cntx->SendError(std::get<facade::ErrorReply>(reply));
    } else {
      DCHECK(!cntx->IsDeferredReply()) << "Sync and deferred?";
      cntx->tx->Blocker()->Wait();
      std::get<AsyncHandlerReplier>(reply)(cntx->rb());
    }
  };
}

void CommandId::WrapAsync(AsyncHandlerReply (*f)(CmdArgList, CommandContext*)) {
  async_handler_ = [f](CmdArgList args, CommandContext* cntx) {
    auto reply = f(args, cntx);
    if (std::holds_alternative<facade::ErrorReply>(reply)) {
      cntx->SendError(std::get<facade::ErrorReply>(reply));
    } else {
      cntx->keepalive_tx = cntx->tx;
      cntx->task_blocker = cntx->tx->Blocker();
      cntx->replier = std::move(std::get<AsyncHandlerReplier>(reply));
    }
  };
}

CommandRegistry::CommandRegistry() {
  cmd_rename_map_ = ParseCmdlineArgMap(FLAGS_rename_command);

  for (const string& name : GetFlag(FLAGS_restricted_commands)) {
    restricted_cmds_.emplace(AsciiStrToUpper(name));
  }

  for (const string& name : GetFlag(FLAGS_oom_deny_commands)) {
    oomdeny_cmds_.emplace(AsciiStrToUpper(name));
  }
}

void CommandRegistry::Init(unsigned int thread_count) {
  const CmdLineMapping original_to_alias = OriginalToAliasMap();
  absl::flat_hash_map<std::string, CommandId> alias_to_command_id;
  alias_to_command_id.reserve(original_to_alias.size());
  for (auto& [_, cmd] : cmd_map_) {
    cmd.Init(thread_count);
    if (auto it = original_to_alias.find(cmd.name()); it != original_to_alias.end()) {
      auto alias_cmd = cmd.Clone(it->second);
      alias_cmd.Init(thread_count);
      alias_to_command_id.insert({it->second, std::move(alias_cmd)});
    }
  }
  std::copy(std::make_move_iterator(alias_to_command_id.begin()),
            std::make_move_iterator(alias_to_command_id.end()),
            std::inserter(cmd_map_, cmd_map_.end()));
}

CommandRegistry& CommandRegistry::operator<<(CommandId cmd) {
  string k = string(cmd.name());

  absl::InlinedVector<std::string_view, 2> maybe_subcommand = StrSplit(cmd.name(), " ");
  const bool is_sub_command = maybe_subcommand.size() == 2;
  if (const auto it = cmd_rename_map_.find(maybe_subcommand.front()); it != cmd_rename_map_.end()) {
    if (it->second.empty()) {
      return *this;  // Incase of empty string we want to remove the command from registry.
    }
    k = is_sub_command ? StrCat(it->second, " ", maybe_subcommand[1]) : it->second;
  }

  if (restricted_cmds_.find(k) != restricted_cmds_.end()) {
    cmd.SetRestricted(true);
  }

  if (oomdeny_cmds_.find(k) != oomdeny_cmds_.end()) {
    cmd.SetFlag(CO::DENYOOM);
  }

  cmd.SetFamily(family_of_commands_.size() - 1);
  if (acl_category_)
    cmd.SetAclCategory(*acl_category_);

  if (!is_sub_command || absl::StartsWith(cmd.name(), "ACL")) {
    cmd.SetBitIndex(1ULL << bit_index_);
    family_of_commands_.back().emplace_back(k);
    ++bit_index_;
  } else {
    DCHECK(absl::StartsWith(k, family_of_commands_.back().back()));
    cmd.SetBitIndex(1ULL << (bit_index_ - 1));
  }
  CHECK(cmd_map_.emplace(k, std::move(cmd)).second) << k;

  return *this;
}

void CommandRegistry::StartFamily(std::optional<uint32_t> acl_category) {
  family_of_commands_.emplace_back();
  bit_index_ = 0;
  acl_category_ = acl_category;
}

std::string_view CommandRegistry::RenamedOrOriginal(std::string_view orig) const {
  if (!cmd_rename_map_.empty() && cmd_rename_map_.contains(orig)) {
    return cmd_rename_map_.find(orig)->second;
  }
  return orig;
}

CommandRegistry::FamiliesVec CommandRegistry::GetFamilies() {
  return std::move(family_of_commands_);
}

std::pair<const CommandId*, ParsedArgs> CommandRegistry::FindExtended(string_view cmd,
                                                                      ParsedArgs tail_args) const {
  if (cmd == RenamedOrOriginal("ACL"sv)) {
    if (tail_args.empty()) {
      return {Find(cmd), {}};
    }

    auto second_cmd = absl::AsciiStrToUpper(tail_args.Front());
    string full_cmd = StrCat(cmd, " ", second_cmd);

    return {Find(full_cmd), tail_args.Tail()};
  }

  const CommandId* res = Find(cmd);
  if (!res)
    return {nullptr, {}};

  // A workaround for XGROUP HELP that does not fit our static taxonomy of commands.
  if (tail_args.size() == 1 && res->name() == "XGROUP") {
    if (absl::EqualsIgnoreCase(tail_args.Front(), "HELP")) {
      res = Find("_XGROUP_HELP");
    }
  }
  return {res, tail_args};
}

absl::flat_hash_map<std::string, hdr_histogram*> CommandRegistry::LatencyMap() const {
  absl::flat_hash_map<std::string, hdr_histogram*> cmd_latencies;
  cmd_latencies.reserve(cmd_map_.size());
  for (const auto& [cmd_name, cmd] : cmd_map_) {
    cmd_latencies.insert({absl::AsciiStrToLower(cmd_name), cmd.GetLatencyHist()});
  }
  return cmd_latencies;
}

}  // namespace dfly
