// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/types/span.h>

#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "base/function2.hpp"
#include "facade/cmd_arg_parser.h"
#include "facade/command_id.h"
#include "facade/facade_types.h"

struct hdr_histogram;

namespace dfly {

namespace CO {

enum CommandOpt : uint32_t {
  READONLY = 1U << 0,
  FAST = 1U << 1,       // Unused?
  JOURNALED = 1U << 2,  // Command is logged to AOF / Journal.
  LOADING = 1U << 3,    // Command allowed during LOADING state.
  DENYOOM = 1U << 4,    // use-memory in redis.

  DANGEROUS = 1U << 5,  // Dangerous commands are logged when used

  VARIADIC_KEYS = 1U << 6,  // arg 2 determines number of keys. Relevant for ZUNIONSTORE, EVAL etc.

  ADMIN = 1U << 7,  // implies NOSCRIPT,
  NOSCRIPT = 1U << 8,
  BLOCKING = 1U << 9,
  HIDDEN = 1U << 10,  // does not show in COMMAND command output
  GLOBAL_TRANS = 1U << 12,
  STORE_LAST_KEY = 1U << 13,  // The command my have a store key as the last argument.

  NO_AUTOJOURNAL = 1U << 15,  // Skip automatically logging command to journal inside transaction.

  // Allows commands without keys to respect transaction ordering and enables journaling by default
  NO_KEY_TRANSACTIONAL = 1U << 16,
  NO_KEY_TX_SPAN_ALL = 1U << 17,  // All shards are active for the no-key-transactional command

  // The same callback can be run multiple times without corrupting the result. Used for
  // opportunistic optimizations where inconsistencies can only be detected afterwards.
  IDEMPOTENT = 1U << 18,
};

};  // namespace CO

// Per thread vector of command stats. Each entry is {cmd_calls, cmd_latency_agg in usec}.
using CmdCallStats = std::pair<uint64_t, uint64_t>;

class CommandId;
class CommandContext;

facade::CmdArgParser MakeParserFromContext(CommandContext* cntx);

// TODO: move it to helio
// Makes sure that the POD T that is passed to the constructor is reset to default state
template <typename T> class MoveOnly {
 public:
  MoveOnly() = default;

  MoveOnly(const MoveOnly&) = delete;
  MoveOnly& operator=(const MoveOnly&) = delete;

  MoveOnly(MoveOnly&& t) noexcept : value_(std::move(t.value_)) {
    t.value_ = T{};  // Reset the passed value to default state
  }

  MoveOnly& operator=(const T& t) noexcept {
    value_ = t;
    return *this;
  }

  operator const T&() const {  // NOLINT
    return value_;
  }

 private:
  T value_{};
};

class CommandId : public facade::CommandId {
 public:
  using CmdArgList = facade::CmdArgList;

  // NOTICE: name must be a literal string, otherwise metrics break! (see cmd_call_stats in
  // metrics.h)
  CommandId(const char* name, uint32_t mask, int8_t arity, int8_t first_key, int8_t last_key,
            std::optional<uint32_t> acl_categories = std::nullopt);

  CommandId(CommandId&& o) = default;

  ~CommandId();

  [[nodiscard]] CommandId Clone(std::string_view name) const;

  void Init(unsigned thread_count) {
    command_stats_ = std::make_unique<StatsCell[]>(thread_count);
  }

  using Handler = fu2::function_base<true, true, fu2::capacity_default, false, false,
                                     void(facade::CmdArgParser, CommandContext*) const>;
  using ArgValidator =
      fu2::function_base<true, true, fu2::capacity_default, false, false,
                         std::optional<facade::ErrorReply>(const facade::ParsedArgs&) const>;

  // Returns the invoke time in usec.
  void Invoke(CmdArgList args, CommandContext* cmd_cntx) const {
    handler_(facade::CmdArgParser{args}, cmd_cntx);
  }

  // Returns error if validation failed, otherwise nullopt
  std::optional<facade::ErrorReply> Validate(const facade::ParsedArgs& args) const;

  bool IsTransactional() const;

  bool IsMultiTransactional() const;

  bool IsReadOnly() const {
    return opt_mask_ & CO::READONLY;
  }

  bool IsJournaled() const {
    return opt_mask_ & CO::JOURNALED;
  }

  bool IsBlocking() const {
    return opt_mask_ & CO::BLOCKING;
  }

  // See deduction logic for details. We don't monitor ADMIN commands
  // and log the final `EXEC` command manually at the end.
  bool CanBeMonitored() const {
    return kind_mask_ & CAN_MONITOR;
  }

  int8_t interleaved_step() const {
    return interleave_step_;
  }

  template <typename RT> CommandId&& SetHandler(RT f(facade::CmdArgParser, CommandContext*)) && {
    handler_ = [f](facade::CmdArgParser parser, CommandContext* cntx) {
      f(std::move(parser), cntx);
    };
    return std::move(*this);
  }

  template <typename RT>
  CommandId&& SetAsyncHandler(RT f(facade::CmdArgParser, CommandContext*)) && {
    kind_mask_ |= SUPPORT_ASYNC;
    return std::move(*this).SetHandler(f);
  }

  CommandId&& SetHandler(Handler f, bool async_support = false) && {
    if (async_support)
      kind_mask_ |= SUPPORT_ASYNC;
    handler_ = std::move(f);
    return std::move(*this);
  }

  CommandId&& SetValidator(ArgValidator f) && {
    validator_ = std::move(f);
    return std::move(*this);
  }

  bool is_multi_key() const {
    return (last_key_ != first_key_) || (opt_mask_ & CO::VARIADIC_KEYS);
  }

  void ResetStats(unsigned thread_index);

  CmdCallStats GetStats(unsigned thread_index) const {
    return command_stats_[thread_index].stats;
  }

  void SetAclCategory(uint32_t mask) {
    if (kind_mask_ & IMPLICIT_ACL)
      acl_categories_ |= mask;
  }

  bool IsAlias() const {
    return kind_mask_ & IS_ALIAS;
  }

  hdr_histogram* GetLatencyHist() const {
    return latency_histogram_;
  }

  // Returns true if this command belongs to the pub/sub family (any variant).
  bool IsPubSub() const {
    return kind_mask_ & (PUBSUB_REGULAR | PUBSUB_PATTERN | PUBSUB_SHARDED);
  }

  bool IsShardedPubSub() const {
    return kind_mask_ & PUBSUB_SHARDED;
  }

  bool IsPatternPubSub() const {
    return kind_mask_ & PUBSUB_PATTERN;
  }

  bool IsSPublish() const {
    return kind_mask_ & SPUBLISH;
  }

  bool IsPublish() const {
    return kind_mask_ & PUBLISH;
  }

  // (UN)SUBSCRIBE and their P*/S* variants - the pub/sub commands that emit one reply per channel
  // and may not run inside a transaction. Excludes PUBLISH / SPUBLISH.
  bool IsSubscribeFamily() const {
    return IsPubSub() && !IsPublish() && !IsSPublish();
  }

  // EVAL / EVAL_RO / EVALSHA / EVALSHA_RO.
  bool IsEvalGroup() const {
    return kind_mask_ & EVAL_CTRL;
  }

  // EXEC / MULTI / DISCARD.
  bool IsExecGroup() const {
    return kind_mask_ & EXEC_CTRL;
  }

  bool IsExec() const {
    return kind_mask_ & EXEC;
  }

  bool IsMulti() const {
    return kind_mask_ & MULTI;
  }

  bool IsReplConf() const {
    return kind_mask_ & REPLCONF;
  }

  bool IsQuit() const {
    return kind_mask_ & QUIT;
  }

  void RecordLatency(unsigned tid, uint64_t latency_usec) const;

  bool SupportsAsync() const {
    return kind_mask_ & SUPPORT_ASYNC;
  }

 private:
  // Engine-derived command identity and attributes, computed once in the constructor from the
  // command name and opt_mask, and stored in kind_mask_ so that hot-path queries are single bit
  // tests instead of string comparisons. This is an internal detail of CommandId, distinct from
  // CO::CommandOpt which holds author-declared capabilities set at registration time.
  enum CmdKind : uint16_t {
    // pub/sub family. The *PUBLISH bits additionally mark the publishing command within a family.
    PUBSUB_REGULAR = 1U << 0,  // PUBLISH / SUBSCRIBE / UNSUBSCRIBE
    PUBSUB_PATTERN = 1U << 1,  // PSUBSCRIBE / PUNSUBSCRIBE
    PUBSUB_SHARDED = 1U << 2,  // SPUBLISH / SSUBSCRIBE / SUNSUBSCRIBE
    PUBLISH = 1U << 3,         // PUBLISH only
    SPUBLISH = 1U << 4,        // SPUBLISH only

    // multi command control family.
    EVAL_CTRL = 1U << 5,  // EVAL / EVAL_RO / EVALSHA / EVALSHA_RO
    EXEC_CTRL = 1U << 6,  // EXEC / MULTI / DISCARD
    EXEC = 1U << 7,       // EXEC only
    MULTI = 1U << 8,      // MULTI only

    REPLCONF = 1U << 9,  // REPLCONF only
    QUIT = 1U << 10,     // QUIT only

    // attributes (formerly standalone bool members).

    // Command shows up in MONITOR output. Set for all non-ADMIN commands except EXEC (the final
    // EXEC of a transaction is logged manually after its body).
    CAN_MONITOR = 1U << 11,
    // ACL categories were derived from opt_mask (no explicit categories given at registration).
    // Only such commands may have categories augmented later via SetAclCategory().
    IMPLICIT_ACL = 1U << 12,
    // This CommandId is an alias of another command, produced by Clone().
    IS_ALIAS = 1U << 13,
    // Command has an async handler (set via SetAsyncHandler / SetHandler with async_support).
    SUPPORT_ASYNC = 1U << 14,
  };

  // CmdKind bits. Trivially copied by the move ctor.
  uint16_t kind_mask_ = 0;
  int8_t interleave_step_{0};

  struct alignas(64) StatsCell {
    CmdCallStats stats;
  };

  std::unique_ptr<StatsCell[]> command_stats_;
  Handler handler_;
  ArgValidator validator_;
  MoveOnly<hdr_histogram*> latency_histogram_;  // Histogram for command latency in usec
};

class CommandRegistry {
 public:
  CommandRegistry();

  void Init(unsigned thread_count);

  CommandRegistry& operator<<(CommandId cmd);

  const CommandId* Find(std::string_view cmd) const {
    auto it = cmd_map_.find(cmd);
    return it == cmd_map_.end() ? nullptr : &it->second;
  }

  CommandId* Find(std::string_view cmd) {
    auto it = cmd_map_.find(cmd);
    return it == cmd_map_.end() ? nullptr : &it->second;
  }

  using TraverseCb = std::function<void(std::string_view, const CommandId&)>;

  void Traverse(TraverseCb cb) {
    for (const auto& k_v : cmd_map_) {
      cb(k_v.first, k_v.second);
    }
  }

  void ResetCallStats(unsigned thread_index) {
    for (auto& k_v : cmd_map_) {
      k_v.second.ResetStats(thread_index);
    }
  }

  size_t size() const {
    return cmd_map_.size();
  }

  // `out` must hold >= size() entries. Reads only this proactor's own cells, so it is race-free
  // when run concurrently inside the GetMetrics fan-out.
  void CollectThreadCallStats(unsigned thread_index, CmdCallStats* out) const {
    size_t i = 0;
    for (const auto& k_v : cmd_map_)
      out[i++] = k_v.second.GetStats(thread_index);
  }

  // Maps the merged per-command counters (registry order, see CollectThreadCallStats) to lowercase
  // names, skipping zero-call commands. `merged` is empty (not collected) or whole-registry sized.
  absl::flat_hash_map<std::string, CmdCallStats> NamedCallStats(
      const std::vector<CmdCallStats>& merged) const;

  void StartFamily(std::optional<uint32_t> acl_category = std::nullopt);

  using FamiliesVec = std::vector<std::vector<std::string>>;
  FamiliesVec GetFamilies();

  std::pair<const CommandId*, facade::ParsedArgs> FindExtended(facade::ParsedArgs args) const;

  absl::flat_hash_map<std::string, hdr_histogram*> LatencyMap() const;

 private:
  absl::flat_hash_map<std::string, CommandId> cmd_map_;
  absl::flat_hash_set<std::string> restricted_cmds_;

  FamiliesVec family_of_commands_;
  size_t bit_index_;
  std::optional<uint32_t> acl_category_;  // category of family currently being built
};

}  // namespace dfly
