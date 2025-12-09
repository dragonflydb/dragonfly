// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/types/span.h>
#include <hdr/hdr_histogram.h>

#include <functional>
#include <optional>

#include "base/function2.hpp"
#include "facade/command_id.h"
#include "facade/facade_types.h"

namespace facade {
class SinkReplyBuilder;
}  // namespace facade

namespace dfly {

class ConnectionContext;
class Transaction;
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
  HIDDEN = 1U << 10,            // does not show in COMMAND command output
  INTERLEAVED_KEYS = 1U << 11,  // keys are interleaved with arguments
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

enum class PubSubKind : uint8_t { REGULAR = 0, PATTERN = 1, SHARDED = 2 };

// Commands controlling any multi command execution.
// They often need to be handled separately from regular commands in many contexts
enum class MultiControlKind : uint8_t {
  EVAL,  // EVAL, EVAL_RO, EVALSHA, EVALSHA_RO
  EXEC,  // EXEC, MULTI, DISCARD
};

};  // namespace CO

// Per thread vector of command stats. Each entry is {cmd_calls, cmd_latency_agg in usec}.
using CmdCallStats = std::pair<uint64_t, uint64_t>;

struct CommandContext {
  CommandContext(Transaction* _tx, facade::SinkReplyBuilder* _rb, ConnectionContext* cntx)
      : tx(_tx), rb(_rb), conn_cntx(cntx) {
  }

  Transaction* tx;
  facade::SinkReplyBuilder* rb;
  ConnectionContext* conn_cntx;
};

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
  T value_;
};

class CommandId : public facade::CommandId {
 public:
  using CmdArgList = facade::CmdArgList;

  // NOTICE: name must be a literal string, otherwise metrics break! (see cmd_stats_map in
  // server_state.h)
  CommandId(const char* name, uint32_t mask, int8_t arity, int8_t first_key, int8_t last_key,
            std::optional<uint32_t> acl_categories = std::nullopt);

  CommandId(CommandId&& o) = default;

  ~CommandId();

  [[nodiscard]] CommandId Clone(std::string_view name) const;

  void Init(unsigned thread_count) {
    command_stats_ = std::make_unique<CmdCallStats[]>(thread_count);
  }

  using Handler3 = fu2::function_base<true, true, fu2::capacity_default, false, false,
                                      void(CmdArgList, const CommandContext&) const>;
  using ArgValidator = fu2::function_base<true, true, fu2::capacity_default, false, false,
                                          std::optional<facade::ErrorReply>(CmdArgList) const>;

  // Returns the invoke time in usec.
  uint64_t Invoke(CmdArgList args, const CommandContext& cmd_cntx) const;

  // Returns error if validation failed, otherwise nullopt
  std::optional<facade::ErrorReply> Validate(CmdArgList tail_args) const;

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

  CommandId&& SetHandler(Handler3 f) && {
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
    return command_stats_[thread_index];
  }

  void SetAclCategory(uint32_t mask) {
    if (implicit_acl_)
      acl_categories_ |= mask;
  }

  bool IsAlias() const {
    return is_alias_;
  }

  hdr_histogram* LatencyHist() const;

  std::optional<CO::PubSubKind> PubSubKind() const {
    return kind_pubsub_;
  }

  // Returns value if this command controls multi command execution (EVAL, EXEC & helpers)
  std::optional<CO::MultiControlKind> MultiControlKind() const {
    return kind_multi_ctr_;
  }

 private:
  std::optional<CO::PubSubKind> kind_pubsub_;
  std::optional<CO::MultiControlKind> kind_multi_ctr_;

  // The following fields must copy manually in the move constructor.
  bool implicit_acl_;
  bool is_alias_{false};
  std::unique_ptr<CmdCallStats[]> command_stats_;
  Handler3 handler_;
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

  void MergeCallStats(unsigned thread_index,
                      std::function<void(std::string_view, const CmdCallStats&)> cb) const {
    for (const auto& k_v : cmd_map_) {
      auto src = k_v.second.GetStats(thread_index);
      if (src.first == 0)
        continue;
      cb(k_v.second.name(), src);
    }
  }

  void StartFamily(std::optional<uint32_t> acl_category = std::nullopt);

  std::string_view RenamedOrOriginal(std::string_view orig) const;

  using FamiliesVec = std::vector<std::vector<std::string>>;
  FamiliesVec GetFamilies();

  std::pair<const CommandId*, facade::ParsedArgs> FindExtended(std::string_view cmd,
                                                               facade::ParsedArgs tail_args) const;

  absl::flat_hash_map<std::string, hdr_histogram*> LatencyMap() const;

 private:
  absl::flat_hash_map<std::string, CommandId> cmd_map_;
  absl::flat_hash_map<std::string, std::string> cmd_rename_map_;
  absl::flat_hash_set<std::string> restricted_cmds_;
  absl::flat_hash_set<std::string> oomdeny_cmds_;

  FamiliesVec family_of_commands_;
  size_t bit_index_;
  std::optional<uint32_t> acl_category_;  // category of family currently being built
};

}  // namespace dfly
