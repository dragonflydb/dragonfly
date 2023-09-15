// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/types/span.h>

#include <functional>
#include <optional>

#include "base/function2.hpp"
#include "facade/command_id.h"

namespace dfly {

class ConnectionContext;

namespace CO {

enum CommandOpt : uint32_t {
  READONLY = 1U << 0,
  FAST = 1U << 1,  // Unused?
  WRITE = 1U << 2,
  LOADING = 1U << 3,  // Command allowed during LOADING state.
  DENYOOM = 1U << 4,  // use-memory in redis.
  REVERSE_MAPPING = 1U << 5,

  VARIADIC_KEYS = 1U << 6,  // arg 2 determines number of keys. Relevant for ZUNIONSTORE, EVAL etc.

  ADMIN = 1U << 7,  // implies NOSCRIPT,
  NOSCRIPT = 1U << 8,
  BLOCKING = 1U << 9,  // implies REVERSE_MAPPING
  HIDDEN = 1U << 10,   // does not show in COMMAND command output

  GLOBAL_TRANS = 1U << 12,

  NO_AUTOJOURNAL = 1U << 15,  // Skip automatically logging command to journal inside transaction.

  // Allows commands without keys to respect transaction ordering and enables journaling by default
  NO_KEY_TRANSACTIONAL = 1U << 16,
};

const char* OptName(CommandOpt fl);

constexpr inline bool IsEvalKind(std::string_view name) {
  return name.compare(0, 4, "EVAL") == 0;
}

constexpr inline bool IsTransKind(std::string_view name) {
  return (name == "EXEC") || (name == "MULTI") || (name == "DISCARD");
}

static_assert(IsEvalKind("EVAL") && IsEvalKind("EVALSHA"));
static_assert(!IsEvalKind(""));

};  // namespace CO

// Per thread vector of command stats. Each entry is {cmd_calls, cmd_sum}.
using CmdCallStats = std::pair<uint64_t, uint64_t>;

class CommandId : public facade::CommandId {
 public:
  // NOTICE: name must be a literal string, otherwise metrics break! (see cmd_stats_map in
  // server_state.h)
  CommandId(const char* name, uint32_t mask, int8_t arity, int8_t first_key, int8_t last_key,
            int8_t step, uint32_t acl_categories);

  CommandId(CommandId&&) = default;

  void Init(unsigned thread_count) {
    command_stats_ = std::make_unique<CmdCallStats[]>(thread_count);
  }

  using Handler =
      fu2::function_base<true /*owns*/, true /*copyable*/, fu2::capacity_default,
                         false /* non-throwing*/, false /* strong exceptions guarantees*/,
                         void(CmdArgList, ConnectionContext*) const>;

  using ArgValidator = fu2::function_base<true, true, fu2::capacity_default, false, false,
                                          std::optional<facade::ErrorReply>(CmdArgList) const>;

  void Invoke(CmdArgList args, ConnectionContext* cntx) const;

  // Returns error if validation failed, otherwise nullopt
  std::optional<facade::ErrorReply> Validate(CmdArgList tail_args) const;

  bool IsTransactional() const;

  static const char* OptName(CO::CommandOpt fl);

  CommandId&& SetHandler(Handler f) && {
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

  void ResetStats(unsigned thread_index) {
    command_stats_[thread_index] = {0, 0};
  }

  CmdCallStats GetStats(unsigned thread_index) const {
    return command_stats_[thread_index];
  }

 private:
  std::unique_ptr<CmdCallStats[]> command_stats_;
  Handler handler_;
  ArgValidator validator_;
};

class CommandRegistry {
  absl::flat_hash_map<std::string_view, CommandId> cmd_map_;
  absl::flat_hash_map<std::string, std::string> cmd_rename_map_;

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
};

}  // namespace dfly
