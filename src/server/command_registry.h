// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/types/span.h>

#include <functional>

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
  NO_KEY_JOURNAL = 1U << 16,  // Command with no keys that need to be journaled
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

class CommandId : public facade::CommandId {
 public:
  CommandId(const char* name, uint32_t mask, int8_t arity, int8_t first_key, int8_t last_key,
            int8_t step);

  using Handler =
      fu2::function_base<true /*owns*/, true /*copyable*/, fu2::capacity_default,
                         false /* non-throwing*/, false /* strong exceptions guarantees*/,
                         void(CmdArgList, ConnectionContext*) const>;

  using ArgValidator = fu2::function_base<true, true, fu2::capacity_default, false, false,
                                          bool(CmdArgList, ConnectionContext*) const>;

  bool is_multi_key() const {
    return (last_key_ != first_key_) || (opt_mask_ & CO::VARIADIC_KEYS);
  }

  CommandId& SetHandler(Handler f) {
    handler_ = std::move(f);
    return *this;
  }

  CommandId& SetValidator(ArgValidator f) {
    validator_ = std::move(f);

    return *this;
  }

  void Invoke(CmdArgList args, ConnectionContext* cntx) const {
    handler_(std::move(args), cntx);
  }

  // Returns true if validation succeeded.
  bool Validate(CmdArgList args, ConnectionContext* cntx) const {
    return !validator_ || validator_(std::move(args), cntx);
  }

  bool IsTransactional() const;

  static const char* OptName(CO::CommandOpt fl);

 private:
  Handler handler_;
  ArgValidator validator_;
};

class CommandRegistry {
  absl::flat_hash_map<std::string_view, CommandId> cmd_map_;

 public:
  CommandRegistry() = default;

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
};

}  // namespace dfly
