// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/types/span.h>

#include <functional>

#include "base/function2.hpp"
#include "server/common_types.h"

namespace dfly {

class ConnectionContext;

namespace CO {

enum CommandOpt : uint32_t {
  READONLY = 1,
  FAST = 2,
  WRITE = 4,
  LOADING = 8,
  DENYOOM = 0x10,  // use-memory in redis.
  STALE = 0x20,
  RANDOM = 0x40,
};

const char* OptName(CommandOpt fl);

};  // namespace CO

class CommandId {
 public:
  using CmdFunc = std::function<void(CmdArgList, ConnectionContext*)>;

  /**
   * @brief Construct a new Command Id object
   *
   * @param name
   * @param mask
   * @param arity -     positive if command has fixed number of required arguments
   *                    negative if command has minimum number of required arguments, but may have
   * more.
   * @param first_key - position of first key in argument list
   * @param last_key  - position of last key in argument list,
   *                    -1 means the last key index is (arg_length - 1), -2 means that the last key
   * index is (arg_length - 2).
   * @param step -      step count for locating repeating keys
   */
  CommandId(const char* name, uint32_t mask, int8_t arity, int8_t first_key, int8_t last_key,
            int8_t step);

  const char* name() const {
    return name_;
  }

  int arity() const {
    return arity_;
  }

  uint32_t opt_mask() const {
    return opt_mask_;
  }

  int8_t first_key_pos() const {
    return first_key_;
  }

  int8_t last_key_pos() const {
    return last_key_;
  }

  bool is_multi_key() const {
    return last_key_ != first_key_;
  }

  int8_t key_arg_step() const {
    return step_key_;
  }

  CommandId& AssignCallback(CmdFunc f) {
    func_ = std::move(f);
    return *this;
  }

  void Invoke(CmdArgList args, ConnectionContext* cntx) const {
    func_(std::move(args), cntx);
  }

  static const char* OptName(CO::CommandOpt fl);
  static uint32_t OptCount(uint32_t mask);

 private:
  const char* name_;

  uint32_t opt_mask_;
  int8_t arity_;
  int8_t first_key_;
  int8_t last_key_;
  int8_t step_key_;

  CmdFunc func_;
};

class CommandRegistry {
  absl::flat_hash_map<std::string_view, CommandId> cmd_map_;

 public:
  CommandRegistry();

  CommandRegistry& operator<<(CommandId cmd) {
    const char* k = cmd.name();
    cmd_map_.emplace(k, std::move(cmd));

    return *this;
  }

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

 private:
  // Implements COMMAND functionality.
  void Command(CmdArgList args, ConnectionContext* cntx);
};

}  // namespace dfly
