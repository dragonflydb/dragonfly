// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/flat_hash_map.h>

namespace facade {

template <typename CMD> class CommandRegistry {
  absl::flat_hash_map<std::string, CMD> cmd_map_;

 public:
  CommandRegistry() = default;

  CommandRegistry& operator<<(CMD cmd);

  const CMD* Find(std::string_view cmd) const {
    auto it = cmd_map_.find(cmd);
    return it == cmd_map_.end() ? nullptr : &it->second;
  }

  CMD* Find(std::string_view cmd) {
    auto it = cmd_map_.find(cmd);
    return it == cmd_map_.end() ? nullptr : &it->second;
  }

  using TraverseCb = std::function<void(std::string_view, const CMD&)>;

  void Traverse(TraverseCb cb) {
    for (const auto& k_v : cmd_map_) {
      cb(k_v.first, k_v.second);
    }
  }
};

template <typename CMD> CommandRegistry<CMD>& CommandRegistry<CMD>::operator<<(CMD cmd) {
  std::string k{cmd.name()};
  cmd_map_.emplace(k, std::move(cmd));

  return *this;
}

}  // namespace facade
