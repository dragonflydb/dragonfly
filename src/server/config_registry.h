// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/container/flat_hash_map.h>
#include <absl/flags/reflection.h>

#include "util/fibers/synchronization.h"

namespace dfly {

class ConfigRegistry {
 public:
  // Accepts the new value as argument. Return true if config was successfully updated.
  using WriteCb = std::function<bool(const absl::CommandLineFlag&)>;

  ConfigRegistry& Register(std::string_view name, WriteCb cb);

  // Returns true if the value was updated.
  bool Set(std::string_view config_name, std::string_view value);

  void Reset();

 private:
  util::fb2::Mutex mu_;
  absl::flat_hash_map<std::string, WriteCb> registry_ ABSL_GUARDED_BY(mu_);
};

extern ConfigRegistry config_registry;

}  // namespace dfly
