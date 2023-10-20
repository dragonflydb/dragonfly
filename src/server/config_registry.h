// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/container/flat_hash_map.h>
#include <absl/flags/reflection.h>

#include "src/server/common.h"
#include "util/fibers/synchronization.h"

namespace dfly {

class ConfigRegistry {
 public:
  // Accepts the new value as argument. Return true if config was successfully updated.
  using WriteCb = std::function<bool(const absl::CommandLineFlag&)>;

  ConfigRegistry& Register(std::string_view name) {
    RegisterInternal(name, false, {});
    return *this;
  }

  ConfigRegistry& RegisterMutable(std::string_view name, WriteCb cb = {}) {
    RegisterInternal(name, true, std::move(cb));
    return *this;
  }

  enum class SetResult : uint8_t {
    OK,
    UNKNOWN,
    READONLY,
    INVALID,
    WRONG_ARGS_NUMBER,
    DUPLICATE,
  };

  SetResult Set(CmdArgList args);

  std::optional<std::string> Get(std::string_view config_name);

  void Reset();

  std::vector<std::string> List(std::string_view glob) const;

 private:
  void RegisterInternal(std::string_view name, bool is_mutable, WriteCb cb);

  mutable util::fb2::Mutex mu_;

  struct Entry {
    WriteCb cb;
    bool is_mutable;
  };

  absl::flat_hash_map<std::string, Entry> registry_ ABSL_GUARDED_BY(mu_);
};

extern ConfigRegistry config_registry;

}  // namespace dfly
