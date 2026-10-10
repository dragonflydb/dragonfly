// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/container/flat_hash_map.h>
#include <absl/flags/reflection.h>
#include <absl/types/span.h>

#include <utility>

#include "util/fibers/synchronization.h"

namespace dfly {

// Allows reading and modifying pre-registered configuration values by string names.
// This class treats dashes (-) are as underscores (_).
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

  template <typename T>
  ConfigRegistry& RegisterSetter(std::string_view name, std::function<void(const T&)> f) {
    ValidateCustomSetter(name,
                         [](const absl::CommandLineFlag& flag) { return flag.IsOfType<T>(); });

    return RegisterMutable(name, [f](const absl::CommandLineFlag& flag) {
      auto res = flag.TryGet<T>();
      if (res.has_value()) {
        f(*res);
        return true;
      }
      return false;
    });
  }

  enum class SetResult : uint8_t {
    OK,
    UNKNOWN,
    READONLY,
    INVALID,
    DUPLICATE,
  };

  // Result of SetMultiple(): the overall result, plus, if not OK, the index into the
  // input `params` span of the pair that caused the failure.
  struct MultiSetResult {
    SetResult result = SetResult::OK;
    size_t failed_index = 0;
  };

  // Returns true if the value was updated.
  SetResult Set(std::string_view config_name, std::string_view value) ABSL_LOCKS_EXCLUDED(mu_);

  // Atomically sets multiple config_name/value pairs: either all of them are applied, or,
  // if any of them fails, none are (pairs already applied earlier in this call are rolled
  // back to their prior values).
  MultiSetResult SetMultiple(absl::Span<const std::pair<std::string_view, std::string_view>> params)
      ABSL_LOCKS_EXCLUDED(mu_);

  std::optional<std::string> Get(std::string_view config_name) ABSL_LOCKS_EXCLUDED(mu_);

  absl::CommandLineFlag* GetFlag(std::string_view config_name) ABSL_LOCKS_EXCLUDED(mu_);

  void Reset();

  std::vector<std::string> List(std::string_view glob) const ABSL_LOCKS_EXCLUDED(mu_);

 private:
  void RegisterInternal(std::string_view name, bool is_mutable, WriteCb cb)
      ABSL_LOCKS_EXCLUDED(mu_);
  void ValidateCustomSetter(std::string_view name, WriteCb setter) const;

  mutable util::fb2::Mutex mu_;

  struct Entry {
    WriteCb cb;
    bool is_mutable;
  };

  absl::flat_hash_map<std::string, Entry> registry_ ABSL_GUARDED_BY(mu_);
};

inline ConfigRegistry config_registry;

// Convert internal flag name back to user-facing format for search parameters
// Example: search_query_string_bytes -> search.query-string-bytes
std::string DenormalizeConfigName(std::string_view name);

}  // namespace dfly
