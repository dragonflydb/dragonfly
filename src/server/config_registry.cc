// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "src/server/config_registry.h"

#include <absl/flags/reflection.h>

#include "base/logging.h"

extern "C" {
#include "redis/util.h"
}

namespace dfly {

using namespace std;

// Returns true if the value was updated.
auto ConfigRegistry::Set(std::string_view config_name, std::string_view value) -> SetResult {
  unique_lock lk(mu_);
  auto it = registry_.find(config_name);
  if (it == registry_.end())
    return SetResult::UNKNOWN;
  if (!it->second.is_mutable)
    return SetResult::READONLY;

  auto cb = it->second.cb;

  absl::CommandLineFlag* flag = absl::FindCommandLineFlag(config_name);
  CHECK(flag);
  if (string error; !flag->ParseFrom(value, &error)) {
    LOG(WARNING) << error;
    return SetResult::INVALID;
  }

  bool success = !cb || cb(*flag);
  return success ? SetResult::OK : SetResult::INVALID;
}

std::optional<std::string> ConfigRegistry::Get(std::string_view config_name) {
  unique_lock lk(mu_);
  if (!registry_.contains(config_name))
    return std::nullopt;
  lk.unlock();

  absl::CommandLineFlag* flag = absl::FindCommandLineFlag(config_name);
  CHECK(flag);
  return flag->CurrentValue();
}

void ConfigRegistry::Reset() {
  unique_lock lk(mu_);
  registry_.clear();
}

vector<string> ConfigRegistry::List(string_view glob) const {
  vector<string> res;
  unique_lock lk(mu_);
  for (const auto& [name, _] : registry_) {
    if (stringmatchlen(glob.data(), glob.size(), name.data(), name.size(), 1))
      res.push_back(name);
  }
  return res;
}

void ConfigRegistry::RegisterInternal(std::string_view name, bool is_mutable, WriteCb cb) {
  absl::CommandLineFlag* flag = absl::FindCommandLineFlag(name);
  CHECK(flag) << "Unknown config name: " << name;

  unique_lock lk(mu_);
  auto [it, inserted] = registry_.emplace(name, Entry{std::move(cb), is_mutable});
  CHECK(inserted) << "Duplicate config name: " << name;
}

ConfigRegistry config_registry;

}  // namespace dfly
