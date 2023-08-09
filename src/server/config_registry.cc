// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "src/server/config_registry.h"

#include <absl/flags/reflection.h>

#include "base/logging.h"

namespace dfly {

using namespace std;

ConfigRegistry& ConfigRegistry::Register(std::string_view name, WriteCb cb) {
  absl::CommandLineFlag* flag = absl::FindCommandLineFlag(name);
  CHECK(flag) << "Unknown config name: " << name;

  unique_lock lk(mu_);
  auto [it, inserted] = registry_.emplace(name, std::move(cb));
  CHECK(inserted) << "Duplicate config name: " << name;
  return *this;
}

// Returns true if the value was updated.
bool ConfigRegistry::Set(std::string_view config_name, std::string_view value) {
  unique_lock lk(mu_);
  auto it = registry_.find(config_name);
  if (it == registry_.end())
    return false;
  auto cb = it->second;
  lk.unlock();

  absl::CommandLineFlag* flag = absl::FindCommandLineFlag(config_name);
  CHECK(flag);
  string error;
  if (!flag->ParseFrom(value, &error))
    return false;

  return cb(*flag);
}

std::optional<std::string> ConfigRegistry::Get(std::string_view config_name) {
  unique_lock lk(mu_);
  auto it = registry_.find(config_name);
  if (it == registry_.end())
    return std::nullopt;
  auto cb = it->second;
  lk.unlock();

  absl::CommandLineFlag* flag = absl::FindCommandLineFlag(config_name);
  CHECK(flag);
  return flag->CurrentValue();
}

void ConfigRegistry::Reset() {
  unique_lock lk(mu_);
  registry_.clear();
}

ConfigRegistry config_registry;

}  // namespace dfly
