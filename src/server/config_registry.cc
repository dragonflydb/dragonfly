// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/config_registry.h"

#include <absl/flags/reflection.h>
#include <absl/strings/str_replace.h>

#include "base/logging.h"
#include "server/common.h"

extern "C" {
#include "redis/util.h"
}

namespace dfly {
namespace {
using namespace std;

string NormalizeConfigName(string_view name) {
  return absl::StrReplaceAll(name, {{"-", "_"}});
}
}  // namespace

// Returns true if the value was updated.
auto ConfigRegistry::Set(string_view config_name, string_view value) -> SetResult {
  string name = NormalizeConfigName(config_name);

  util::fb2::LockGuard lk(mu_);
  auto it = registry_.find(name);
  if (it == registry_.end())
    return SetResult::UNKNOWN;
  if (!it->second.is_mutable)
    return SetResult::READONLY;

  auto cb = it->second.cb;

  absl::CommandLineFlag* flag = absl::FindCommandLineFlag(name);
  CHECK(flag) << config_name;
  if (string error; !flag->ParseFrom(value, &error)) {
    LOG(WARNING) << error;
    return SetResult::INVALID;
  }

  bool success = !cb || cb(*flag);
  return success ? SetResult::OK : SetResult::INVALID;
}

optional<string> ConfigRegistry::Get(string_view config_name) {
  string name = NormalizeConfigName(config_name);

  {
    util::fb2::LockGuard lk(mu_);
    if (!registry_.contains(name))
      return nullopt;
  }

  absl::CommandLineFlag* flag = absl::FindCommandLineFlag(name);
  CHECK(flag);
  return flag->CurrentValue();
}

void ConfigRegistry::Reset() {
  util::fb2::LockGuard lk(mu_);
  registry_.clear();
}

vector<string> ConfigRegistry::List(string_view glob) const {
  string normalized_glob = NormalizeConfigName(glob);

  vector<string> res;
  util::fb2::LockGuard lk(mu_);
  for (const auto& [name, _] : registry_) {
    if (stringmatchlen(normalized_glob.data(), normalized_glob.size(), name.data(), name.size(), 1))
      res.push_back(name);
  }
  return res;
}

void ConfigRegistry::RegisterInternal(string_view config_name, bool is_mutable, WriteCb cb) {
  string name = NormalizeConfigName(config_name);

  absl::CommandLineFlag* flag = absl::FindCommandLineFlag(name);
  CHECK(flag) << "Unknown config name: " << name;

  util::fb2::LockGuard lk(mu_);
  auto [it, inserted] = registry_.emplace(name, Entry{std::move(cb), is_mutable});
  CHECK(inserted) << "Duplicate config name: " << name;
}

ConfigRegistry config_registry;

}  // namespace dfly
