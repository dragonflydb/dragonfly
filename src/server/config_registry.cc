// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/config_registry.h"

#include <absl/flags/reflection.h>
#include <absl/strings/match.h>
#include <absl/strings/str_replace.h>

#include "base/logging.h"
#include "core/glob_matcher.h"
#include "server/common.h"

namespace dfly {
namespace {
using namespace std;

string NormalizeConfigName(string_view name) {
  return absl::StrReplaceAll(name, {{"-", "_"}, {".", "_"}});
}
}  // namespace

// Convert internal flag name back to user-facing format
// Example: search_query_string_bytes -> search.query-string-bytes
string DenormalizeConfigName(string_view name) {
  string result{name};
  if (absl::StartsWith(result, "search_")) {
    // Replace first underscore after "search" with dot
    result.replace(6, 1, ".");
    // Replace remaining underscores with dashes
    for (size_t i = 7; i < result.size(); ++i) {
      if (result[i] == '_') {
        result[i] = '-';
      }
    }
  }
  return result;
}

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

absl::CommandLineFlag* ConfigRegistry::GetFlag(std::string_view config_name) {
  string name = NormalizeConfigName(config_name);

  {
    util::fb2::LockGuard lk(mu_);
    if (!registry_.contains(name))
      return nullptr;
  }

  absl::CommandLineFlag* flag = absl::FindCommandLineFlag(name);
  CHECK(flag);
  return flag;
}

optional<string> ConfigRegistry::Get(string_view config_name) {
  absl::CommandLineFlag* flag = GetFlag(config_name);
  return flag ? flag->CurrentValue() : optional<string>();
}

void ConfigRegistry::Reset() {
  util::fb2::LockGuard lk(mu_);
  registry_.clear();
}

vector<string> ConfigRegistry::List(string_view glob) const {
  string normalized_glob = NormalizeConfigName(glob);
  GlobMatcher matcher(normalized_glob, false /* case insensitive*/);

  vector<string> res;
  util::fb2::LockGuard lk(mu_);

  for (const auto& [name, _] : registry_) {
    if (matcher.Matches(name))
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

void ConfigRegistry::ValidateCustomSetter(std::string_view name, WriteCb setter) const {
  absl::CommandLineFlag* flag = absl::FindCommandLineFlag(name);
  CHECK(flag) << "Unknown config name: " << name;
  if (setter) {
    bool cb_match = setter(*flag);
    CHECK(cb_match) << "Possible type mismatch with setter for flag " << name;
  }
}

}  // namespace dfly
