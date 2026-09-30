// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/config_registry.h"

#include <absl/container/flat_hash_set.h>
#include <absl/flags/reflection.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_replace.h>

#include "base/logging.h"
#include "core/glob_matcher.h"
#include "strings/human_readable.h"

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
  string old_value = flag->CurrentValue();
  if (string error; !flag->ParseFrom(value, &error)) {
    LOG(WARNING) << error;
    return SetResult::INVALID;
  }

  bool success = !cb || cb(*flag);
  if (!success) {
    // rollback to the old value in case cb validation did not work. Otherwise, we end up with
    // a value that should not be supported by the flag.
    string error;
    if (!flag->ParseFrom(old_value, &error))
      LOG(ERROR) << "Failed to restore " << config_name << " to its previous value: " << error;
  }
  return success ? SetResult::OK : SetResult::INVALID;
}

auto ConfigRegistry::SetMultiple(absl::Span<const pair<string_view, string_view>> params)
    -> MultiSetResult {
  util::fb2::LockGuard lk(mu_);

  // Pass 1: validate every directive before mutating any of them - unknown/read-only
  // directives, and directives repeated within the same call ("duplicate parameter",
  // matching Valkey), are rejected without touching any flag.
  vector<string> names(params.size());
  absl::flat_hash_set<string_view> seen;
  seen.reserve(params.size());
  for (size_t i = 0; i < params.size(); ++i) {
    names[i] = NormalizeConfigName(params[i].first);
    auto it = registry_.find(names[i]);
    if (it == registry_.end())
      return MultiSetResult{SetResult::UNKNOWN, i};
    if (!it->second.is_mutable)
      return MultiSetResult{SetResult::READONLY, i};
    if (!seen.insert(names[i]).second)
      return MultiSetResult{SetResult::DUPLICATE, i};
  }

  // Pass 2: setting multiple parameters is atomic: either all of them are applied, or, in
  // case one of them fails (e.g. an invalid value), none are applied. Keep track of the
  // previous values of already applied parameters so we can roll them back if a later one
  // fails. We hold mu_ for the whole batch (same locking granularity as a single Set() call)
  // so this is also atomic with respect to concurrent CONFIG SET calls on other fibers.
  vector<pair<string_view, string>> applied;
  applied.reserve(params.size());

  for (size_t i = 0; i < params.size(); ++i) {
    const auto& [config_name, value] = params[i];
    const string& name = names[i];

    auto it = registry_.find(name);
    absl::CommandLineFlag* flag = absl::FindCommandLineFlag(name);
    CHECK(flag) << config_name;
    string old_value = flag->CurrentValue();

    SetResult result;
    if (string error; !flag->ParseFrom(value, &error)) {
      LOG(WARNING) << error;
      result = SetResult::INVALID;
    } else {
      bool success = !it->second.cb || it->second.cb(*flag);
      if (!success) {
        // rollback to the old value in case cb validation did not work. Otherwise, we
        // end up with a value that should not be supported by the flag.
        string error;
        if (!flag->ParseFrom(old_value, &error))
          LOG(ERROR) << "Failed to restore " << config_name << " to its previous value: " << error;
      }
      result = success ? SetResult::OK : SetResult::INVALID;
    }

    if (result != SetResult::OK) {
      // Roll back everything already applied in this call, in reverse order. Restore each
      // flag's previous value and re-invoke its callback (if any), so that runtime state
      // driven by the callback (e.g. listener limits for "maxclients") is reverted too, not
      // just the flag's string value.
      for (auto ait = applied.rbegin(); ait != applied.rend(); ++ait) {
        string ait_name = NormalizeConfigName(ait->first);
        absl::CommandLineFlag* aflag = absl::FindCommandLineFlag(ait_name);
        CHECK(aflag) << ait->first;
        string error;
        if (!aflag->ParseFrom(ait->second, &error)) {
          LOG(ERROR) << "Failed to restore " << ait->first << " to its previous value: " << error;
          continue;
        }
        auto ait_entry = registry_.find(ait_name);
        if (ait_entry != registry_.end() && ait_entry->second.cb && !ait_entry->second.cb(*aflag)) {
          LOG(ERROR) << "Failed to re-apply callback while restoring " << ait->first
                     << " to its previous value";
        }
      }
      return MultiSetResult{result, i};
    }
    applied.emplace_back(config_name, std::move(old_value));
  }
  return MultiSetResult{SetResult::OK, 0};
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
  if (!flag) {
    return nullopt;
  }

  // For MemoryBytesFlag, return numeric bytes for compatibility.
  if (flag->IsOfType<strings::MemoryBytesFlag>()) {
    auto val = flag->TryGet<strings::MemoryBytesFlag>();
    if (val.has_value()) {
      return absl::StrCat(val->value);
    }
  }

  return flag->CurrentValue();
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
