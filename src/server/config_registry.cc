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

struct ConfigUpdate {
  std::string_view name;
  std::string_view new_value;
  std::string prev_value;
  absl::CommandLineFlag* flag;
  ConfigRegistry::WriteCb cb;
};

void RollbackUpdates(const std::vector<ConfigUpdate>& updates, size_t applied);

// TODO(andydunstall): Add unit test coverage
// TODO(andydunstall): Return the invalid argument to return to user
auto ConfigRegistry::Set(CmdArgList args) -> SetResult {
  lock_guard lk(mu_);

  if (args.empty() || args.size() % 2 != 0) {
    return SetResult::WRONG_ARGS_NUMBER;
  }

  std::vector<ConfigUpdate> updates;
  std::unordered_set<std::string_view> updated_keys;

  // Verify all keys exist and check for duplicates. Store the previous value
  // so we can roll back if needed.
  for (size_t args_indx = 0; args_indx < args.size(); args_indx += 2) {
    ToLower(&args[args_indx]);

    ConfigUpdate update;
    update.name = ArgS(args, args_indx);
    update.new_value = ArgS(args, args_indx + 1);

    auto it = registry_.find(update.name);
    if (it == registry_.end()) {
      // No need to roll back as no config has been updated.
      return SetResult::UNKNOWN;
    }
    if (!it->second.is_mutable) {
      // No need to roll back as no config has been updated.
      return SetResult::READONLY;
    }
    update.cb = it->second.cb;

    if (updated_keys.contains(update.name)) {
      // No need to roll back as no config has been updated.
      return SetResult::DUPLICATE;
    }

    update.flag = absl::FindCommandLineFlag(update.name);
    CHECK(update.flag);
    update.prev_value = std::string(update.flag->CurrentValue());

    updates.push_back(update);
    updated_keys.emplace(update.name);

    DVLOG(1) << "update config; name=" << update.name << "; prev_value=" << update.prev_value
             << "; new_value=" << update.new_value;
  }

  // Update the flags without applying any registered callbacks.
  for (size_t i = 0; i != updates.size(); i++) {
    const ConfigUpdate update = updates[i];
    std::string error;

    if (!update.flag->ParseFrom(update.new_value, &error)) {
      // Revert all updated flags.
      RollbackUpdates(updates, 0);
      return SetResult::INVALID;
    }
  }

  // Apply the registered callbacks.
  for (size_t i = 0; i != updates.size(); i++) {
    const ConfigUpdate update = updates[i];
    std::string error;
    if (update.cb && !update.cb(*update.flag)) {
      // Roll back any applied updates. Note only roll back updates already
      // applied which excludes the latest failed callback.
      RollbackUpdates(updates, i);
      return SetResult::INVALID;
    }
  }

  return SetResult::OK;
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

// Reverts all flags to their previous values and roll backs any applied
// callbacks with their previous values.
void RollbackUpdates(const std::vector<ConfigUpdate>& updates, size_t applied) {
  // Revert all flags.
  for (const ConfigUpdate& update : updates) {
    DVLOG(1) << "rollback: revert flag; name=" << update.name
             << "; prev_value=" << update.prev_value;

    std::string error;
    if (!update.flag->ParseFrom(update.prev_value, &error)) {
      // This should never happen as we're rolling back to a previous value.
      LOG(ERROR) << "rollback failed: could not parse update; key=" << update.name
                 << "; value=" << update.prev_value;
    }
  }

  // Only rollback the already applied values.
  for (size_t i = 0; i != applied; i++) {
    DVLOG(1) << "rollback: rollback update; name=" << updates[i].name;

    if (updates[i].cb && !updates[i].cb(*updates[i].flag)) {
      // This should never happen as we're rolling back to a previous value.
      LOG(ERROR) << "rollback failed: could not apply update; key=" << updates[i].name;
    }
  }
}

ConfigRegistry config_registry;

}  // namespace dfly
