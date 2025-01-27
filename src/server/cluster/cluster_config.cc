// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "cluster_config.h"

#include <absl/container/flat_hash_set.h>

#include <optional>
#include <string_view>

#include "base/logging.h"
#include "core/json/json_object.h"

using namespace std;

namespace dfly::cluster {

namespace {
bool HasValidNodeIds(const ClusterShardInfos& new_config) {
  absl::flat_hash_set<string_view> nodes;

  auto CheckAndInsertNode = [&](string_view node) {
    auto [_, inserted] = nodes.insert(node);
    return inserted;
  };

  for (const auto& shard : new_config) {
    if (!CheckAndInsertNode(shard.master.id)) {
      LOG(WARNING) << "Master " << shard.master.id << " appears more than once";
      return false;
    }
    for (const auto& replica : shard.replicas) {
      if (!CheckAndInsertNode(replica.id)) {
        LOG(WARNING) << "Replica " << replica.id << " appears more than once";
        return false;
      }
    }
  }

  return true;
}

bool IsConfigValid(const ClusterShardInfos& new_config) {
  // Make sure that all slots are set exactly once.
  vector<bool> slots_found(kMaxSlotNum + 1);

  if (!HasValidNodeIds(new_config)) {
    return false;
  }

  for (const auto& shard : new_config) {
    for (const auto& slot_range : shard.slot_ranges) {
      if (slot_range.start > slot_range.end) {
        LOG(WARNING) << "Invalid cluster config: start=" << slot_range.start
                     << " is larger than end=" << slot_range.end;
        return false;
      }

      for (SlotId slot = slot_range.start; slot <= slot_range.end; ++slot) {
        if (slot >= slots_found.size()) {
          LOG(WARNING) << "Invalid cluster config: slot=" << slot
                       << " is bigger than allowed max=" << slots_found.size();
          return false;
        }

        if (slots_found[slot]) {
          LOG(WARNING) << "Invalid cluster config: slot=" << slot
                       << " was already configured by another slot range.";
          return false;
        }

        slots_found[slot] = true;
      }
    }
  }

  if (!all_of(slots_found.begin(), slots_found.end(), [](bool b) { return b; }) > 0UL) {
    LOG(WARNING) << "Invalid cluster config: some slots were missing.";
    return false;
  }

  return true;
}
}  // namespace

/* static */
shared_ptr<ClusterConfig> ClusterConfig::CreateFromConfig(string_view my_id,
                                                          const ClusterShardInfos& config) {
  if (!IsConfigValid(config)) {
    return nullptr;
  }

  shared_ptr<ClusterConfig> result(new ClusterConfig());

  result->my_id_ = my_id;
  result->config_ = config;

  for (const auto& shard : result->config_) {
    bool owned_by_me = shard.master.id == my_id ||
                       any_of(shard.replicas.begin(), shard.replicas.end(),
                              [&](const ClusterNodeInfo& node) { return node.id == my_id; });
    if (owned_by_me) {
      result->my_slots_.Set(shard.slot_ranges, true);
      if (shard.master.id == my_id) {
        result->my_outgoing_migrations_ = shard.migrations;
      }
    } else {
      for (const auto& m : shard.migrations) {
        if (my_id == m.node_info.id) {
          auto incoming_migration = m;
          // for incoming migration we need the source node
          incoming_migration.node_info.id = shard.master.id;
          result->my_incoming_migrations_.push_back(std::move(incoming_migration));
        }
      }
    }
  }

  return result;
}

namespace {
constexpr string_view kInvalidConfigPrefix = "Invalid JSON cluster config: "sv;

template <typename T> optional<T> ReadNumeric(const JsonType& obj) {
  if (!obj.is_number()) {
    LOG(WARNING) << kInvalidConfigPrefix << "object is not a number " << obj;
    return nullopt;
  }

  return obj.as<T>();
}

optional<SlotRanges> GetClusterSlotRanges(const JsonType& slots) {
  if (!slots.is_array()) {
    LOG(WARNING) << kInvalidConfigPrefix << "slot_ranges is not an array " << slots;
    return nullopt;
  }

  std::vector<SlotRange> ranges;

  for (const auto& range : slots.array_range()) {
    if (!range.is_object()) {
      LOG(WARNING) << kInvalidConfigPrefix << "slot_ranges element is not an object " << range;
      return nullopt;
    }

    optional<SlotId> start = ReadNumeric<SlotId>(range.at_or_null("start"));
    optional<SlotId> end = ReadNumeric<SlotId>(range.at_or_null("end"));
    if (!start.has_value() || !end.has_value()) {
      return nullopt;
    }

    ranges.push_back({.start = start.value(), .end = end.value()});
  }

  return SlotRanges(ranges);
}

optional<ClusterNodeInfo> ParseClusterNode(const JsonType& json) {
  if (!json.is_object()) {
    LOG(WARNING) << kInvalidConfigPrefix << "node config is not an object " << json;
    return nullopt;
  }

  ClusterNodeInfo node;

  {
    auto id = json.at_or_null("id");
    if (!id.is_string()) {
      LOG(WARNING) << kInvalidConfigPrefix << "invalid id for node " << json;
      return nullopt;
    }
    node.id = std::move(id).as_string();
  }

  {
    auto ip = json.at_or_null("ip");
    if (!ip.is_string()) {
      LOG(WARNING) << kInvalidConfigPrefix << "invalid ip for node " << json;
      return nullopt;
    }
    node.ip = std::move(ip).as_string();
  }

  {
    auto port = ReadNumeric<uint16_t>(json.at_or_null("port"));
    if (!port.has_value()) {
      return nullopt;
    }
    node.port = port.value();
  }

  return node;
}

optional<std::vector<MigrationInfo>> ParseMigrations(const JsonType& json) {
  std::vector<MigrationInfo> res;
  if (json.is_null()) {
    return res;
  }

  if (!json.is_array()) {
    LOG(INFO) << "no migrations found: " << json;
    return nullopt;
  }

  for (const auto& element : json.array_range()) {
    auto node_id = element.at_or_null("node_id");
    auto ip = element.at_or_null("ip");
    auto port = ReadNumeric<uint16_t>(element.at_or_null("port"));
    auto slots = GetClusterSlotRanges(element.at_or_null("slot_ranges"));

    if (!node_id.is_string() || !ip.is_string() || !port || !slots) {
      LOG(WARNING) << kInvalidConfigPrefix << "invalid migration json " << json;
      return nullopt;
    }

    res.emplace_back(MigrationInfo{
        .slot_ranges = std::move(*slots),
        .node_info =
            ClusterNodeInfo{.id = node_id.as_string(), .ip = ip.as_string(), .port = *port}});
  }
  return res;
}

optional<ClusterShardInfos> BuildClusterConfigFromJson(const JsonType& json) {
  std::vector<ClusterShardInfo> config;

  if (!json.is_array()) {
    LOG(WARNING) << kInvalidConfigPrefix << "not an array " << json;
    return nullopt;
  }

  for (const auto& element : json.array_range()) {
    ClusterShardInfo shard;

    if (!element.is_object()) {
      LOG(WARNING) << kInvalidConfigPrefix << "shard element is not an object " << element;
      return nullopt;
    }

    auto slots = GetClusterSlotRanges(element.at_or_null("slot_ranges"));
    if (!slots.has_value()) {
      return nullopt;
    }
    shard.slot_ranges = std::move(slots).value();

    auto master = ParseClusterNode(element.at_or_null("master"));
    if (!master.has_value()) {
      return nullopt;
    }
    shard.master = std::move(master).value();

    auto replicas = element.at_or_null("replicas");
    if (!replicas.is_array()) {
      LOG(WARNING) << kInvalidConfigPrefix << "replicas is not an array " << replicas;
      return nullopt;
    }

    for (const auto& replica : replicas.array_range()) {
      auto node = ParseClusterNode(replica);
      if (!node.has_value()) {
        return nullopt;
      }
      shard.replicas.push_back(std::move(node).value());
    }

    auto migrations = ParseMigrations(element.at_or_null("migrations"));
    if (!migrations) {
      return nullopt;
    }
    shard.migrations = std::move(*migrations);

    config.push_back(std::move(shard));
  }

  return ClusterShardInfos(config);
}
}  // namespace

/* static */
shared_ptr<ClusterConfig> ClusterConfig::CreateFromConfig(string_view my_id,
                                                          std::string_view json_str) {
  optional<JsonType> json_config = JsonFromString(json_str, PMR_NS::get_default_resource());
  if (!json_config.has_value()) {
    LOG(WARNING) << "Can't parse JSON for ClusterConfig " << json_str;
    return nullptr;
  }

  optional<ClusterShardInfos> config = BuildClusterConfigFromJson(json_config);
  if (!config.has_value()) {
    return nullptr;
  }

  return CreateFromConfig(my_id, config.value());
}

std::shared_ptr<ClusterConfig> ClusterConfig::CloneWithChanges(
    const SlotRanges& enable_slots, const SlotRanges& disable_slots) const {
  auto new_config = std::make_shared<ClusterConfig>(*this);
  new_config->my_slots_.Set(enable_slots, true);
  new_config->my_slots_.Set(disable_slots, false);
  return new_config;
}

std::shared_ptr<ClusterConfig> ClusterConfig::CloneWithoutMigrations() const {
  auto new_config = std::make_shared<ClusterConfig>(*this);
  new_config->my_incoming_migrations_.clear();
  new_config->my_outgoing_migrations_.clear();
  return new_config;
}

bool ClusterConfig::IsMySlot(SlotId id) const {
  if (id > kMaxSlotNum) {
    DCHECK(false) << "Requesting a non-existing slot id " << id;
    return false;
  }

  return my_slots_.Contains(id);
}

bool ClusterConfig::IsMySlot(std::string_view key) const {
  return IsMySlot(KeySlot(key));
}

ClusterNodeInfo ClusterConfig::GetMasterNodeForSlot(SlotId id) const {
  CHECK_LE(id, kMaxSlotNum) << "Requesting a non-existing slot id " << id;

  for (const auto& shard : config_) {
    if (shard.slot_ranges.Contains(id)) {
      if (shard.master.id == my_id_) {
        // The only reason why this function call and shard.master == my_id_ is the slot was
        // migrated
        for (const auto& m : shard.migrations) {
          if (m.slot_ranges.Contains(id)) {
            return m.node_info;
          }
        }
      }
      return shard.master;
    }
  }

  DCHECK(false) << "Can't find master node for slot " << id;
  return {};
}

ClusterShardInfos ClusterConfig::GetConfig() const {
  return config_;
}

const SlotSet& ClusterConfig::GetOwnedSlots() const {
  return my_slots_;
}

static std::vector<MigrationInfo> GetMissingMigrations(const std::vector<MigrationInfo>& haystack,
                                                       const std::vector<MigrationInfo>& needle) {
  std::vector<MigrationInfo> res;
  for (const auto& h : haystack) {
    if (find(needle.begin(), needle.end(), h) == needle.end()) {
      res.push_back(h);
    }
  }
  return res;
}

std::vector<MigrationInfo> ClusterConfig::GetNewOutgoingMigrations(
    const std::shared_ptr<ClusterConfig>& prev) const {
  return prev ? GetMissingMigrations(my_outgoing_migrations_, prev->my_outgoing_migrations_)
              : my_outgoing_migrations_;
}

std::vector<MigrationInfo> ClusterConfig::GetNewIncomingMigrations(
    const std::shared_ptr<ClusterConfig>& prev) const {
  return prev ? GetMissingMigrations(my_incoming_migrations_, prev->my_incoming_migrations_)
              : my_incoming_migrations_;
}

std::vector<MigrationInfo> ClusterConfig::GetFinishedOutgoingMigrations(
    const std::shared_ptr<ClusterConfig>& prev) const {
  return prev ? GetMissingMigrations(prev->my_outgoing_migrations_, my_outgoing_migrations_)
              : std::vector<MigrationInfo>();
}

std::vector<MigrationInfo> ClusterConfig::GetFinishedIncomingMigrations(
    const std::shared_ptr<ClusterConfig>& prev) const {
  return prev ? GetMissingMigrations(prev->my_incoming_migrations_, my_incoming_migrations_)
              : std::vector<MigrationInfo>();
}

}  // namespace dfly::cluster
