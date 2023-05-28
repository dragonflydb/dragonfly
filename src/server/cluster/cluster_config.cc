extern "C" {
#include "redis/crc16.h"
}

#include <jsoncons/json.hpp>
#include <shared_mutex>
#include <string_view>

#include "base/logging.h"
#include "cluster_config.h"

using namespace std;

namespace dfly {

bool ClusterConfig::cluster_enabled = false;

string_view ClusterConfig::KeyTag(string_view key) {
  size_t start = key.find('{');
  if (start == key.npos) {
    return key;
  }
  size_t end = key.find('}', start + 1);
  if (end == key.npos || end == start + 1) {
    return key;
  }
  return key.substr(start + 1, end - start - 1);
}

SlotId ClusterConfig::KeySlot(string_view key) {
  string_view tag = KeyTag(key);
  return crc16(tag.data(), tag.length()) & kMaxSlotNum;
}

ClusterConfig::ClusterConfig(string_view my_id) : my_id_(my_id) {
  cluster_enabled = true;
}

namespace {
bool HasValidNodeIds(const ClusterConfig::ClusterShards& new_config) {
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
}  // namespace

bool ClusterConfig::IsConfigValid(const ClusterShards& new_config) {
  // Make sure that all slots are set exactly once.
  array<bool, tuple_size<decltype(slots_)>::value> slots_found = {};

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

optional<SlotSet> ClusterConfig::SetConfig(const vector<ClusterShard>& new_config) {
  if (!IsConfigValid(new_config)) {
    return nullopt;
  }

  // When set config is called the first time, deleted_slots will contain all slots which are not
  // allocated to this node. This makes sure that if there is data in server that was loaded from
  // disk (rdb_load), than after the call to set config the server data will contain only data from
  // the node owned slots.
  bool is_first_config = !IsConfigured();

  lock_guard gu(mu_);

  config_ = new_config;

  SlotSet deleted_slots;
  for (const auto& shard : config_) {
    for (const auto& slot_range : shard.slot_ranges) {
      bool owned_by_me =
          shard.master.id == my_id_ || any_of(shard.replicas.begin(), shard.replicas.end(),
                                              [&](const Node& node) { return node.id == my_id_; });
      for (SlotId i = slot_range.start; i <= slot_range.end; ++i) {
        if ((slots_[i].owned_by_me && !owned_by_me) || (is_first_config && !owned_by_me)) {
          deleted_slots.insert(i);
        }
        slots_[i] = {.shard = &shard, .owned_by_me = owned_by_me};
      }
    }
  }

  return deleted_slots;
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

optional<vector<ClusterConfig::SlotRange>> GetClusterSlotRanges(const JsonType& slots) {
  if (!slots.is_array()) {
    LOG(WARNING) << kInvalidConfigPrefix << "slot_ranges is not an array " << slots;
    return nullopt;
  }

  vector<ClusterConfig::SlotRange> ranges;

  for (const auto& range : slots.array_value()) {
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

  return ranges;
}

optional<ClusterConfig::Node> ParseClusterNode(const JsonType& json) {
  if (!json.is_object()) {
    LOG(WARNING) << kInvalidConfigPrefix << "node config is not an object " << json;
    return nullopt;
  }

  ClusterConfig::Node node;

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

optional<ClusterConfig::ClusterShards> BuildClusterConfigFromJson(const JsonType& json) {
  ClusterConfig::ClusterShards config;

  if (!json.is_array()) {
    LOG(WARNING) << kInvalidConfigPrefix << "not an array " << json;
    return nullopt;
  }

  for (const auto& element : json.array_value()) {
    ClusterConfig::ClusterShard shard;

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

    for (const auto& replica : replicas.array_value()) {
      auto node = ParseClusterNode(replica);
      if (!node.has_value()) {
        return nullopt;
      }
      shard.replicas.push_back(std::move(node).value());
    }

    config.push_back(std::move(shard));
  }

  return config;
}
}  // namespace

optional<SlotSet> ClusterConfig::SetConfig(const JsonType& json) {
  optional<ClusterShards> config = BuildClusterConfigFromJson(json);
  if (!config.has_value()) {
    return nullopt;
  }

  return SetConfig(config.value());
}

bool ClusterConfig::IsMySlot(SlotId id) const {
  if (id >= slots_.size()) {
    DCHECK(false) << "Requesting a non-existing slot id " << id;
    return false;
  }

  shared_lock gu(mu_);
  return slots_[id].owned_by_me;
}

ClusterConfig::Node ClusterConfig::GetMasterNodeForSlot(SlotId id) const {
  shared_lock gu(mu_);

  CHECK_LT(id, slots_.size()) << "Requesting a non-existing slot id " << id;
  CHECK_NE(slots_[id].shard, nullptr)
      << "Calling GetMasterNodeForSlot(" << id << ") before SetConfig()";

  return slots_[id].shard->master;
}

ClusterConfig::ClusterShards ClusterConfig::GetConfig() const {
  shared_lock gu(mu_);
  return config_;
}

bool ClusterConfig::IsConfigured() const {
  shared_lock gu(mu_);
  return !config_.empty();
}

}  // namespace dfly
