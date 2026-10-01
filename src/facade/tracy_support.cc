// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "facade/tracy_support.h"

#include <algorithm>
#include <array>
#include <string>
#include <unordered_set>
#include <vector>

#ifdef TRACY_ENABLE
#include <absl/flags/flag.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>

#include "base/logging.h"
#include "util/fibers/fibers.h"
#include "util/fibers/proactor_base.h"

ABSL_FLAG(std::string, tracy_scopes, "all",
          "Comma-separated Tracy scopes to emit: connection,dispatch,squasher,reply,memory,all");
ABSL_FLAG(std::string, tracy_manual_zones, "",
          "Comma-separated manual Tracy zone IDs or names to emit, all, or empty to disable");
ABSL_FLAG(std::string, tracy_sampled_scopes, "",
          "Comma-separated Tracy scopes to emit only during sample windows");
ABSL_FLAG(std::string, tracy_sampled_manual_zones, "",
          "Comma-separated manual Tracy zone IDs or names to emit only during sample windows");
ABSL_FLAG(uint32_t, tracy_sample_every_fiber_switches, 0,
          "Open a Tracy sample window every N fiber switches per proactor; 0 disables sampling");
ABSL_FLAG(uint32_t, tracy_sample_window_fiber_switches, 0,
          "Number of fiber switches per Tracy sample window");
ABSL_FLAG(std::string, tracy_proactors, "",
          "Comma-separated proactor IDs whose Tracy zones are emitted");
ABSL_FLAG(
    std::string, tracy_fibers, "",
    "Comma-separated Tracy fiber names, connection IDs, or known groups: "
    "connections,dispatchers,l2_workers,shard_workers,periodic_workers,workers,acceptors,watchers");
#ifdef DFLY_TRACY_PLOTS
ABSL_FLAG(std::string, tracy_queue_connections, "",
          "Comma-separated client IDs whose V2 queue plots are emitted; empty selects all");
ABSL_FLAG(std::string, tracy_queue_proactors, "",
          "Comma-separated proactor IDs whose V2 queue plots are emitted; empty selects all");
ABSL_FLAG(bool, tracy_backpressure_plots, false,
          "Emit V2 per-proactor backpressure plots when a selected connection parks");
#endif
#endif

namespace facade {
namespace {

#ifdef TRACY_ENABLE
using ManualZoneMask = std::array<uint64_t, kTracyManualZoneMaskWords>;

constexpr uint32_t kCompiledTracyScopes =
#if DFLY_TRACY_BUILD_CONNECTION
    static_cast<uint32_t>(TracyScope::kConnection) |
#endif
#if DFLY_TRACY_BUILD_DISPATCH
    static_cast<uint32_t>(TracyScope::kDispatch) |
#endif
#if DFLY_TRACY_BUILD_SQUASHER
    static_cast<uint32_t>(TracyScope::kSquasher) |
#endif
#if DFLY_TRACY_BUILD_REPLY
    static_cast<uint32_t>(TracyScope::kReply) |
#endif
#if DFLY_TRACY_BUILD_MEMORY
    static_cast<uint32_t>(TracyScope::kMemory) |
#endif
    0;

std::unordered_set<unsigned> tracy_proactors;
struct TracyFiberSelector {
  std::string value;
  bool is_prefix;
};
std::vector<TracyFiberSelector> tracy_fibers;

constexpr std::pair<std::string_view, std::string_view> kKnownFiberGroups[] = {
    {"connections", "DflyConn_"},
    {"dispatchers", "_dispatch_p"},
    {"l2_workers", "l2_queue_p"},
    {"shard_workers", "shard_queue_p"},
    {"periodic_workers", "heartbeat_periodic"},
    {"periodic_workers", "shard_handler_periodic"},
    {"acceptors", "AcceptLoop_p"},
    {"watchers", "ConnectionsWatcher_p"},
    {"workers", "l2_queue_p"},
    {"workers", "shard_queue_p"},
    {"workers", "heartbeat_periodic"},
    {"workers", "shard_handler_periodic"},
};

template <typename T> bool ParseIdFilter(std::string_view value, std::unordered_set<T>* output) {
  output->clear();
  for (std::string_view entry : absl::StrSplit(value, ',')) {
    entry = absl::StripAsciiWhitespace(entry);
    if (entry.empty())
      continue;
    T id{};
    if (!absl::SimpleAtoi(entry, &id))
      return false;
    output->insert(id);
  }
  return true;
}

bool ParseFiberFilter(std::string_view value, std::vector<TracyFiberSelector>* output) {
  output->clear();
  for (std::string_view entry : absl::StrSplit(value, ',')) {
    entry = absl::StripAsciiWhitespace(entry);
    if (entry.empty())
      continue;

    uint32_t connection_id;
    if (absl::SimpleAtoi(entry, &connection_id)) {
      output->push_back({absl::StrCat("DflyConn_", connection_id), false});
      continue;
    }

    bool known_group = false;
    for (const auto& [group, prefix] : kKnownFiberGroups) {
      if (absl::EqualsIgnoreCase(entry, group)) {
        output->push_back({std::string{prefix}, true});
        known_group = true;
      }
    }
    if (!known_group)
      output->push_back({std::string{entry}, false});
  }
  return true;
}

bool IsTracyFiberNameSelected(std::string_view fiber_name) {
  const bool fiber_selected = std::any_of(
      tracy_fibers.begin(), tracy_fibers.end(), [fiber_name](const TracyFiberSelector& selector) {
        return selector.is_prefix ? fiber_name.starts_with(selector.value)
                                  : fiber_name == selector.value;
      });
  auto* proactor = util::fb2::ProactorBase::me();
  const bool proactor_selected = proactor && tracy_proactors.contains(proactor->GetPoolIndex());
  return (tracy_fibers.empty() && tracy_proactors.empty()) || fiber_selected || proactor_selected;
}

#ifdef DFLY_TRACY_PLOTS
std::unordered_set<uint32_t> tracy_queue_connections;
std::unordered_set<unsigned> tracy_queue_proactors;
#endif

bool ParseManualZones(std::string_view zone_list, ManualZoneMask* zones) {
  zones->fill(0);
  bool all_requested = false;
  bool named_zone_requested = false;
  for (std::string_view entry : absl::StrSplit(zone_list, ',')) {
    entry = absl::StripAsciiWhitespace(entry);
    if (entry.empty())
      continue;
    if (absl::EqualsIgnoreCase(entry, "all")) {
      if (all_requested || named_zone_requested)
        return false;
      all_requested = true;
      for (unsigned id = 1; id <= DFLY_TRACY_MANUAL_ZONE_COUNT; ++id) {
        if (kTracyManualZoneNames[id] != nullptr)
          (*zones)[id / 64] |= uint64_t{1} << (id % 64);
      }
      continue;
    }
    if (all_requested)
      return false;
    named_zone_requested = true;

    unsigned id{};
    if (absl::SimpleAtoi(entry, &id)) {
      if ((id == 0) || (id > DFLY_TRACY_MANUAL_ZONE_COUNT) ||
          (kTracyManualZoneNames[id] == nullptr))
        return false;
      (*zones)[id / 64] |= uint64_t{1} << (id % 64);
      continue;
    }

    bool found = false;
    for (unsigned id = 1; id <= DFLY_TRACY_MANUAL_ZONE_COUNT; ++id) {
      if (kTracyManualZoneNames[id] != nullptr &&
          absl::EqualsIgnoreCase(entry, kTracyManualZoneNames[id])) {
        (*zones)[id / 64] |= uint64_t{1} << (id % 64);
        found = true;
        break;
      }
    }
    if (!found)
      return false;
  }
  return true;
}

uint32_t ParseScopes(std::string_view scope_list, std::string_view flag_name) {
  uint32_t scopes = 0;
  bool all_requested = false;
  bool named_scope_requested = false;
  for (std::string_view scope : absl::StrSplit(scope_list, ',')) {
    std::string normalized_scope = absl::AsciiStrToLower(absl::StripAsciiWhitespace(scope));
    if (normalized_scope.empty())
      continue;
    if (normalized_scope == "all") {
      if (all_requested || named_scope_requested)
        LOG(FATAL) << "--" << flag_name << "=all cannot be combined with other scopes";
      all_requested = true;
      scopes |= kCompiledTracyScopes;
      continue;
    }
    if (all_requested)
      LOG(FATAL) << "--" << flag_name << "=all cannot be combined with other scopes";
    named_scope_requested = true;
    if (normalized_scope == "connection")
      scopes |= static_cast<uint32_t>(TracyScope::kConnection);
    else if (normalized_scope == "dispatch")
      scopes |= static_cast<uint32_t>(TracyScope::kDispatch);
    else if (normalized_scope == "squasher")
      scopes |= static_cast<uint32_t>(TracyScope::kSquasher);
    else if (normalized_scope == "reply")
      scopes |= static_cast<uint32_t>(TracyScope::kReply);
    else if (normalized_scope == "memory")
      scopes |= static_cast<uint32_t>(TracyScope::kMemory);
    else
      LOG(FATAL) << "Unknown --" << flag_name << " entry: " << normalized_scope;
  }

  if (scopes & ~kCompiledTracyScopes) {
    LOG(FATAL) << "--" << flag_name << " requests scopes excluded by DFLY_TRACY_SCOPES";
  }
  return scopes;
}

bool HasEnabledManualZone(const std::array<uint64_t, kTracyManualZoneMaskWords>& zones) {
  for (uint64_t word : zones) {
    if (word != 0)
      return true;
  }
  return false;
}

#endif

}  // namespace

std::array<uint64_t, kTracyManualZoneMaskWords> tracy_enabled_manual_zones;
std::array<uint64_t, kTracyManualZoneMaskWords> tracy_sampled_manual_zones;
uint32_t tracy_sampled_scopes = 0;
bool tracy_has_sampled_manual_zones = false;
uint32_t tracy_sample_every_fiber_switches = 0;
uint32_t tracy_sample_window_fiber_switches = 0;

uint32_t tracy_enabled_scopes =
#ifdef TRACY_ENABLE
    kCompiledTracyScopes;
#else
    0;
#endif

void InitTracyScopes() {
#ifndef TRACY_ENABLE
  return;
#else
  ParseFiberFilter(absl::GetFlag(FLAGS_tracy_fibers), &tracy_fibers);
  util::fb2::SetTracyFiberFilter(&IsTracyFiberNameSelected);
  if (!ParseIdFilter(absl::GetFlag(FLAGS_tracy_proactors), &tracy_proactors))
    LOG(FATAL) << "Invalid --tracy_proactors list";
#ifdef DFLY_TRACY_PLOTS
  if (!ParseIdFilter(absl::GetFlag(FLAGS_tracy_queue_connections), &tracy_queue_connections))
    LOG(FATAL) << "Invalid --tracy_queue_connections list";
  if (!ParseIdFilter(absl::GetFlag(FLAGS_tracy_queue_proactors), &tracy_queue_proactors))
    LOG(FATAL) << "Invalid --tracy_queue_proactors list";
#endif
#if DFLY_TRACY_BUILD_MANUAL
  ManualZoneMask compiled_manual_zones;
  if (!ParseManualZones(DFLY_TRACY_MANUAL_BUILD_ZONES, &compiled_manual_zones)) {
    LOG(FATAL) << "Unknown DFLY_TRACY_MANUAL_ZONES entry";
  }
  ManualZoneMask requested_manual_zones;
  const std::string manual_zone_list = absl::GetFlag(FLAGS_tracy_manual_zones);
  if (!ParseManualZones(manual_zone_list, &requested_manual_zones))
    LOG(FATAL) << "Invalid --tracy_manual_zones value '" << manual_zone_list << "'";
  for (size_t word = 0; word < kTracyManualZoneMaskWords; ++word) {
    tracy_enabled_manual_zones[word] = compiled_manual_zones[word] & requested_manual_zones[word];
  }

  ManualZoneMask requested_sampled_manual_zones;
  const std::string sampled_manual_zone_list = absl::GetFlag(FLAGS_tracy_sampled_manual_zones);
  if (!ParseManualZones(sampled_manual_zone_list, &requested_sampled_manual_zones)) {
    LOG(FATAL) << "Invalid --tracy_sampled_manual_zones value '" << sampled_manual_zone_list << "'";
  }
  for (size_t word = 0; word < kTracyManualZoneMaskWords; ++word) {
    tracy_sampled_manual_zones[word] =
        compiled_manual_zones[word] & requested_sampled_manual_zones[word];
  }
#else
  tracy_enabled_manual_zones.fill(0);
  tracy_sampled_manual_zones.fill(0);
#endif

  uint32_t scopes = ParseScopes(absl::GetFlag(FLAGS_tracy_scopes), "tracy_scopes");
  tracy_enabled_scopes = scopes;

  uint32_t sampled_scopes =
      ParseScopes(absl::GetFlag(FLAGS_tracy_sampled_scopes), "tracy_sampled_scopes");
  tracy_sampled_scopes = sampled_scopes;

  const bool has_sampled_selection =
      sampled_scopes != 0 || HasEnabledManualZone(tracy_sampled_manual_zones);
  const uint32_t sample_every = absl::GetFlag(FLAGS_tracy_sample_every_fiber_switches);
  const uint32_t sample_window = absl::GetFlag(FLAGS_tracy_sample_window_fiber_switches);
  if (has_sampled_selection && (sample_every == 0 || sample_window == 0)) {
    LOG(FATAL) << "Sampled Tracy scopes require positive --tracy_sample_every_fiber_switches "
                  "and --tracy_sample_window_fiber_switches";
  }
  if (sample_window > sample_every && sample_every != 0) {
    LOG(FATAL) << "--tracy_sample_window_fiber_switches must not exceed "
                  "--tracy_sample_every_fiber_switches";
  }
  tracy_has_sampled_manual_zones = HasEnabledManualZone(tracy_sampled_manual_zones);
  tracy_sample_every_fiber_switches = sample_every;
  tracy_sample_window_fiber_switches = sample_window;
#endif
}

bool IsTracySampleWindowActive() {
#ifndef TRACY_ENABLE
  return false;
#else
  const uint32_t sample_every = tracy_sample_every_fiber_switches;
  if (sample_every == 0)
    return false;
  thread_local uint64_t cached_epoch = std::numeric_limits<uint64_t>::max();
  thread_local bool sample_window_active;
  const uint64_t epoch = util::fb2::FiberSwitchEpoch();
  if (epoch != cached_epoch) {
    cached_epoch = epoch;
    sample_window_active = epoch % sample_every < tracy_sample_window_fiber_switches;
  }
  return sample_window_active;
#endif
}

bool IsTracyFiberTargetEnabled() {
#ifndef TRACY_ENABLE
  return false;
#else
  return IsTracyFiberNameSelected(util::ThisFiber::GetName());
#endif
}

bool IsTracyConnectionTargetEnabled(uint32_t, unsigned proactor_id) {
#ifndef TRACY_ENABLE
  return false;
#else
  return IsTracyFiberNameSelected(util::ThisFiber::GetName());
#endif
}

#ifdef DFLY_TRACY_PLOTS
bool ShouldEmitTracyQueueTelemetry(uint32_t client_id, unsigned proactor_id) {
  return IsTracyConnectionTargetEnabled(client_id, proactor_id) &&
         (tracy_queue_connections.empty() || tracy_queue_connections.contains(client_id)) &&
         (tracy_queue_proactors.empty() || tracy_queue_proactors.contains(proactor_id));
}

bool TracyBackpressurePlotsEnabled() {
  return absl::GetFlag(FLAGS_tracy_backpressure_plots);
}
#endif

}  // namespace facade
