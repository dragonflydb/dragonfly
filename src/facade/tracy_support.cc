// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "facade/tracy_support.h"

#include <array>
#include <string>
#include <unordered_set>

#ifdef TRACY_ENABLE
#include <absl/flags/flag.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_split.h>

#include "base/logging.h"
#include "util/fibers/fibers.h"
#include "util/fibers/proactor_base.h"

ABSL_FLAG(
    std::string, tracy_scopes, "all",
    "Comma-separated Tracy scopes to emit: connection,dispatch,squasher,reply,memory,manual,all");
ABSL_FLAG(std::string, tracy_manual_zones, "all",
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
          "Comma-separated proactor IDs whose connection-fiber Tracy zones are emitted");
ABSL_FLAG(std::string, tracy_connections, "",
          "Comma-separated connection IDs whose connection-fiber Tracy zones are emitted");
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

std::unordered_set<uint32_t> tracy_connections;
std::unordered_set<unsigned> tracy_proactors;
bool tracy_connection_target_filter_enabled = false;

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

#ifdef DFLY_TRACY_PLOTS
std::unordered_set<uint32_t> tracy_queue_connections;
std::unordered_set<unsigned> tracy_queue_proactors;
#endif

bool ParseManualZones(std::string_view zone_list, ManualZoneMask* zones) {
  zones->fill(0);
  for (std::string_view entry : absl::StrSplit(zone_list, ',')) {
    entry = absl::StripAsciiWhitespace(entry);
    if (entry.empty())
      continue;
    if (absl::EqualsIgnoreCase(entry, "all")) {
      for (unsigned id = 1; id <= DFLY_TRACY_MANUAL_ZONE_COUNT; ++id) {
        if (kTracyManualZoneNames[id] != nullptr)
          (*zones)[id / 64] |= uint64_t{1} << (id % 64);
      }
      continue;
    }

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
  for (std::string_view scope : absl::StrSplit(scope_list, ',')) {
    std::string normalized_scope = absl::AsciiStrToLower(absl::StripAsciiWhitespace(scope));
    if (normalized_scope.empty())
      continue;
    if (normalized_scope == "all") {
      scopes = kCompiledTracyScopes;
      break;
    }
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
    else if (normalized_scope == "manual")
      scopes |= static_cast<uint32_t>(TracyScope::kManual);
    else
      LOG(FATAL) << "Unknown --" << flag_name << " entry: " << normalized_scope;
  }

  constexpr uint32_t kRuntimeOnlyScopes = static_cast<uint32_t>(TracyScope::kManual);
  if (scopes & ~(kCompiledTracyScopes | kRuntimeOnlyScopes)) {
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
  if (!ParseIdFilter(absl::GetFlag(FLAGS_tracy_connections), &tracy_connections))
    LOG(FATAL) << "Invalid --tracy_connections list";
  if (!ParseIdFilter(absl::GetFlag(FLAGS_tracy_proactors), &tracy_proactors))
    LOG(FATAL) << "Invalid --tracy_proactors list";
  tracy_connection_target_filter_enabled = !tracy_connections.empty() || !tracy_proactors.empty();
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
  const bool empty_manual_zone_list = manual_zone_list.empty();
  const bool invalid_manual_zone_list =
      !ParseManualZones(manual_zone_list, &requested_manual_zones);
  if (invalid_manual_zone_list) {
    LOG(WARNING) << "Invalid --tracy_manual_zones value '" << manual_zone_list
                 << "'; disabling manual Tracy zones";
    requested_manual_zones.fill(0);
  }
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
  if ((scopes & static_cast<uint32_t>(TracyScope::kManual)) != 0) {
    if (!HasEnabledManualZone(tracy_enabled_manual_zones)) {
      if (empty_manual_zone_list) {
        LOG(WARNING) << "--tracy_manual_zones is empty; disabling manual Tracy scope";
      } else if (!invalid_manual_zone_list) {
        LOG(WARNING) << "--tracy_scopes=manual requested but no manual Tracy zones are enabled; "
                        "disabling manual scope";
      }
      scopes &= ~static_cast<uint32_t>(TracyScope::kManual);
    }
  }
  tracy_enabled_scopes = scopes;

  uint32_t sampled_scopes =
      ParseScopes(absl::GetFlag(FLAGS_tracy_sampled_scopes), "tracy_sampled_scopes");
  if ((sampled_scopes & static_cast<uint32_t>(TracyScope::kManual)) != 0 &&
      !HasEnabledManualZone(tracy_sampled_manual_zones)) {
    LOG(WARNING) << "--tracy_sampled_scopes=manual requested but no sampled manual Tracy zones "
                    "are enabled; disabling sampled manual scope";
    sampled_scopes &= ~static_cast<uint32_t>(TracyScope::kManual);
  }
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

bool IsTracyProactorTargetEnabled() {
#ifndef TRACY_ENABLE
  return false;
#else
  if (!tracy_connection_target_filter_enabled)
    return true;
  auto* proactor = util::fb2::ProactorBase::me();
  return proactor && tracy_proactors.contains(proactor->GetPoolIndex());
#endif
}

bool IsTracyConnectionTargetEnabled(uint32_t connection_id, unsigned proactor_id) {
#ifndef TRACY_ENABLE
  return false;
#else
  return !tracy_connection_target_filter_enabled || tracy_connections.contains(connection_id) ||
         tracy_proactors.contains(proactor_id);
#endif
}

#ifdef DFLY_TRACY_PLOTS
bool ShouldEmitTracyQueueTelemetry(uint32_t client_id, unsigned proactor_id) {
  return (tracy_queue_connections.empty() || tracy_queue_connections.contains(client_id)) &&
         (tracy_queue_proactors.empty() || tracy_queue_proactors.contains(proactor_id));
}

bool TracyBackpressurePlotsEnabled() {
  return absl::GetFlag(FLAGS_tracy_backpressure_plots);
}
#endif

}  // namespace facade
