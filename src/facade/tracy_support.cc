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

ABSL_FLAG(
    std::string, tracy_scopes, "all",
    "Comma-separated Tracy scopes to emit: connection,dispatch,squasher,reply,memory,manual,all");
ABSL_FLAG(std::string, tracy_manual_zones, "all",
          "Comma-separated manual Tracy zone IDs or names to emit, all, or empty to disable");
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

#ifdef DFLY_TRACY_PLOTS
std::unordered_set<uint32_t> tracy_queue_connections;
std::unordered_set<unsigned> tracy_queue_proactors;

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
#endif

bool ParseManualZones(std::string_view zone_list, ManualZoneMask* zones) {
  zones->fill(0);
  for (std::string_view entry : absl::StrSplit(zone_list, ',')) {
    entry = absl::StripAsciiWhitespace(entry);
    if (entry.empty())
      continue;
    if (absl::EqualsIgnoreCase(entry, "all")) {
      zones->fill(~uint64_t{0});
      (*zones)[0] &= ~uint64_t{1};
      continue;
    }

    unsigned id{};
    if (absl::SimpleAtoi(entry, &id)) {
      if ((id == 0) || (id > DFLY_TRACY_MANUAL_ZONE_COUNT))
        return false;
      (*zones)[id / 64] |= uint64_t{1} << (id % 64);
      continue;
    }

    bool found = false;
    for (unsigned id = 1; id <= DFLY_TRACY_MANUAL_ZONE_COUNT; ++id) {
      if (absl::EqualsIgnoreCase(entry, kTracyManualZoneNames[id])) {
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
#endif

}  // namespace

std::array<std::atomic_uint64_t, kTracyManualZoneMaskWords> tracy_enabled_manual_zones;

std::atomic_uint32_t tracy_enabled_scopes{
#ifdef TRACY_ENABLE
    kCompiledTracyScopes
#else
    0
#endif
};

void InitTracyScopes() {
#ifndef TRACY_ENABLE
  return;
#else
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
    tracy_enabled_manual_zones[word].store(
        compiled_manual_zones[word] & requested_manual_zones[word], std::memory_order_relaxed);
  }
#else
  for (std::atomic_uint64_t& word : tracy_enabled_manual_zones)
    word.store(0, std::memory_order_relaxed);
#endif

  std::string scope_list = absl::GetFlag(FLAGS_tracy_scopes);
  if (scope_list.empty()) {
    tracy_enabled_scopes.store(0, std::memory_order_relaxed);
    return;
  }

  uint32_t scopes = 0;
  for (std::string_view scope : absl::StrSplit(scope_list, ',')) {
    std::string normalized_scope = absl::AsciiStrToLower(scope);
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
      LOG(FATAL) << "Unknown --tracy_scopes entry: " << normalized_scope;
  }
  constexpr uint32_t kRuntimeOnlyScopes = static_cast<uint32_t>(TracyScope::kManual);
  if (scopes & ~(kCompiledTracyScopes | kRuntimeOnlyScopes)) {
    LOG(FATAL) << "--tracy_scopes requests scopes excluded by DFLY_TRACY_SCOPES";
  }
  if ((scopes & static_cast<uint32_t>(TracyScope::kManual)) != 0) {
    bool has_enabled_manual_zone = false;
    for (const std::atomic_uint64_t& word : tracy_enabled_manual_zones) {
      has_enabled_manual_zone |= word.load(std::memory_order_relaxed) != 0;
    }
    if (!has_enabled_manual_zone) {
      if (empty_manual_zone_list) {
        LOG(WARNING) << "--tracy_manual_zones is empty; disabling manual Tracy scope";
      } else if (!invalid_manual_zone_list) {
        LOG(WARNING) << "--tracy_scopes=manual requested but no manual Tracy zones are enabled; "
                        "disabling manual scope";
      }
      scopes &= ~static_cast<uint32_t>(TracyScope::kManual);
    }
  }
  tracy_enabled_scopes.store(scopes, std::memory_order_relaxed);
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
