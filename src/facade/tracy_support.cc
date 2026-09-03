// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "facade/tracy_support.h"

#include <array>
#include <string>

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
          "Comma-separated manual Tracy zone IDs or names to emit, or all");
#endif

namespace facade {
namespace {

#ifdef TRACY_ENABLE
using ManualZoneMask = std::array<uint64_t, kTracyManualZoneMaskWords>;

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
#if DFLY_TRACY_BUILD_MANUAL
  ManualZoneMask compiled_manual_zones;
  if (!ParseManualZones(DFLY_TRACY_MANUAL_BUILD_ZONES, &compiled_manual_zones)) {
    LOG(FATAL) << "Unknown DFLY_TRACY_MANUAL_ZONES entry";
  }
  ManualZoneMask requested_manual_zones;
  if (!ParseManualZones(absl::GetFlag(FLAGS_tracy_manual_zones), &requested_manual_zones)) {
    LOG(FATAL) << "Unknown --tracy_manual_zones entry";
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
    bool has_compiled_manual_zone = false;
    for (const std::atomic_uint64_t& word : tracy_enabled_manual_zones) {
      has_compiled_manual_zone |= word.load(std::memory_order_relaxed) != 0;
    }
    if (!has_compiled_manual_zone)
      LOG(FATAL) << "--tracy_scopes=manual requested but no manual zones were compiled";
  }
  tracy_enabled_scopes.store(scopes, std::memory_order_relaxed);
#endif
}

}  // namespace facade
