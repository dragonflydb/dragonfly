// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "facade/tracy_support.h"

#include <string>

#ifdef TRACY_ENABLE
#include <absl/flags/flag.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_split.h>

#include "base/logging.h"

ABSL_FLAG(std::string, tracy_scopes, "all",
          "Comma-separated Tracy scopes to emit: connection,dispatch,squasher,reply,memory,all");
#endif

namespace facade {
namespace {

#ifdef TRACY_ENABLE
constexpr uint32_t kCompiledTracyScopes =
#ifdef DFLY_TRACY_BUILD_CONNECTION
    static_cast<uint32_t>(TracyScope::kConnection) |
#endif
#ifdef DFLY_TRACY_BUILD_DISPATCH
    static_cast<uint32_t>(TracyScope::kDispatch) |
#endif
#ifdef DFLY_TRACY_BUILD_SQUASHER
    static_cast<uint32_t>(TracyScope::kSquasher) |
#endif
#ifdef DFLY_TRACY_BUILD_REPLY
    static_cast<uint32_t>(TracyScope::kReply) |
#endif
#ifdef DFLY_TRACY_BUILD_MEMORY
    static_cast<uint32_t>(TracyScope::kMemory) |
#endif
    0;
#endif

}  // namespace

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
    else
      LOG(FATAL) << "Unknown --tracy_scopes entry: " << normalized_scope;
  }
  if (scopes & ~kCompiledTracyScopes) {
    LOG(FATAL) << "--tracy_scopes requests scopes excluded by DFLY_TRACY_SCOPES";
  }
  tracy_enabled_scopes.store(scopes, std::memory_order_relaxed);
#endif
}

}  // namespace facade
