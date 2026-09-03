// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <string_view>

#include "facade/tracy_manual_zones.h"

namespace facade {

enum class TracyScope : uint32_t {
  kConnection = 1U << 0,
  kDispatch = 1U << 1,
  kSquasher = 1U << 2,
  kReply = 1U << 3,
  kMemory = 1U << 4,
  kManual = 1U << 5,
};

void InitTracyScopes();
extern std::atomic_uint32_t tracy_enabled_scopes;
inline constexpr size_t kTracyManualZoneMaskWords = (DFLY_TRACY_MANUAL_ZONE_COUNT / 64) + 1;
extern std::array<std::atomic_uint64_t, kTracyManualZoneMaskWords> tracy_enabled_manual_zones;

inline bool IsTracyZoneEnabled(TracyScope scope, TracyManualZone zone) {
  const uint32_t enabled_scopes = tracy_enabled_scopes.load(std::memory_order_relaxed);
  if ((enabled_scopes & static_cast<uint32_t>(scope)) != 0)
    return true;
  if ((enabled_scopes & static_cast<uint32_t>(TracyScope::kManual)) == 0)
    return false;

  const unsigned id = static_cast<unsigned>(zone);
  return (tracy_enabled_manual_zones[id / 64].load(std::memory_order_relaxed) &
          (uint64_t{1} << (id % 64))) != 0;
}

inline bool IsTracyScopeEnabled(TracyScope scope) {
  return tracy_enabled_scopes.load(std::memory_order_relaxed) & static_cast<uint32_t>(scope);
}

}  // namespace facade

// Thin wrapper around the Tracy profiler client.
//
// When Dragonfly is configured with -DWITH_TRACY=ON, the build fetches and links the Tracy
// client. Linking Tracy::TracyClient (PUBLIC) propagates its TRACY_ENABLE define to this header,
// so these macros forward to Tracy. Otherwise they expand to nothing, so instrumentation is
// zero-overhead and safe to leave in the code by default.
//
// The build uses Tracy's on-demand mode: an enabled binary does not profile until a Tracy
// viewer connects, which is the effective runtime on/off switch (connect = start capturing).
//
// Usage:
//   #include "facade/tracy_support.h"
//
//   void Foo() {
//     DFLY_TRACY_ZONE("Foo");            // scoped timing zone named "Foo" (RAII, ends at scope)
//     ...
//   }
//
//   DFLY_TRACY_FRAME_MARK();             // marks a frame boundary (e.g. one loop iteration)
//   DFLY_TRACY_PLOT("pipeline_len", n);  // plots a numeric value over time
//   DFLY_TRACY_THREAD_NAME("Proactor0"); // names the current OS thread in the trace

#ifdef TRACY_ENABLE

// liburing's BLOCK_SIZE macro collides with Tracy's ConcurrentQueue internals.
#pragma push_macro("BLOCK_SIZE")
#undef BLOCK_SIZE
#include <tracy/Tracy.hpp>
#pragma pop_macro("BLOCK_SIZE")

// Scoped zone. `name` must be a string literal.
#define DFLY_TRACY_ZONE(name) ZoneScopedN(name)
// Attach dynamic text to the current zone.
#define DFLY_TRACY_ZONE_TEXT(txt, size) ZoneText(txt, size)
// Attach a std::string_view as text to the current zone. The argument is evaluated only when Tracy
// is enabled (the no-op fallback leaves it unevaluated via sizeof), so callers pay nothing by
// default.
#define DFLY_TRACY_ZONE_TEXT_SV(sv)                   \
  do {                                                \
    ::std::string_view _dfly_tz_sv = (sv);            \
    ZoneText(_dfly_tz_sv.data(), _dfly_tz_sv.size()); \
  } while (0)
#define DFLY_TRACY_ZONE_TEXT_F(...) ZoneTextF(__VA_ARGS__)
#define DFLY_TRACY_ZONE_VALUE(value) ZoneValue(value)
#define DFLY_TRACY_FRAME_MARK() FrameMark
#define DFLY_TRACY_PLOT(name, val) TracyPlot(name, val)
#define DFLY_TRACY_MESSAGE(txt, size) TracyMessage(txt, size)
#define DFLY_TRACY_THREAD_NAME(name) tracy::SetThreadName(name)
// Scoped zone with an explicit 0xRRGGBB color.
#define DFLY_TRACY_ZONE_C(name, color) ZoneScopedNC(name, color)
// A "wait" zone: the fiber is parked / blocked / yielding here and does NO CPU work. Colored red
// so wait time is visually and statistically distinct from work zones. Use it for await / yield /
// cond-wait / blocking-recv / join - NOT for functions that do work but may internally preempt
// (those stay normal-colored; their fiber-lane gaps already reveal the preemption).
#define DFLY_TRACY_WAIT(name) ZoneScopedNC(name, 0xC0392B)

#ifdef DFLY_TRACY_FORENSIC
// High-volume per-command detail. Enable with -DWITH_TRACY_FORENSIC=ON.
#define DFLY_TRACY_ZONE_FORENSIC(name) ZoneScopedN(name)
#define DFLY_TRACY_ZONE_FORENSIC_TEXT_SV(sv) DFLY_TRACY_ZONE_TEXT_SV(sv)
#define DFLY_TRACY_ZONE_FORENSIC_VALUE(value) DFLY_TRACY_ZONE_VALUE(value)
#else
#define DFLY_TRACY_ZONE_FORENSIC(name) (void)sizeof(name)
#define DFLY_TRACY_ZONE_FORENSIC_TEXT_SV(sv) (void)sizeof(sv)
#define DFLY_TRACY_ZONE_FORENSIC_VALUE(value) (void)sizeof(value)
#endif

// A grouped site is present only when its broad group or exact manual ID was compiled.
#define DFLY_TRACY_OR_00 0
#define DFLY_TRACY_OR_01 1
#define DFLY_TRACY_OR_10 1
#define DFLY_TRACY_OR_11 1
#define DFLY_TRACY_OR(lhs, rhs) DFLY_TRACY_OR_I(lhs, rhs)
#define DFLY_TRACY_OR_I(lhs, rhs) DFLY_TRACY_OR_II(lhs, rhs)
#define DFLY_TRACY_OR_II(lhs, rhs) DFLY_TRACY_OR_##lhs##rhs
#define DFLY_TRACY_IF_0(...)
#define DFLY_TRACY_IF_1(...) __VA_ARGS__
#define DFLY_TRACY_IF(value, ...) DFLY_TRACY_IF_I(value, __VA_ARGS__)
#define DFLY_TRACY_IF_I(value, ...) DFLY_TRACY_IF_##value(__VA_ARGS__)
#define DFLY_TRACY_SITE_ENABLED(scope, symbol) \
  DFLY_TRACY_OR(DFLY_TRACY_BUILD_##scope, DFLY_TRACY_MANUAL_BUILD_##symbol)
#define DFLY_TRACY_ZONE_IMPL(scope, tracy_scope, symbol)                                          \
  DFLY_TRACY_IF(                                                                                  \
      DFLY_TRACY_SITE_ENABLED(scope, symbol),                                                     \
      SuppressVarShadowWarning(ZoneNamedN(                                                        \
          ___tracy_scoped_zone, ::facade::TracyManualZoneName(::facade::TracyManualZone::symbol), \
          ::facade::IsTracyZoneEnabled(tracy_scope, ::facade::TracyManualZone::symbol))))
#define DFLY_TRACY_WAIT_IMPL(scope, tracy_scope, symbol)                                          \
  DFLY_TRACY_IF(                                                                                  \
      DFLY_TRACY_SITE_ENABLED(scope, symbol),                                                     \
      SuppressVarShadowWarning(ZoneNamedNC(                                                       \
          ___tracy_scoped_zone, ::facade::TracyManualZoneName(::facade::TracyManualZone::symbol), \
          0xC0392B,                                                                               \
          ::facade::IsTracyZoneEnabled(tracy_scope, ::facade::TracyManualZone::symbol))))
#define DFLY_TRACY_VALUE_IMPL(scope, tracy_scope, symbol, value)                          \
  DFLY_TRACY_IF(                                                                          \
      DFLY_TRACY_SITE_ENABLED(scope, symbol), do {                                        \
        if (::facade::IsTracyZoneEnabled(tracy_scope, ::facade::TracyManualZone::symbol)) \
          ZoneValue(value);                                                               \
      } while (0))
#define DFLY_TRACY_TEXT_SV_IMPL(scope, tracy_scope, symbol, value)                        \
  DFLY_TRACY_IF(                                                                          \
      DFLY_TRACY_SITE_ENABLED(scope, symbol), do {                                        \
        if (::facade::IsTracyZoneEnabled(tracy_scope, ::facade::TracyManualZone::symbol)) \
          DFLY_TRACY_ZONE_TEXT_SV(value);                                                 \
      } while (0))
#define DFLY_TRACY_TEXT_F_IMPL(scope, tracy_scope, symbol, ...)                           \
  DFLY_TRACY_IF(                                                                          \
      DFLY_TRACY_SITE_ENABLED(scope, symbol), do {                                        \
        if (::facade::IsTracyZoneEnabled(tracy_scope, ::facade::TracyManualZone::symbol)) \
          ZoneTextF(__VA_ARGS__);                                                         \
      } while (0))
#define DFLY_TRACY_PLOT_IMPL(scope, tracy_scope, symbol, value)                               \
  DFLY_TRACY_IF(                                                                              \
      DFLY_TRACY_SITE_ENABLED(scope, symbol), do {                                            \
        if (::facade::IsTracyZoneEnabled(tracy_scope, ::facade::TracyManualZone::symbol))     \
          TracyPlot(::facade::TracyManualZoneName(::facade::TracyManualZone::symbol), value); \
      } while (0))

#define DFLY_TRACY_CONNECTION_ZONE(symbol) \
  DFLY_TRACY_ZONE_IMPL(CONNECTION, ::facade::TracyScope::kConnection, symbol)
#define DFLY_TRACY_CONNECTION_WAIT(symbol) \
  DFLY_TRACY_WAIT_IMPL(CONNECTION, ::facade::TracyScope::kConnection, symbol)
#ifdef DFLY_TRACY_FORENSIC
#define DFLY_TRACY_CONNECTION_FORENSIC_ZONE(symbol) DFLY_TRACY_CONNECTION_ZONE(symbol)
#else
#define DFLY_TRACY_CONNECTION_FORENSIC_ZONE(symbol) (void)0
#endif
#define DFLY_TRACY_CONNECTION_PLOT(symbol, value) \
  DFLY_TRACY_PLOT_IMPL(CONNECTION, ::facade::TracyScope::kConnection, symbol, value)
#define DFLY_TRACY_CONNECTION_TEXT_SV(symbol, value) \
  DFLY_TRACY_TEXT_SV_IMPL(CONNECTION, ::facade::TracyScope::kConnection, symbol, value)

#define DFLY_TRACY_DISPATCH_ZONE(symbol) \
  DFLY_TRACY_ZONE_IMPL(DISPATCH, ::facade::TracyScope::kDispatch, symbol)
#ifdef DFLY_TRACY_FORENSIC
#define DFLY_TRACY_DISPATCH_FORENSIC_ZONE(symbol) DFLY_TRACY_DISPATCH_ZONE(symbol)
#define DFLY_TRACY_DISPATCH_FORENSIC_TEXT_SV(symbol, value) \
  DFLY_TRACY_TEXT_SV_IMPL(DISPATCH, ::facade::TracyScope::kDispatch, symbol, value)
#else
#define DFLY_TRACY_DISPATCH_FORENSIC_ZONE(symbol) (void)0
#define DFLY_TRACY_DISPATCH_FORENSIC_TEXT_SV(symbol, value) (void)sizeof(value)
#endif

#define DFLY_TRACY_SQUASHER_ZONE(symbol) \
  DFLY_TRACY_ZONE_IMPL(SQUASHER, ::facade::TracyScope::kSquasher, symbol)
#ifdef DFLY_TRACY_FORENSIC
#define DFLY_TRACY_SQUASHER_FORENSIC_ZONE(symbol) DFLY_TRACY_SQUASHER_ZONE(symbol)
#else
#define DFLY_TRACY_SQUASHER_FORENSIC_ZONE(symbol) (void)0
#endif
#define DFLY_TRACY_SQUASHER_WAIT(symbol) \
  DFLY_TRACY_WAIT_IMPL(SQUASHER, ::facade::TracyScope::kSquasher, symbol)
#define DFLY_TRACY_SQUASHER_VALUE(symbol, value) \
  DFLY_TRACY_VALUE_IMPL(SQUASHER, ::facade::TracyScope::kSquasher, symbol, value)
#define DFLY_TRACY_SQUASHER_TEXT(symbol, value) \
  DFLY_TRACY_TEXT_SV_IMPL(SQUASHER, ::facade::TracyScope::kSquasher, symbol, value)
#define DFLY_TRACY_SQUASHER_TEXT_F(symbol, ...) \
  DFLY_TRACY_TEXT_F_IMPL(SQUASHER, ::facade::TracyScope::kSquasher, symbol, __VA_ARGS__)

#define DFLY_TRACY_REPLY_ZONE(symbol) \
  DFLY_TRACY_ZONE_IMPL(REPLY, ::facade::TracyScope::kReply, symbol)
#define DFLY_TRACY_REPLY_WAIT(symbol) \
  DFLY_TRACY_WAIT_IMPL(REPLY, ::facade::TracyScope::kReply, symbol)
#ifdef DFLY_TRACY_FORENSIC
#define DFLY_TRACY_REPLY_FORENSIC_ZONE(symbol) DFLY_TRACY_REPLY_ZONE(symbol)
#define DFLY_TRACY_REPLY_FORENSIC_VALUE(symbol, value) \
  DFLY_TRACY_VALUE_IMPL(REPLY, ::facade::TracyScope::kReply, symbol, value)
#else
#define DFLY_TRACY_REPLY_FORENSIC_ZONE(symbol) (void)0
#define DFLY_TRACY_REPLY_FORENSIC_VALUE(symbol, value) (void)sizeof(value)
#endif
#define DFLY_TRACY_REPLY_VALUE(symbol, value) \
  DFLY_TRACY_VALUE_IMPL(REPLY, ::facade::TracyScope::kReply, symbol, value)

#define DFLY_TRACY_MEMORY_ZONE(symbol) \
  DFLY_TRACY_ZONE_IMPL(MEMORY, ::facade::TracyScope::kMemory, symbol)

#else  // !TRACY_ENABLE

// No-op fallbacks. Use sizeof to keep arguments unevaluated while silencing unused warnings.
#define DFLY_TRACY_ZONE(name) (void)sizeof(name)
#define DFLY_TRACY_ZONE_TEXT(txt, size) \
  do {                                  \
    (void)sizeof(txt);                  \
    (void)sizeof(size);                 \
  } while (0)
#define DFLY_TRACY_ZONE_TEXT_SV(sv) (void)sizeof(sv)
#define DFLY_TRACY_ZONE_TEXT_F(...) (void)sizeof(#__VA_ARGS__)
#define DFLY_TRACY_ZONE_VALUE(value) (void)sizeof(value)
#define DFLY_TRACY_FRAME_MARK() (void)0
#define DFLY_TRACY_PLOT(name, val) \
  do {                             \
    (void)sizeof(name);            \
    (void)sizeof(val);             \
  } while (0)
#define DFLY_TRACY_MESSAGE(txt, size) \
  do {                                \
    (void)sizeof(txt);                \
    (void)sizeof(size);               \
  } while (0)
#define DFLY_TRACY_THREAD_NAME(name) (void)sizeof(name)
#define DFLY_TRACY_ZONE_C(name, color) \
  do {                                 \
    (void)sizeof(name);                \
    (void)sizeof(color);               \
  } while (0)
#define DFLY_TRACY_WAIT(name) (void)sizeof(name)
#define DFLY_TRACY_ZONE_FORENSIC(name) (void)sizeof(name)
#define DFLY_TRACY_ZONE_FORENSIC_TEXT_SV(sv) (void)sizeof(sv)
#define DFLY_TRACY_ZONE_FORENSIC_VALUE(value) (void)sizeof(value)
#define DFLY_TRACY_CONNECTION_ZONE(name) (void)0
#define DFLY_TRACY_CONNECTION_WAIT(name) (void)0
#define DFLY_TRACY_CONNECTION_FORENSIC_ZONE(name) (void)0
#define DFLY_TRACY_CONNECTION_PLOT(symbol, value) \
  do {                                            \
    (void)sizeof(value);                          \
  } while (0)
#define DFLY_TRACY_CONNECTION_TEXT_SV(symbol, value) (void)sizeof(value)
#define DFLY_TRACY_DISPATCH_ZONE(name) (void)0
#define DFLY_TRACY_DISPATCH_FORENSIC_ZONE(name) (void)0
#define DFLY_TRACY_DISPATCH_FORENSIC_TEXT_SV(symbol, value) (void)sizeof(value)
#define DFLY_TRACY_SQUASHER_ZONE(name) (void)0
#define DFLY_TRACY_SQUASHER_FORENSIC_ZONE(name) (void)0
#define DFLY_TRACY_SQUASHER_WAIT(name) (void)0
#define DFLY_TRACY_SQUASHER_VALUE(symbol, value) (void)sizeof(value)
#define DFLY_TRACY_SQUASHER_TEXT(symbol, value) (void)sizeof(value)
#define DFLY_TRACY_SQUASHER_TEXT_F(symbol, ...) (void)sizeof(#__VA_ARGS__)
#define DFLY_TRACY_REPLY_ZONE(name) (void)0
#define DFLY_TRACY_REPLY_WAIT(name) (void)0
#define DFLY_TRACY_REPLY_FORENSIC_ZONE(name) (void)0
#define DFLY_TRACY_REPLY_FORENSIC_VALUE(symbol, value) (void)sizeof(value)
#define DFLY_TRACY_REPLY_VALUE(symbol, value) (void)sizeof(value)
#define DFLY_TRACY_MEMORY_ZONE(name) (void)0

#endif  // TRACY_ENABLE
