// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <atomic>
#include <cstdint>
#include <string_view>

namespace facade {

enum class TracyScope : uint32_t {
  kConnection = 1U << 0,
  kDispatch = 1U << 1,
  kSquasher = 1U << 2,
  kReply = 1U << 3,
  kMemory = 1U << 4,
};

void InitTracyScopes();
extern std::atomic_uint32_t tracy_enabled_scopes;

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

// Grouped payload macros must share a lexical scope with a zone from the same group. Pairing them
// across groups can attach data to an enclosing zone when one group is excluded at build time.
#ifdef DFLY_TRACY_BUILD_CONNECTION
#define DFLY_TRACY_CONNECTION_ZONE(name)     \
  SuppressVarShadowWarning(                  \
      ZoneNamedN(___tracy_scoped_zone, name, \
                 ::facade::IsTracyScopeEnabled(::facade::TracyScope::kConnection)))
#define DFLY_TRACY_CONNECTION_WAIT(name)                \
  SuppressVarShadowWarning(                             \
      ZoneNamedNC(___tracy_scoped_zone, name, 0xC0392B, \
                  ::facade::IsTracyScopeEnabled(::facade::TracyScope::kConnection)))
#ifdef DFLY_TRACY_FORENSIC
#define DFLY_TRACY_CONNECTION_FORENSIC_ZONE(name) DFLY_TRACY_CONNECTION_ZONE(name)
#else
#define DFLY_TRACY_CONNECTION_FORENSIC_ZONE(name) (void)sizeof(name)
#endif
#define DFLY_TRACY_CONNECTION_PLOT(name, value)                           \
  do {                                                                    \
    if (::facade::IsTracyScopeEnabled(::facade::TracyScope::kConnection)) \
      TracyPlot(name, value);                                             \
  } while (0)
#define DFLY_TRACY_CONNECTION_TEXT_SV(value)                              \
  do {                                                                    \
    if (::facade::IsTracyScopeEnabled(::facade::TracyScope::kConnection)) \
      DFLY_TRACY_ZONE_TEXT_SV(value);                                     \
  } while (0)
#else
#define DFLY_TRACY_CONNECTION_ZONE(name) (void)sizeof(name)
#define DFLY_TRACY_CONNECTION_WAIT(name) (void)sizeof(name)
#define DFLY_TRACY_CONNECTION_FORENSIC_ZONE(name) (void)sizeof(name)
#define DFLY_TRACY_CONNECTION_PLOT(name, value) \
  do {                                          \
    (void)sizeof(name);                         \
    (void)sizeof(value);                        \
  } while (0)
#define DFLY_TRACY_CONNECTION_TEXT_SV(value) (void)sizeof(value)
#endif
#ifdef DFLY_TRACY_BUILD_DISPATCH
#define DFLY_TRACY_DISPATCH_ZONE(name) \
  SuppressVarShadowWarning(ZoneNamedN( \
      ___tracy_scoped_zone, name, ::facade::IsTracyScopeEnabled(::facade::TracyScope::kDispatch)))
#ifdef DFLY_TRACY_FORENSIC
#define DFLY_TRACY_DISPATCH_FORENSIC_ZONE(name) DFLY_TRACY_DISPATCH_ZONE(name)
#define DFLY_TRACY_DISPATCH_FORENSIC_TEXT_SV(value)                     \
  do {                                                                  \
    if (::facade::IsTracyScopeEnabled(::facade::TracyScope::kDispatch)) \
      DFLY_TRACY_ZONE_TEXT_SV(value);                                   \
  } while (0)
#else
#define DFLY_TRACY_DISPATCH_FORENSIC_ZONE(name) (void)sizeof(name)
#define DFLY_TRACY_DISPATCH_FORENSIC_TEXT_SV(value) (void)sizeof(value)
#endif
#else
#define DFLY_TRACY_DISPATCH_ZONE(name) (void)sizeof(name)
#define DFLY_TRACY_DISPATCH_FORENSIC_ZONE(name) (void)sizeof(name)
#define DFLY_TRACY_DISPATCH_FORENSIC_TEXT_SV(value) (void)sizeof(value)
#endif
#ifdef DFLY_TRACY_BUILD_SQUASHER
#define DFLY_TRACY_SQUASHER_ZONE(name) \
  SuppressVarShadowWarning(ZoneNamedN( \
      ___tracy_scoped_zone, name, ::facade::IsTracyScopeEnabled(::facade::TracyScope::kSquasher)))
#ifdef DFLY_TRACY_FORENSIC
#define DFLY_TRACY_SQUASHER_FORENSIC_ZONE(name) DFLY_TRACY_SQUASHER_ZONE(name)
#else
#define DFLY_TRACY_SQUASHER_FORENSIC_ZONE(name) (void)sizeof(name)
#endif
#define DFLY_TRACY_SQUASHER_WAIT(name)                  \
  SuppressVarShadowWarning(                             \
      ZoneNamedNC(___tracy_scoped_zone, name, 0xC0392B, \
                  ::facade::IsTracyScopeEnabled(::facade::TracyScope::kSquasher)))
#define DFLY_TRACY_SQUASHER_VALUE(value)                                \
  do {                                                                  \
    if (::facade::IsTracyScopeEnabled(::facade::TracyScope::kSquasher)) \
      ZoneValue(value);                                                 \
  } while (0)
#define DFLY_TRACY_SQUASHER_TEXT(value)                                 \
  do {                                                                  \
    if (::facade::IsTracyScopeEnabled(::facade::TracyScope::kSquasher)) \
      DFLY_TRACY_ZONE_TEXT_SV(value);                                   \
  } while (0)
#define DFLY_TRACY_SQUASHER_TEXT_F(...)                                 \
  do {                                                                  \
    if (::facade::IsTracyScopeEnabled(::facade::TracyScope::kSquasher)) \
      ZoneTextF(__VA_ARGS__);                                           \
  } while (0)
#else
#define DFLY_TRACY_SQUASHER_ZONE(name) (void)sizeof(name)
#define DFLY_TRACY_SQUASHER_FORENSIC_ZONE(name) (void)sizeof(name)
#define DFLY_TRACY_SQUASHER_WAIT(name) (void)sizeof(name)
#define DFLY_TRACY_SQUASHER_VALUE(value) (void)sizeof(value)
#define DFLY_TRACY_SQUASHER_TEXT(value) (void)sizeof(value)
#define DFLY_TRACY_SQUASHER_TEXT_F(...) (void)sizeof(#__VA_ARGS__)
#endif
#ifdef DFLY_TRACY_BUILD_REPLY
#define DFLY_TRACY_REPLY_ZONE(name)    \
  SuppressVarShadowWarning(ZoneNamedN( \
      ___tracy_scoped_zone, name, ::facade::IsTracyScopeEnabled(::facade::TracyScope::kReply)))
#define DFLY_TRACY_REPLY_WAIT(name)                     \
  SuppressVarShadowWarning(                             \
      ZoneNamedNC(___tracy_scoped_zone, name, 0xC0392B, \
                  ::facade::IsTracyScopeEnabled(::facade::TracyScope::kReply)))
#ifdef DFLY_TRACY_FORENSIC
#define DFLY_TRACY_REPLY_FORENSIC_ZONE(name) DFLY_TRACY_REPLY_ZONE(name)
#else
#define DFLY_TRACY_REPLY_FORENSIC_ZONE(name) (void)sizeof(name)
#endif
#ifdef DFLY_TRACY_FORENSIC
#define DFLY_TRACY_REPLY_FORENSIC_VALUE(value)                       \
  do {                                                               \
    if (::facade::IsTracyScopeEnabled(::facade::TracyScope::kReply)) \
      ZoneValue(value);                                              \
  } while (0)
#else
#define DFLY_TRACY_REPLY_FORENSIC_VALUE(value) (void)sizeof(value)
#endif
#define DFLY_TRACY_REPLY_VALUE(value)                                \
  do {                                                               \
    if (::facade::IsTracyScopeEnabled(::facade::TracyScope::kReply)) \
      ZoneValue(value);                                              \
  } while (0)
#else
#define DFLY_TRACY_REPLY_ZONE(name) (void)sizeof(name)
#define DFLY_TRACY_REPLY_WAIT(name) (void)sizeof(name)
#define DFLY_TRACY_REPLY_FORENSIC_ZONE(name) (void)sizeof(name)
#define DFLY_TRACY_REPLY_FORENSIC_VALUE(value) (void)sizeof(value)
#define DFLY_TRACY_REPLY_VALUE(value) (void)sizeof(value)
#endif
#ifdef DFLY_TRACY_BUILD_MEMORY
#define DFLY_TRACY_MEMORY_ZONE(name)   \
  SuppressVarShadowWarning(ZoneNamedN( \
      ___tracy_scoped_zone, name, ::facade::IsTracyScopeEnabled(::facade::TracyScope::kMemory)))
#else
#define DFLY_TRACY_MEMORY_ZONE(name) (void)sizeof(name)
#endif

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
#define DFLY_TRACY_CONNECTION_ZONE(name) (void)sizeof(name)
#define DFLY_TRACY_CONNECTION_WAIT(name) (void)sizeof(name)
#define DFLY_TRACY_CONNECTION_FORENSIC_ZONE(name) (void)sizeof(name)
#define DFLY_TRACY_CONNECTION_PLOT(name, value) \
  do {                                          \
    (void)sizeof(name);                         \
    (void)sizeof(value);                        \
  } while (0)
#define DFLY_TRACY_CONNECTION_TEXT_SV(value) (void)sizeof(value)
#define DFLY_TRACY_DISPATCH_ZONE(name) (void)sizeof(name)
#define DFLY_TRACY_DISPATCH_FORENSIC_ZONE(name) (void)sizeof(name)
#define DFLY_TRACY_DISPATCH_FORENSIC_TEXT_SV(value) (void)sizeof(value)
#define DFLY_TRACY_SQUASHER_ZONE(name) (void)sizeof(name)
#define DFLY_TRACY_SQUASHER_FORENSIC_ZONE(name) (void)sizeof(name)
#define DFLY_TRACY_SQUASHER_WAIT(name) (void)sizeof(name)
#define DFLY_TRACY_SQUASHER_VALUE(value) (void)sizeof(value)
#define DFLY_TRACY_SQUASHER_TEXT(value) (void)sizeof(value)
#define DFLY_TRACY_SQUASHER_TEXT_F(...) (void)sizeof(#__VA_ARGS__)
#define DFLY_TRACY_REPLY_ZONE(name) (void)sizeof(name)
#define DFLY_TRACY_REPLY_WAIT(name) (void)sizeof(name)
#define DFLY_TRACY_REPLY_FORENSIC_ZONE(name) (void)sizeof(name)
#define DFLY_TRACY_REPLY_FORENSIC_VALUE(value) (void)sizeof(value)
#define DFLY_TRACY_REPLY_VALUE(value) (void)sizeof(value)
#define DFLY_TRACY_MEMORY_ZONE(name) (void)sizeof(name)

#endif  // TRACY_ENABLE
