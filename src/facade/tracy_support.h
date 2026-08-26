// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

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

#include <string_view>
#include <tracy/Tracy.hpp>

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

#else  // !TRACY_ENABLE

// No-op fallbacks. Use sizeof to keep arguments unevaluated while silencing unused warnings.
#define DFLY_TRACY_ZONE(name) (void)sizeof(name)
#define DFLY_TRACY_ZONE_TEXT(txt, size) \
  do {                                  \
    (void)sizeof(txt);                  \
    (void)sizeof(size);                 \
  } while (0)
#define DFLY_TRACY_ZONE_TEXT_SV(sv) (void)sizeof(sv)
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

#endif  // TRACY_ENABLE
