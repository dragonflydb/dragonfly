// UBSan self-test for Dragonfly.
//
// PURPOSE
//   Prove that every UBSan check enabled in the build is actually live.
//   Each case deliberately triggers ONE compile-time check. Run this against
//   any sanitized dragonfly binary to verify the toolchain + flags are working
//   before trusting a clean run result.
//
//   Usage: apply tools/sanitizers/ubsan/inject_ubsan_selftest.patch, build
//   dragonfly, then run ./dragonfly -- the patched binary runs RunUbsanSelfTest()
//   at startup and exits before serving traffic.
//
//   Build/run with UBSAN_OPTIONS=halt_on_error=0 (UBSan's default) so that every
//   case reports and the process keeps going; otherwise it stops at the first
//   finding.
//
// CHECK TIERS: each case is tagged "base" or "strict".
//   base   -> fires with stock `-fsanitize=undefined` (WITH_UBSAN): the
//             array-bounds and signed-integer-overflow cases.
//   strict -> fires only with the extended checks (WITH_UBSAN_STRICT): the
//             implicit-conversion class, integer, function, vptr, object-size,
//             nullability. These are Clang-only.
//   A WITH_UBSAN-only build triggers just the base case(s); the other cases run
//   but produce no diagnostic. There is no per-check compile-time macro, so the
//   tier tags are informational -- the CI builds with WITH_UBSAN_STRICT=ON and
//   asserts that every case fired.
//
// RUNTIME-OPTION COVERAGE (not separate code cases - re-run this binary with
//   different UBSAN_OPTIONS to exercise each):
//     report_error_type=1         names the exact check in the SUMMARY line
//     halt_on_error=1             aborts on the first finding
//     log_path=<dir>/prefix       writes diagnostics to <dir>/prefix.<pid>
//     print_stacktrace=1          full symbolized stack per finding
//     silence_unsigned_overflow=1 mutes case 8 (unsigned wrap)
//     suppressions=<file>         skips entries in tools/sanitizers/ubsan/ubsan-suppressions.txt
//
// This file is NOT compiled into dragonfly by default. It is intentionally
// full of undefined / lossy operations and must only ever be built with UBSan.

#include <cstdint>
#include <cstdio>
#include <cstring>

// This translation unit is *meant* to contain UB / lossy conversions. Silence
// the corresponding -W warnings so it compiles even under -Werror.
#if defined(__clang__)
#pragma clang diagnostic ignored "-Wconversion"
#pragma clang diagnostic ignored "-Wimplicit-int-conversion"
#pragma clang diagnostic ignored "-Wshorten-64-to-32"
#pragma clang diagnostic ignored "-Wsign-conversion"
#pragma clang diagnostic ignored "-Wint-conversion"
#pragma clang diagnostic ignored "-Wcast-function-type"
#pragma clang diagnostic ignored "-Warray-bounds"
#pragma clang diagnostic ignored "-Wunused-variable"
#pragma clang diagnostic ignored "-Wreturn-type"
#endif

namespace {

// volatile sink prevents the optimizer from eliding our intentional UB at -O1.
volatile uint64_t g_sink = 0;

// optnone (keeps the UB from being optimized away at -O1) is Clang-only. Degrade to
// plain noinline on GCC so the patched self-test still compiles under -Werror.
#if defined(__clang__)
#define UBSAN_CASE __attribute__((noinline, optnone))
#else
#define UBSAN_CASE __attribute__((noinline))
#endif

// Compile-time check #1: implicit-unsigned-integer-truncation.
// PR #7562 pattern: 1ULL<<32 narrowed to unsigned silently becomes 0.
UBSAN_CASE void Case_ImplicitUnsignedTruncation() {
  volatile uint64_t big = 1ULL << 32;  // 4294967296
  unsigned narrowed = big;             // truncates to 0
  g_sink = narrowed;
  std::printf("[1] implicit-unsigned-integer-truncation: %llu -> %u\n",
              static_cast<unsigned long long>(big), narrowed);
}

// Compile-time check #2: implicit-signed-integer-truncation
UBSAN_CASE void Case_ImplicitSignedTruncation() {
  volatile int64_t big = 0x1'0000'0001LL;  // does not fit in int32
  int narrowed = big;
  g_sink = static_cast<uint64_t>(narrowed);
  std::printf("[2] implicit-signed-integer-truncation\n");
}

// Compile-time check #3: implicit-integer-sign-change
UBSAN_CASE void Case_ImplicitSignChange() {
  volatile int neg = -1;
  unsigned u = neg;  // sign change -1 -> UINT_MAX
  g_sink = u;
  std::printf("[3] implicit-integer-sign-change\n");
}

// Compile-time check #4: array-bounds (base: -fsanitize=undefined).
// Indexing past an array's declared extent. Caught recoverably by array-bounds.
// (local-bounds is intentionally NOT enabled: it traps with SIGILL and cannot be
// made recoverable, which would abort the rest of the run.)
UBSAN_CASE void Case_ArrayBounds() {
  int arr[4] = {0, 1, 2, 3};
  volatile int idx = 7;  // out of [0,4)
  g_sink = static_cast<uint64_t>(arr[idx]);
  std::printf("[4] array-bounds\n");
}

// Compile-time check #5: nullability.
UBSAN_CASE static void TakesNonNull(int* _Nonnull p) {
  g_sink = reinterpret_cast<uintptr_t>(p);
}
UBSAN_CASE void Case_Nullability() {
  int* volatile p = nullptr;
  TakesNonNull(p);  // null passed where _Nonnull expected
  std::printf("[5] nullability\n");
}

// Compile-time check #6: float-divide-by-zero
UBSAN_CASE void Case_FloatDivByZero() {
  volatile double num = 1.0;
  volatile double den = 0.0;
  volatile double r = num / den;  // +inf, flagged
  g_sink = static_cast<uint64_t>(r != r);
  std::printf("[6] float-divide-by-zero\n");
}

// Compile-time check #7: signed-integer-overflow (in -fsanitize=undefined)
UBSAN_CASE void Case_SignedOverflow() {
  volatile int32_t a = 0x7fffffff;  // INT_MAX
  int32_t b = a + 1;                // signed overflow == UB
  g_sink = static_cast<uint64_t>(b);
  std::printf("[7] signed-integer-overflow\n");
}

// Compile-time check #8: unsigned-integer-overflow (STRICT: integer)
UBSAN_CASE void Case_UnsignedOverflow() {
  volatile uint32_t a = 0xffffffffu;  // UINT_MAX
  uint32_t b = a + 1;                 // wraps to 0 (well-defined, but flagged)
  g_sink = b;
  std::printf("[8] unsigned-integer-overflow\n");
}

// Compile-time check #9: unsigned-shift-base (STRICT: integer)
UBSAN_CASE void Case_UnsignedShiftBase() {
  volatile uint32_t a = 0xff000000u;
  uint32_t b = a << 8;  // shifts bits out of a 32-bit value
  g_sink = b;
  std::printf("[9] unsigned-shift-base\n");
}

// Compile-time check #10: function (STRICT)
UBSAN_CASE static void RealTakesInt(int x) {
  g_sink = static_cast<uint64_t>(x);
}
UBSAN_CASE void Case_FunctionType() {
  using WrongFn = void (*)(const char*);
  WrongFn fp = reinterpret_cast<WrongFn>(&RealTakesInt);
  fp("hello");  // indirect call through wrong function-pointer type
  std::printf("[10] function\n");
}

// Compile-time check #11: vptr (STRICT)
struct VBase {
  virtual ~VBase() = default;
  virtual void f() {
    g_sink = 0xB;
  }
};

struct VDerived : VBase {
  void f() override {
    g_sink = 0xD;
  }
  int extra = 0;
};

UBSAN_CASE void Case_Vptr() {
  VBase base;
  // 'base' is really a VBase, but we access it as VDerived and make a virtual
  // call. -fsanitize=vptr reports the type mismatch here. In recoverable mode it
  // reports and then dispatches through base's real vtable (VBase::f), so it is
  // safe to continue.
  VDerived* d = reinterpret_cast<VDerived*>(&base);
  d->f();
  std::printf("[11] vptr\n");
}

// Compile-time check #12: object-size (STRICT; needs -O1).
// object-size is the one check that needs the optimizer: __builtin_object_size is
// "unknown" under -O0/optnone. So this case must NOT use UBSAN_CASE (it adds
// optnone) -- plain noinline keeps the build's -O1 and still gives a clean frame.
// Store through a pointer, not memset: object-size instruments scalar stores, not
// the memset libcall.
__attribute__((noinline)) void Case_ObjectSize() {
  char buf[4] = {};
  char* p = buf;  // pointer, not array: array-bounds ignores p[]; object-size checks it.
  // Offset must be a compile-time constant: objectsize(p + off) only folds to a
  // known size (0 here, past the 4-byte buf) for a constant off -- a runtime/volatile
  // index folds to "unknown" and never fires. The dead store survives because the
  // check itself is a call with side effects.
  p[8] = 7;  // store past buf; objectsize(p + 8) == 0 -> flagged at -O1+
  g_sink = static_cast<uint64_t>(buf[0]);
  std::printf("[12] object-size (effective only at -O1+)\n");
}

}  // namespace

// Runs every UBSan self-test case. Declared (not in a header) in dfly_main.cc by
// the inject patch (tools/sanitizers/ubsan/inject_ubsan_selftest.patch).
extern "C" void RunUbsanSelfTest() {
  std::printf("=== Dragonfly UBSan self-test ===\n");
  struct {
    int id;
    const char* tier;  // "base" = -fsanitize=undefined, "strict" = WITH_UBSAN_STRICT
    void (*fn)();
  } cases[] = {
      {1, "strict", &Case_ImplicitUnsignedTruncation},
      {2, "strict", &Case_ImplicitSignedTruncation},
      {3, "strict", &Case_ImplicitSignChange},
      {4, "base", &Case_ArrayBounds},
      {5, "strict", &Case_Nullability},
      {6, "strict", &Case_FloatDivByZero},
      {7, "base", &Case_SignedOverflow},
      {8, "strict", &Case_UnsignedOverflow},
      {9, "strict", &Case_UnsignedShiftBase},
      {10, "strict", &Case_FunctionType},
      {11, "strict", &Case_Vptr},
      {12, "strict", &Case_ObjectSize},
  };
  for (auto& c : cases) {
    std::printf("-- case %d [%s] --\n", c.id, c.tier);
    c.fn();
  }
  std::printf("=== UBSan self-test done (sink=%llu) ===\n",
              static_cast<unsigned long long>(g_sink));
}
