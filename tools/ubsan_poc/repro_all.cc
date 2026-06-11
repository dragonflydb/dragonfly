// Expanded UBSan POC: 5 categories of integer bugs that hide silently.
// Build:  clang++ -O0 -g -fsanitize=undefined,implicit-conversion,integer \
//                 -fno-sanitize-recover=all repro_all.cc -o repro_all
// Run a single case with: ./repro_all <case_number>

#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstdlib>

// --- Case 1: implicit unsigned integer truncation (the PR #7562 bug) ---
// NOT UB. Standard says wrap. -fsanitize=undefined ALONE WILL MISS IT.
// Caught by: -fsanitize=implicit-unsigned-integer-truncation (or umbrella implicit-conversion)
thread_local uint64_t big = 1ULL << 32;
__attribute__((noinline)) unsigned narrow_unsigned(unsigned v) {
  return v;
}
int case1_unsigned_truncation() {
  unsigned r = narrow_unsigned(big);  // 2^32 -> 0
  std::printf("case1 unsigned-truncation: got %u\n", r);
  return r;
}

// --- Case 2: implicit signed integer truncation ---
// Same idea, signed types. NOT UB but almost always a bug.
// Caught by: -fsanitize=implicit-signed-integer-truncation (umbrella implicit-conversion)
__attribute__((noinline)) int16_t narrow_signed(int64_t v) {
  return v;
}
int case2_signed_truncation() {
  int64_t v = 70000;  // doesn't fit in int16_t (max 32767)
  int16_t r = narrow_signed(v);
  std::printf("case2 signed-truncation:   got %d\n", r);
  return r;
}

// --- Case 3: implicit integer sign change ---
// NOT UB. Negative signed -> large unsigned. Common in size_t / index APIs.
// Caught by: -fsanitize=implicit-integer-sign-change (umbrella implicit-conversion)
__attribute__((noinline)) void take_size_t(size_t n) {
  std::printf("case3 sign-change:         got size_t=%zu\n", n);
}
int case3_sign_change() {
  int idx = -1;
  take_size_t(idx);  // -1 -> SIZE_MAX
  return 0;
}

// --- Case 4: signed integer overflow ---
// IS UB. Caught by the default -fsanitize=undefined group.
__attribute__((noinline)) int sadd(int a, int b) {
  return a + b;
}
int case4_signed_overflow() {
  int r = sadd(INT_MAX, 1);
  std::printf("case4 signed-overflow:     got %d\n", r);
  return r;
}

// --- Case 5: unsigned integer overflow ---
// NOT UB (wrap is well-defined). But routinely a bug in length math etc.
// Caught ONLY by: -fsanitize=unsigned-integer-overflow (umbrella -fsanitize=integer)
__attribute__((noinline)) unsigned uadd(unsigned a, unsigned b) {
  return a + b;
}
int case5_unsigned_overflow() {
  unsigned r = uadd(UINT_MAX, 1);
  std::printf("case5 unsigned-overflow:   got %u\n", r);
  return r;
}

// --- Case 6: char/short overflow (google/sanitizers#940) ---
// Promoted to int first, then truncated -> NOT signed overflow, hidden bug.
// Caught by: -fsanitize=implicit-signed-integer-truncation.
__attribute__((noinline)) int8_t inc_char(int8_t c) {
  return c + 1;
}
int case6_char_overflow() {
  int8_t r = inc_char(SCHAR_MAX);  // 127 + 1 promoted to int(128), truncated to -128
  std::printf("case6 char-overflow:       got %d\n", r);
  return r;
}

int main(int argc, char** argv) {
  int which = argc > 1 ? std::atoi(argv[1]) : 0;
  if (which == 0 || which == 1)
    case1_unsigned_truncation();
  if (which == 0 || which == 2)
    case2_signed_truncation();
  if (which == 0 || which == 3)
    case3_sign_change();
  if (which == 0 || which == 4)
    case4_signed_overflow();
  if (which == 0 || which == 5)
    case5_unsigned_overflow();
  if (which == 0 || which == 6)
    case6_char_overflow();
  return 0;
}
