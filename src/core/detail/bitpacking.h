// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstddef>
#include <cstdint>

namespace dfly {

namespace detail {

bool validate_ascii_fast(const char* src, size_t len);

// unpacks 8->7 encoded blob back to ascii.
// generally, we can not unpack inplace because ascii (dest) buffer is 8/7 bigger than
// the source buffer.
// however, if binary data is positioned on the right of the ascii buffer with empty space on the
// left than we can unpack inplace.
void ascii_unpack(const uint8_t* bin, size_t ascii_len, char* ascii);
void ascii_unpack_simd(const uint8_t* bin, size_t ascii_len, char* ascii);

// Access a single byte in a 7-bit ASCII-packed string without unpacking the entire buffer.
// These helpers read/write the ASCII byte at logical position `idx` in the unpacked string
// directly from/into the packed `bin` representation.
// It's up to caller to verify:
// `1. idx` must be less than `ascii_len` to avoid out-of-bounds access.
// 2. `ascii` must be less than 128 (7-bit ASCII) for packing.
uint8_t ascii_unpack_byte(const uint8_t* bin, size_t ascii_len, size_t idx);
void ascii_pack_byte(uint8_t* bin, size_t ascii_len, size_t idx, uint8_t ascii);

// packs ascii string (does not verify) into binary form saving 1 bit per byte on average (12.5%).
void ascii_pack(const char* ascii, size_t len, uint8_t* bin);
void ascii_pack2(const char* ascii, size_t len, uint8_t* bin);

// SIMD implementation 1 of ascii_pack.
void ascii_pack_simd(const char* ascii, size_t len, uint8_t* bin);

// SIMD implementation 2 of ascii_pack.
void ascii_pack_simd2(const char* ascii, size_t len, uint8_t* bin);

bool compare_packed(const uint8_t* packed, const char* ascii, size_t ascii_len);

// maps ascii len to 7-bit packed length. Each 8 bytes are converted to 7 bytes.
inline constexpr size_t binpacked_len(size_t ascii_len) {
  // Avoid multiplication overflow.
  return ascii_len - ascii_len / 8;
}

// converts 7-bit packed length back to ascii length. Note that this conversion
// is not accurate since it maps 7 bytes to 8 bytes (rounds up), while we may have
// 7 byte strings converted to 7 byte as well.
inline constexpr size_t ascii_len(size_t bin_len) {
  return (bin_len * 8) / 7;
}

// ---------------------------------------------------------------------------
// ascii_try_pack / ascii_unpack_fast performance
//
// Throughput in GiB/s (see BM_AsciiCodec in compact_object_test.cc, run with
// `--bench --benchmark_filter=AsciiCodec` to reproduce). ascii_pack* neither validate nor report
// failure; "validate + X" adds a separate validate_ascii_fast() pass to match what ascii_try_pack
// does in a single pass.
//
// Release build, x86-64 AVX2/BMI2 dispatch, AMD Ryzen AI 7 350:
//                       32     64   1024   4096
//   ascii_pack            4.1    4.9    5.8    6.0
//   ascii_pack2           5.6    6.9    8.7    8.6
//   ascii_pack_simd       4.8    9.4   19.1   18.5
//   ascii_pack_simd2      5.3   10.2   32.5   32.8
//   validate + pack2      5.0    6.4    7.8    7.3
//   validate + simd2      4.8    8.8   20.4   18.2
//   ascii_try_pack       12.9   17.9   47.3   51.9
//
//   ascii_unpack          3.6    3.9    4.1    4.1
//   ascii_unpack_simd     4.3    8.4   18.4   18.1
//   ascii_unpack_fast    19.2   28.6   49.8   44.3
//
// Release build, aarch64 NEON dispatch, AWS Graviton2 / Neoverse-N1:
//                       32     64   1024   4096
//   ascii_pack            1.7    1.9    2.2    2.2
//   ascii_pack2           2.3    2.7    3.3    3.3
//   ascii_pack_simd       2.0    2.8    3.9    4.0
//   ascii_pack_simd2      2.0    2.8    4.0    4.1
//   validate + pack2      1.6    1.9    2.4    2.4
//   validate + simd2      1.4    2.0    2.8    2.8
//   ascii_try_pack        2.8    3.9    6.0    6.1
//
//   ascii_unpack          1.7    1.8    1.9    1.9
//   ascii_unpack_simd     2.1    2.6    3.6    3.6
//   ascii_unpack_fast     3.8    4.0    4.9    4.9
//
// Why it's faster:
//  - ascii_try_pack validates and packs in one pass (each loaded vector is OR'ed into an error
//    accumulator right where it is already being packed), instead of two full passes over memory
//    like the "validate + X" rows above.
//  - x86: AVX2 packs 32 ascii bytes (28 packed) per iteration, twice the legacy SSE3 path's 16
//    (14 packed), and merges the 7-bit fields with a multiply-based trick (2 ops replace 2
//    mask+shift+or stages). The scalar fallback uses a single BMI2 pext/pdep instruction in place
//    of a multi-step manual bit-scatter.
//  - aarch64: the block width is unchanged (NEON is 128-bit in both the legacy and the new code).
//    The gain instead comes from vsli's fused shift-and-insert, which merges the 7-bit fields in 3
//    instructions with no masks needed, versus the mask+shift+or chain that ascii_pack_simd/_simd2
//    also use on this architecture; and from an exact 14-byte NEON store that keeps lengths that
//    are 0 or 1 modulo 16 on that path, where x86 (lacking a cheap exact-14-byte store) drops to
//    the scalar tail.
//  - Leftover bytes (any length not a multiple of the block size) are copied by a small helper
//    doing at most two overlapping loads and stores sized to the remainder, instead of the
//    byte-at-a-time loop the legacy functions fall back to above.
// ---------------------------------------------------------------------------

// The two functions below only ever run the widest kernel this build targets, so a caller can
// never reach the others and they would go untested. Every kernel therefore also gets its own
// entry point here. They all honour the contract of the function they implement, for any length,
// so they are interchangeable and tests can drive them side by side over the same input.
namespace impl {

#if defined(__SSE3__) || defined(__aarch64__)
#define DFLY_ASCII_TRY_PACK_HAS_SIMD 1
#endif
#if defined(__AVX2__) && defined(__x86_64__)
#define DFLY_ASCII_TRY_PACK_HAS_AVX2 1
#endif

size_t ascii_try_pack_scalar(const char* ascii, size_t len, uint8_t* bin);
#ifdef DFLY_ASCII_TRY_PACK_HAS_SIMD
size_t ascii_try_pack_simd(const char* ascii, size_t len, uint8_t* bin);  // sse3 or neon
#endif
#ifdef DFLY_ASCII_TRY_PACK_HAS_AVX2
size_t ascii_try_pack_avx2(const char* ascii, size_t len, uint8_t* bin);
#endif

#if defined(__AVX2__) && defined(__x86_64__)
#define DFLY_ASCII_UNPACK_FAST_HAS_AVX2 1
#elif defined(__aarch64__)
#define DFLY_ASCII_UNPACK_FAST_HAS_NEON 1
#endif

void ascii_unpack_fast_scalar(const uint8_t* bin, size_t ascii_len, char* ascii);
#ifdef DFLY_ASCII_UNPACK_FAST_HAS_AVX2
void ascii_unpack_fast_avx2(const uint8_t* bin, size_t ascii_len, char* ascii);
#endif
#ifdef DFLY_ASCII_UNPACK_FAST_HAS_NEON
void ascii_unpack_fast_neon(const uint8_t* bin, size_t ascii_len, char* ascii);
#endif

}  // namespace impl

// Validates and packs a string of any length. Returns binpacked_len(len), or 0 if any byte is not
// 7-bit ASCII. `bin` must not overlap `ascii`, needs binpacked_len(len) bytes, and is written even
// on failure. Prefer this over ascii_pack when validation is needed; empty input also returns 0.
// Inline, so that a caller reaches the widest kernel this build targets with a single direct call
// instead of bouncing through an out of line dispatcher.
inline size_t ascii_try_pack(const char* ascii, size_t len, uint8_t* bin) {
#if defined(DFLY_ASCII_TRY_PACK_HAS_AVX2)
  return impl::ascii_try_pack_avx2(ascii, len, bin);
#elif defined(DFLY_ASCII_TRY_PACK_HAS_SIMD)
  return impl::ascii_try_pack_simd(ascii, len, bin);
#else
  return impl::ascii_try_pack_scalar(ascii, len, bin);
#endif
}

// Preferred unpack implementation, for a string of any length. Buffers need
// binpacked_len(ascii_len) readable and ascii_len writable bytes. In-place input must be
// right-aligned.
inline void ascii_unpack_fast(const uint8_t* bin, size_t ascii_len, char* ascii) {
#if defined(DFLY_ASCII_UNPACK_FAST_HAS_AVX2)
  impl::ascii_unpack_fast_avx2(bin, ascii_len, ascii);
#elif defined(DFLY_ASCII_UNPACK_FAST_HAS_NEON)
  impl::ascii_unpack_fast_neon(bin, ascii_len, ascii);
#else
  impl::ascii_unpack_fast_scalar(bin, ascii_len, ascii);
#endif
}

}  // namespace detail
}  // namespace dfly
