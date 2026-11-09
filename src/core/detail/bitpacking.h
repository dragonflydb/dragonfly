// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstddef>
#include <cstdint>

namespace dfly {

namespace detail {

bool validate_ascii_fast(const char* src, size_t len);

// Access a single byte in a 7-bit ASCII-packed string without unpacking the entire buffer.
// These helpers read/write the ASCII byte at logical position `idx` in the unpacked string
// directly from/into the packed `bin` representation.
// It's up to caller to verify:
// 1. `idx` must be less than `ascii_len` to avoid out-of-bounds access.
// 2. `ascii` must be less than 128 (7-bit ASCII) for packing.
uint8_t ascii_unpack_byte(const uint8_t* bin, size_t ascii_len, size_t idx);
void ascii_pack_byte(uint8_t* bin, size_t ascii_len, size_t idx, uint8_t ascii);

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

// The two functions below only ever run the widest kernel this build targets, so a caller can
// never reach the others and they would go untested. Every kernel therefore also gets its own
// entry point here. They all honour the contract of the function they implement, for any length,
// so they are interchangeable and tests can drive them side by side over the same input.
// Compare kernels with compact_object_test --bench --benchmark_filter=AsciiCodec.
namespace impl {

#if defined(__AVX2__) && defined(__x86_64__)
#define DFLY_ASCII_TRY_PACK_HAS_AVX2 1
#endif
#if defined(__SSSE3__)
#define DFLY_ASCII_TRY_PACK_HAS_SSE 1
#endif

// All vector paths use the shared 128-bit kernels.
#if defined(DFLY_ASCII_TRY_PACK_HAS_AVX2) || defined(DFLY_ASCII_TRY_PACK_HAS_SSE) || \
    defined(__aarch64__)
#define DFLY_ASCII_TRY_PACK_HAS_SIMD 1
#endif

size_t ascii_try_pack_scalar(const char* ascii, size_t len, uint8_t* bin);
#ifdef DFLY_ASCII_TRY_PACK_HAS_SSE
size_t ascii_try_pack_sse(const char* ascii, size_t len, uint8_t* bin);
#endif
#ifdef DFLY_ASCII_TRY_PACK_HAS_SIMD
size_t ascii_try_pack_simd(const char* ascii, size_t len, uint8_t* bin);  // ssse3 or neon
#endif
#ifdef DFLY_ASCII_TRY_PACK_HAS_AVX2
size_t ascii_try_pack_avx2(const char* ascii, size_t len, uint8_t* bin);
#endif

#if defined(__AVX2__) && defined(__x86_64__)
#define DFLY_ASCII_UNPACK_FAST_HAS_AVX2 1
#elif defined(__aarch64__)
#define DFLY_ASCII_UNPACK_FAST_HAS_NEON 1
#endif
// Needs SSE4.1 for blend_epi16, which the release builds target (see Makefile).
#if defined(__SSE4_1__) && defined(__x86_64__)
#define DFLY_ASCII_UNPACK_FAST_HAS_SSE 1
#endif

void ascii_unpack_fast_scalar(const uint8_t* bin, size_t ascii_len, char* ascii);
#ifdef DFLY_ASCII_UNPACK_FAST_HAS_SSE
void ascii_unpack_fast_sse(const uint8_t* bin, size_t ascii_len, char* ascii);
#endif
#ifdef DFLY_ASCII_UNPACK_FAST_HAS_AVX2
void ascii_unpack_fast_avx2(const uint8_t* bin, size_t ascii_len, char* ascii);
#endif
#ifdef DFLY_ASCII_UNPACK_FAST_HAS_NEON
void ascii_unpack_fast_neon(const uint8_t* bin, size_t ascii_len, char* ascii);
#endif

}  // namespace impl

// Validates and packs a string of any length. Returns binpacked_len(len), or 0 if any byte is not
// 7-bit ASCII. `bin` must not overlap `ascii`, needs binpacked_len(len) bytes, and is written even
// on failure. Empty input also returns 0. Full groups of 8 bytes pack into 7 bytes; the remaining
// bytes are copied verbatim.
// Inline, so that a caller reaches the widest kernel this build targets with a single direct call
// instead of bouncing through an out of line dispatcher.
inline size_t ascii_try_pack(const char* ascii, size_t len, uint8_t* bin) {
#if defined(DFLY_ASCII_TRY_PACK_HAS_AVX2)
  return impl::ascii_try_pack_avx2(ascii, len, bin);
#elif defined(DFLY_ASCII_TRY_PACK_HAS_SSE)
  return impl::ascii_try_pack_sse(ascii, len, bin);
#elif defined(DFLY_ASCII_TRY_PACK_HAS_SIMD)
  return impl::ascii_try_pack_simd(ascii, len, bin);
#else
  return impl::ascii_try_pack_scalar(ascii, len, bin);
#endif
}

// Unpacks a string of any decoded length. Buffers need
// binpacked_len(ascii_len) readable and ascii_len writable bytes. In-place input must be
// right-aligned.
inline void ascii_unpack_fast(const uint8_t* bin, size_t ascii_len, char* ascii) {
#if defined(DFLY_ASCII_UNPACK_FAST_HAS_AVX2)
  impl::ascii_unpack_fast_avx2(bin, ascii_len, ascii);
#elif defined(DFLY_ASCII_UNPACK_FAST_HAS_SSE)
  impl::ascii_unpack_fast_sse(bin, ascii_len, ascii);
#elif defined(DFLY_ASCII_UNPACK_FAST_HAS_NEON)
  impl::ascii_unpack_fast_neon(bin, ascii_len, ascii);
#else
  impl::ascii_unpack_fast_scalar(bin, ascii_len, ascii);
#endif
}

}  // namespace detail
}  // namespace dfly
