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

size_t ascii_try_pack_scalar(const char* ascii, size_t len, uint8_t* bin);
void ascii_unpack_fast_scalar(const uint8_t* bin, size_t ascii_len, char* ascii);

#if defined(__AVX2__)
#define ASCII_AVX2 1
#define ASCII_SSE4_1 1
#define ASCII_SSSE3 1
size_t ascii_try_pack_avx2(const char* ascii, size_t len, uint8_t* bin);
void ascii_unpack_fast_avx2(const uint8_t* bin, size_t ascii_len, char* ascii);
size_t ascii_try_pack_sse(const char* ascii, size_t len, uint8_t* bin);
void ascii_unpack_fast_sse(const uint8_t* bin, size_t ascii_len, char* ascii);
#elif defined(__SSE4_1__)
#define ASCII_SSE4_1 1
#define ASCII_SSSE3 1
size_t ascii_try_pack_sse(const char* ascii, size_t len, uint8_t* bin);

// Needs SSE4.1 for blend_epi16, which the release builds target (see Makefile).
void ascii_unpack_fast_sse(const uint8_t* bin, size_t ascii_len, char* ascii);
#elif defined(__SSSE3__)
#define ASCII_SSSE3 1
size_t ascii_try_pack_sse(const char* ascii, size_t len, uint8_t* bin);
#elif defined(__aarch64__)
#define ASCII_NEON 1
size_t ascii_try_pack_neon(const char* ascii, size_t len, uint8_t* bin);
void ascii_unpack_fast_neon(const uint8_t* bin, size_t ascii_len, char* ascii);
#endif

}  // namespace impl

// Validates and packs a string of any length. Returns binpacked_len(len), or 0 if any byte is not
// 7-bit ASCII. `bin` must not overlap `ascii` and needs binpacked_len(len) bytes; its contents are
// unspecified on failure. Empty input also returns 0. Full groups of 8 bytes pack into 7 bytes; the
// remaining bytes are copied verbatim.
// Inline, so that a caller reaches the widest kernel this build targets with a single direct call
// instead of bouncing through an out of line dispatcher.
inline size_t ascii_try_pack(const char* ascii, size_t len, uint8_t* bin) {
#if defined(ASCII_AVX2)
  return impl::ascii_try_pack_avx2(ascii, len, bin);
#endif
#if defined(ASCII_SSSE3)
  return impl::ascii_try_pack_sse(ascii, len, bin);
#endif
#if defined(ASCII_NEON)
  return impl::ascii_try_pack_neon(ascii, len, bin);
#endif
  return impl::ascii_try_pack_scalar(ascii, len, bin);
}

// Unpacks a string of any decoded length. Buffers need
// binpacked_len(ascii_len) readable and ascii_len writable bytes. In-place input must be
// right-aligned.
inline void ascii_unpack_fast(const uint8_t* bin, size_t ascii_len, char* ascii) {
#if defined(ASCII_AVX2)
  return impl::ascii_unpack_fast_avx2(bin, ascii_len, ascii);
#endif
#if defined(ASCII_SSE4_1)
  return impl::ascii_unpack_fast_sse(bin, ascii_len, ascii);
#endif
#if defined(ASCII_NEON)
  return impl::ascii_unpack_fast_neon(bin, ascii_len, ascii);
#endif
  return impl::ascii_unpack_fast_scalar(bin, ascii_len, ascii);
}

}  // namespace detail
}  // namespace dfly
