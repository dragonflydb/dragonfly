// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "src/core/detail/bitpacking.h"

#include <absl/base/internal/endian.h>

#include "base/logging.h"
#include "core/sse_port.h"

using namespace std;

namespace dfly {

namespace detail {

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC push_options
#pragma GCC optimize("Ofast")
#endif

static inline uint64_t Compress8x7bit(uint64_t x) {
  x = ((x & 0x7F007F007F007F00) >> 1) | (x & 0x007F007F007F007F);
  x = ((x & 0x3FFF00003FFF0000) >> 2) | (x & 0x00003FFF00003FFF);
  x = ((x & 0x0FFFFFFF00000000) >> 4) | (x & 0x000000000FFFFFFF);

  return x;
}

#if defined(__SSE3__) || defined(__aarch64__)
static inline pair<const char*, uint8_t*> simd_variant1_pack(const char* ascii, const char* end,
                                                             uint8_t* bin) {
  __m128i val, rpart, lpart;

  // Skips 8th byte (indexc 7) in the lower 8-byte part.
  const __m128i control = _mm_set_epi8(-1, -1, 14, 13, 12, 11, 10, 9, 8, 6, 5, 4, 3, 2, 1, 0);

  // Based on the question I asked here: https://stackoverflow.com/q/74831843/2280111
  while (ascii <= end) {
    val = mm_loadu_si128(reinterpret_cast<const __m128i*>(ascii));

    /*
    x = ((x & 0x7F007F007F007F00) >> 1) | (x & 0x007F007F007F007F);
    x = ((x & 0x3FFF00003FFF0000) >> 2) | (x & 0x00003FFF00003FFF);
    x = ((x & 0x0FFFFFFF00000000) >> 4) | (x & 0x000000000FFFFFFF);
    */

    rpart = _mm_and_si128(val, _mm_set1_epi64x(0x007F007F007F007F));
    lpart = _mm_and_si128(val, _mm_set1_epi64x(0x7F007F007F007F00));
    val = _mm_or_si128(_mm_srli_epi64(lpart, 1), rpart);

    rpart = _mm_and_si128(val, _mm_set1_epi64x(0x00003FFF00003FFF));
    lpart = _mm_and_si128(val, _mm_set1_epi64x(0x3FFF00003FFF0000));
    val = _mm_or_si128(_mm_srli_epi64(lpart, 2), rpart);

    rpart = _mm_and_si128(val, _mm_set1_epi64x(0x000000000FFFFFFF));
    lpart = _mm_and_si128(val, _mm_set1_epi64x(0x0FFFFFFF00000000));
    val = _mm_or_si128(_mm_srli_epi64(lpart, 4), rpart);

    val = _mm_shuffle_epi8(val, control);
    _mm_storeu_si128(reinterpret_cast<__m128i*>(bin), val);
    bin += 14;
    ascii += 16;
  }

  return make_pair(ascii, bin);
}

static inline pair<const char*, uint8_t*> simd_variant2_pack(const char* ascii, const char* end,
                                                             uint8_t* bin) {
  // Skips 8th byte (indexc 7) in the lower 8-byte part.
  const __m128i control = _mm_set_epi8(-1, -1, 14, 13, 12, 11, 10, 9, 8, 6, 5, 4, 3, 2, 1, 0);

  __m128i val, rpart, lpart;

  // Based on the question I asked here: https://stackoverflow.com/q/74831843/2280111
  while (ascii <= end) {
    val = mm_loadu_si128(reinterpret_cast<const __m128i*>(ascii));

    /*
    x = ((x & 0x7F007F007F007F00) >> 1) | (x & 0x007F007F007F007F);
    x = ((x & 0x3FFF00003FFF0000) >> 2) | (x & 0x00003FFF00003FFF);
    x = ((x & 0x0FFFFFFF00000000) >> 4) | (x & 0x000000000FFFFFFF);
    */
    val = _mm_maddubs_epi16(_mm_set1_epi16(0x8001), val);
    val = _mm_madd_epi16(_mm_set1_epi32(0x40000001), val);

    rpart = _mm_and_si128(val, _mm_set1_epi64x(0x000000000FFFFFFF));
    lpart = _mm_and_si128(val, _mm_set1_epi64x(0x0FFFFFFF00000000));
    val = _mm_or_si128(_mm_srli_epi64(lpart, 4), rpart);

    val = _mm_shuffle_epi8(val, control);
    _mm_storeu_si128(reinterpret_cast<__m128i*>(bin), val);
    bin += 14;
    ascii += 16;
  }
  return make_pair(ascii, bin);
}

#endif

// Daniel Lemire's function validate_ascii_fast() - under Apache/MIT license.
// See https://github.com/lemire/fastvalidate-utf-8/
// The function returns true (1) if all chars passed in src are
// 7-bit values (0x00..0x7F). Otherwise, it returns false (0).
#ifdef __s390x__
bool validate_ascii_fast(const char* src, size_t len) {
  size_t i = 0;

  // Initialize a vector in which all the elements are set to zero.
  vector unsigned char has_error = vec_splat_s8(0);
  if (len >= 16) {
    for (; i <= len - 16; i += 16) {
      // Load 16 bytes from buffer into a vector.
      vector unsigned char current_bytes = vec_load_len((signed char*)(src + i), 16);
      // Perform a bitwise OR operation between the current and the previously loaded contents.
      has_error = vec_orc(has_error, current_bytes);
    }
  }

  // Initialize a vector in which all the elements are set to an invalid ASCII value.
  vector unsigned char rep_invalid_values = vec_splat_s8(0x80);

  // Perform bitwise AND-complement operation between two vectors.
  vector unsigned char andc_result = vec_andc(rep_invalid_values, has_error);

  // Tests whether any of corresponding elements of the given vectors are not equal.
  // After the bitwise operation, both vectors should be equal if ASCII values.
  if (!vec_all_eq(rep_invalid_values, andc_result)) {
    return false;
  }

  for (; i < len; i++) {
    if (src[i] & 0x80) {
      return false;
    }
  }

  return true;
}
#else
bool validate_ascii_fast(const char* src, size_t len) {
  size_t i = 0;
  __m128i has_error = _mm_setzero_si128();
  if (len >= 16) {
    for (; i <= len - 16; i += 16) {
      __m128i current_bytes = mm_loadu_si128((const __m128i*)(src + i));
      has_error = _mm_or_si128(has_error, current_bytes);
    }
  }
  int error_mask = _mm_movemask_epi8(has_error);

  char tail_has_error = 0;
  for (; i < len; i++) {
    tail_has_error |= src[i];
  }
  error_mask |= (tail_has_error & 0x80);

  return !error_mask;
}
#endif

// len must be at least 16
void ascii_pack(const char* ascii, size_t len, uint8_t* bin) {
  uint64_t val;
  const char* end = ascii + len;

  while (ascii + 8 <= end) {
    val = absl::little_endian::Load64(ascii);
    uint64_t dest = (val & 0xFF);
    for (unsigned i = 1; i <= 7; ++i) {
      val >>= 1;
      dest |= (val & (0x7FUL << 7 * i));
    }
    memcpy(bin, &dest, 7);
    bin += 7;
    ascii += 8;
  }

  // epilog - we do not pack since we have less than 8 bytes.
  while (ascii < end) {
    *bin++ = *ascii++;
  }
}

void ascii_pack2(const char* ascii, size_t len, uint8_t* bin) {
  uint64_t val;
  const char* end = ascii + len;

  while (ascii + 8 <= end) {
    val = absl::little_endian::Load64(ascii);
    val = Compress8x7bit(val);
    memcpy(bin, &val, 7);
    bin += 7;
    ascii += 8;
  }

  // epilog - we do not pack since we have less than 8 bytes.
  while (ascii < end) {
    *bin++ = *ascii++;
  }
}

// The algo - do in parallel what ascii_pack does on two uint64_t integers
void ascii_pack_simd(const char* ascii, size_t len, uint8_t* bin) {
#if defined(__SSE3__) || defined(__aarch64__)
  // I leave out 16 bytes in addition to 16 that we load in the loop
  // because we store into bin full 16 bytes instead of 14. To prevent data
  // overwrite we finish loop one iteration earlier.
  const char* end = ascii + len - 32;

  tie(ascii, bin) = simd_variant1_pack(ascii, end, bin);

  end += 32;  // Bring back end.
  DCHECK(ascii < end);
  ascii_pack(ascii, end - ascii, bin);
#else
  ascii_pack(ascii, len, bin);
#endif
}

void ascii_pack_simd2(const char* ascii, size_t len, uint8_t* bin) {
#if defined(__SSE3__) || defined(__aarch64__)
  // I leave out 16 bytes in addition to 16 that we load in the loop
  // because we store into bin full 16 bytes instead of 14. To prevent data
  // overwrite we finish loop one iteration earlier.
  const char* end = ascii + len - 32;

  // on arm var
#if defined(__aarch64__)
  tie(ascii, bin) = simd_variant1_pack(ascii, end, bin);
#else
  tie(ascii, bin) = simd_variant2_pack(ascii, end, bin);
#endif

  end += 32;  // Bring back end.
  DCHECK(ascii < end);
  ascii_pack(ascii, end - ascii, bin);
#else
  ascii_pack(ascii, len, bin);
#endif
}

// unpacks 8->7 encoded blob back to ascii.
// generally, we can not unpack inplace because ascii (dest) buffer is 8/7 bigger than
// the source buffer.
// however, if binary data is positioned on the right of the ascii buffer with empty space on the
// left than we can unpack inplace.
void ascii_unpack(const uint8_t* bin, size_t ascii_len, char* ascii) {
  constexpr uint8_t kM = 0x7F;
  uint8_t p = 0;
  unsigned i = 0;

  while (ascii_len >= 8) {
    for (i = 0; i < 7; ++i) {
      uint8_t src = *bin;  // keep on stack in case we unpack inplace.
      *ascii++ = (p >> (8 - i)) | ((src << i) & kM);
      p = src;
      ++bin;
    }

    ascii_len -= 8;
    *ascii++ = p >> 1;
  }

  DCHECK_LT(ascii_len, 8u);
  for (i = 0; i < ascii_len; ++i) {
    *ascii++ = *bin++;
  }
}

// See CompactObjectTest.AsanTriggerReadOverflow for more details.
void ascii_unpack_simd(const uint8_t* bin, size_t ascii_len, char* ascii) {
#if defined(__SSE3__) || defined(__aarch64__)

  if (ascii_len < 18) {  // ascii_len >=18 means bin length >=16.
    ascii_unpack(bin, ascii_len, ascii);
    return;
  }

  __m128i val, rpart, lpart;

  // we read 16 bytes from bin even when we need only 14 bytes.
  // So for last iteration we may access 2 bytes outside of the bin buffer.
  // To prevent this we need to round down the length of the bin buffer but since we
  // limit by ascii_len we reduce the ascii_len by two before computing number of iterations.
  size_t simd_len = ((ascii_len - 2) / 16) * 16;
  const char* end = ascii + simd_len;

  // shifts the second 7-byte blob to the left.
  const __m128i control = _mm_set_epi8(14, 13, 12, 11, 10, 9, 8, 7, -1, 6, 5, 4, 3, 2, 1, 0);

  while (ascii < end) {
    val = mm_loadu_si128(reinterpret_cast<const __m128i*>(bin));
    val = _mm_shuffle_epi8(val, control);

    rpart = _mm_and_si128(val, _mm_set1_epi64x(0x000000000FFFFFFF));
    lpart = _mm_and_si128(val, _mm_set1_epi64x(0x00FFFFFFF0000000));
    val = _mm_or_si128(_mm_slli_epi64(lpart, 4), rpart);

    rpart = _mm_and_si128(val, _mm_set1_epi64x(0x00003FFF00003FFF));
    lpart = _mm_and_si128(val, _mm_set1_epi64x(0xFFFFC000FFFFC000));
    val = _mm_or_si128(_mm_slli_epi64(lpart, 2), rpart);

    rpart = _mm_and_si128(val, _mm_set1_epi64x(0x007F007F007F007F));
    lpart = _mm_and_si128(val, _mm_set1_epi64x(0x7F807F807F807F80));
    val = _mm_or_si128(_mm_slli_epi64(lpart, 1), rpart);

    _mm_storeu_si128(reinterpret_cast<__m128i*>(ascii), val);
    ascii += 16;
    bin += 14;
  }

  ascii_len -= simd_len;
  if (ascii_len)
    ascii_unpack(bin, ascii_len, ascii);
#else
  ascii_unpack(bin, ascii_len, ascii);
#endif
}

// compares packed and unpacked strings. packed must be of length = binpacked_len(ascii_len).
bool compare_packed(const uint8_t* packed, const char* ascii, size_t ascii_len) {
  unsigned i = 0;
  bool res = true;
  const char* end = ascii + ascii_len;

  while (ascii + 8 <= end) {
    for (i = 0; i < 7; ++i) {
      uint8_t conv = (ascii[0] >> i) | (ascii[1] << (7 - i));
      res &= (conv == *packed);
      ++ascii;
      ++packed;
    }

    if (!res)
      return false;

    ++ascii;
  }

  while (ascii < end) {
    if (*ascii++ != *packed++) {
      return false;
    }
  }

  return true;
}

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC pop_options
#endif

}  // namespace detail

}  // namespace dfly
