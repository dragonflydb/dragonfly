// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "src/core/detail/bitpacking.h"

#include <absl/base/internal/endian.h>

#ifdef __x86_64__
#include <immintrin.h>
#endif

#include "base/logging.h"
#include "core/sse_port.h"

using namespace std;

namespace dfly {

namespace detail {

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC push_options
#pragma GCC optimize("Ofast")
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

uint8_t ascii_unpack_byte(const uint8_t* bin, size_t ascii_len, size_t idx) {
  DCHECK(idx < ascii_len) << "Index oob for ascii byte unpacking: " << idx << " >= " << ascii_len;
  const size_t packed_groups = ascii_len / 8;
  const size_t group = idx / 8;
  const size_t idx_in_group = idx % 8;

  // Tail bytes (after the last full 8-char group) are stored unpacked.
  if (group >= packed_groups) {
    return bin[packed_groups * 7 + idx_in_group];
  }

  // Unpack ascii group and return byte at idx.
  char buf[8];
  impl::ascii_unpack_fast_scalar(bin + group * 7, 8, buf);
  return buf[idx_in_group];
}

void ascii_pack_byte(uint8_t* bin, size_t ascii_len, size_t idx, uint8_t val) {
  DCHECK(idx < ascii_len) << "Index oob for ascii byte packing: " << idx << " >= " << ascii_len;
  DCHECK_LT(val, 128u) << "Only 7-bit ASCII values can be packed";

  const size_t packed_groups = ascii_len / 8;
  const size_t group = idx / 8;
  const size_t idx_in_group = idx % 8;

  // Tail bytes (after the last full 8-char group) are stored unpacked.
  if (group >= packed_groups) {
    bin[packed_groups * 7 + idx_in_group] = val;
    return;
  }

  // Unpack ascii group and return, modify byte at idx and pack back.
  uint8_t* group_bin = bin + group * 7;
  char buf[8];
  impl::ascii_unpack_fast_scalar(group_bin, 8, buf);
  buf[idx_in_group] = val;
  ascii_try_pack(buf, 8, group_bin);
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

// ---------------------------------------------------------------------------
// ascii_try_pack / ascii_unpack_fast
//
// ---------------------------------------------------------------------------

// Internal-linkage kernels: only the impl:: entry points below are called from outside this file.
namespace {

// Expects the top bit of every byte to be clear.
#if defined(__BMI2__) && defined(__x86_64__)
inline uint64_t Pack8BytesTo7(uint64_t val) {
  // pext ("parallel bit extract") keeps only the bits the mask selects and slides them down into
  // one contiguous run.
  return _pext_u64(val, 0x7F7F7F7F7F7F7F7FULL);
}
#else
inline uint64_t Pack8BytesTo7(uint64_t val) {
  val = ((val & 0x7F007F007F007F00) >> 1) | (val & 0x007F007F007F007F);
  val = ((val & 0x3FFF00003FFF0000) >> 2) | (val & 0x00003FFF00003FFF);
  return ((val & 0x0FFFFFFF00000000) >> 4) | (val & 0x000000000FFFFFFF);
}
#endif

#if defined(__BMI2__) && defined(__x86_64__)
inline uint64_t Unpack7BytesTo8(uint64_t val) {
  // pdep is pext run backwards: it scatters the low bits into the positions the mask selects.
  return _pdep_u64(val, 0x7F7F7F7F7F7F7F7FULL);
}
#else
inline uint64_t Unpack7BytesTo8(uint64_t val) {
  // Without pdep the gaps are opened a few at a time: each step cuts the value in half and pushes
  // the upper part further left, so one gap becomes two and two become four.
  val = ((val & 0x00FFFFFFF0000000) << 4) | (val & 0x000000000FFFFFFF);
  val = ((val & 0xFFFFC000FFFFC000) << 2) | (val & 0x00003FFF00003FFF);
  return ((val & 0x7F807F807F807F80) << 1) | (val & 0x007F007F007F007F);
}
#endif

// Copies less than 8 bytes with at most two overlapping loads and stores. Returns the OR of all
// copied bytes so callers can test them for the high bit. Loads precede stores, so `src` and `dest`
// may overlap as long as `dest` is not above `src`.
inline uint64_t CopyTailBytes(const uint8_t* src, size_t len, uint8_t* dest) {
  if (len >= 4) {
    const uint32_t head = absl::little_endian::Load32(src);
    const uint32_t tail = absl::little_endian::Load32(src + len - 4);
    absl::little_endian::Store32(dest, head);
    absl::little_endian::Store32(dest + len - 4, tail);
    return head | tail;
  }

  if (len >= 2) {
    const uint16_t head = absl::little_endian::Load16(src);
    const uint16_t tail = absl::little_endian::Load16(src + len - 2);
    absl::little_endian::Store16(dest, head);
    absl::little_endian::Store16(dest + len - 2, tail);
    return head | tail;
  }

  if (len == 1) {
    dest[0] = src[0];
    return src[0];
  }

  return 0;
}

constexpr uint64_t kAsciiHighBits = 0x8080808080808080ULL;

// Returns bits that are set only when a non-ascii byte was seen.
inline __attribute__((always_inline)) uint64_t AsciiTryPackScalarTail(const uint8_t* src,
                                                                      size_t remaining,
                                                                      uint8_t* out) {
  uint64_t has_error = 0;

  if (remaining < 8)  // nothing was emitted here yet, so there is no history to reach back over
    return CopyTailBytes(src, remaining, out);

  while (remaining >= 9) {  // a single 8-byte store, the 8th byte belongs to the next group
    const uint64_t val = absl::little_endian::Load64(src);
    has_error |= val;
    absl::little_endian::Store64(out, Pack8BytesTo7(val));
    src += 8;
    out += 7;
    remaining -= 8;
  }

  if (remaining == 8) {
    uint64_t val = absl::little_endian::Load64(src);
    has_error |= val;
    val = Pack8BytesTo7(val);
#if defined(ABSL_IS_BIG_ENDIAN)
    val = __builtin_bswap64(val);
#endif
    memcpy(out, &val, 7);
    return has_error;
  }

  // A branchless CopyTailBytesOverPrev could reuse the last packed group in `prev`, but keeping
  // `prev` live would make every loop iteration feed the tail, so the loop could no longer run
  // ahead of its stores. The plain overlapping copy wins.
  return has_error | CopyTailBytes(src, remaining, out);
}

#ifdef DFLY_ASCII_TRY_PACK_HAS_SIMD
constexpr size_t kAsciiPerTryPackSimdBlock = 16;
constexpr size_t kPackedPerTryPackSimdBlock = 14;
constexpr size_t kTryPackSimdStoreOverlap = 2;
#if defined(__aarch64__)
constexpr size_t kTryPackSimdMin = kAsciiPerTryPackSimdBlock;
#else
constexpr size_t kTryPackSimdMin = kAsciiPerTryPackSimdBlock + kTryPackSimdStoreOverlap;
#endif

// Leaves the byte that separates the two 64-bit lanes for the shuffles below to drop. Both
// versions run the same three stages, each merging neighbouring fields and so doubling their
// width: 7 -> 14 -> 28 -> 56 bits.
// kSse selects the optimized variant used by ascii_try_pack_sse().
#if defined(__aarch64__)
template <bool kSse> inline __m128i Pack2x8BytesTo2x7(__m128i val) {
  // vsli ("shift left and insert") keeps the low n bits of the first operand and lays the second
  // one shifted up by n on top. vshr is a plain right shift by an immediate; shifting the lane down
  // by one field width before handing it to vsli closes the gap between each pair of neighbours,
  // and since the inputs are 7-bit no mask is needed.
  uint16x8_t v16 = vreinterpretq_u16_m128i(val);
  v16 = vsliq_n_u16(v16, vshrq_n_u16(v16, 8), 7);
  uint32x4_t v32 = vreinterpretq_u32_u16(v16);
  v32 = vsliq_n_u32(v32, vshrq_n_u32(v32, 16), 14);
  uint64x2_t v64 = vreinterpretq_u64_u32(v32);
  v64 = vsliq_n_u64(v64, vshrq_n_u64(v64, 32), 28);
  return vreinterpretq_m128i_u64(v64);
}
#else
template <bool kSse> inline __m128i Pack2x8BytesTo2x7(__m128i val) {
  // maddubs multiplies the bytes of the two operands pairwise and sums each neighbouring pair into
  // a 16-bit lane. The constant supplies 1 and 0x80, so a pair collapses to even | (odd << 7): the
  // multiplier is used purely as a shift, and the add as the merge.
  val = _mm_maddubs_epi16(_mm_set1_epi16(0x8001), val);
  // madd is the same trick one level up, over 16-bit lanes into 32-bit ones, where 1 and 0x4000
  // join two 14-bit fields into 28.
  val = _mm_madd_epi16(_mm_set1_epi32(0x40000001), val);

  if constexpr (kSse) {
    // Each lane is a + (b << 32), with a and b occupying 28 bits.
    // Subtract b * (2^32 - 2^28) to close the four-bit gap.
    const __m128i high = _mm_srli_epi64(val, 32);
    return _mm_sub_epi64(val, _mm_mul_epu32(high, _mm_set1_epi64x(0x00000000F0000000ULL)));
  } else {
    // Mask apart the two 28-bit fields and join them within each 64-bit lane.
    const __m128i rpart = _mm_and_si128(val, _mm_set1_epi64x(0x000000000FFFFFFF));
    const __m128i lpart = _mm_and_si128(val, _mm_set1_epi64x(0x0FFFFFFF00000000));
    return _mm_or_si128(_mm_srli_epi64(lpart, 4), rpart);
  }
}
#endif

// Leaves the packed bytes contiguous at the bottom and zeroes the two above them.
template <bool kSse> inline __m128i Pack16BytesTo14(__m128i val) {
  // shuffle_epi8 rebuilds a vector by taking, for every output byte, the input byte that `control`
  // names, and writing zero where the index is negative. set_epi8 lists lanes from the top down,
  // so read the row backwards: bytes 0..6 then 8..14, closing the gap the compaction left at 7.
  const __m128i control = _mm_set_epi8(-1, -1, 14, 13, 12, 11, 10, 9, 8, 6, 5, 4, 3, 2, 1, 0);
  return _mm_shuffle_epi8(Pack2x8BytesTo2x7<kSse>(val), control);
}

#if defined(__aarch64__)
inline uint64_t AsciiTryPackSimdErrorFlag(__m128i simd_error) {
  // neon has no movemask, but vmaxvq takes the largest of the 16 bytes, and that is at least 0x80
  // exactly when one of them had its high bit set.
  return (vmaxvq_u8(vreinterpretq_u8_m128i(simd_error)) & 0x80) ? kAsciiHighBits : 0;
}
#else
inline uint64_t AsciiTryPackSimdErrorFlag(__m128i simd_error) {
  // movemask collects the high bit of each of the 16 bytes into an integer, one bit per byte.
  return _mm_movemask_epi8(simd_error) ? kAsciiHighBits : 0;
}
#endif

// Handles a final 16-byte block without writing past its 14 packed bytes.
#if defined(__aarch64__)
template <bool kSse>
inline void PackTrailing16BytesTo14(const uint8_t*& src, size_t& remaining, uint8_t*& out,
                                    __m128i& simd_error) {
  if (remaining < kAsciiPerTryPackSimdBlock)
    return;

  const __m128i val = mm_loadu_si128(reinterpret_cast<const __m128i*>(src));
  simd_error = _mm_or_si128(simd_error, val);

  // The shuffle lands packed[0..7] and packed[6..13] in the two halves, so two overlapping 8-byte
  // stores cover the block and agree on the pair they both write. vqtbl1q is neon's byte permute:
  // like shuffle_epi8 it reads one index per output byte, but zeroes only for indices above 15.
  const uint8x16_t control = {0, 1, 2, 3, 4, 5, 6, 8, 6, 8, 9, 10, 11, 12, 13, 14};
  const uint8x16_t packed =
      vqtbl1q_u8(vreinterpretq_u8_m128i(Pack2x8BytesTo2x7<kSse>(val)), control);
  vst1_u8(out, vget_low_u8(packed));
  vst1_u8(out + 6, vget_high_u8(packed));

  src += kAsciiPerTryPackSimdBlock;
  out += kPackedPerTryPackSimdBlock;
  remaining -= kAsciiPerTryPackSimdBlock;
}
#else
template <bool kSse>
inline void PackTrailing16BytesTo14(const uint8_t*& src, size_t& remaining, uint8_t*& out,
                                    __m128i& simd_error) {
  if constexpr (kSse) {
    if (remaining < kAsciiPerTryPackSimdBlock)
      return;

    const __m128i val = mm_loadu_si128(reinterpret_cast<const __m128i*>(src));
    simd_error = _mm_or_si128(simd_error, val);
    const __m128i packed = Pack16BytesTo14<kSse>(val);

    // The overlapping stores cover exactly 14 bytes.
    _mm_storel_epi64(reinterpret_cast<__m128i*>(out), packed);
    _mm_storel_epi64(reinterpret_cast<__m128i*>(out + 6), _mm_srli_si128(packed, 6));
    src += kAsciiPerTryPackSimdBlock;
    out += kPackedPerTryPackSimdBlock;
    remaining -= kAsciiPerTryPackSimdBlock;
  }
}
#endif

template <bool kSse>
inline __attribute__((always_inline)) uint64_t AsciiTryPackSimdLoop(const uint8_t* src,
                                                                    size_t remaining,
                                                                    uint8_t* out) {
  uint64_t has_error = 0;

  constexpr size_t kMinLen = kSse ? kAsciiPerTryPackSimdBlock : kTryPackSimdMin;
  if (remaining >= kMinLen) {
    __m128i simd_error = _mm_setzero_si128();
    // Each block stores 16 bytes although only 14 of them are packed data: the next block or the
    // tail always overwrites the two extra bytes, which is cheaper than an exact 14-byte store.
    while (remaining >= kAsciiPerTryPackSimdBlock + kTryPackSimdStoreOverlap) {
      const __m128i val = mm_loadu_si128(reinterpret_cast<const __m128i*>(src));
      simd_error = _mm_or_si128(simd_error, val);
      _mm_storeu_si128(reinterpret_cast<__m128i*>(out), Pack16BytesTo14<kSse>(val));
      src += kAsciiPerTryPackSimdBlock;
      out += kPackedPerTryPackSimdBlock;
      remaining -= kAsciiPerTryPackSimdBlock;
    }

    if constexpr (kSse) {
      // Finish bulk validation before the tail so the accumulator need not stay live across it.
      has_error = AsciiTryPackSimdErrorFlag(simd_error);
      simd_error = _mm_setzero_si128();
    }
    PackTrailing16BytesTo14<kSse>(src, remaining, out, simd_error);
    has_error |= AsciiTryPackSimdErrorFlag(simd_error);
  }

  return has_error | AsciiTryPackScalarTail(src, remaining, out);
}

#ifdef DFLY_ASCII_TRY_PACK_HAS_AVX2
constexpr size_t kAsciiPerTryPackAvx2Block = 2 * kAsciiPerTryPackSimdBlock;
constexpr size_t kPackedPerTryPackAvx2Block = 2 * kPackedPerTryPackSimdBlock;

// The 256-bit path has to live in its own function (see AsciiTryPackAvx2Long), so it only pays off
// once its blocks save more than the call and the setup of the wider constants cost.
constexpr size_t kTryPackAvx2Min = 3 * kAsciiPerTryPackAvx2Block;

// Up to this length a single 128-bit block plus the scalar tail covers the whole input.
constexpr size_t kTryPackSimdShortMax = kAsciiPerTryPackSimdBlock + kTryPackSimdMin - 1;

// The 28 bytes do not come out contiguous: each 128-bit half holds its own 14 at the bottom.
inline __m256i Pack32BytesTo28(__m256i val) {
  val = _mm256_maddubs_epi16(_mm256_set1_epi16(0x8001), val);
  val = _mm256_madd_epi16(_mm256_set1_epi32(0x40000001), val);

  const __m256i rpart = _mm256_and_si256(val, _mm256_set1_epi64x(0x000000000FFFFFFF));
  const __m256i lpart = _mm256_and_si256(val, _mm256_set1_epi64x(0x0FFFFFFF00000000));
  val = _mm256_or_si256(_mm256_srli_epi64(lpart, 4), rpart);

  // The 256-bit shuffle is really two independent 128-bit ones, it never moves a byte across the
  // half way mark, so broadcasting the same control into both halves gives each its own 14 bytes.
  const __m128i control = _mm_set_epi8(-1, -1, 14, 13, 12, 11, 10, 9, 8, 6, 5, 4, 3, 2, 1, 0);
  return _mm256_shuffle_epi8(val, _mm256_broadcastsi128_si256(control));
}

// Writes 16 + 8 + 4 + 2 bytes, the last three from the upper half, so that nothing past the 28 is
// touched. The 2 bytes of slack the first store leaves are covered by the second.
inline void StorePacked28Bytes(uint8_t* dest, __m256i packed) {
  _mm_storeu_si128(reinterpret_cast<__m128i*>(dest), _mm256_castsi256_si128(packed));
  const __m128i high = _mm256_extracti128_si256(packed, 1);
  _mm_storel_epi64(reinterpret_cast<__m128i*>(dest + kPackedPerTryPackSimdBlock), high);
  // srli_si128 slides the vector down by whole bytes and cvtsi128_si64 moves its low half to a
  // general register, which is the cheapest way to reach lanes 8..13 with narrow stores.
  const uint64_t rest = _mm_cvtsi128_si64(_mm_srli_si128(high, 8));
  absl::little_endian::Store32(dest + kPackedPerTryPackSimdBlock + 8, static_cast<uint32_t>(rest));
  absl::little_endian::Store16(dest + kPackedPerTryPackSimdBlock + 12,
                               static_cast<uint16_t>(rest >> 32));
}

// Deliberately loop free: a loop here costs both this path and its caller extra induction
// variables.
inline __attribute__((always_inline)) uint64_t AsciiTryPackSimdShort(const uint8_t* src,
                                                                     size_t remaining,
                                                                     uint8_t* out) {
  uint64_t has_error = 0;

  if (remaining >= kTryPackSimdMin) {
    const __m128i val = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src));
    has_error = _mm_movemask_epi8(val) ? kAsciiHighBits : 0;
    _mm_storeu_si128(reinterpret_cast<__m128i*>(out), Pack16BytesTo14<false>(val));
    src += kAsciiPerTryPackSimdBlock;
    out += kPackedPerTryPackSimdBlock;
    remaining -= kAsciiPerTryPackSimdBlock;
  }

  return has_error | AsciiTryPackScalarTail(src, remaining, out);
}

// Kept out of line on purpose: 256-bit code inside ascii_try_pack would force a 32-byte aligned
// frame on it and slow down every short string.
__attribute__((noinline)) uint64_t AsciiTryPackAvx2Long(const uint8_t* src, size_t remaining,
                                                        uint8_t* out) {
  // Two full 16-byte stores are cheaper than an exact 28-byte store, and the two extra bytes they
  // write are always overwritten by the next block or by the tail below.
  const size_t bulk_blocks = (remaining - kTryPackSimdStoreOverlap) / kAsciiPerTryPackAvx2Block;
  __m256i simd_error = _mm256_setzero_si256();

  // A counted loop, so that its body stays free of induction variables the tail below needs.
  for (size_t i = 0; i < bulk_blocks; ++i) {
    __m256i val = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src));
    simd_error = _mm256_or_si256(simd_error, val);
    val = Pack32BytesTo28(val);
    _mm_storeu_si128(reinterpret_cast<__m128i*>(out), _mm256_castsi256_si128(val));
    _mm_storeu_si128(reinterpret_cast<__m128i*>(out + kPackedPerTryPackSimdBlock),
                     _mm256_extracti128_si256(val, 1));
    src += kAsciiPerTryPackAvx2Block;
    out += kPackedPerTryPackAvx2Block;
  }
  remaining -= bulk_blocks * kAsciiPerTryPackAvx2Block;

  // Consume the accumulator before the last block, otherwise it stays live across the loop above.
  uint64_t has_error = _mm256_movemask_epi8(simd_error) ? kAsciiHighBits : 0;

  if (remaining >= kAsciiPerTryPackAvx2Block) {  // nothing follows, store exactly 28 bytes
    const __m256i val = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src));
    has_error |= _mm256_movemask_epi8(val) ? kAsciiHighBits : 0;
    StorePacked28Bytes(out, Pack32BytesTo28(val));
    src += kAsciiPerTryPackAvx2Block;
    out += kPackedPerTryPackAvx2Block;
    remaining -= kAsciiPerTryPackAvx2Block;
  }

  return has_error | AsciiTryPackSimdShort(src, remaining, out);
}
#endif
#endif

inline size_t PackedLenIfAscii(uint64_t has_error, size_t len) {
  return (has_error & kAsciiHighBits) ? 0 : binpacked_len(len);
}

}  // namespace

// One entry point per kernel, so that a test can drive them side by side. Each honours the
// ascii_try_pack() contract for any length, no matter how wide it goes internally.
namespace impl {

size_t ascii_try_pack_scalar(const char* ascii, size_t len, uint8_t* bin) {
  return PackedLenIfAscii(AsciiTryPackScalarTail(reinterpret_cast<const uint8_t*>(ascii), len, bin),
                          len);
}

#ifdef DFLY_ASCII_TRY_PACK_HAS_SSE
size_t ascii_try_pack_sse(const char* ascii, size_t len, uint8_t* bin) {
  return PackedLenIfAscii(
      AsciiTryPackSimdLoop<true>(reinterpret_cast<const uint8_t*>(ascii), len, bin), len);
}
#endif

#ifdef DFLY_ASCII_TRY_PACK_HAS_SIMD
size_t ascii_try_pack_simd(const char* ascii, size_t len, uint8_t* bin) {
  return PackedLenIfAscii(
      AsciiTryPackSimdLoop<false>(reinterpret_cast<const uint8_t*>(ascii), len, bin), len);
}
#endif

#ifdef DFLY_ASCII_TRY_PACK_HAS_AVX2
size_t ascii_try_pack_avx2(const char* ascii, size_t len, uint8_t* bin) {
  const uint8_t* src = reinterpret_cast<const uint8_t*>(ascii);

  // AsciiTryPackAvx2Long only pays for its wider setup once a few 32-byte blocks follow, and
  // AsciiTryPackSimdShort covers what a single 128-bit block plus the tail can finish.
  uint64_t has_error;
  if (len > kTryPackSimdShortMax)
    has_error = len >= kTryPackAvx2Min ? AsciiTryPackAvx2Long(src, len, bin)
                                       : AsciiTryPackSimdLoop<false>(src, len, bin);
  else
    has_error = AsciiTryPackSimdShort(src, len, bin);

  return PackedLenIfAscii(has_error, len);
}
#endif

}  // namespace impl

namespace {

inline __attribute__((always_inline)) void AsciiUnpackFastScalar(const uint8_t* bin,
                                                                 size_t ascii_len, char* ascii) {
  constexpr uint64_t kSevenBytes = 0x00FFFFFFFFFFFFFFULL;

  if (ascii_len < 8) {
    CopyTailBytes(bin, ascii_len, reinterpret_cast<uint8_t*>(ascii));
    return;
  }

  while (ascii_len >= 9) {
    const uint64_t val = absl::little_endian::Load64(bin) & kSevenBytes;
    absl::little_endian::Store64(ascii, Unpack7BytesTo8(val));
    bin += 7;
    ascii += 8;
    ascii_len -= 8;
  }

  if (ascii_len == 8) {
    // Two overlapping loads read exactly seven bytes.
    const uint64_t val = absl::little_endian::Load32(bin) |
                         (static_cast<uint64_t>(absl::little_endian::Load32(bin + 3)) << 24);
    absl::little_endian::Store64(ascii, Unpack7BytesTo8(val));
    return;
  }

  // CopyTailBytesOverPrev would reuse the last expanded word, but keeping it live makes every loop
  // iteration feed the tail, so the loop can no longer run ahead of its stores.
  CopyTailBytes(bin, ascii_len, reinterpret_cast<uint8_t*>(ascii));
}

#ifdef DFLY_ASCII_UNPACK_FAST_HAS_SSE
constexpr size_t kAsciiPerUnpackSseBlock = 16;
constexpr size_t kPackedPerUnpackSseBlock = 14;

// Expects seven packed bytes in the low 56 bits of every 64-bit lane, and runs the stages of
// Unpack28BytesTo32 below at 128 bits.
inline __m128i Unpack14BytesTo16(__m128i val) {
  // Without AVX2 there is only the 16-bit blend, but taking 16-bit lanes 2, 3, 6 and 7 (0xCC) from
  // the shifted vector is the same as taking the upper 32-bit lane of each 64-bit one.
  val = _mm_blend_epi16(val, _mm_slli_epi64(val, 4), 0xCC);
  val = _mm_blend_epi16(val, _mm_slli_epi32(val, 2), 0xAA);
  // Adding the upper nine bits of every 16-bit lane to the lane once more doubles them, which
  // moves them up by one bit without a byte blend.
  val = _mm_add_epi16(val, _mm_and_si128(val, _mm_set1_epi16(static_cast<int16_t>(0xFF80))));
  return _mm_and_si128(val, _mm_set1_epi8(0x7F));
}
#endif

#ifdef DFLY_ASCII_UNPACK_FAST_HAS_AVX2
constexpr size_t kAsciiPerUnpackAvx2Block = 32;
constexpr size_t kPackedPerUnpackAvx2Block = 28;

// The caller loads the two halves 14 bytes apart, so each arrives with its own 14 packed bytes at
// the bottom.
inline __m256i Unpack28BytesTo32(__m256i val) {
  // Read the control backwards: bytes 0..6, then a zero, then 7..14. Opening a hole at byte 7 of
  // each half leaves seven packed bytes sitting in the low 56 bits of every 64-bit lane.
  const __m128i control128 = _mm_set_epi8(14, 13, 12, 11, 10, 9, 8, 7, -1, 6, 5, 4, 3, 2, 1, 0);
  val = _mm256_shuffle_epi8(val, _mm256_broadcastsi128_si256(control128));

  // Every stage splits each field into two halves of equal width. The blend keeps the lower half
  // in place, so only the shifted copy needs selecting, and the bits that shift in from below are
  // dropped by the final mask. blend picks its operand per lane from a compile time pattern: 0xAA
  // is 10101010, so the odd lanes, the upper half of each field, come from the shifted vector.
  val = _mm256_blend_epi32(val, _mm256_slli_epi64(val, 4), 0xAA);
  val = _mm256_blend_epi16(val, _mm256_slli_epi32(val, 2), 0xAA);
  // No byte-granular blend with an immediate exists, so the last stage selects with a vector mask
  // instead: blendv takes each byte from the second operand where the mask byte is negative.
  val = _mm256_blendv_epi8(val, _mm256_slli_epi16(val, 1),
                           _mm256_set1_epi16(static_cast<int16_t>(0xFF00)));
  return _mm256_and_si256(val, _mm256_set1_epi8(0x7F));
}

__attribute__((noinline)) void AsciiUnpackAvx2Loop(const uint8_t* bin, size_t ascii_len,
                                                   char* ascii) {
  // A block's two halves are 14 bytes apart, so they are loaded separately and joined: castsi128
  // widens the low one for free and inserti128 drops the other into the upper half. Each load
  // takes 16 bytes and therefore reads 2 past its half, which is why the loop stops 2 bytes early.
  while (ascii_len >= kAsciiPerUnpackAvx2Block + 2) {
    const __m128i low = _mm_loadu_si128(reinterpret_cast<const __m128i*>(bin));
    const __m128i high = _mm_loadu_si128(reinterpret_cast<const __m128i*>(bin + 14));
    __m256i val = _mm256_inserti128_si256(_mm256_castsi128_si256(low), high, 1);
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(ascii), Unpack28BytesTo32(val));
    bin += kPackedPerUnpackAvx2Block;
    ascii += kAsciiPerUnpackAvx2Block;
    ascii_len -= kAsciiPerUnpackAvx2Block;
  }
  // The final block has nothing after it to over-read, so its upper half is loaded 2 bytes early
  // and slid back down: srli_si128 shifts the vector right by whole bytes.
  if (ascii_len >= kAsciiPerUnpackAvx2Block) {
    const __m128i low = _mm_loadu_si128(reinterpret_cast<const __m128i*>(bin));
    const __m128i high =
        _mm_srli_si128(_mm_loadu_si128(reinterpret_cast<const __m128i*>(bin + 12)), 2);
    __m256i val = _mm256_inserti128_si256(_mm256_castsi128_si256(low), high, 1);
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(ascii), Unpack28BytesTo32(val));
    bin += kPackedPerUnpackAvx2Block;
    ascii += kAsciiPerUnpackAvx2Block;
    ascii_len -= kAsciiPerUnpackAvx2Block;
  }
  AsciiUnpackFastScalar(bin, ascii_len, ascii);
}
#elif defined(DFLY_ASCII_UNPACK_FAST_HAS_NEON)
// Expects the packed bytes in the low 56 bits of each 64-bit lane, so the stages run widest first
// and vsli lays the upper half of every field back above the lower one.
inline uint8x16_t Unpack14BytesTo16(uint8x16_t val, uint8x16_t ascii_mask) {
  uint64x2_t v64 = vreinterpretq_u64_u8(val);
  v64 = vsliq_n_u64(v64, vshrq_n_u64(v64, 28), 32);
  uint32x4_t v32 = vreinterpretq_u32_u64(v64);
  v32 = vsliq_n_u32(v32, vshrq_n_u32(v32, 14), 16);
  uint16x8_t v16 = vreinterpretq_u16_u32(v32);
  v16 = vsliq_n_u16(v16, vshrq_n_u16(v16, 7), 8);
  return vandq_u8(vreinterpretq_u8_u16(v16), ascii_mask);  // clears what shifted in from below
}

// Two 8-byte loads stay inside the 14 packed bytes, unlike the 16-byte load the loop uses, so this
// also serves the last block of a buffer.
inline void UnpackTail14BytesTo16(const uint8_t* bin, char* ascii, uint8x16_t ascii_mask) {
  // Indices of 0xFF are out of range for vqtbl1q, which makes it emit zero: that is how the gap
  // at byte 7 of each half is opened, leaving seven packed bytes in the low 56 bits of each lane.
  const uint8x16_t control = {0, 1, 2, 3, 4, 5, 6, 0xFF, 9, 10, 11, 12, 13, 14, 15, 0xFF};
  const uint8x16_t raw = vcombine_u8(vld1_u8(bin), vld1_u8(bin + 6));
  vst1q_u8(reinterpret_cast<uint8_t*>(ascii),
           Unpack14BytesTo16(vqtbl1q_u8(raw, control), ascii_mask));
}

// Decodes 14-byte blocks two at a time while they last, so that the two independent chains keep
// the store pipe busy.
inline void AsciiUnpackNeonLoop(const uint8_t* bin, size_t ascii_len, char* ascii) {
  const uint8x16_t ascii_mask = vdupq_n_u8(0x7F);
  const uint8x16_t control = {0, 1, 2, 3, 4, 5, 6, 0xFF, 7, 8, 9, 10, 11, 12, 13, 0xFF};
  // Each 16-byte load reads 2 bytes past the 14 it needs, so the last block is left to the tail.
  const size_t simd_len = ((ascii_len - 2) / 16) * 16;
  const char* end = ascii + simd_len;
  const char* paired_end = ascii + (simd_len & ~size_t{31});

  while (ascii < paired_end) {
    const uint8x16_t low = vqtbl1q_u8(vld1q_u8(bin), control);
    const uint8x16_t high = vqtbl1q_u8(vld1q_u8(bin + 14), control);
    uint8_t* dest = reinterpret_cast<uint8_t*>(ascii);
    vst1q_u8(dest, Unpack14BytesTo16(low, ascii_mask));
    vst1q_u8(dest + 16, Unpack14BytesTo16(high, ascii_mask));
    ascii += 32;
    bin += 28;
  }

  if (ascii < end) {
    vst1q_u8(reinterpret_cast<uint8_t*>(ascii),
             Unpack14BytesTo16(vqtbl1q_u8(vld1q_u8(bin), control), ascii_mask));
    ascii += 16;
    bin += 14;
  }
  ascii_len -= simd_len;

  if (ascii_len >= 16) {
    UnpackTail14BytesTo16(bin, ascii, ascii_mask);
    ascii += 16;
    bin += 14;
    ascii_len -= 16;
  }

  AsciiUnpackFastScalar(bin, ascii_len, ascii);
}
#endif

}  // namespace

// One entry point per kernel, like ascii_try_pack above.
namespace impl {

void ascii_unpack_fast_scalar(const uint8_t* bin, size_t ascii_len, char* ascii) {
  AsciiUnpackFastScalar(bin, ascii_len, ascii);
}

#ifdef DFLY_ASCII_UNPACK_FAST_HAS_SSE
// No load reaches past the packed bytes, see CompactObjectTest.AsanTriggerReadOverflow.
void ascii_unpack_fast_sse(const uint8_t* bin, size_t ascii_len, char* ascii) {
  // Testing this first keeps short strings from paying for the block checks below.
  if (ascii_len < kAsciiPerUnpackSseBlock) {
    AsciiUnpackFastScalar(bin, ascii_len, ascii);
    return;
  }

  // Read the control backwards: bytes 0..6, a zero, then 7..14, the same as in Unpack28BytesTo32.
  const __m128i control = _mm_set_epi8(14, 13, 12, 11, 10, 9, 8, 7, -1, 6, 5, 4, 3, 2, 1, 0);

  // Each load takes 16 bytes and so reads 2 past the block, which is why the loop stops 2 early.
  while (ascii_len >= kAsciiPerUnpackSseBlock + 2) {
    const __m128i val = _mm_loadu_si128(reinterpret_cast<const __m128i*>(bin));
    _mm_storeu_si128(reinterpret_cast<__m128i*>(ascii),
                     Unpack14BytesTo16(_mm_shuffle_epi8(val, control)));
    bin += kPackedPerUnpackSseBlock;
    ascii += kAsciiPerUnpackSseBlock;
    ascii_len -= kAsciiPerUnpackSseBlock;
  }

  // The final block has nothing after it to over-read, so two 8-byte loads fetch exactly its bytes,
  // 0..7 and 6..13, and the control skips the two they share.
  if (ascii_len >= kAsciiPerUnpackSseBlock) {
    const __m128i control_last =
        _mm_set_epi8(-1, 15, 14, 13, 12, 11, 10, 9, -1, 6, 5, 4, 3, 2, 1, 0);
    const __m128i val =
        _mm_unpacklo_epi64(_mm_loadl_epi64(reinterpret_cast<const __m128i*>(bin)),
                           _mm_loadl_epi64(reinterpret_cast<const __m128i*>(bin + 6)));
    _mm_storeu_si128(reinterpret_cast<__m128i*>(ascii),
                     Unpack14BytesTo16(_mm_shuffle_epi8(val, control_last)));
    bin += kPackedPerUnpackSseBlock;
    ascii += kAsciiPerUnpackSseBlock;
    ascii_len -= kAsciiPerUnpackSseBlock;
  }

  AsciiUnpackFastScalar(bin, ascii_len, ascii);
}
#endif

#ifdef DFLY_ASCII_UNPACK_FAST_HAS_AVX2
void ascii_unpack_fast_avx2(const uint8_t* bin, size_t ascii_len, char* ascii) {
  if (ascii_len >= kAsciiPerUnpackAvx2Block) {
    AsciiUnpackAvx2Loop(bin, ascii_len, ascii);
  } else {
    AsciiUnpackFastScalar(bin, ascii_len, ascii);
  }
}
#endif

#ifdef DFLY_ASCII_UNPACK_FAST_HAS_NEON
void ascii_unpack_fast_neon(const uint8_t* bin, size_t ascii_len, char* ascii) {
  if (ascii_len < 16) {
    AsciiUnpackFastScalar(bin, ascii_len, ascii);
  } else if (ascii_len < 32) {
    UnpackTail14BytesTo16(bin, ascii, vdupq_n_u8(0x7F));
    AsciiUnpackFastScalar(bin + 14, ascii_len - 16, ascii + 16);
  } else {
    AsciiUnpackNeonLoop(bin, ascii_len, ascii);
  }
}
#endif

}  // namespace impl

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC pop_options
#endif

}  // namespace detail

}  // namespace dfly
