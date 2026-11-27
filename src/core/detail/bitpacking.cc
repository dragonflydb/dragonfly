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

// ---------------------------------------------------------------------------
// ascii_try_pack / ascii_unpack_fast / ascii_compare_packed
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

// Compares less than 8 bytes with the same loads as CopyTailBytes. Returns zero if they are equal.
inline uint64_t DiffTailBytes(const uint8_t* a, const uint8_t* b, size_t len) {
  if (len >= 4) {
    return (absl::little_endian::Load32(a) ^ absl::little_endian::Load32(b)) |
           (absl::little_endian::Load32(a + len - 4) ^ absl::little_endian::Load32(b + len - 4));
  }

  if (len >= 2) {
    return (absl::little_endian::Load16(a) ^ absl::little_endian::Load16(b)) |
           (absl::little_endian::Load16(a + len - 2) ^ absl::little_endian::Load16(b + len - 2));
  }

  if (len == 1)
    return a[0] ^ b[0];

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
    const uint64_t val = absl::little_endian::Load64(src);
    has_error |= val;

    // Two overlapping 4-byte stores write exactly 7 bytes. A memcpy from a local would take its
    // address, and the stack protector would then add a canary check to every kernel.
    const uint64_t packed = Pack8BytesTo7(val);
    absl::little_endian::Store32(out, static_cast<uint32_t>(packed));
    absl::little_endian::Store32(out + 3, static_cast<uint32_t>(packed >> 24));
    return has_error;
  }

  // A branchless CopyTailBytesOverPrev could reuse the last packed group in `prev`, but keeping
  // `prev` live would make every loop iteration feed the tail, so the loop could no longer run
  // ahead of its stores. The plain overlapping copy wins.
  return has_error | CopyTailBytes(src, remaining, out);
}

inline size_t PackedLenIfAscii(uint64_t has_error, size_t len) {
  return (has_error & kAsciiHighBits) ? 0 : binpacked_len(len);
}

#if defined(ASCII_SSSE3) || defined(ASCII_NEON)
constexpr size_t kAsciiPerTryPackSimdBlock = 16;
constexpr size_t kPackedPerTryPackSimdBlock = 14;
constexpr size_t kTryPackSimdStoreOverlap = 2;

// Leaves the byte that separates the two 64-bit lanes for the shuffles below to drop. Both
// versions run the same three stages, each merging neighbouring fields and so doubling their
// width: 7 -> 14 -> 28 -> 56 bits.
#if defined(ASCII_NEON)
inline __m128i Pack2x8BytesTo2x7(__m128i val) {
  // vsli ("shift left and insert") keeps the low n bits of the first operand and lays the second
  // one shifted up by n on top. vrev swaps the two halves of every lane, so the second operand
  // brings the upper field of each pair down, and the shift lays it right next to the lower one.
  // What the swap brings up lands above the merged field and is never cleared: the next stage
  // keeps only the merged field of its first operand, and after the last stage the leftovers sit
  // in the top byte of each 64-bit lane, which the shuffles below drop.
  //
  // vrev stands in for a right shift by the field width: on Neoverse N1 class cores every vector
  // shift, vsli included, issues to only one of the two vector pipes, while vrev can use either.
  const uint8x16_t v8 = vreinterpretq_u8_m128i(val);
  uint16x8_t v16 = vreinterpretq_u16_u8(v8);
  v16 = vsliq_n_u16(v16, vreinterpretq_u16_u8(vrev16q_u8(v8)), 7);
  uint32x4_t v32 = vreinterpretq_u32_u16(v16);
  v32 = vsliq_n_u32(v32, vreinterpretq_u32_u16(vrev32q_u16(v16)), 14);
  uint64x2_t v64 = vreinterpretq_u64_u32(v32);
  v64 = vsliq_n_u64(v64, vreinterpretq_u64_u32(vrev64q_u32(v32)), 28);
  return vreinterpretq_m128i_u64(v64);
}
#else
inline __m128i Pack2x8BytesTo2x7(__m128i val) {
  // maddubs multiplies the bytes of the two operands pairwise and sums each neighbouring pair into
  // a 16-bit lane. The constant supplies 1 and 0x80, so a pair collapses to even | (odd << 7): the
  // multiplier is used purely as a shift, and the add as the merge.
  val = _mm_maddubs_epi16(_mm_set1_epi16(0x8001), val);
  // madd is the same trick one level up, over 16-bit lanes into 32-bit ones, where 1 and 0x4000
  // join two 14-bit fields into 28.
  val = _mm_madd_epi16(_mm_set1_epi32(0x40000001), val);

  // Each lane is a + (b << 32), with a and b occupying 28 bits.
  // Subtract b * (2^32 - 2^28) to close the four-bit gap.
  const __m128i high = _mm_srli_epi64(val, 32);
  return _mm_sub_epi64(val, _mm_mul_epu32(high, _mm_set1_epi64x(0x00000000F0000000ULL)));
}
#endif

// The avx2 kernel has a cheaper Pack2x8BytesTo2x7 of its own (Pack2x8BytesTo2x7Avx2). The helpers
// below take the packer as a parameter, so that each kernel keeps to its own instruction set.
using Pack2x8Fn = __m128i (*)(__m128i);

// Leaves the packed bytes contiguous at the bottom and zeroes the two above them.
template <Pack2x8Fn Pack2x8> inline __m128i Pack16BytesTo14(__m128i val) {
  // shuffle_epi8 rebuilds a vector by taking, for every output byte, the input byte that `control`
  // names, and writing zero where the index is negative. set_epi8 lists lanes from the top down,
  // so read the row backwards: bytes 0..6 then 8..14, closing the gap the compaction left at 7.
  const __m128i control = _mm_set_epi8(-1, -1, 14, 13, 12, 11, 10, 9, 8, 6, 5, 4, 3, 2, 1, 0);
  return _mm_shuffle_epi8(Pack2x8(val), control);
}

#if defined(ASCII_NEON)
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
#if defined(ASCII_NEON)
template <Pack2x8Fn Pack2x8>
inline void PackTrailing16BytesTo14(const uint8_t*& src, size_t& remaining, uint8_t*& out,
                                    __m128i& /* simd_error */) {
  if (remaining < kAsciiPerTryPackSimdBlock)
    return;

  // AsciiTryPackSimdBulk has already checked this block.
  const __m128i val = mm_loadu_si128(reinterpret_cast<const __m128i*>(src));

  // The shuffle lands packed[0..7] and packed[6..13] in the two halves, so two overlapping 8-byte
  // stores cover the block and agree on the pair they both write. vqtbl1q is neon's byte permute:
  // like shuffle_epi8 it reads one index per output byte, but zeroes only for indices above 15.
  const uint8x16_t control = {0, 1, 2, 3, 4, 5, 6, 8, 6, 8, 9, 10, 11, 12, 13, 14};
  const uint8x16_t packed = vqtbl1q_u8(vreinterpretq_u8_m128i(Pack2x8(val)), control);
  vst1_u8(out, vget_low_u8(packed));
  vst1_u8(out + 6, vget_high_u8(packed));

  src += kAsciiPerTryPackSimdBlock;
  out += kPackedPerTryPackSimdBlock;
  remaining -= kAsciiPerTryPackSimdBlock;
}
#else
template <Pack2x8Fn Pack2x8>
inline void PackTrailing16BytesTo14(const uint8_t*& src, size_t& remaining, uint8_t*& out,
                                    __m128i& simd_error) {
  if (remaining < kAsciiPerTryPackSimdBlock)
    return;

  const __m128i val = mm_loadu_si128(reinterpret_cast<const __m128i*>(src));
  simd_error = _mm_or_si128(simd_error, val);
  const __m128i packed = Pack16BytesTo14<Pack2x8>(val);

  // The overlapping stores cover exactly 14 bytes.
  _mm_storel_epi64(reinterpret_cast<__m128i*>(out), packed);
  _mm_storel_epi64(reinterpret_cast<__m128i*>(out + 6), _mm_srli_si128(packed, 6));
  src += kAsciiPerTryPackSimdBlock;
  out += kPackedPerTryPackSimdBlock;
  remaining -= kAsciiPerTryPackSimdBlock;
}
#endif

#if defined(ASCII_NEON)
// Ors the two halves of a 16-byte block, which keeps a high bit set if any of the bytes had one.
inline uint64_t Fold16BytesTo8(const uint8_t* src) {
  return absl::little_endian::Load64(src) | absl::little_endian::Load64(src + 8);
}

template <Pack2x8Fn Pack2x8>
inline __attribute__((always_inline)) bool AsciiTryPackSimdBulk(const uint8_t*& src,
                                                                size_t& remaining, uint8_t*& out,
                                                                __m128i& /* simd_error */) {
  // Validate each block before storing it, so non-ascii input is rejected within 16 bytes. The
  // check runs on the integer pipes, which are mostly idle while the vector ones pack, so it is
  // almost free for ascii input. A vector check would compete with the packing instead, and neon
  // has no cheap way to move its result out: vmaxvq issues to a single pipe and takes several
  // cycles.
#pragma GCC unroll 2
  while (remaining >= kAsciiPerTryPackSimdBlock + kTryPackSimdStoreOverlap) {
    if (Fold16BytesTo8(src) & kAsciiHighBits)
      return true;
    const __m128i val = mm_loadu_si128(reinterpret_cast<const __m128i*>(src));
    vst1q_u8(out, vreinterpretq_u8_m128i(Pack16BytesTo14<Pack2x8>(val)));
    src += kAsciiPerTryPackSimdBlock;
    out += kPackedPerTryPackSimdBlock;
    remaining -= kAsciiPerTryPackSimdBlock;
  }

  // Check the block PackTrailing16BytesTo14 packs as well. simd_error then stays clear, and the
  // caller's vmaxvq on it folds away.
  return remaining >= kAsciiPerTryPackSimdBlock && (Fold16BytesTo8(src) & kAsciiHighBits);
}
#else
template <Pack2x8Fn Pack2x8>
inline __attribute__((always_inline)) bool AsciiTryPackSimdBulk(const uint8_t*& src,
                                                                size_t& remaining, uint8_t*& out,
                                                                __m128i& simd_error) {
  // Check each pair of blocks after packing and storing it, so non-ascii input is rejected within
  // 32 bytes; the contract allows the bytes stored for a rejected pair to stay behind. Checking
  // first is up to 8% slower on ascii input: the or then needs a copy of `lo`, and gcc moves copies
  // of the packing constants into the loop. In this order the loop is as tight as an unchecked one.
#pragma GCC unroll 2
  while (remaining >= 2 * kAsciiPerTryPackSimdBlock + kTryPackSimdStoreOverlap) {
    const __m128i lo = mm_loadu_si128(reinterpret_cast<const __m128i*>(src));
    const __m128i hi =
        mm_loadu_si128(reinterpret_cast<const __m128i*>(src + kAsciiPerTryPackSimdBlock));
    _mm_storeu_si128(reinterpret_cast<__m128i*>(out), Pack16BytesTo14<Pack2x8>(lo));
    _mm_storeu_si128(reinterpret_cast<__m128i*>(out + kPackedPerTryPackSimdBlock),
                     Pack16BytesTo14<Pack2x8>(hi));
    src += 2 * kAsciiPerTryPackSimdBlock;
    out += 2 * kPackedPerTryPackSimdBlock;
    remaining -= 2 * kAsciiPerTryPackSimdBlock;
    if (_mm_movemask_epi8(_mm_or_si128(lo, hi)))
      return true;
  }

  // At most one block remains before the tail; the caller checks it together with the tail.
  if (remaining >= kAsciiPerTryPackSimdBlock + kTryPackSimdStoreOverlap) {
    const __m128i val = mm_loadu_si128(reinterpret_cast<const __m128i*>(src));
    simd_error = _mm_or_si128(simd_error, val);
    _mm_storeu_si128(reinterpret_cast<__m128i*>(out), Pack16BytesTo14<Pack2x8>(val));
    src += kAsciiPerTryPackSimdBlock;
    out += kPackedPerTryPackSimdBlock;
    remaining -= kAsciiPerTryPackSimdBlock;
  }
  return false;
}
#endif

template <Pack2x8Fn Pack2x8>
inline __attribute__((always_inline)) uint64_t AsciiTryPackSimdLoop(const uint8_t* src,
                                                                    size_t remaining,
                                                                    uint8_t* out) {
  uint64_t has_error = 0;

  if (remaining >= kAsciiPerTryPackSimdBlock) {
    __m128i simd_error = _mm_setzero_si128();

    // Each block stores 16 bytes although only 14 of them are packed data: the next block or the
    // tail overwrites the extra bytes on success, avoiding an exact 14-byte store.
    if (AsciiTryPackSimdBulk<Pack2x8>(src, remaining, out, simd_error))
      return kAsciiHighBits;

    // Finish bulk validation before the tail so the accumulator need not stay live across it.
    has_error = AsciiTryPackSimdErrorFlag(simd_error);
    simd_error = _mm_setzero_si128();
    PackTrailing16BytesTo14<Pack2x8>(src, remaining, out, simd_error);
    has_error |= AsciiTryPackSimdErrorFlag(simd_error);
  }

  return has_error | AsciiTryPackScalarTail(src, remaining, out);
}

#if defined(ASCII_AVX2)
constexpr size_t kTryPackSimdMin = kAsciiPerTryPackSimdBlock + kTryPackSimdStoreOverlap;
constexpr size_t kAsciiPerTryPackAvx2Block = 2 * kAsciiPerTryPackSimdBlock;
constexpr size_t kPackedPerTryPackAvx2Block = 2 * kPackedPerTryPackSimdBlock;

// The 256-bit path has to live in its own function (see AsciiTryPackAvx2Long), so it only pays off
// once its blocks save more than the call and the setup of the wider constants cost.
constexpr size_t kTryPackAvx2Min = 3 * kAsciiPerTryPackAvx2Block;

// Up to this length a single 128-bit block plus the scalar tail covers the whole input.
constexpr size_t kTryPackSimdShortMax = kAsciiPerTryPackSimdBlock + kTryPackSimdMin - 1;

// Pack2x8BytesTo2x7 for the avx2 kernel. The first two stages are the same, but the lanes then
// hold a + (b << 32) and avx2 closes the gap without the multiply: sllv shifts every 32-bit lane by
// its own count, so lifting a by four bits and then the whole 64-bit lane down by four leaves
// a + (b << 28).
inline __m128i Pack2x8BytesTo2x7Avx2(__m128i val) {
  val = _mm_maddubs_epi16(_mm_set1_epi16(0x8001), val);
  val = _mm_madd_epi16(_mm_set1_epi32(0x40000001), val);
  return _mm_srli_epi64(_mm_sllv_epi32(val, _mm_set_epi32(0, 4, 0, 4)), 4);
}

// The 28 bytes do not come out contiguous: each 128-bit half holds its own 14 at the bottom.
inline __m256i Pack32BytesTo28(__m256i val) {
  // The same stages as Pack2x8BytesTo2x7Avx2, over twice the lanes.
  val = _mm256_maddubs_epi16(_mm256_set1_epi16(0x8001), val);
  val = _mm256_madd_epi16(_mm256_set1_epi32(0x40000001), val);
  val = _mm256_srli_epi64(_mm256_sllv_epi32(val, _mm256_set_epi32(0, 4, 0, 4, 0, 4, 0, 4)), 4);

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

// Writes the 28 bytes with two 16-byte stores, which is cheaper than StorePacked28Bytes but leaves
// 2 bytes of garbage after them for the next block or the tail to overwrite.
inline void StorePacked28BytesPadded(uint8_t* dest, __m256i packed) {
  _mm_storeu_si128(reinterpret_cast<__m128i*>(dest), _mm256_castsi256_si128(packed));
  _mm_storeu_si128(reinterpret_cast<__m128i*>(dest + kPackedPerTryPackSimdBlock),
                   _mm256_extracti128_si256(packed, 1));
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
    _mm_storeu_si128(reinterpret_cast<__m128i*>(out), Pack16BytesTo14<Pack2x8BytesTo2x7Avx2>(val));
    src += kAsciiPerTryPackSimdBlock;
    out += kPackedPerTryPackSimdBlock;
    remaining -= kAsciiPerTryPackSimdBlock;
  }

  return has_error | AsciiTryPackScalarTail(src, remaining, out);
}

// Kept out of line on purpose: 256-bit code inside ascii_try_pack would force a 32-byte aligned
// frame on it and slow down every short string. It returns the packed length itself, so the caller
// can jump to it: anything the caller still needed after a call would have to be saved across it,
// and that costs short strings a stack frame too.
__attribute__((noinline)) size_t AsciiTryPackAvx2Long(const uint8_t* src, size_t len,
                                                      uint8_t* out) {
  size_t remaining = len;

  // Each pair of blocks is checked after it is stored, so non-ascii input is rejected within 64
  // bytes; the contract allows the bytes stored for a rejected pair to stay behind. A check before
  // every block costs a movemask and a branch per 32 bytes, and on Zen 5 that loop ran up to 1.4x
  // slower depending on where the code was placed.
  while (remaining >= 2 * kAsciiPerTryPackAvx2Block + kTryPackSimdStoreOverlap) {
    const __m256i lo = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src));
    const __m256i hi =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + kAsciiPerTryPackAvx2Block));
    StorePacked28BytesPadded(out, Pack32BytesTo28(lo));
    StorePacked28BytesPadded(out + kPackedPerTryPackAvx2Block, Pack32BytesTo28(hi));
    src += 2 * kAsciiPerTryPackAvx2Block;
    out += 2 * kPackedPerTryPackAvx2Block;
    remaining -= 2 * kAsciiPerTryPackAvx2Block;
    if (_mm256_movemask_epi8(_mm256_or_si256(lo, hi)))
      return 0;
  }

  // One more block may still have room for the padded stores.
  uint64_t has_error = 0;
  if (remaining >= kAsciiPerTryPackAvx2Block + kTryPackSimdStoreOverlap) {
    const __m256i val = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src));
    has_error = _mm256_movemask_epi8(val) ? kAsciiHighBits : 0;
    StorePacked28BytesPadded(out, Pack32BytesTo28(val));
    src += kAsciiPerTryPackAvx2Block;
    out += kPackedPerTryPackAvx2Block;
    remaining -= kAsciiPerTryPackAvx2Block;
  }

  if (remaining >= kAsciiPerTryPackAvx2Block) {  // nothing follows, store exactly 28 bytes
    const __m256i val = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src));
    has_error |= _mm256_movemask_epi8(val) ? kAsciiHighBits : 0;
    StorePacked28Bytes(out, Pack32BytesTo28(val));
    src += kAsciiPerTryPackAvx2Block;
    out += kPackedPerTryPackAvx2Block;
    remaining -= kAsciiPerTryPackAvx2Block;
  }

  return PackedLenIfAscii(has_error | AsciiTryPackSimdShort(src, remaining, out), len);
}
#endif
#endif

}  // namespace

// One entry point per kernel, so that a test can drive them side by side. Each honours the
// ascii_try_pack() contract for any length, no matter how wide it goes internally.
namespace impl {

size_t ascii_try_pack_scalar(const char* ascii, size_t len, uint8_t* bin) {
  return PackedLenIfAscii(AsciiTryPackScalarTail(reinterpret_cast<const uint8_t*>(ascii), len, bin),
                          len);
}

#if defined(ASCII_SSSE3)
size_t ascii_try_pack_sse(const char* ascii, size_t len, uint8_t* bin) {
  const uint8_t* src = reinterpret_cast<const uint8_t*>(ascii);
  return PackedLenIfAscii(AsciiTryPackSimdLoop<Pack2x8BytesTo2x7>(src, len, bin), len);
}
#elif defined(ASCII_NEON)
size_t ascii_try_pack_neon(const char* ascii, size_t len, uint8_t* bin) {
  const uint8_t* src = reinterpret_cast<const uint8_t*>(ascii);
  return PackedLenIfAscii(AsciiTryPackSimdLoop<Pack2x8BytesTo2x7>(src, len, bin), len);
}
#endif

#if defined(ASCII_AVX2)
size_t ascii_try_pack_avx2(const char* ascii, size_t len, uint8_t* bin) {
  const uint8_t* src = reinterpret_cast<const uint8_t*>(ascii);

  // AsciiTryPackAvx2Long only pays for its wider setup once a few 32-byte blocks follow, and
  // AsciiTryPackSimdShort covers what a single 128-bit block plus the tail can finish.
  if (len <= kTryPackSimdShortMax)
    return PackedLenIfAscii(AsciiTryPackSimdShort(src, len, bin), len);
  if (len < kTryPackAvx2Min)
    return PackedLenIfAscii(AsciiTryPackSimdLoop<Pack2x8BytesTo2x7Avx2>(src, len, bin), len);
  return AsciiTryPackAvx2Long(src, len, bin);
}
#endif

}  // namespace impl

namespace {

// The unpack kernels below hand what they decode to a sink, so the same loops either store it or
// compare it against a string that is already unpacked. Every call returns whether to go on, which
// lets a comparison stop at the first difference. Kernels take their sink by value: behind a
// reference, every store through `ascii_` could alias the pointer itself and force a reload.
class StoreSink {
 public:
  explicit StoreSink(char* ascii) : ascii_(reinterpret_cast<uint8_t*>(ascii)) {
  }

  bool Word(uint64_t val) {
    absl::little_endian::Store64(ascii_, val);
    ascii_ += 8;
    return true;
  }

#if defined(ASCII_SSE4_1)
  bool Block(__m128i val) {
    _mm_storeu_si128(reinterpret_cast<__m128i*>(ascii_), val);
    ascii_ += 16;
    return true;
  }
#endif

#if defined(ASCII_AVX2)
  bool Block(__m256i val) {
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(ascii_), val);
    ascii_ += 32;
    return true;
  }
#endif

#if defined(ASCII_NEON)
  bool Block(uint8x16_t val) {
    vst1q_u8(ascii_, val);
    ascii_ += 16;
    return true;
  }

  bool Block(uint8x16x2_t val) {
    vst1q_u8(ascii_, val.val[0]);
    vst1q_u8(ascii_ + 16, val.val[1]);
    ascii_ += 32;
    return true;
  }
#endif

  // Takes the last bytes, less than 8, which are kept verbatim in `bin`.
  bool Tail(const uint8_t* bin, size_t len) {
    CopyTailBytes(bin, len, ascii_);
    return true;
  }

 private:
  uint8_t* ascii_;
};

// Unpacked bytes never have the high bit set, so a non-ascii byte in `ascii` is just a mismatch.
class CompareSink {
 public:
  explicit CompareSink(const char* ascii) : ascii_(reinterpret_cast<const uint8_t*>(ascii)) {
  }

  bool Word(uint64_t val) {
    const bool equal = absl::little_endian::Load64(ascii_) == val;
    ascii_ += 8;
    return equal;
  }

#if defined(ASCII_SSE4_1)
  bool Block(__m128i val) {
    const __m128i diff =
        _mm_xor_si128(val, _mm_loadu_si128(reinterpret_cast<const __m128i*>(ascii_)));
    ascii_ += 16;
    return _mm_testz_si128(diff, diff);
  }
#endif

#if defined(ASCII_AVX2)
  bool Block(__m256i val) {
    const __m256i diff =
        _mm256_xor_si256(val, _mm256_loadu_si256(reinterpret_cast<const __m256i*>(ascii_)));
    ascii_ += 32;
    return _mm256_testz_si256(diff, diff);
  }
#endif

#if defined(ASCII_NEON)
  bool Block(uint8x16_t val) {
    const uint8x16_t diff = veorq_u8(val, vld1q_u8(ascii_));
    ascii_ += 16;
    return IsZero(diff);
  }

  // Folds both halves into one vector first, so that they cost a single test and branch.
  bool Block(uint8x16x2_t val) {
    const uint8x16_t diff = vorrq_u8(veorq_u8(val.val[0], vld1q_u8(ascii_)),
                                     veorq_u8(val.val[1], vld1q_u8(ascii_ + 16)));
    ascii_ += 32;
    return IsZero(diff);
  }
#endif

  bool Tail(const uint8_t* bin, size_t len) {
    return DiffTailBytes(bin, ascii_, len) == 0;
  }

 private:
#if defined(ASCII_NEON)
  // vpmaxq folds neighbouring bytes, so the low 64 bits are zero only if all 16 bytes are. vmaxvq
  // would fold all 16 at once, but it takes 6 cycles on a single pipe.
  static bool IsZero(uint8x16_t val) {
    return vgetq_lane_u64(vreinterpretq_u64_u8(vpmaxq_u8(val, val)), 0) == 0;
  }
#endif

  const uint8_t* ascii_;
};

template <typename Sink>
inline __attribute__((always_inline)) bool AsciiDecodeScalar(const uint8_t* bin, size_t ascii_len,
                                                             Sink sink) {
  constexpr uint64_t kSevenBytes = 0x00FFFFFFFFFFFFFFULL;

  if (ascii_len < 8)
    return sink.Tail(bin, ascii_len);

  while (ascii_len >= 9) {
    const uint64_t val = absl::little_endian::Load64(bin) & kSevenBytes;
    if (!sink.Word(Unpack7BytesTo8(val)))
      return false;
    bin += 7;
    ascii_len -= 8;
  }

  if (ascii_len == 8) {
    // Two overlapping loads read exactly seven bytes.
    const uint64_t val = absl::little_endian::Load32(bin) |
                         (static_cast<uint64_t>(absl::little_endian::Load32(bin + 3)) << 24);
    return sink.Word(Unpack7BytesTo8(val));
  }

  // CopyTailBytesOverPrev would reuse the last expanded word, but keeping it live makes every loop
  // iteration feed the tail, so the loop can no longer run ahead of its stores.
  return sink.Tail(bin, ascii_len);
}

inline __attribute__((always_inline)) void AsciiUnpackFastScalar(const uint8_t* bin,
                                                                 size_t ascii_len, char* ascii) {
  AsciiDecodeScalar(bin, ascii_len, StoreSink{ascii});
}

#if defined(ASCII_SSE4_1)
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

// No load reaches past the packed bytes, see CompactObjectTest.AsanTriggerReadOverflow.
template <typename Sink>
inline __attribute__((always_inline)) bool AsciiDecodeSse(const uint8_t* bin, size_t ascii_len,
                                                          Sink sink) {
  // Testing this first keeps short strings from paying for the block checks below.
  if (ascii_len < kAsciiPerUnpackSseBlock)
    return AsciiDecodeScalar(bin, ascii_len, sink);

  // Read the control backwards: bytes 0..6, a zero, then 7..14, the same as in Unpack28BytesTo32.
  const __m128i control = _mm_set_epi8(14, 13, 12, 11, 10, 9, 8, 7, -1, 6, 5, 4, 3, 2, 1, 0);

  // Each load takes 16 bytes and so reads 2 past the block, which is why the loop stops 2 early.
  while (ascii_len >= kAsciiPerUnpackSseBlock + 2) {
    const __m128i val = _mm_loadu_si128(reinterpret_cast<const __m128i*>(bin));
    if (!sink.Block(Unpack14BytesTo16(_mm_shuffle_epi8(val, control))))
      return false;
    bin += kPackedPerUnpackSseBlock;
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
    if (!sink.Block(Unpack14BytesTo16(_mm_shuffle_epi8(val, control_last))))
      return false;
    bin += kPackedPerUnpackSseBlock;
    ascii_len -= kAsciiPerUnpackSseBlock;
  }

  return AsciiDecodeScalar(bin, ascii_len, sink);
}
#endif

#if defined(ASCII_AVX2)
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

template <typename Sink>
__attribute__((noinline)) bool AsciiDecodeAvx2Loop(const uint8_t* bin, size_t ascii_len,
                                                   Sink sink) {
  // A block's two halves are 14 bytes apart, so they are loaded separately and joined: castsi128
  // widens the low one for free and inserti128 drops the other into the upper half. Each load
  // takes 16 bytes and therefore reads 2 past its half, which is why the loop stops 2 bytes early.
  while (ascii_len >= kAsciiPerUnpackAvx2Block + 2) {
    const __m128i low = _mm_loadu_si128(reinterpret_cast<const __m128i*>(bin));
    const __m128i high = _mm_loadu_si128(reinterpret_cast<const __m128i*>(bin + 14));
    __m256i val = _mm256_inserti128_si256(_mm256_castsi128_si256(low), high, 1);
    if (!sink.Block(Unpack28BytesTo32(val)))
      return false;
    bin += kPackedPerUnpackAvx2Block;
    ascii_len -= kAsciiPerUnpackAvx2Block;
  }
  // The final block has nothing after it to over-read, so its upper half is loaded 2 bytes early
  // and slid back down: srli_si128 shifts the vector right by whole bytes.
  if (ascii_len >= kAsciiPerUnpackAvx2Block) {
    const __m128i low = _mm_loadu_si128(reinterpret_cast<const __m128i*>(bin));
    const __m128i high =
        _mm_srli_si128(_mm_loadu_si128(reinterpret_cast<const __m128i*>(bin + 12)), 2);
    __m256i val = _mm256_inserti128_si256(_mm256_castsi128_si256(low), high, 1);
    if (!sink.Block(Unpack28BytesTo32(val)))
      return false;
    bin += kPackedPerUnpackAvx2Block;
    ascii_len -= kAsciiPerUnpackAvx2Block;
  }
  return AsciiDecodeScalar(bin, ascii_len, sink);
}

template <typename Sink>
inline bool AsciiDecodeAvx2(const uint8_t* bin, size_t ascii_len, Sink sink) {
  if (ascii_len >= kAsciiPerUnpackAvx2Block)
    return AsciiDecodeAvx2Loop(bin, ascii_len, sink);
  return AsciiDecodeScalar(bin, ascii_len, sink);
}
#elif defined(ASCII_NEON)
constexpr size_t kAsciiPerUnpackNeonBlock = 16;
constexpr size_t kPackedPerUnpackNeonBlock = 14;

// Character j of a group of seven packed bytes occupies bits [7j, 7j + 7), which lie within the
// 16-bit window that starts at byte 7j / 8. A table gathers these windows into 16-bit lanes, and a
// per-lane shift right by 7j % 8 brings each character down to bit 0. Vector shifts issue to only
// one of the two vector pipes, and this way a block needs just two of them. The first group starts
// at byte 0 of `raw`, the second at byte kSecondGroup.
template <uint8_t kSecondGroup> inline uint8x16_t Unpack14BytesTo16(uint8x16_t raw) {
  // The result doesn't depend on the upper bytes of lanes 0 and 7: lane 0 isn't shifted, and the
  // one bit lane 7 shifts in is cleared by the mask below. So their windows may point anywhere,
  // even out of range, where vqtbl1q emits zero.
  const uint8x16_t windows = {0, 1, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7};
  const int16x8_t shifts = {0, -7, -6, -5, -4, -3, -2, -1};  // negative counts shift right
  const uint8x16_t second_windows = vaddq_u8(windows, vdupq_n_u8(kSecondGroup));
  const uint16x8_t low = vshlq_u16(vreinterpretq_u16_u8(vqtbl1q_u8(raw, windows)), shifts);
  const uint16x8_t high = vshlq_u16(vreinterpretq_u16_u8(vqtbl1q_u8(raw, second_windows)), shifts);

  // Every character is now in the low byte of its lane, under one bit of the next character.
  return vandq_u8(vuzp1q_u8(vreinterpretq_u8_u16(low), vreinterpretq_u8_u16(high)),
                  vdupq_n_u8(0x7F));
}

// Two 8-byte loads stay inside the 14 packed bytes, unlike the 16-byte loads below, so this also
// serves the last block of a buffer. They fetch bytes 0..7 and 6..13, which moves the second group
// up to byte 9.
inline uint8x16_t UnpackLast14BytesTo16(const uint8_t* bin) {
  return Unpack14BytesTo16<9>(vcombine_u8(vld1_u8(bin), vld1_u8(bin + 6)));
}

template <typename Sink>
inline __attribute__((always_inline)) bool AsciiDecodeNeon(const uint8_t* bin, size_t ascii_len,
                                                           Sink sink) {
  if (ascii_len < kAsciiPerUnpackNeonBlock)
    return AsciiDecodeScalar(bin, ascii_len, sink);

  // A single block plus the scalar tail covers these. They could take the steps below as well,
  // but that measured slower at 18..31 bytes.
  if (ascii_len < 2 * kAsciiPerUnpackNeonBlock) {
    if (!sink.Block(UnpackLast14BytesTo16(bin)))
      return false;
    return AsciiDecodeScalar(bin + kPackedPerUnpackNeonBlock, ascii_len - kAsciiPerUnpackNeonBlock,
                             sink);
  }

  // Two blocks per iteration halve the loop overhead, and CompareSink tests them with one branch.
  // Each 16-byte load reads 2 bytes past the 14 it needs, which is why the next two steps stop 2
  // bytes early.
  while (ascii_len >= 2 * kAsciiPerUnpackNeonBlock + 2) {
    const uint8x16_t low = vld1q_u8(bin);
    const uint8x16_t high = vld1q_u8(bin + kPackedPerUnpackNeonBlock);
    if (!sink.Block(uint8x16x2_t{{Unpack14BytesTo16<7>(low), Unpack14BytesTo16<7>(high)}}))
      return false;
    bin += 2 * kPackedPerUnpackNeonBlock;
    ascii_len -= 2 * kAsciiPerUnpackNeonBlock;
  }

  if (ascii_len >= kAsciiPerUnpackNeonBlock + 2) {
    if (!sink.Block(Unpack14BytesTo16<7>(vld1q_u8(bin))))
      return false;
    bin += kPackedPerUnpackNeonBlock;
    ascii_len -= kAsciiPerUnpackNeonBlock;
  }

  if (ascii_len >= kAsciiPerUnpackNeonBlock) {
    if (!sink.Block(UnpackLast14BytesTo16(bin)))
      return false;
    bin += kPackedPerUnpackNeonBlock;
    ascii_len -= kAsciiPerUnpackNeonBlock;
  }

  return AsciiDecodeScalar(bin, ascii_len, sink);
}
#endif

}  // namespace

// One entry point per kernel, like ascii_try_pack above.
namespace impl {

void ascii_unpack_fast_scalar(const uint8_t* bin, size_t ascii_len, char* ascii) {
  AsciiUnpackFastScalar(bin, ascii_len, ascii);
}

bool ascii_compare_packed_scalar(const uint8_t* bin, size_t ascii_len, const char* ascii) {
  return AsciiDecodeScalar(bin, ascii_len, CompareSink{ascii});
}

#if defined(ASCII_SSE4_1)
void ascii_unpack_fast_sse(const uint8_t* bin, size_t ascii_len, char* ascii) {
  AsciiDecodeSse(bin, ascii_len, StoreSink{ascii});
}

bool ascii_compare_packed_sse(const uint8_t* bin, size_t ascii_len, const char* ascii) {
  return AsciiDecodeSse(bin, ascii_len, CompareSink{ascii});
}
#endif

#if defined(ASCII_AVX2)
void ascii_unpack_fast_avx2(const uint8_t* bin, size_t ascii_len, char* ascii) {
  AsciiDecodeAvx2(bin, ascii_len, StoreSink{ascii});
}

bool ascii_compare_packed_avx2(const uint8_t* bin, size_t ascii_len, const char* ascii) {
  return AsciiDecodeAvx2(bin, ascii_len, CompareSink{ascii});
}
#elif defined(ASCII_NEON)
void ascii_unpack_fast_neon(const uint8_t* bin, size_t ascii_len, char* ascii) {
  AsciiDecodeNeon(bin, ascii_len, StoreSink{ascii});
}

bool ascii_compare_packed_neon(const uint8_t* bin, size_t ascii_len, const char* ascii) {
  return AsciiDecodeNeon(bin, ascii_len, CompareSink{ascii});
}
#endif

}  // namespace impl

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC pop_options
#endif

}  // namespace detail

}  // namespace dfly
