// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "cluster_support.h"

#ifdef DFLY_CRC16_HAS_CLMUL
#include "core/sse_port.h"
#ifdef __x86_64__
#include <wmmintrin.h>
#endif
#endif

#include "base/endian.h"
#include "base/flags.h"
#include "base/logging.h"
#include "common.h"

using namespace std;

ABSL_FLAG(string, cluster_mode, "",
          "Cluster mode supported. Possible values are "
          "'emulated', 'yes' or ''");

ABSL_FLAG(bool, experimental_cluster_shard_by_slot, false,
          "If true, cluster mode is enabled and sharding is done by slot. "
          "Otherwise, sharding is done by hash tag.");

namespace dfly {

// ---------------------------------------------------------------------------
// Crc16
//
// CRC16-XMODEM reads the input as one polynomial over GF(2), the top bit of the first byte being
// the highest power, and returns the remainder of input(x) * x^16 divided by
// P(x) = x^16 + x^12 + x^5 + 1. Addition over GF(2) is XOR, which has no carries, so remainders
// of parts of the input can be computed separately and combined.
//
// Instead of a lookup table, both kernels divide whole words with Barrett reduction, which turns
// the division into a multiplication with a precomputed reciprocal of P. The quotient of
// V * x^16 / P only needs its low 16 bits: the low 16 bits of the dividend are zero, so the
// remainder is just the low 16 bits of quotient * P.
// ---------------------------------------------------------------------------

namespace detail::impl {

// Internal-linkage helpers: only the detail::impl entry points below are called from outside.
namespace {

// Low 16 bits of q * P, the remainder for Barrett quotient q. P's x^16 term only reaches above.
inline uint16_t RemainderFromQuotient(uint64_t q) {
  return q ^ (q << 5) ^ (q << 12);
}

// Appends 8 bytes, `data` read big-endian, to an input whose CRC so far is `crc`, i.e. returns
// (crc * x^64 + data * x^16) mod P. The quotient is V * x^16 / P for V = crc * x^48 + data, and
// x^16 / P = 1 / (1 + u) with u = x^-4 + x^-11 + x^-16. Over GF(2) that is the product
// (1 + u)(1 + u^2)(1 + u^4)(1 + u^8)..., and since every factor is 1 plus negative powers of x,
// the fractions that right shifts drop could never move back up: multiplying V by one factor after
// the other yields the exact quotient. The factors after (1 + u^8) only reach below x^-64.
inline uint16_t Step64(uint16_t crc, uint64_t data) {
  uint64_t q = data ^ (uint64_t{crc} << 48);
  q ^= (q >> 4) ^ (q >> 11) ^ (q >> 16);  // * (1 + u)
  q ^= (q >> 8) ^ (q >> 22) ^ (q >> 32);  // * (1 + u^2)
  q ^= (q >> 16) ^ (q >> 44);             // * (1 + u^4), whose x^-64 drops every bit
  q ^= q >> 32;                           // * (1 + u^8)
  return RemainderFromQuotient(q);
}

// Reads 4 to 8 bytes as one big-endian number with two overlapping loads. OR keeps the bytes both
// loads cover, as they are equal.
inline uint64_t Load4To8(const uint8_t* p, size_t len) {
  return uint64_t{absl::big_endian::Load32(p)} << (8 * (len - 4)) |
         absl::big_endian::Load32(p + len - 4);
}

#ifdef DFLY_CRC16_HAS_CLMUL

// floor(x^144 / P) = x^128 + kMu * x^64 + kMuLow is the Barrett reciprocal for 128-bit values, and
// its top part x^64 + kMu = floor(x^80 / P) the one for 64-bit values.
constexpr uint64_t kMu = 0x11303471a041b343;
constexpr uint64_t kMuLow = 0xe569d503417ca4a2;

// pclmulqdq multiplies two 64-bit polynomials without carries (the partial products are XOR-ed
// instead of added) into a 128-bit product. Its selector picks the 64-bit half of each operand:
// 0x00 the low halves, 0x11 the high ones. aarch64 has the same as pmull and pmull2.
DFLY_CRC16_CLMUL_TARGET inline __m128i ClmulLow(__m128i a, __m128i b) {
  return _mm_clmulepi64_si128(a, b, 0x00);
}

#if defined(__aarch64__)
inline __m128i ClmulHigh(__m128i a, __m128i b) {
  // Request pmull2 explicitly, sse2neon would extract the high halves first.
  poly128_t product = vmull_high_p64(vreinterpretq_p64_s64(a), vreinterpretq_p64_s64(b));
  return vreinterpretq_m128i_u64(vreinterpretq_u64_p128(product));
}

// pshufb builds a vector out of the bytes of another: result byte i is the one control byte i
// indexes, or zero if that control byte has its top bit set. aarch64's tbl zeroes any index past
// 15, which covers those too, while sse2neon would mask every index first.
inline __m128i Shuffle(__m128i bytes, __m128i control) {
  return vreinterpretq_m128i_u8(
      vqtbl1q_u8(vreinterpretq_u8_m128i(bytes), vreinterpretq_u8_m128i(control)));
}
#else

DFLY_CRC16_CLMUL_TARGET inline __m128i ClmulHigh(__m128i a, __m128i b) {
  return _mm_clmulepi64_si128(a, b, 0x11);
}

DFLY_CRC16_CLMUL_TARGET inline __m128i Shuffle(__m128i bytes, __m128i control) {
  return _mm_shuffle_epi8(bytes, control);
}
#endif

DFLY_CRC16_CLMUL_TARGET inline __m128i ByteIndexes() {
  return _mm_setr_epi8(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
}

// Reads 16 bytes as one big-endian number. Vector loads are little-endian, so the bytes are
// reversed.
DFLY_CRC16_CLMUL_TARGET inline __m128i LoadBlock(const uint8_t* p) {
  const __m128i reverse = _mm_setr_epi8(15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0);
  return Shuffle(mm_loadu_si128(reinterpret_cast<const __m128i*>(p)), reverse);
}

// Reads the first n (1 to 16) of the 16 bytes at p as a big-endian number. Control byte
// i = n - 1 - i reverses them, and turns negative, zeroing the byte, past the first n.
DFLY_CRC16_CLMUL_TARGET inline __m128i LoadFirstBytes(const uint8_t* p, size_t n) {
  const __m128i control = _mm_sub_epi8(_mm_set1_epi8(static_cast<char>(n - 1)), ByteIndexes());
  return Shuffle(mm_loadu_si128(reinterpret_cast<const __m128i*>(p)), control);
}

// Reads 8 to 16 bytes as a big-endian number. Two overlapping 8-byte loads put the first 8 bytes
// into the low half and the last 8 into the high half. The low half of the result reverses the
// last 8 (controls 15 down to 8), and its high half the bytes before them (controls len - 9 down),
// running negative past the input.
DFLY_CRC16_CLMUL_TARGET inline __m128i Load8To16(const uint8_t* p, size_t len) {
  const __m128i first = _mm_loadl_epi64(reinterpret_cast<const __m128i*>(p));
  const __m128i last = _mm_loadl_epi64(reinterpret_cast<const __m128i*>(p + len - 8));
  constexpr uint64_t kEveryByte = 0x0101010101010101;
  const __m128i starts = _mm_set_epi64x((len - 1) * kEveryByte, 15 * kEveryByte);
  return Shuffle(_mm_unpacklo_epi64(first, last), _mm_sub_epi8(starts, ByteIndexes()));
}

// The CRC of a 64-bit number x. Its quotient floor(x * (x^64 + kMu) / x^64) is x plus the high
// half of x * kMu.
DFLY_CRC16_CLMUL_TARGET inline uint16_t Reduce64(uint64_t x) {
  const __m128i product = ClmulLow(_mm_cvtsi64_si128(x), _mm_set1_epi64x(kMu));
  return RemainderFromQuotient(_mm_extract_epi16(product, 4) ^ x);
}

// The CRC of a 128-bit number v = H * x^64 + L. Multiplied out with the 128-bit reciprocal, the low
// 16 bits of its quotient are those of L + H * kMu + (the high half of H * kMuLow + L * kMu). The
// three products don't depend on each other, so the CPU runs them at the same time.
DFLY_CRC16_CLMUL_TARGET inline uint16_t Reduce128(__m128i v) {
  const __m128i mu = _mm_set1_epi64x(kMu);
  const __m128i mu_low = _mm_set_epi64x(kMuLow, 0);
  const __m128i high = _mm_xor_si128(ClmulHigh(v, mu_low), ClmulLow(v, mu));
  const __m128i low = _mm_xor_si128(ClmulHigh(v, mu), v);
  return RemainderFromQuotient(uint32_t(_mm_extract_epi16(high, 4)) ^
                               uint32_t(_mm_cvtsi128_si32(low)));
}

// Given factors {x^(d + 64) mod P, x^d mod P}, returns H * (x^(d + 64) mod P) + L * (x^d mod P) for
// v = H * x^64 + L: a number of at most 80 bits with the same CRC as v followed by d / 8 zero
// bytes, which the caller then XORs the bytes that really follow into.
DFLY_CRC16_CLMUL_TARGET inline __m128i Fold(__m128i v, __m128i factors) {
  return _mm_xor_si128(ClmulHigh(v, factors), ClmulLow(v, factors));
}

#endif  // DFLY_CRC16_HAS_CLMUL

}  // namespace

// One entry point per kernel, so that a test can drive them side by side.
uint16_t Crc16Scalar(std::string_view data) {
  const uint8_t* p = reinterpret_cast<const uint8_t*>(data.data());
  size_t len = data.size();
  if (len < 8) {
    if (len >= 4)
      return Step64(0, Load4To8(p, len));
    return len == 0 ? 0 : Crc16For1To3Bytes(data);
  }

  // Leading zero bytes leave a CRC that starts at zero unchanged, so the first (len - 1) % 8 + 1
  // bytes, zero-extended, count as a whole word, and whole words follow up to the end.
  const size_t head = ((len - 1) & 7) + 1;
  uint16_t crc = Step64(0, absl::big_endian::Load64(p) >> (8 * (8 - head)));
  for (p += head, len -= head; len > 0; p += 8, len -= 8)
    crc = Step64(crc, absl::big_endian::Load64(p));
  return crc;
}

#ifdef DFLY_CRC16_HAS_CLMUL
DFLY_CRC16_CLMUL_TARGET uint16_t Crc16Clmul(std::string_view data) {
  const uint8_t* p = reinterpret_cast<const uint8_t*>(data.data());
  size_t len = data.size();
  if (len < 16) {
    if (len > 8)
      return Reduce128(Load8To16(p, len));
    if (len >= 4)
      return Reduce64(Load4To8(p, len));
    return len == 0 ? 0 : Crc16For1To3Bytes(data);
  }

  // Fold() factors for d = 128, 256 and 384 bits.
  const __m128i k128 = _mm_set_epi64x(0x650b, 0xaefc);
  const __m128i k256 = _mm_set_epi64x(0x26aa, 0x8e29);
  const __m128i k384 = _mm_set_epi64x(0x2535, 0xcde2);

  // As in Crc16Scalar, a first block of 1 to 16 bytes leaves whole blocks up to the end.
  const size_t head = ((len - 1) & 15) + 1;
  __m128i v = LoadFirstBytes(p, head);
  p += head;
  size_t blocks = (len - head) / 16;

  // Folding block after block into v would make every multiplication wait for the one before. So
  // four lanes, 64 bytes apart, fold their own blocks instead, and are merged at the end.
  if (blocks >= 3) {
    const __m128i k512 = _mm_set_epi64x(0x8832, 0x13fc);
    __m128i a = v, b = LoadBlock(p), c = LoadBlock(p + 16), d = LoadBlock(p + 32);
    p += 48;
    blocks -= 3;
    for (; blocks >= 4; blocks -= 4, p += 64) {
      a = _mm_xor_si128(Fold(a, k512), LoadBlock(p));
      b = _mm_xor_si128(Fold(b, k512), LoadBlock(p + 16));
      c = _mm_xor_si128(Fold(c, k512), LoadBlock(p + 32));
      d = _mm_xor_si128(Fold(d, k512), LoadBlock(p + 48));
    }
    v = _mm_xor_si128(_mm_xor_si128(Fold(a, k384), Fold(b, k256)), _mm_xor_si128(Fold(c, k128), d));
  }

  // Up to 3 blocks remain. Each is folded straight to the end, independently of the others.
  switch (blocks) {
    case 1:
      v = _mm_xor_si128(Fold(v, k128), LoadBlock(p));
      break;
    case 2:
      v = _mm_xor_si128(_mm_xor_si128(Fold(v, k256), Fold(LoadBlock(p), k128)), LoadBlock(p + 16));
      break;
    case 3:
      v = _mm_xor_si128(_mm_xor_si128(Fold(v, k384), Fold(LoadBlock(p), k256)),
                        _mm_xor_si128(Fold(LoadBlock(p + 16), k128), LoadBlock(p + 32)));
      break;
  }
  return Reduce128(v);
}
#endif  // DFLY_CRC16_HAS_CLMUL

}  // namespace detail::impl

void UniqueSlotChecker::Add(std::string_view key) {
  if (!IsClusterEnabled()) {
    return;
  }

  Add(KeySlot(key));
}

void UniqueSlotChecker::Add(SlotId slot_id) {
  if (!IsClusterEnabled()) {
    return;
  }

  if (slot_id_ == kNoSlotId) {
    slot_id_ = slot_id;
  } else if (slot_id_ != slot_id) {
    slot_id_ = kCrossSlot;
  }
}

optional<SlotId> UniqueSlotChecker::GetUniqueSlotId() const {
  return slot_id_ > kMaxSlotNum ? optional<SlotId>() : slot_id_;
}

using namespace detail;

void InitializeCluster() {
  string cluster_mode_str = absl::GetFlag(FLAGS_cluster_mode);

  if (cluster_mode_str == "emulated") {
    cluster_mode = ClusterMode::kEmulatedCluster;
  } else if (cluster_mode_str == "yes") {
    cluster_mode = ClusterMode::kRealCluster;
  } else if (cluster_mode_str.empty()) {
    cluster_mode = ClusterMode::kNoCluster;
  } else {
    LOG(ERROR) << "Invalid value for flag --cluster_mode. Exiting...";
    exit(1);
  }

  if (cluster_mode == ClusterMode::kNoCluster) {
    // Reset the cached value so it doesn't leak in tests.
    cluster_shard_by_slot = false;
  } else {
    cluster_shard_by_slot = absl::GetFlag(FLAGS_experimental_cluster_shard_by_slot);
  }
}

SlotId KeySlot(std::string_view key) {
  return Crc16(LockTagOptions::instance().Tag(key)) & kMaxSlotNum;
}

bool IsClusterShardedByTag() {
  return IsClusterEnabledOrEmulated() || LockTagOptions::instance().enabled;
}

}  // namespace dfly
