// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <optional>
#include <string_view>

#include "server/common_types.h"

namespace dfly {

namespace detail {

enum class ClusterMode {
  kUninitialized,
  kNoCluster,
  kEmulatedCluster,
  kRealCluster,
};

inline ClusterMode cluster_mode = ClusterMode::kUninitialized;
inline bool cluster_shard_by_slot = false;

};  // namespace detail

constexpr SlotId kMaxSlotNum = 0x3FFF;

namespace detail {

// ---------------------------------------------------------------------------
// Crc16 performance
//
// Nanoseconds per call by input length in bytes, median of three runs with three repetitions each.
// Run build-opt/cluster_family_test with --gtest_filter=NONE --bench --benchmark_filter=Crc16
// --benchmark_min_time=0.05s --benchmark_repetitions=3 under taskset -c 3.
// crc16 is the table-driven function of Redis (src/redis/crc16.c).
//
// Release, GCC 13.3.0, -O3 -march=haswell -mtune=skylake, AMD Ryzen AI MAX+ 395:
//                       1      3      4      8     16     32     64    256   1024
//   crc16             1.7    2.4    3.2    7.8   21.5   57.6  154.1  743.0 3075.1
//   Crc16Scalar       2.4    2.4    2.9    3.0    6.2   14.2   36.4  183.8  771.0
//   Crc16Clmul        2.5    2.5    2.7    2.8    3.4    4.4    7.1   22.3   87.2
//   Crc16             2.2    2.2    2.8    2.7    3.7    4.7    7.5   22.4   87.4
//
// Crc16 runs Crc16Clmul from 4 bytes on when available. This build enables PCLMUL and SSSE3 at
// compile time, so its capability check folds away; portable release builds check at runtime.
//
// Why it's faster:
//  - crc16 looks up one byte at a time, and every lookup waits for the one before. Crc16Scalar
//    divides 8 bytes at once with shifts and XORs, and Crc16Clmul folds 16 bytes at a time with
//    two carry-less multiplications, in four independent lanes past 48 bytes, so that the
//    multiplications overlap.
//  - Nothing is looked up in memory, so no table takes up cache or misses it.
//  - Short inputs take no loop: 1 to 3 bytes are computed inline with shifts, without a call,
//    4 to 8 from two overlapping 4-byte loads, and 9 to 15 from two overlapping 8-byte loads with
//    one 128-bit reduction, whose three multiplications don't wait for each other.
//  - An input that isn't a whole number of words starts with a partial word, padded with leading
//    zeros, which leave the CRC unchanged, instead of ending with a byte-by-byte tail.
// ---------------------------------------------------------------------------

// Crc16() only ever runs the fastest kernel the CPU supports, so a caller can never reach the other
// one and it would go untested. Every kernel therefore also gets its own entry point here. They all
// compute the same CRC for any input, so tests can drive them side by side.
namespace impl {

// The CRC of 1 to 3 bytes, which Crc16() computes in place, sparing short hash tags like the 1 of
// {1} a call. As in Step64() in cluster_support.cc, the remainder comes from the quotient
// m * x^16 / P = m / (1 + x^-4 + x^-11 + x^-16), here multiplied out and cut off where the shifts
// leave nothing of a 24-bit m: m * (1 + x^-8)(1 + x^-4 + x^-11) + m * (x^-20 + x^-22).
inline uint16_t Crc16For1To3Bytes(std::string_view data) {
  const uint8_t* p = reinterpret_cast<const uint8_t*>(data.data());
  const size_t len = data.size();

  // The first, middle and last bytes are the input if there are 3, and the shift drops repeats.
  const uint32_t m =
      (uint32_t{p[0]} << 16 | uint32_t{p[len / 2]} << 8 | p[len - 1]) >> (24 - 8 * len);
  const uint32_t b = m ^ (m >> 8);
  const uint32_t q = b ^ (b >> 4) ^ (b >> 11) ^ (m >> 20) ^ (m >> 22);
  return q ^ (q << 5) ^ (q << 12);  // The low 16 bits of q * P.
}

uint16_t Crc16Scalar(std::string_view data);

// Whether the CLMUL kernel is compiled for this target.
#if defined(__x86_64__) || \
    (defined(__aarch64__) && (defined(__ARM_FEATURE_AES) || defined(__ARM_FEATURE_CRYPTO)))
#define DFLY_CRC16_HAS_CLMUL 1
static constexpr bool kHasClmul = true;
#else
static constexpr bool kHasClmul = false;
#endif

#if defined(__x86_64__) && (!defined(__PCLMUL__) || !defined(__SSSE3__))

// Release builds target x86 CPUs that predate carry-less multiplication, so the kernel is compiled
// for it on its own and only runs once kSupportsClmul() has confirmed that the CPU supports it.
#define DFLY_CRC16_CLMUL_TARGET __attribute__((target("pclmul,ssse3")))

inline constexpr auto kSupportsClmul = [] {
  return __builtin_cpu_supports("pclmul") && __builtin_cpu_supports("ssse3");
};
#else
#define DFLY_CRC16_CLMUL_TARGET

inline constexpr auto kSupportsClmul = [] { return kHasClmul; };
#endif

// Declared on all targets for constexpr dispatch; defined only when kHasClmul is true.
DFLY_CRC16_CLMUL_TARGET uint16_t Crc16Clmul(std::string_view data);  // pclmulqdq or pmull

}  // namespace impl

// CRC16-XMODEM (polynomial 0x1021, zero initial value), which Redis Cluster maps keys to slots
// with. Inline, so that a caller reaches the kernel with a single direct call instead of bouncing
// through an out of line dispatcher.
inline uint16_t Crc16(std::string_view data) {
  // Empty data wraps around to the kernels. Marked unlikely only to keep the call of longer inputs
  // on the fall-through path, so they don't pay for a taken branch.
  if (data.size() - 1 < 3) [[unlikely]]
    return impl::Crc16For1To3Bytes(data);
  if constexpr (impl::kHasClmul) {
    if (impl::kSupportsClmul())
      return impl::Crc16Clmul(data);
  }
  return impl::Crc16Scalar(data);
}

}  // namespace detail

// A simple utility class that "aggregates" SlotId-s and can tell whether all inputs were the same.
// Only works when cluster is enabled.
class UniqueSlotChecker {
 public:
  void Add(std::string_view key);
  void Add(SlotId slot_id);

  std::optional<SlotId> GetUniqueSlotId() const;

  bool IsCrossSlot() const {
    return slot_id_ == kCrossSlot;
  }

  void Reset() {
    slot_id_ = kNoSlotId;
  }

 private:
  // kNoSlotId - if slot wasn't set at all
  static constexpr SlotId kNoSlotId = kMaxSlotNum + 1;
  // kCrossSlot - if several different slots were set
  static constexpr SlotId kCrossSlot = kNoSlotId + 1;

  SlotId slot_id_ = kNoSlotId;
};

SlotId KeySlot(std::string_view key);

void InitializeCluster();

inline bool IsClusterEnabled() {
  return detail::cluster_mode == detail::ClusterMode::kRealCluster;
}

inline bool IsClusterEmulated() {
  return detail::cluster_mode == detail::ClusterMode::kEmulatedCluster;
}

inline bool IsClusterEnabledOrEmulated() {
  return IsClusterEnabled() || IsClusterEmulated();
}

inline bool IsClusterShardedBySlot() {
  return detail::cluster_shard_by_slot;
}

bool IsClusterShardedByTag();

}  // namespace dfly
