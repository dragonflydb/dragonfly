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
