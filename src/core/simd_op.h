// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>

#if defined(__AVX2__)
#include <immintrin.h>
#elif defined(__ARM_NEON)
#include <arm_neon.h>
#endif

namespace dfly {

// SimdOp<T, N> wraps N consecutive uint64_t lanes behind the GCC/Clang
// vector-extension type. Compare ops return another SimdOp with all-ones or
// all-zeros lanes; ToBits() compresses such a mask to a uint32_t bitmask
// (LSB = lane 0).
template <class T, std::size_t N> class SimdOp {
  static_assert(std::is_same_v<T, std::uint64_t>, "only uint64_t is supported today");
  static_assert(N == 2 || N == 4, "only N=2 (SSE/NEON) and N=4 (AVX2) are supported today");

  using Vec __attribute__((vector_size(sizeof(T) * N))) = T;

 public:
  using BitsType = std::uint32_t;
  static constexpr std::size_t kLanes = N;

  SimdOp() = default;

  // Broadcasting via `Vec{} + value` lowers to vpbroadcast on AVX2 / dup on
  // NEON; a per-lane scalar loop pessimizes to vpinsrq + vperm2i128.
  static SimdOp Broadcast(T value) {
    return Vec{} + value;
  }

  static SimdOp Load(const T* ptr) {
    Vec v;
    std::memcpy(&v, ptr, sizeof(Vec));
    return v;
  }

  SimdOp operator&(const SimdOp& o) const {
    return v_ & o.v_;
  }

  SimdOp operator|(const SimdOp& o) const {
    return v_ | o.v_;
  }

  SimdOp operator>>(unsigned shift) const {
    return v_ >> shift;
  }

  SimdOp operator~() const {
    return ~v_;
  }

  SimdOp operator==(const SimdOp& o) const {  // NOLINT
    return Vec(v_ == o.v_);
  }

  SimdOp operator==(T value) const {  // NOLINT
    return Vec(v_ == (Vec{} + value));
  }

  // The architecture branches exist because no portable formulation lowers to
  // a single movemask instruction — every plain vector-extension version we
  // tried measured ~5% slower on OAHSet's hot path.
  BitsType ToBits() const {
#if defined(__AVX2__)
    if constexpr (N == 4) {
      __m256i raw;
      std::memcpy(&raw, &v_, sizeof(raw));
      return static_cast<BitsType>(_mm256_movemask_pd(_mm256_castsi256_pd(raw)));
    } else {
      __m128i raw;
      std::memcpy(&raw, &v_, sizeof(raw));
      return static_cast<BitsType>(_mm_movemask_pd(_mm_castsi128_pd(raw)));
    }
#elif defined(__ARM_NEON)
    // NEON has no movemask; shrn collapses each 64-bit lane to its top 32
    // bits in one instruction. For N=4 we do it on both 128-bit halves.
    uint64x2_t halves[N / 2];
    std::memcpy(halves, &v_, sizeof(v_));
    BitsType bits = 0;
    for (std::size_t h = 0; h < N / 2; ++h) {
      uint32x2_t narrow = vshrn_n_u64(halves[h], 32);
      std::uint64_t packed;
      std::memcpy(&packed, &narrow, sizeof(packed));
      bits |= (static_cast<BitsType>((packed & 1u) | ((packed >> 31) & 2u))) << (2 * h);
    }
    return bits;
#else
    BitsType m = 0;
    for (std::size_t i = 0; i < N; ++i)
      m |= static_cast<BitsType>(v_[i] != 0) << i;
    return m;
#endif
  }

 private:
  SimdOp(Vec v) : v_(v) {  // NOLINT(google-explicit-constructor)
  }

  Vec v_{};
};

}  // namespace dfly
