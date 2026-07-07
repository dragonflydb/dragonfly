// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <array>
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

// Wide (>256-bit) instances return/pass vectors by value in unoptimized builds, which GCC flags
// with -Wpsabi (an AVX512 ABI note). SimdOp is header-only and always inlined in optimized builds,
// so no real ABI boundary exists; silence the false positive on GCC (clang does not emit it).
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpsabi"
#endif

// SimdOp<T, N> wraps N consecutive uint64_t lanes behind the GCC/Clang
// vector-extension type. Compare ops produce another SimdOp whose lanes are
// all-ones or all-zeros, which GetMSBs() then compresses to a scalar.
//
// vector_size requires a power-of-two lane count, so the storage vector is widened to
// kVecLanes = bit_ceil(N); the padding lanes are loaded as zero and masked out of
// GetMSBs(). For power-of-two N there is no padding and codegen is unchanged.
template <class T, std::size_t N> class SimdOp {
  static_assert(std::is_integral_v<T>, "lane type must be integral");
  static_assert(sizeof(T) == 8, "only 64-bit lanes are supported today");
  static_assert(N >= 2 && N <= 64 && N % 2 == 0, "N must be an even lane count in [2, 64]");

  static constexpr std::size_t kVecLanes = std::bit_ceil(N);
  using Vec __attribute__((vector_size(sizeof(T) * kVecLanes))) = T;

 public:
  // Up to 32 lanes fit a uint32_t mask; 64 lanes need 64 bits.
  using BitsType = std::conditional_t<(N > 32), std::uint64_t, std::uint32_t>;
  static constexpr std::size_t kLanes = N;

  constexpr SimdOp() noexcept = default;

  // Filling via `Vec{} + value` lowers to vpbroadcast on AVX2 / dup on
  // NEON; a per-lane scalar loop pessimizes to vpinsrq + vperm2i128.
  static constexpr SimdOp Fill(T value) noexcept {
    return Vec{} + value;
  }

  static SimdOp Load(const T* ptr) noexcept {
    if constexpr (N == kVecLanes) {
      Vec v;
      std::memcpy(&v, ptr, sizeof(Vec));
      return v;
    } else {
      Vec v{};  // zero the padding lanes so they never set a bit in GetMSBs
      std::memcpy(&v, ptr, sizeof(T) * N);
      return v;
    }
  }

  constexpr SimdOp operator&(const SimdOp& o) const noexcept {
    return v_ & o.v_;
  }

  constexpr SimdOp operator|(const SimdOp& o) const noexcept {
    return v_ | o.v_;
  }

  constexpr SimdOp operator>>(unsigned shift) const noexcept {
    return v_ >> shift;
  }

  constexpr SimdOp operator~() const noexcept {
    return ~v_;
  }

  constexpr SimdOp operator==(const SimdOp& o) const noexcept {  // NOLINT
    return Vec(v_ == o.v_);
  }

  constexpr SimdOp operator==(T value) const noexcept {  // NOLINT
    return Vec(v_ == (Vec{} + value));
  }

  // Packs each lane's sign bit (bit 63) into a bitmask (LSB = lane 0), x86 movemask semantics on
  // all ISAs. Per-group masks are combined with a fold expression (unrolled, independent groups);
  // the trailing mask strips padding lanes when N is not a power of two.
  BitsType GetMSBs() const noexcept {
    // Hand-written per-ISA movemask: no portable formulation lowers to a single movemask, and
    // every alternative measured ~5% slower on OAHSet's hot path.
    BitsType bits = 0;
#if defined(__AVX2__)
    if constexpr (kVecLanes == 2) {
      __m128i raw;
      std::memcpy(&raw, &v_, sizeof(raw));
      bits = static_cast<BitsType>(_mm_movemask_pd(_mm_castsi128_pd(raw)));
    } else {
      [&]<std::size_t... G>(std::index_sequence<G...>) {
        ((bits |= static_cast<BitsType>(Movemask256<G>()) << (4 * G)), ...);
      }
      (std::make_index_sequence<kVecLanes / 4>{});
    }
#elif defined(__ARM_NEON)
    // NEON has no movemask; vshrn_n_u64 narrows a 128-bit half to 2 sign bits per group.
    [&]<std::size_t... H>(std::index_sequence<H...>) {
      ((bits |= static_cast<BitsType>(NeonPack2<H>()) << (2 * H)), ...);
    }
    (std::make_index_sequence<kVecLanes / 2>{});
#else
    [&]<std::size_t... I>(std::index_sequence<I...>) {
      ((bits |= static_cast<BitsType>(static_cast<std::make_unsigned_t<T> >(v_[I]) >> 63) << I),
       ...);
    }
    (std::make_index_sequence<kVecLanes>{});
#endif
    if constexpr (N != kVecLanes) {
      bits &= static_cast<BitsType>((BitsType{1} << N) - 1);  // drop padding lanes
    }
    return bits;
  }

 private:
#if defined(__AVX2__)
  // movemask of the G-th 256-bit group (lanes [4G, 4G+4)); the offset is a compile-time constant.
  template <std::size_t G> int Movemask256() const noexcept {
    __m256i raw;
    std::memcpy(&raw, reinterpret_cast<const char*>(&v_) + 32 * G, sizeof(raw));
    return _mm256_movemask_pd(_mm256_castsi256_pd(raw));
  }
#elif defined(__ARM_NEON)
  // 2-bit sign mask of the H-th 128-bit group (lanes [2H, 2H+2)).
  template <std::size_t H> unsigned NeonPack2() const noexcept {
    uint64x2_t half;
    std::memcpy(&half, reinterpret_cast<const char*>(&v_) + 16 * H, sizeof(half));
    uint32x2_t narrow = vshrn_n_u64(half, 32);
    std::uint64_t packed;
    std::memcpy(&packed, &narrow, sizeof(packed));
    return static_cast<unsigned>(((packed >> 31) & 1u) | ((packed >> 62) & 2u));
  }
#endif

  constexpr SimdOp(Vec v) noexcept : v_(v) {  // NOLINT(google-explicit-constructor)
  }

  Vec v_{};
};

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic pop
#endif

}  // namespace dfly
