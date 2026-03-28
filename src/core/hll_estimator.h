// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstdint>
#include <cstring>

namespace dfly {
namespace hll {

constexpr unsigned kRegisterLen = 1024;
constexpr uint32_t kRegisterMask = kRegisterLen - 1;
constexpr unsigned kRegisterBits = 10;
constexpr unsigned kRankBits = 32 - kRegisterBits;

inline void UpdateRegister(uint32_t h, uint8_t* registers) {
  uint32_t index = h & kRegisterMask;
  // Use upper bits for rank calculation, ensuring it's never zero
  uint32_t w = (h >> kRegisterBits) | (1u << kRankBits);
  uint8_t rank = std::countr_zero(w) + 1;
  registers[index] = std::max(registers[index], rank);
}

inline double EstimateCardinality(const uint8_t* registers) {
  double sum = 0.0;
  int zero_registers = 0;
  for (unsigned i = 0; i < kRegisterLen; ++i) {
    if (registers[i] == 0) {
      zero_registers++;
    }
    sum += 1.0 / (1 << registers[i]);
  }

  // alpha_m * m^2 where m = kRegisterLen
  // Constants from original HyperLogLog paper (Flajolet et al.)
  constexpr double kAlphaInf = 0.7213;
  constexpr double kAlphaCorrection = 1.079;
  constexpr double kM = static_cast<double>(kRegisterLen);
  constexpr double kAlphaM2 = (kAlphaInf / (1.0 + kAlphaCorrection / kM)) * (kM * kM);
  double estimate = kAlphaM2 / sum;

  // Small range correction
  constexpr double kSmallRangeThreshold = 2.5 * kM;
  if (estimate <= kSmallRangeThreshold && zero_registers > 0) {
    estimate = kM * std::log(kM / zero_registers);
  }
  return estimate;
}

// Merge src registers into dst (element-wise max).
inline void MergeRegisters(const uint8_t* src, uint8_t* dst) {
  for (unsigned i = 0; i < kRegisterLen; ++i) {
    dst[i] = std::max(dst[i], src[i]);
  }
}

}  // namespace hll
}  // namespace dfly
