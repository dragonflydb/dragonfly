// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstdint>

namespace dfly {

// Lightweight HyperLogLog cardinality estimator with 1024 registers.
class HllEstimator {
 public:
  static constexpr unsigned kRegisterLen = 1024;

  void Add(uint32_t h) {
    constexpr uint32_t kRegisterMask = kRegisterLen - 1;
    constexpr unsigned kRegisterBits = 10;
    constexpr unsigned kRankBits = 32 - kRegisterBits;

    uint32_t index = h & kRegisterMask;
    // Use upper bits for rank calculation, ensuring it's never zero
    uint32_t w = (h >> kRegisterBits) | (1u << kRankBits);
    uint8_t rank = std::countr_zero(w) + 1;
    registers_[index] = std::max(registers_[index], rank);
  }

  double EstimateCardinality() const {
    double sum = 0.0;
    int zero_registers = 0;
    for (unsigned i = 0; i < kRegisterLen; ++i) {
      if (registers_[i] == 0) {
        zero_registers++;
      }
      sum += 1.0 / (1 << registers_[i]);
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

 private:
  std::array<uint8_t, kRegisterLen> registers_{};
};

}  // namespace dfly
