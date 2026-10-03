// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "common/dtoa.h"

#include <absl/strings/numbers.h>
#include <dragonbox/dragonbox.h>

#include <cmath>
#include <cstdint>
#include <cstring>

namespace cmn {

namespace {

// Decimal exponent range [kFixedLow, kFixedHigh) that is printed using fixed notation.
constexpr int kFixedLow = -6;
constexpr int kFixedHigh = 21;

// Computes the shortest decimal representation that round-trips to |d|:
// |d| == <digits> * 10^exp10, where digits has no trailing zeros.
// Writes the digits (at most 17, null-terminated) and returns their count.
// d must be finite and non-zero.
unsigned ShortestDigits(double d, char* digits, int32_t* exp10) {
  uint64_t significand;
  double abs_d = std::fabs(d);

  // Fast path for integers in [1, 2^53), they are represented exactly.
  if (abs_d < 9007199254740992.0 && abs_d == std::floor(abs_d)) {
    significand = abs_d;
    *exp10 = 0;
    while (significand % 10 == 0) {
      significand /= 10;
      ++*exp10;
    }
  } else {
    auto dec = jkj::dragonbox::to_decimal(d, jkj::dragonbox::policy::sign::ignore);
    significand = dec.significand;
    *exp10 = dec.exponent;
  }

  return absl::numbers_internal::FastIntToBuffer(significand, digits) - digits;
}

}  // namespace

size_t FormatDoubleShortest(double d, bool exp_plus_sign, char* dest) {
  char* next = dest;

  if (std::isnan(d)) {
    memcpy(next, "nan", 3);
    next += 3;
  } else if (d == 0) {  // Both +0 and -0.
    *next++ = '0';
  } else {
    if (std::signbit(d))
      *next++ = '-';

    if (std::isinf(d)) {
      memcpy(next, "inf", 3);
      next += 3;
    } else {
      char digits[absl::numbers_internal::kFastToBufferSize];
      int32_t exp10;
      int len = ShortestDigits(d, digits, &exp10);

      // d = 0.<digits> * 10^decimal_point.
      int decimal_point = len + exp10;
      int exponent = decimal_point - 1;

      if (exponent >= kFixedLow && exponent < kFixedHigh) {
        if (decimal_point <= 0) {  // 0.000ddd
          *next++ = '0';
          *next++ = '.';
          memset(next, '0', -decimal_point);
          next += -decimal_point;
          memcpy(next, digits, len);
          next += len;
        } else if (decimal_point >= len) {  // ddd000
          memcpy(next, digits, len);
          next += len;
          memset(next, '0', decimal_point - len);
          next += decimal_point - len;
        } else {  // dd.ddd
          memcpy(next, digits, decimal_point);
          next += decimal_point;
          *next++ = '.';
          memcpy(next, digits + decimal_point, len - decimal_point);
          next += len - decimal_point;
        }
      } else {  // d.ddde[+-]xx
        *next++ = digits[0];
        if (len > 1) {
          *next++ = '.';
          memcpy(next, digits + 1, len - 1);
          next += len - 1;
        }
        *next++ = 'e';
        if (exponent < 0) {
          *next++ = '-';
          exponent = -exponent;
        } else if (exp_plus_sign) {
          *next++ = '+';
        }

        // |exponent| <= 324.
        if (exponent >= 100) {
          *next++ = '0' + exponent / 100;
          exponent %= 100;
          *next++ = '0' + exponent / 10;
        } else if (exponent >= 10) {
          *next++ = '0' + exponent / 10;
        }
        *next++ = '0' + exponent % 10;
      }
    }
  }

  *next = '\0';
  return next - dest;
}

}  // namespace cmn
