// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstddef>

namespace cmn {

// Buffer size that is sufficient for any output of FormatDoubleShortest, including the
// terminating null character. The longest output is "-0.00000" followed by 17 digits.
constexpr size_t kMaxDoubleStrLen = 26;

// Writes the shortest decimal representation of d that round-trips back to d.
// Uses fixed notation for decimal exponents in [-6, 21) and scientific notation otherwise,
// e.g. "0.1", "1e-7", "1.5e21". If exp_plus_sign is true, positive exponents are prefixed with
// '+' ("1.5e+21"). -0 is written as "0", infinities as "inf"/"-inf" and NaN as "nan".
// dest must have room for at least kMaxDoubleStrLen chars. The output is null-terminated.
// Returns the length of the output, excluding the null character.
size_t FormatDoubleShortest(double d, bool exp_plus_sign, char* dest);

}  // namespace cmn
