// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once
#if defined(__aarch64__)
#include "base/sse2neon.h"
#elif defined(__riscv) || defined(__riscv__)
#include "base/sse2rvv.h"
#elif defined(__s390x__)
#include <vecintrin.h>
#else
#include <emmintrin.h>
#include <tmmintrin.h>
#endif

namespace dfly {

#ifndef __s390x__
inline __m128i mm_loadu_si128(const __m128i* ptr) {
  return _mm_loadu_si128(ptr);
}
#endif

}  // namespace dfly
