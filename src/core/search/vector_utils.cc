// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/vector_utils.h"

#include <cmath>
#include <iomanip>
#include <memory>

#include "base/logging.h"

namespace dfly::search {

using namespace std;

namespace {

#ifdef USE_SIMSIMD
// Enable dynamic dispatch for automatic backend selection at runtime
#define SIMSIMD_DYNAMIC_DISPATCH 1
// Native float16/bfloat16 support is controlled via CMake option SIMSIMD_NATIVE_F16
// Value is passed from CMake to both the SimSIMD library build and compile definitions
// Default: OFF (disabled) for maximum compatibility across different compilers/platforms
#include <simsimd/simsimd.h>

// Initialize dynamic dispatch on first use
static bool InitializeSimSIMD() {
  static bool initialized = false;
  if (!initialized) {
    LOG(INFO) << "=== SimSIMD Dynamic Dispatch Initialization ===";

    // Initialize and get capabilities
    simsimd_capability_t caps = simsimd_capabilities();
    LOG(INFO) << "simsimd_capabilities() returned: 0x" << std::hex << caps << std::dec;

    // Log dynamic dispatch status
    LOG(INFO) << "Dynamic dispatch enabled: " << (simsimd_uses_dynamic_dispatch() ? "YES" : "NO");

    // Log x86 backend capabilities
    LOG(INFO) << "=== x86/x64 Backend Detection ===";
    LOG(INFO) << "  Haswell (AVX2):       " << (simsimd_uses_haswell() ? "YES" : "NO");
    LOG(INFO) << "  Skylake (AVX512F):    " << (simsimd_uses_skylake() ? "YES" : "NO");
    LOG(INFO) << "  Ice Lake (AVX512BW):  " << (simsimd_uses_ice() ? "YES" : "NO");
    LOG(INFO) << "  Genoa (AVX512BF16):   " << (simsimd_uses_genoa() ? "YES" : "NO");
    LOG(INFO) << "  Sapphire Rapids:      " << (simsimd_uses_sapphire() ? "YES" : "NO");
    LOG(INFO) << "  Turin (AVX512VNNI):   " << (simsimd_uses_turin() ? "YES" : "NO");
    LOG(INFO) << "  Sierra (AVX10):       " << (simsimd_uses_sierra() ? "YES" : "NO");

    // Log ARM backend capabilities
    LOG(INFO) << "=== ARM Backend Detection ===";
    LOG(INFO) << "  NEON:                 " << (simsimd_uses_neon() ? "YES" : "NO");
    LOG(INFO) << "  NEON F16:             " << (simsimd_uses_neon_f16() ? "YES" : "NO");
    LOG(INFO) << "  NEON BF16:            " << (simsimd_uses_neon_bf16() ? "YES" : "NO");
    LOG(INFO) << "  NEON I8:              " << (simsimd_uses_neon_i8() ? "YES" : "NO");
    LOG(INFO) << "  SVE:                  " << (simsimd_uses_sve() ? "YES" : "NO");
    LOG(INFO) << "  SVE F16:              " << (simsimd_uses_sve_f16() ? "YES" : "NO");
    LOG(INFO) << "  SVE BF16:             " << (simsimd_uses_sve_bf16() ? "YES" : "NO");
    LOG(INFO) << "  SVE I8:               " << (simsimd_uses_sve_i8() ? "YES" : "NO");

    // Log compilation options
    LOG(INFO) << "=== SimSIMD Compilation Options ===";
#if defined(SIMSIMD_NATIVE_F16)
    LOG(INFO) << "  SIMSIMD_NATIVE_F16:   " << SIMSIMD_NATIVE_F16;
#else
    LOG(INFO) << "  SIMSIMD_NATIVE_F16:   NOT_DEFINED";
#endif
#if defined(SIMSIMD_NATIVE_BF16)
    LOG(INFO) << "  SIMSIMD_NATIVE_BF16:  " << SIMSIMD_NATIVE_BF16;
#else
    LOG(INFO) << "  SIMSIMD_NATIVE_BF16:  NOT_DEFINED";
#endif
#if defined(SIMSIMD_DYNAMIC_DISPATCH)
    LOG(INFO) << "  SIMSIMD_DYNAMIC_DISPATCH: " << SIMSIMD_DYNAMIC_DISPATCH;
#else
    LOG(INFO) << "  SIMSIMD_DYNAMIC_DISPATCH: NOT_DEFINED";
#endif

    LOG(INFO) << "=== Function Usage Information ===";
    LOG(INFO) << "Functions simsimd_l2_f32, simsimd_dot_f32, simsimd_cos_f32 will use";
    LOG(INFO) << "the best available backend detected above for f32 operations.";
    LOG(INFO) << "===============================================";

    initialized = true;
  }
  return true;
}

// Ensure initialization happens
static const bool simsimd_init_guard = InitializeSimSIMD();
#endif

#if defined(__GNUC__) && !defined(__clang__)
#define FAST_MATH __attribute__((optimize("fast-math")))
#else
#define FAST_MATH
#endif

// Euclidean vector distance: sqrt( sum: (u[i] - v[i])^2  )
FAST_MATH float L2Distance(const float* u, const float* v, size_t dims) {
#ifdef USE_SIMSIMD
  simsimd_distance_t distance = 0;
  simsimd_l2_f32(u, v, dims, &distance);
  return static_cast<float>(distance);
#else
  float sum = 0;
  for (size_t i = 0; i < dims; i++)
    sum += (u[i] - v[i]) * (u[i] - v[i]);
  return sqrt(sum);
#endif
}

// Inner product distance: 1 - dot_product(u, v)
// For normalized vectors, this is equivalent to cosine distance
FAST_MATH float IPDistance(const float* u, const float* v, size_t dims) {
#ifdef USE_SIMSIMD
  // Use SimSIMD dot product and convert to inner product distance: 1 - dot(u, v).
  simsimd_distance_t dot = 0;
  simsimd_dot_f32(u, v, dims, &dot);
  return 1.0f - static_cast<float>(dot);
#else
  float sum_uv = 0;
  for (size_t i = 0; i < dims; i++)
    sum_uv += u[i] * v[i];
  return 1.0f - sum_uv;
#endif
}

FAST_MATH float CosineDistance(const float* u, const float* v, size_t dims) {
#ifdef USE_SIMSIMD
  simsimd_distance_t distance = 0;
  simsimd_cos_f32(u, v, dims, &distance);
  return static_cast<float>(distance);
#else
  float sum_uv = 0, sum_uu = 0, sum_vv = 0;
  for (size_t i = 0; i < dims; i++) {
    sum_uv += u[i] * v[i];
    sum_uu += u[i] * u[i];
    sum_vv += v[i] * v[i];
  }

  if (float denom = sum_uu * sum_vv; denom != 0.0f)
    return 1 - sum_uv / sqrt(denom);
  return 0.0f;
#endif
}

OwnedFtVector ConvertToFtVector(string_view value) {
  // Value cannot be casted directly as it might be not aligned as a float (4 bytes).
  // Misaligned memory access is UB.
  size_t size = value.size() / sizeof(float);
  auto out = make_unique<float[]>(size);
  memcpy(out.get(), value.data(), size * sizeof(float));

  return OwnedFtVector{std::move(out), size};
}

}  // namespace

OwnedFtVector BytesToFtVector(string_view value) {
  DCHECK_EQ(value.size() % sizeof(float), 0u) << value.size();
  return ConvertToFtVector(value);
}

std::optional<OwnedFtVector> BytesToFtVectorSafe(string_view value) {
  if (value.size() % sizeof(float)) {
    return std::nullopt;
  }
  return ConvertToFtVector(value);
}

float VectorDistance(const float* u, const float* v, size_t dims, VectorSimilarity sim) {
  switch (sim) {
    case VectorSimilarity::L2:
      return L2Distance(u, v, dims);
    case VectorSimilarity::IP:
      return IPDistance(u, v, dims);
    case VectorSimilarity::COSINE:
      return CosineDistance(u, v, dims);
  };
  return 0.0f;
}

}  // namespace dfly::search
