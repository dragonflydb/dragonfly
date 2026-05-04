// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/vector_utils.h"

#include <cmath>
#include <memory>

#include "base/logging.h"

namespace dfly::search {

using namespace std;

namespace {

#ifdef WITH_SIMSIMD
#include <simsimd/simsimd.h>
#endif

#if defined(__GNUC__) && !defined(__clang__)
#define FAST_MATH __attribute__((optimize("fast-math")))
#else
#define FAST_MATH
#endif

OwnedFtVector ConvertToFtVector(string_view value) {
  // Value cannot be casted directly as it might be not aligned as a float (4 bytes).
  // Misaligned memory access is UB.
  size_t size = value.size() / sizeof(float);
  auto out = make_unique<float[]>(size);
  memcpy(out.get(), value.data(), size * sizeof(float));

  return OwnedFtVector{std::move(out), size};
}

}  // namespace

// Euclidean vector distance: sqrt( sum: (u[i] - v[i])^2  )
FAST_MATH float L2Distance(const float* u, const float* v, size_t dims) {
#ifdef WITH_SIMSIMD
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
#ifdef WITH_SIMSIMD
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

// Cosine distance: 1 - (dot_product(u, v) / (||u|| * ||v||))
FAST_MATH float CosineDistance(const float* u, const float* v, size_t dims) {
#ifdef WITH_SIMSIMD
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

// float32 × float32 dot product
FAST_MATH float DotProductF32(const float* u, const float* v, size_t dims) {
#ifdef WITH_SIMSIMD
  simsimd_distance_t result = 0;
  simsimd_dot_f32(u, v, dims, &result);
  return static_cast<float>(result);
#else
  float dot = 0.0f;
  for (size_t i = 0; i < dims; i++)
    dot += u[i] * v[i];
  return dot;
#endif
}

// float32 × int8 mixed dot product.
// SimSIMD has no native f32×i8 kerne so we implement it manually.
FAST_MATH float DotProductF32I8(const float* u, const int8_t* v, size_t dims) {
  float dot = 0.0f;
  for (size_t i = 0; i < dims; i++)
    dot += u[i] * static_cast<float>(v[i]);
  return dot;
}

// int8 × int8 dot product
FAST_MATH float DotProductI8(const int8_t* u, const int8_t* v, size_t dims) {
#ifdef WITH_SIMSIMD
  simsimd_distance_t result = 0;
  simsimd_dot_i8(u, v, dims, &result);
  return static_cast<float>(result);
#else
  float dot = 0.0f;
  for (size_t i = 0; i < dims; i++)
    dot += static_cast<float>(u[i]) * static_cast<float>(v[i]);
  return dot;
#endif
}

void InitSimSIMD() {
#if defined(WITH_SIMSIMD)
  (void)simsimd_capabilities();
#endif
}

}  // namespace dfly::search
