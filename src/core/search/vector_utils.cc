// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/vector_utils.h"

#include <bit>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>

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

float HalfToFloat(uint16_t h) {
  uint32_t sign = static_cast<uint32_t>(h & 0x8000) << 16;
  uint32_t exp = (h >> 10) & 0x1F;
  uint32_t mant = h & 0x3FF;
  uint32_t bits;
  if (exp == 0) {
    if (mant == 0) {
      bits = sign;  // +/- zero
    } else {
      // Normalize the subnormal: shift the mantissa left until bit 10 is set. `mant` is in
      // [1, 0x3FF], so countl_zero is in [22, 31] and the shift in [1, 10].
      int shift = std::countl_zero(mant) - 21;
      exp = (127 - 15 + 1) - shift;
      mant = (mant << shift) & 0x3FF;
      bits = sign | (exp << 23) | (mant << 13);
    }
  } else if (exp == 0x1F) {
    bits = sign | 0x7F800000u | (mant << 13);  // inf / nan
  } else {
    bits = sign | ((exp + 112) << 23) | (mant << 13);  // rebias 127 - 15 = 112
  }
  float out;
  memcpy(&out, &bits, sizeof(out));
  return out;
}

float Bf16ToFloat(uint16_t b) {
  uint32_t bits = static_cast<uint32_t>(b) << 16;
  float out;
  memcpy(&out, &bits, sizeof(out));
  return out;
}

namespace {

uint16_t LoadU16(const void* base, size_t i) {
  uint16_t v;
  memcpy(&v, static_cast<const char*>(base) + i * sizeof(uint16_t), sizeof(v));
  return v;
}

// Reads element i of type Elem via memcpy — byte-safe for unaligned native-width blobs (e.g.
// borrowed keyspace storage) — and widens it to Acc.
template <class Elem, class AccT> struct PodReader {
  using Acc = AccT;
  static Acc Get(const void* p, size_t i) {
    Elem v;
    memcpy(&v, static_cast<const char*>(p) + i * sizeof(Elem), sizeof(v));
    return static_cast<Acc>(v);
  }
};

// Reads a 16-bit half/bfloat element and widens it to float via the given decoder.
template <float (*Decode)(uint16_t)> struct HalfReader {
  using Acc = float;
  static float Get(const void* p, size_t i) {
    return Decode(LoadU16(p, i));
  }
};

using ReaderF32 = PodReader<float, float>;
using ReaderF64 = PodReader<double, double>;
using ReaderI8 = PodReader<int8_t, float>;
using ReaderU8 = PodReader<uint8_t, float>;
using ReaderF16 = HalfReader<HalfToFloat>;
using ReaderBF16 = HalfReader<Bf16ToFloat>;

template <class R> float L2Typed(const void* u, const void* v, size_t dims) {
  using Acc = typename R::Acc;
  Acc sum = 0;
  for (size_t i = 0; i < dims; i++) {
    Acc d = R::Get(u, i) - R::Get(v, i);
    sum += d * d;
  }
  return static_cast<float>(std::sqrt(sum));
}

template <class R> float IPTyped(const void* u, const void* v, size_t dims) {
  using Acc = typename R::Acc;
  Acc sum = 0;
  for (size_t i = 0; i < dims; i++)
    sum += R::Get(u, i) * R::Get(v, i);
  return static_cast<float>(Acc(1) - sum);
}

template <class R> float CosineTyped(const void* u, const void* v, size_t dims) {
  using Acc = typename R::Acc;
  Acc uv = 0, uu = 0, vv = 0;
  for (size_t i = 0; i < dims; i++) {
    Acc a = R::Get(u, i), b = R::Get(v, i);
    uv += a * b;
    uu += a * a;
    vv += b * b;
  }
  if (Acc denom = uu * vv; denom != Acc(0))
    return static_cast<float>(Acc(1) - uv / std::sqrt(denom));
  return 0.0f;
}

template <class R>
float DistByMetric(const void* u, const void* v, size_t dims, VectorSimilarity sim) {
  switch (sim) {
    case VectorSimilarity::L2:
      return L2Typed<R>(u, v, dims);
    case VectorSimilarity::IP:
      return IPTyped<R>(u, v, dims);
    case VectorSimilarity::COSINE:
      return CosineTyped<R>(u, v, dims);
  }
  return 0.0f;
}

}  // namespace

float VectorDistance(const void* u, const void* v, size_t dims, VectorSimilarity sim,
                     VectorDataType dt) {
  switch (dt) {
    case VectorDataType::FLOAT32:
      return DistByMetric<ReaderF32>(u, v, dims, sim);
    case VectorDataType::FLOAT64:
      return DistByMetric<ReaderF64>(u, v, dims, sim);
    case VectorDataType::FLOAT16:
      return DistByMetric<ReaderF16>(u, v, dims, sim);
    case VectorDataType::BFLOAT16:
      return DistByMetric<ReaderBF16>(u, v, dims, sim);
    case VectorDataType::INT8:
      return DistByMetric<ReaderI8>(u, v, dims, sim);
    case VectorDataType::UINT8:
      return DistByMetric<ReaderU8>(u, v, dims, sim);
  }
  return 0.0f;
}

std::string_view VectorSimilarityToString(VectorSimilarity sim) {
  switch (sim) {
    case VectorSimilarity::L2:
      return "L2";
    case VectorSimilarity::IP:
      return "IP";
    case VectorSimilarity::COSINE:
      return "COSINE";
  }
  DCHECK(false) << "Unhandled VectorSimilarity enum value: " << static_cast<int>(sim);
  return "L2";
}

std::string_view VectorDataTypeToString(VectorDataType dt) {
  switch (dt) {
    case VectorDataType::FLOAT32:
      return "FLOAT32";
    case VectorDataType::FLOAT64:
      return "FLOAT64";
    case VectorDataType::FLOAT16:
      return "FLOAT16";
    case VectorDataType::BFLOAT16:
      return "BFLOAT16";
    case VectorDataType::INT8:
      return "INT8";
    case VectorDataType::UINT8:
      return "UINT8";
  }
  DCHECK(false) << "Unhandled VectorDataType enum value: " << static_cast<int>(dt);
  return "FLOAT32";
}

std::optional<VectorDataType> ParseVectorDataType(std::string_view name) {
  if (name == "FLOAT32")
    return VectorDataType::FLOAT32;
  if (name == "FLOAT64")
    return VectorDataType::FLOAT64;
  if (name == "FLOAT16")
    return VectorDataType::FLOAT16;
  if (name == "BFLOAT16")
    return VectorDataType::BFLOAT16;
  if (name == "INT8")
    return VectorDataType::INT8;
  if (name == "UINT8")
    return VectorDataType::UINT8;
  return std::nullopt;
}

float DistanceToSimilarity(float distance, VectorSimilarity sim) {
  switch (sim) {
    case VectorSimilarity::L2:
      return 1.0f / (1.0f + distance * distance);
    case VectorSimilarity::IP:
    case VectorSimilarity::COSINE:
      return (2.0f - distance) / 2.0f;
  }
  DCHECK(false) << "Unhandled VectorSimilarity enum value: " << static_cast<int>(sim);
  return 0.0f;
}

void InitSimSIMD() {
#if defined(WITH_SIMSIMD)
  (void)simsimd_capabilities();
#endif
}

}  // namespace dfly::search
