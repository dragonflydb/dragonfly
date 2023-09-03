// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/vector.h"

#include <cmath>
#include <memory>

#include "base/logging.h"

namespace dfly::search {

using namespace std;

namespace {

// Euclidean vector distance: sqrt( sum: (u[i] - v[i])^2  )
__attribute__((optimize("fast-math"))) float L2Distance(const float* u, const float* v,
                                                        size_t dims) {
  float sum = 0;
  for (size_t i = 0; i < dims; i++)
    sum += (u[i] - v[i]) * (u[i] - v[i]);
  return sqrt(sum);
}

__attribute__((optimize("fast-math"))) float CosineDistance(const float* u, const float* v,
                                                            size_t dims) {
  float sum_uv = 0, sum_uu = 0, sum_vv = 0;
  for (size_t i = 0; i < dims; i++) {
    sum_uv += u[i] * v[i];
    sum_uu += u[i] * u[i];
    sum_vv += v[i] * v[i];
  }

  if (float denom = sum_uu * sum_vv; denom != 0.0f)
    return sum_uv / sqrt(denom);
  return 0.0f;
}

}  // namespace

OwnedFtVector BytesToFtVector(string_view value) {
  DCHECK_EQ(value.size() % sizeof(float), 0u) << value.size();

  size_t size = value.size() / sizeof(float);
  auto out = make_unique<float[]>(size);
  memcpy(out.get(), value.data(), size * sizeof(float));

  return {std::move(out), size};
}

float VectorDistance(const float* u, const float* v, size_t dims, VectorSimilarity sim) {
  switch (sim) {
    case VectorSimilarity::L2:
      return L2Distance(u, v, dims);
    case VectorSimilarity::CONSINE:
      return CosineDistance(u, v, dims);
  };
  return 0.0f;
}

}  // namespace dfly::search
