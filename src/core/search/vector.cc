// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/vector.h"

#include <cmath>
#include <memory>

#include "base/logging.h"

namespace dfly::search {

using namespace std;

FtVector BytesToFtVector(string_view value) {
  DCHECK_EQ(value.size() % sizeof(float), 0u);
  FtVector out(value.size() / sizeof(float));

  // Create copy for aligned access
  unique_ptr<float[]> float_ptr = make_unique<float[]>(out.size());
  memcpy(float_ptr.get(), value.data(), value.size());

  for (size_t i = 0; i < out.size(); i++)
    out[i] = float_ptr[i];
  return out;
}

// Euclidean vector distance: sqrt( sum: (u[i] - v[i])^2  )
__attribute__((optimize("fast-math"))) float VectorDistance(const FtVector& u, const FtVector& v) {
  DCHECK_EQ(u.size(), v.size());
  float sum = 0;
  for (size_t i = 0; i < u.size(); i++)
    sum += (u[i] - v[i]) * (u[i] - v[i]);
  return sqrt(sum);
}

}  // namespace dfly::search
