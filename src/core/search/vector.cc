// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/vector.h"

#include <cmath>
#include <cstddef>

#include "glog/logging.h"

namespace dfly::search {

using namespace std;

// Euclidean vector distance: sqrt( sum: (u[i] - v[i])^2  )
__attribute__((optimize("fast-math"))) float VectorDistance(const FtVector& u, const FtVector& v) {
  DCHECK_EQ(u.size(), v.size());
  float sum = 0;
  for (size_t i = 0; i < u.size(); i++)
    sum += (u[i] - v[i]) * (u[i] - v[i]);
  return sqrt(sum);
}

}  // namespace dfly::search
