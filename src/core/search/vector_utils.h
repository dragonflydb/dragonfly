// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "core/search/base.h"

namespace dfly::search {

OwnedFtVector BytesToFtVector(std::string_view value);

float VectorDistance(const float* u, const float* v, size_t dims, VectorSimilarity sim);

}  // namespace dfly::search
