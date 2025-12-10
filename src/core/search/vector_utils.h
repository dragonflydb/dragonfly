// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "core/search/base.h"

namespace dfly::search {

// Initializes SimSIMD runtime if dynamic dispatch is enabled.
void InitSimSIMD();

OwnedFtVector BytesToFtVector(std::string_view value);

// Returns std::nullopt if value can not be converted to the vector
// TODO: Remove unsafe version
std::optional<OwnedFtVector> BytesToFtVectorSafe(std::string_view value);

float L2Distance(const float* u, const float* v, size_t dims);
float IPDistance(const float* u, const float* v, size_t dims);
float VectorDistance(const float* u, const float* v, size_t dims, VectorSimilarity sim);

}  // namespace dfly::search
