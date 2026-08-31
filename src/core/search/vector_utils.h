// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstddef>
#include <vector>

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
float CosineDistance(const float* u, const float* v, size_t dims);
float VectorDistance(const float* u, const float* v, size_t dims, VectorSimilarity sim);

// Distance between two native-width vector blobs of the given dtype. Elements are widened to
// float (double for FLOAT64) before the metric is applied, matching the reference engine.
float VectorDistance(const void* u, const void* v, size_t dims, VectorSimilarity sim,
                     VectorDataType dt);

// Widen a half-precision (h) or bfloat16 (b) element, given as its raw 16-bit pattern, to float.
float HalfToFloat(uint16_t h);
float Bf16ToFloat(uint16_t b);

// Narrow a float to half precision / bfloat16 (round to nearest even). Used to encode JSON numbers.
uint16_t FloatToHalf(float f);
uint16_t FloatToBf16(float f);

// Builds a `dim`-element blob with every element set to 1.0 encoded in the given dtype.
std::vector<std::byte> EncodeOnesVector(size_t dim, VectorDataType dt);

std::string_view VectorSimilarityToString(VectorSimilarity sim);

std::string_view VectorDataTypeToString(VectorDataType dt);

// Parses a vector TYPE token (e.g. "INT8"), which must be uppercase; std::nullopt if unsupported.
std::optional<VectorDataType> ParseVectorDataType(std::string_view name);

// L2: 1/(1+d*d) -- knn_dist is raw L2 here, so squaring matches the 1/(1+L2_sq) similarity.
// IP/COSINE: (2-d)/2 -- knn_dist is (1 - score) for both metrics.
float DistanceToSimilarity(float distance, VectorSimilarity sim);

}  // namespace dfly::search
