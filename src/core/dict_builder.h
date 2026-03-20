// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/types/span.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>

namespace dfly {

// Estimates compressibility by counting unique 6-byte d-mers using HyperLogLog.
// data_pieces: spans of raw data (e.g., one per QList node).
// step: sampling stride (1 = every offset, higher = faster but less accurate).
// Returns a value in [0, 1] where 0 means very compressible, and 1 means incompressible.
double EstimateCompressibility(absl::Span<const std::pair<const uint8_t*, size_t>> data_pieces,
                               unsigned step);

// Trains a compression dictionary from a collection of sample data.
//
// Arguments:
//   data_pieces:  Input data sources (spans of bytes) to extract dictionary segments from.
//   dict_size:    The maximum target size of the resulting dictionary in bytes.
//   segment_size: The size of continuous byte segments chosen and appended per iteration.
//
// Returns a raw string containing the trained dictionary up to `dict_size` bytes.
std::string TrainDictionary(absl::Span<const std::pair<const uint8_t*, size_t>> data_pieces,
                            size_t dict_size = 4096, size_t segment_size = 256);

}  // namespace dfly
