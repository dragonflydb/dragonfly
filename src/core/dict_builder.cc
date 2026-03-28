// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/dict_builder.h"

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstring>
#include <memory>
#include <vector>

#include "base/logging.h"
#include "core/hll_estimator.h"

namespace dfly {

using namespace std;
namespace {

constexpr unsigned kDmerLength = 6;

// Fast hash for 6-byte d-mers. Uses a simple multiplicative hash.
inline uint32_t HashDmer(const uint8_t* data) {
  uint64_t val = 0;
  memcpy(&val, data, 6);

  // ZSTD_hash6 algorithm
  constexpr uint64_t kPrime6Bytes = 227718039650203ULL;
  uint64_t hash64 = ((val << 16) * kPrime6Bytes) >> 32;
  return static_cast<uint32_t>(hash64);
}

uint32_t CalculateFreqTableSize(absl::Span<const std::pair<const uint8_t*, size_t>> data_pieces) {
  size_t total_input_size = 0;
  for (const auto& [data, sz] : data_pieces) {
    total_input_size += sz;
  }
  size_t target_size = std::max<size_t>(1024, total_input_size);
  return std::bit_ceil(static_cast<uint32_t>(std::min<size_t>(target_size, 1u << 24)));
}

// Scans all provided data pieces to compute a histogram of 6-byte sequence (d-mer) hashes.
void PopulateFrequencyTable(absl::Span<const std::pair<const uint8_t*, size_t>> data_pieces,
                            uint16_t* freq, uint32_t freq_table_mask) {
  for (const auto& [data, sz] : data_pieces) {
    if (sz < kDmerLength)
      continue;

    size_t limit = sz - kDmerLength + 1;
    for (size_t i = 0; i < limit; ++i) {
      uint32_t idx = HashDmer(data + i) & freq_table_mask;
      if (freq[idx] < UINT16_MAX) {
        ++freq[idx];
      }
    }
  }
}

struct BestSegmentResult {
  std::pair<const uint8_t*, size_t> data_piece{nullptr, 0};
  uint64_t score = 0;
};

// Iterates across all data pieces to find a contiguous byte window of `segment_size`
// that maximizes the sum of previously computed sequence frequencies.
BestSegmentResult FindBestSegment(absl::Span<const std::pair<const uint8_t*, size_t>> data_pieces,
                                  size_t segment_size, const uint16_t* freq,
                                  uint32_t freq_table_mask) {
  BestSegmentResult best;

  for (const auto& [data, sz] : data_pieces) {
    if (sz < segment_size)
      continue;

    size_t window_dmers = segment_size - kDmerLength + 1;
    uint64_t score = 0;

    // Compute initial window score
    for (size_t j = 0; j < window_dmers; ++j) {
      score += freq[HashDmer(data + j) & freq_table_mask];
    }

    if (score > best.score) {
      best.score = score;
      best.data_piece = {data, segment_size};
    }

    // Slide the window
    size_t limit = sz - segment_size;
    for (size_t i = 1; i <= limit; ++i) {
      score -= freq[HashDmer(data + i - 1) & freq_table_mask];
      score += freq[HashDmer(data + i + window_dmers - 1) & freq_table_mask];

      if (score > best.score) {
        best.score = score;
        best.data_piece = {data + i, segment_size};
      }
    }
  }

  return best;
}

void ZeroOutFrequencies(std::pair<const uint8_t*, size_t> data_piece, uint16_t* freq,
                        uint32_t freq_table_mask) {
  if (data_piece.second < kDmerLength)
    return;
  size_t seg_dmers = data_piece.second - kDmerLength + 1;
  for (size_t j = 0; j < seg_dmers; ++j) {
    freq[HashDmer(data_piece.first + j) & freq_table_mask] = 0;
  }
}

}  // namespace

// Estimates dictionary compressibility by observing the cardinality
// of unique 6-byte substrings via a simplified internal HyperLogLog.
double EstimateCompressibility(absl::Span<const std::pair<const uint8_t*, size_t>> data_pieces,
                               unsigned step) {
  DCHECK_GT(step, 0u);

  unique_ptr<uint8_t[]> registers(new uint8_t[hll::kRegisterLen]());
  uint64_t total_dmers = 0;

  for (const auto& [data, sz] : data_pieces) {
    if (sz < kDmerLength)
      continue;
    size_t limit = sz - kDmerLength + 1;
    for (size_t i = 0; i < limit; i += step) {
      hll::UpdateRegister(HashDmer(data + i), registers.get());
      ++total_dmers;
    }
  }

  if (total_dmers == 0) {
    return 1.0;  // No d-mers - we consider it incompressible
  }

  double estimate = hll::EstimateCardinality(registers.get());
  double ratio = estimate / static_cast<double>(total_dmers);
  return std::min(ratio, 1.0);
}

// Trains a dictionary using FastCover-style iterative segment selection.
// 1. Builds a frequency table of 6-byte d-mer hashes.
// 2. For each data piece (epoch), selects the segment of segment_size bytes
//    that maximizes the sum of d-mer frequencies.
// 3. Appends selected segment to dictionary, zeros out its d-mer frequencies.
// Returns raw dictionary bytes of approximately dict_size.
string TrainDictionary(absl::Span<const pair<const uint8_t*, size_t>> data_pieces, size_t dict_size,
                       size_t segment_size) {
  DCHECK_GT(dict_size, 0u);
  DCHECK_GT(segment_size, kDmerLength);

  uint32_t freq_table_size = CalculateFreqTableSize(data_pieces);
  uint32_t freq_table_mask = freq_table_size - 1;

  unique_ptr<uint16_t[]> freq(new uint16_t[freq_table_size]());
  PopulateFrequencyTable(data_pieces, freq.get(), freq_table_mask);

  std::string dictionary;
  dictionary.reserve(dict_size);

  while (dictionary.size() < dict_size) {
    auto best = FindBestSegment(data_pieces, segment_size, freq.get(), freq_table_mask);

    if (!best.data_piece.first || best.score == 0) {
      break;  // No useful segments left.
    }

    size_t append_size = std::min(best.data_piece.second, dict_size - dictionary.size());
    dictionary.append(reinterpret_cast<const char*>(best.data_piece.first), append_size);

    ZeroOutFrequencies(best.data_piece, freq.get(), freq_table_mask);
  }

  return dictionary;
}

}  // namespace dfly
