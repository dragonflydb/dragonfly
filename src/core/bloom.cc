// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/bloom.h"

#include <absl/base/internal/endian.h>
#include <absl/numeric/bits.h>
#include <mimalloc.h>

#include <cmath>

#define XXH_STATIC_LINKING_ONLY
#include <xxhash.h>

#include "base/logging.h"

namespace dfly {

using namespace std;

namespace {

inline XXH128_hash_t Hash(string_view str) {
  return XXH3_128bits_withSeed(str.data(), str.size(), 0xc6a4a7935bd1e995ULL);  // murmur2 seed
}

inline uint64_t BitIndex(const XXH128_hash_t& hash, unsigned i, uint64_t mask) {
  return (hash.low64 + hash.high64 * i) % mask;
}

}  // namespace

Bloom::Bloom(uint32_t entries, double error, mi_heap_t* heap) {
  CHECK(error > 0 && error < 1);

  if (entries < 1024)
    entries = 1024;

  constexpr double kDenom = M_LN2 * M_LN2;
  double bpe = -log(error) / kDenom;

  hash_cnt_ = ceil(M_LN2 * bpe);

  uint64_t bits = uint64_t(ceil(entries * bpe));
  bits = absl::bit_ceil(bits);  // make it power of 2.
  if (bits < 1024) {
    bits = 1024;
  }

  uint64_t length = bits / 8;

  bf_ = (uint8_t*)mi_heap_calloc(heap, length, 1);

  static_assert(absl::countr_zero(8u) == 3);
  bit_log_ = absl::countr_zero(bits);
  DCHECK_EQ(1UL << bit_log_, bits);
}

Bloom::~Bloom() {
  mi_free(bf_);
}

bool Bloom::Exists(std::string_view str) const {
  XXH128_hash_t hash = Hash(str);

  uint64_t mask = GetMask();
  for (unsigned i = 0; i < hash_cnt_; ++i) {
    uint64_t index = BitIndex(hash, i, mask);
    if (!IsSet(index))
      return false;
  }
  return true;
}

bool Bloom::Add(std::string_view str) {
  XXH128_hash_t hash = Hash(str);
  uint64_t mask = GetMask();

  unsigned changes = 0;
  for (uint64_t i = 0; i < hash_cnt_; i++) {
    uint64_t index = BitIndex(hash, i, mask);
    changes += Set(index);
  }

  return changes != 0;
}

inline bool Bloom::IsSet(size_t bit_idx) const {
  uint64_t byte_idx = bit_idx / 8;
  bit_idx %= 8;  // index within the byte
  uint8_t b = bf_[byte_idx];
  return (b & (1 << bit_idx)) != 0;
}

inline bool Bloom::Set(size_t bit_idx) {
  uint64_t byte_idx = bit_idx / 8;
  bit_idx %= 8;

  uint8_t b = bf_[byte_idx];
  bf_[byte_idx] |= (1 << bit_idx);
  return bf_[byte_idx] != b;
}

}  // namespace dfly
