// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/bloom.h"

#include <absl/base/internal/endian.h>
#include <absl/numeric/bits.h>
#include <xxhash.h>

#include <cmath>

#include "base/logging.h"

namespace dfly {

using namespace std;

namespace {

inline XXH128_hash_t Hash(string_view str) {
  return XXH3_128bits_withSeed(str.data(), str.size(), 0xc6a4a7935bd1e995ULL);  // murmur2 seed
}

uint64_t GetMask(unsigned log) {
  return (1ULL << log) - 1;
}

inline uint64_t BitIndex(uint64_t low, uint64_t hi, unsigned i, uint64_t mask) {
  return (low + hi * i) & mask;
}

constexpr double kDenom = M_LN2 * M_LN2;
constexpr double kSBFErrorFactor = 0.5;

inline double BPE(double fp_prob) {
  return -log(fp_prob) / kDenom;
}

}  // namespace

Bloom::~Bloom() {
  CHECK(bf_ == nullptr);
}

Bloom::Bloom(Bloom&& o) : hash_cnt_(o.hash_cnt_), bit_log_(o.bit_log_), bf_(o.bf_) {
  o.bf_ = nullptr;
}

void Bloom::Init(uint64_t entries, double fp_prob, PMR_NS::memory_resource* heap) {
  CHECK(bf_ == nullptr);
  CHECK(fp_prob > 0 && fp_prob < 1);

  if (entries < 1024)
    entries = 1024;

  double bpe = BPE(fp_prob);

  hash_cnt_ = ceil(M_LN2 * bpe);

  uint64_t bits = uint64_t(ceil(entries * bpe));
  bits = absl::bit_ceil(bits);  // make it power of 2.
  if (bits < 1024) {
    bits = 1024;
  }

  uint64_t length = bits / 8;
  bf_ = (uint8_t*)heap->allocate(length);
  memset(bf_, 0, length);
  bit_log_ = absl::countr_zero(bits);
}

void Bloom::Destroy(PMR_NS::memory_resource* resource) {
  resource->deallocate(CHECK_NOTNULL(bf_), bitlen() / 8);
  bf_ = nullptr;
}

bool Bloom::Exists(std::string_view str) const {
  XXH128_hash_t hash = Hash(str);
  uint64_t fp[2] = {hash.low64, hash.high64};

  return Exists(fp);
}

bool Bloom::Exists(uint64_t fp[2]) const {
  uint64_t mask = GetMask(bit_log_);
  for (unsigned i = 0; i < hash_cnt_; ++i) {
    uint64_t index = BitIndex(fp[0], fp[1], i, mask);
    if (!IsSet(index))
      return false;
  }
  return true;
}

bool Bloom::Add(std::string_view str) {
  XXH128_hash_t hash = Hash(str);
  uint64_t fp[2] = {hash.low64, hash.high64};
  return Add(fp);
}

bool Bloom::Add(uint64_t fp[2]) {
  uint64_t mask = GetMask(bit_log_);

  unsigned changes = 0;
  for (uint64_t i = 0; i < hash_cnt_; i++) {
    uint64_t index = BitIndex(fp[0], fp[1], i, mask);
    changes += Set(index);
  }

  return changes != 0;
}

size_t Bloom::Capacity(double fp_prob) const {
  double bpe = BPE(fp_prob);
  return floor(bitlen() / bpe);
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

///////////////////////////////////////////////////////////////////////////////
// SBF implementation
///////////////////////////////////////////////////////////////////////////////
SBF::SBF(uint64_t initial_capacity, double fp_prob, double grow_factor, PMR_NS::memory_resource* mr)
    : filters_(1, mr), grow_factor_(grow_factor), fp_prob_(fp_prob * kSBFErrorFactor) {
  filters_.front().Init(initial_capacity, fp_prob_, mr);
  max_capacity_ = filters_.front().Capacity(fp_prob_);
}

SBF::~SBF() {
  PMR_NS::memory_resource* mr = filters_.get_allocator().resource();
  for (auto& f : filters_)
    f.Destroy(mr);
}

bool SBF::Add(std::string_view str) {
  DCHECK_LT(current_size_, max_capacity_);
  auto it = filters_.rbegin();  // largest filter
  auto cur = it++;

  XXH128_hash_t hash = Hash(str);
  uint64_t fp[2] = {hash.low64, hash.high64};

  // Check for all other filters whether the item exists.
  for (; it != filters_.rend(); ++it) {
    if (it->Exists(fp))
      return false;
  }

  if (!cur->Add(fp))
    return false;

  ++current_size_;

  // Based on the paper, the optimal fill ratio for SBF is 50%.
  // Lets add a new slice if we reach it.
  if (current_size_ >= max_capacity_) {
    fp_prob_ *= kSBFErrorFactor;
    filters_.emplace_back().Init(max_capacity_ * grow_factor_, fp_prob_,
                                 filters_.get_allocator().resource());
    current_size_ = 0;
    max_capacity_ = filters_.back().Capacity(fp_prob_);
  }
  return true;
}

bool SBF::Exists(std::string_view str) const {
  XXH128_hash_t hash = Hash(str);
  uint64_t fp[2] = {hash.low64, hash.high64};

  for (auto it = filters_.crbegin(); it != filters_.crend(); ++it) {
    if (it->Exists(fp)) {
      return true;
    }
  }
  return false;
}

}  // namespace dfly
