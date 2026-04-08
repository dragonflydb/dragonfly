// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/bloom.h"

#include <absl/base/internal/endian.h>
#include <absl/numeric/bits.h>
#include <xxhash.h>

#include <algorithm>
#include <cmath>

#include "base/logging.h"

namespace dfly {

using namespace std;

namespace {

XXH128_hash_t Hash(string_view str) {
  return XXH3_128bits_withSeed(str.data(), str.size(), 0xc6a4a7935bd1e995ULL);  // murmur2 seed
}

uint64_t GetMask(unsigned log) {
  return (1ULL << log) - 1;
}

uint64_t BitIndex(uint64_t low, uint64_t hi, unsigned i, uint64_t mask) {
  return (low + hi * i) & mask;
}

constexpr double kDenom = M_LN2 * M_LN2;
constexpr double kSBFErrorFactor = 0.5;

double BPE(double fp_prob) {
  return -log(fp_prob) / kDenom;
}

}  // namespace

Bloom::~Bloom() {
  CHECK(bf_ == nullptr);
}

Bloom::Bloom(Bloom&& o) noexcept : hash_cnt_(o.hash_cnt_), bit_log_(o.bit_log_), bf_(o.bf_) {
  o.bf_ = nullptr;
}

void Bloom::Init(uint64_t entries, double fp_prob, PMR_NS::memory_resource* heap) {
  CHECK(bf_ == nullptr);
  CHECK(fp_prob > 0 && fp_prob < 1);

  if (fp_prob > 0.5)
    fp_prob = 0.5;
  double bpe = BPE(fp_prob);

  hash_cnt_ = ceil(M_LN2 * bpe);

  uint64_t bits = uint64_t(ceil(entries * bpe));
  if (bits < 512) {
    bits = 512;
  }
  bits = absl::bit_ceil(bits);  // make it power of 2.

  uint64_t length = bits / 8;
  bf_ = (uint8_t*)heap->allocate(length);
  memset(bf_, 0, length);
  bit_log_ = absl::countr_zero(bits);
}

void Bloom::Init(uint8_t* blob, size_t len, unsigned hash_cnt) {
  DCHECK_EQ(len * 8, absl::bit_ceil(len * 8));  // must be power of two.
  CHECK(bf_ == nullptr);
  hash_cnt_ = hash_cnt;
  bf_ = blob;
  bit_log_ = absl::countr_zero(len * 8);
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

bool Bloom::Exists(const uint64_t fp[2]) const {
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

bool Bloom::Add(const uint64_t fp[2]) {
  uint64_t mask = GetMask(bit_log_);

  unsigned changes = 0;
  for (uint64_t i = 0; i < hash_cnt_; i++) {
    uint64_t index = BitIndex(fp[0], fp[1], i, mask);
    changes += Set(index);
  }

  return changes != 0;
}

size_t Bloom::Capacity(double fp_prob) const {
  if (fp_prob > 0.5)
    fp_prob = 0.5;
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

SBF::SBF(double grow_factor, double fp_prob, size_t max_capacity, size_t prev_size,
         size_t current_size, PMR_NS::memory_resource* mr)
    : filters_(mr),
      grow_factor_(grow_factor),
      fp_prob_(fp_prob),
      prev_size_(prev_size),
      current_size_(current_size),
      max_capacity_(max_capacity) {
}

SBF::~SBF() {
  PMR_NS::memory_resource* mr = filters_.get_allocator().resource();
  for (auto& f : filters_)
    f.Destroy(mr);
}

SBF& SBF::operator=(SBF&& src) noexcept {
  filters_.clear();
  filters_.swap(src.filters_);
  grow_factor_ = src.grow_factor_;
  fp_prob_ = src.fp_prob_;
  current_size_ = src.current_size_;
  max_capacity_ = src.max_capacity_;

  return *this;
}

void SBF::AddFilter(const std::string& blob, unsigned hash_cnt) {
  PMR_NS::memory_resource* mr = filters_.get_allocator().resource();
  uint8_t* ptr = (uint8_t*)mr->allocate(blob.size(), 1);
  memcpy(ptr, blob.data(), blob.size());
  filters_.emplace_back().Init(ptr, blob.size(), hash_cnt);
}

bool SBF::Add(std::string_view str) {
  DCHECK_LT(current_size_, max_capacity_);

  XXH128_hash_t hash = Hash(str);
  uint64_t fp[2] = {hash.low64, hash.high64};

  auto exists = [fp](const Bloom& b) { return b.Exists(fp); };

  // Check for all the previous filters whether the item exists.
  if (any_of(next(filters_.crbegin()), filters_.crend(), exists)) {
    return false;
  }

  if (!filters_.back().Add(fp))
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

  auto exists = [fp](const Bloom& b) { return b.Exists(fp); };

  return any_of(filters_.crbegin(), filters_.crend(), exists);
}

size_t SBF::MallocUsed() const {
  size_t res = filters_.capacity() * sizeof(Bloom);
  for (const auto& b : filters_) {
    res += (b.bitlen() / 8);
  }
  res += sizeof(SBF);

  return res;
}

SBFDumpIterator::SBFDumpIterator(const SBF& sbf, int64_t cursor) : sbf_{sbf}, cursor_{cursor} {
  ResolveCursorToPos();
}

SBFChunk SBFDumpIterator::Next() {
  if (!header_sent_) {
    header_sent_ = true;
    cursor_ = 1;
    return {cursor_, SerializeHeader()};
  }

  if (filter_index_ >= sbf_.num_filters()) {
    return {0, {}};
  }

  const string_view data = sbf_.data(filter_index_);
  const size_t remaining = data.size() - byte_offset_;
  const size_t chunk_len = std::min(kMaxChunkSize, remaining);
  const string_view chunk = data.substr(byte_offset_, chunk_len);

  byte_offset_ += chunk_len;
  cursor_ += chunk_len;

  if (byte_offset_ >= data.size()) {
    filter_index_++;
    byte_offset_ = 0;
  }

  return {cursor_, string{chunk}};
}

bool SBFDumpIterator::Done() const {
  return filter_index_ >= sbf_.num_filters() && header_sent_;
}

std::string SBFDumpIterator::SerializeHeader() const {
  constexpr uint32_t kSbfDumpVersion = 1;

  constexpr size_t kDumpHeaderFixedSize = 48;  // version + 5x u64 + num_filters
  constexpr size_t kDumpFilterMetaSize = 12;   // hash_cnt + data_length

  const uint32_t num_filters = sbf_.num_filters();

  std::string out;
  out.reserve(kDumpHeaderFixedSize + num_filters * kDumpFilterMetaSize);

  auto append_u32 = [&out](uint32_t v) {
    char buf[sizeof(v)];
    absl::little_endian::Store32(buf, v);
    out.append(buf, sizeof(buf));
  };

  auto append_u64 = [&out](uint64_t v) {
    char buf[sizeof(v)];
    absl::little_endian::Store64(buf, v);
    out.append(buf, sizeof(buf));
  };

  append_u32(kSbfDumpVersion);
  append_u64(std::bit_cast<uint64_t>(sbf_.grow_factor()));
  append_u64(std::bit_cast<uint64_t>(sbf_.fp_probability()));
  append_u64(sbf_.prev_size());
  append_u64(sbf_.current_size());
  append_u64(sbf_.max_capacity());
  append_u32(num_filters);

  for (uint32_t i = 0; i < num_filters; ++i) {
    append_u32(sbf_.hashfunc_cnt(i));
    append_u64(sbf_.data(i).size());
  }

  return out;
}

void SBFDumpIterator::ResolveCursorToPos() {
  header_sent_ = cursor_ > 0;
  if (cursor_ == 0) {
    filter_index_ = 0;
    byte_offset_ = 0;
    return;
  }

  size_t global_offset = cursor_ - 1;
  for (uint32_t i = 0; i < sbf_.num_filters(); ++i) {
    const size_t curr_filter_size = sbf_.data(i).size();
    if (global_offset < curr_filter_size) {
      filter_index_ = i;
      byte_offset_ = global_offset;
      return;
    }
    global_offset -= curr_filter_size;
  }

  // cursor is beyond data boundary
  filter_index_ = sbf_.num_filters();
  byte_offset_ = 0;
}

}  // namespace dfly
