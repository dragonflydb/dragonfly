// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/cuckoo.h"

#include <xxhash.h>

#include <cmath>
#include <cstring>

#include "absl/base/internal/endian.h"
#include "absl/numeric/bits.h"
#include "base/logging.h"
#include "core/compact_object.h"

namespace dfly {

namespace {

bool IsPowerOfTwo(uint64_t n) {
  return absl::has_single_bit(n);
}

uint64_t NextPowerOfTwo(uint64_t n) {
  return absl::bit_ceil(n);
}

// Result is in [1, 255] — 0 is reserved as "empty slot".
uint8_t Fingerprint(uint64_t hash) {
  return static_cast<uint8_t>(hash % 255 + 1);
}

// 0x5bd1e995 is the MurmurHash2 mixing constant (Austin Appleby),
// chosen for good bit-avalanche properties.
// AltIndex symmetry requires num_buckets to be a power of two. Power-of-2 modulo
// is a bitmask (x % N == x & (N-1)), and bitmasks commute with XOR:
//   (a XOR b) & mask == (a & mask) XOR (b & mask)
// This means AltIndex(fp, h1 & mask) & mask == AltIndex(fp, h1) & mask, so
//   AltIndex(fp, AltIndex(fp, i) % N) % N == i % N  holds.
// With arbitrary N, modulo is not a bitmask and the identity breaks, corrupting
// KO-insert rollback and deletions.
// Requirement from: Fan et al., "Cuckoo Filter: Practically Better Than Bloom" (2014).
uint64_t AltIndex(uint8_t fp, uint64_t index) {
  return index ^ (static_cast<uint64_t>(fp) * 0x5bd1e995);
}

}  // namespace

CuckooFilter::CuckooFilter(const Options& options, std::pmr::memory_resource* mr)
    : slots_per_bucket_(options.slots_per_bucket),
      max_iterations_(options.max_iterations),
      expansion_(options.expansion ? NextPowerOfTwo(options.expansion) : 0),
      mr_(mr),
      filters_(mr) {
  DCHECK(mr);
  DCHECK_GT(slots_per_bucket_, 0);
  num_buckets_ = slots_per_bucket_ ? NextPowerOfTwo(options.capacity / slots_per_bucket_) : 1;
  if (num_buckets_ == 0)
    num_buckets_ = 1;
  DCHECK(IsPowerOfTwo(num_buckets_));
  AddNewSubFilter();
}

bool CuckooFilter::Insert(uint64_t hash) {
  DCHECK(!filters_.empty());
  const LookupParams p = LookupParamsFromHash(hash);

  for (;;) {
    for (size_t i = filters_.size(); i-- > 0;) {
      SubFilter& sf = filters_[i];
      auto [i1, i2] = BucketIndices(sf, p);
      for (uint64_t idx : {i1, i2}) {
        const size_t base = idx * slots_per_bucket_;
        for (uint8_t s = 0; s < slots_per_bucket_; ++s) {
          if (sf[base + s] == 0) {
            sf[base + s] = p.fp;
            ++num_items_;
            return true;
          }
        }
      }
    }

    if (KOInsert(p, filters_.back())) {
      ++num_items_;
      ++num_ko_inserts_;
      return true;
    }

    if (expansion_ == 0 || !AddNewSubFilter()) {
      return false;
    }
    // Loop: the new SubFilter has empty slots, insert will succeed on next iteration.
  }
}

bool CuckooFilter::InsertUnique(uint64_t hash) {
  if (Exists(hash))
    return false;
  return Insert(hash);
}

bool CuckooFilter::Exists(uint64_t hash) const {
  DCHECK(!filters_.empty());
  const LookupParams p = LookupParamsFromHash(hash);

  for (const SubFilter& sf : filters_) {
    auto [i1, i2] = BucketIndices(sf, p);
    for (uint64_t idx : {i1, i2}) {
      const size_t base = idx * slots_per_bucket_;
      for (uint8_t s = 0; s < slots_per_bucket_; ++s) {
        if (sf[base + s] == p.fp)
          return true;
      }
    }
  }
  return false;
}

size_t CuckooFilter::Count(uint64_t hash) const {
  const LookupParams p = LookupParamsFromHash(hash);

  size_t count = 0;
  for (const SubFilter& sf : filters_) {
    auto [i1, i2] = BucketIndices(sf, p);
    for (uint64_t idx : {i1, i2}) {
      const size_t base = idx * slots_per_bucket_;
      for (uint8_t s = 0; s < slots_per_bucket_; ++s) {
        if (sf[base + s] == p.fp)
          ++count;
      }
    }
  }
  return count;
}

bool CuckooFilter::Delete(uint64_t hash) {
  DCHECK(!filters_.empty());
  const LookupParams p = LookupParamsFromHash(hash);

  for (size_t i = filters_.size(); i-- > 0;) {
    SubFilter& sf = filters_[i];
    auto [i1, i2] = BucketIndices(sf, p);
    for (uint64_t idx : {i1, i2}) {
      const size_t base = idx * slots_per_bucket_;
      for (uint8_t s = 0; s < slots_per_bucket_; ++s) {
        if (sf[base + s] == p.fp) {
          sf[base + s] = 0;
          --num_items_;
          ++num_deletes_;
          return true;
        }
      }
    }
  }
  return false;
}

void CuckooFilter::Deserialize(const SerializedDataView& data) {
  std::pmr::vector<SubFilter> new_filters(filters_.get_allocator());
  new_filters.reserve(data.filters.size());
  for (const std::string& blob : data.filters) {
    SubFilter sf(blob.begin(), blob.end(), mr_);
    new_filters.push_back(std::move(sf));
  }

  // Nothing below can throw.
  slots_per_bucket_ = data.slots_per_bucket;
  max_iterations_ = data.max_iterations;
  expansion_ = data.expansion;
  num_buckets_ = data.num_buckets;
  num_items_ = data.num_items;
  num_deletes_ = data.num_deletes;
  num_ko_inserts_ = 0;
  filters_.swap(new_filters);
}

void CuckooFilter::AppendFilter(std::string_view blob) {
  SubFilter sf(reinterpret_cast<const uint8_t*>(blob.data()),
               reinterpret_cast<const uint8_t*>(blob.data()) + blob.size(), mr_);
  filters_.push_back(std::move(sf));
}

bool CuckooFilter::InitForChunkedLoad(uint8_t slots_per_bucket, uint16_t max_iterations,
                                      uint16_t expansion, uint64_t num_buckets, uint64_t num_items,
                                      uint64_t num_deletes, uint64_t num_filters) {
  // Same bound AddNewSubFilter enforces on organically-grown filters (kMaxBuckets there).
  // With expansion >= 2 that check alone caps num_filters at well under 64; this guards the
  // expansion == 0 case (growth never advances, so the per-filter check below never trips)
  // and protects `reserve(num_filters)` below from a huge attacker-supplied count.
  if (num_filters == 0 || num_filters > 64 || slots_per_bucket == 0 || num_buckets == 0)
    return false;

  static constexpr uint64_t kMaxBuckets = (1ULL << 56) - 1;

  std::pmr::vector<SubFilter> new_filters(filters_.get_allocator());
  new_filters.reserve(num_filters);
  uint64_t growth = 1;
  uint64_t total_bytes = 0;
  for (uint64_t i = 0; i < num_filters; ++i) {
    if (growth > kMaxBuckets / num_buckets)
      return false;
    const uint64_t bucket_count = num_buckets * growth;
    if (bucket_count > SIZE_MAX / slots_per_bucket)
      return false;

    const uint64_t sf_size = bucket_count * slots_per_bucket;
    SubFilter sf(sf_size, uint8_t{0}, mr_);
    new_filters.push_back(std::move(sf));
    total_bytes += sf_size;

    if (expansion != 0) {
      if (growth > UINT64_MAX / expansion)
        return false;
      growth *= expansion;
    }
  }

  slots_per_bucket_ = slots_per_bucket;
  max_iterations_ = max_iterations;
  expansion_ = expansion;
  num_buckets_ = num_buckets;
  num_items_ = num_items;
  num_deletes_ = num_deletes;
  num_ko_inserts_ = 0;
  filters_.swap(new_filters);

  loading_ = true;
  pending_load_bytes_ = total_bytes;
  return true;
}

void CuckooFilter::WriteFilterBytes(size_t idx, size_t offset, std::string_view data) {
  DCHECK_LT(idx, filters_.size());
  SubFilter& sf = filters_[idx];
  DCHECK_LE(offset + data.size(), sf.size());
  std::memcpy(sf.data() + offset, data.data(), data.size());

  if (loading_) {
    DCHECK_GE(pending_load_bytes_, data.size());
    pending_load_bytes_ -= data.size();
    if (pending_load_bytes_ == 0)
      loading_ = false;
  }
}

uint64_t CuckooFilter::Hash(std::string_view item) {
  return XXH3_64bits_withSeed(item.data(), item.size(), 0xc6a4a7935bd1e995ULL);
}

size_t CuckooFilter::MallocUsed() const {
  size_t res = sizeof(CuckooFilter) + filters_.capacity() * sizeof(SubFilter);
  for (const SubFilter& sf : filters_) {
    res += sf.size();
  }
  return res;
}

CuckooFilter::LookupParams CuckooFilter::LookupParamsFromHash(uint64_t hash) const {
  const uint8_t fp = Fingerprint(hash);
  return {fp, hash, AltIndex(fp, hash)};
}

std::pair<uint64_t, uint64_t> CuckooFilter::BucketIndices(const SubFilter& sf,
                                                          const LookupParams& p) const {
  const uint64_t n = NumBuckets(sf);
  return {p.h1 % n, p.h2 % n};
}

uint64_t CuckooFilter::NumBuckets(const SubFilter& sf) const {
  return sf.size() / slots_per_bucket_;
}

bool CuckooFilter::AddNewSubFilter() {
  static constexpr uint64_t kMaxBuckets =
      (1ULL << 56) - 1;  // preserve semantics with SubFilter numBuckets field (56-bit)

  const uint64_t growth = static_cast<uint64_t>(std::pow(expansion_, filters_.size()));

  if (growth > (kMaxBuckets / num_buckets_)) {
    return false;
  }

  const uint64_t bucket_count = num_buckets_ * growth;
  if (bucket_count > (SIZE_MAX / slots_per_bucket_)) {
    return false;
  }

  SubFilter sf(bucket_count * slots_per_bucket_, uint8_t{0}, mr_);
  filters_.push_back(std::move(sf));
  return true;
}

bool CuckooFilter::RelocateSlot(size_t filter_idx, uint64_t bucket_idx, uint8_t slot_idx) {
  SubFilter& sf = filters_[filter_idx];
  uint8_t& slot = sf[bucket_idx * slots_per_bucket_ + slot_idx];
  if (slot == 0)
    return true;

  const uint8_t fp = slot;
  // bucket_idx is this sub-filter's bucket index, not the raw hash. Reusing it works because
  // each sub-filter's bucket count is a power-of-two multiple of every earlier sub-filter's
  // count, so bucket_idx % earlier_n == raw_hash % earlier_n. Same symmetry argument as the
  // one documented above AltIndex's definition.
  const uint64_t alt_bucket_idx = AltIndex(fp, bucket_idx);

  for (size_t prior = 0; prior < filter_idx; ++prior) {
    SubFilter& prior_sf = filters_[prior];
    const uint64_t n = NumBuckets(prior_sf);
    for (uint64_t idx : {bucket_idx % n, alt_bucket_idx % n}) {
      const size_t base = idx * slots_per_bucket_;
      for (uint8_t s = 0; s < slots_per_bucket_; ++s) {
        if (prior_sf[base + s] == 0) {
          prior_sf[base + s] = fp;
          slot = 0;
          return true;
        }
      }
    }
  }
  return false;
}

bool CuckooFilter::CompactSingleFilter(size_t filter_idx) {
  const uint64_t n = NumBuckets(filters_[filter_idx]);
  bool fully_emptied = true;
  for (uint64_t bucket_idx = 0; bucket_idx < n; ++bucket_idx) {
    for (uint8_t slot_idx = 0; slot_idx < slots_per_bucket_; ++slot_idx) {
      if (!RelocateSlot(filter_idx, bucket_idx, slot_idx))
        fully_emptied = false;
    }
  }
  // Only the newest sub-filter can ever be freed: freeing a middle one would leave a gap that
  // breaks the "bucket count grows by expansion_ per index" invariant RelocateSlot relies on.
  if (fully_emptied && filter_idx == filters_.size() - 1) {
    filters_.pop_back();
  }
  return fully_emptied;
}

void CuckooFilter::Compact(bool cont) {
  for (size_t i = filters_.size(); i-- > 1;) {
    if (!CompactSingleFilter(i) && !cont)
      break;
  }
  num_deletes_ = 0;
}

bool CuckooFilter::KOInsert(const LookupParams& p, SubFilter& sf) {
  const uint64_t n = NumBuckets(sf);
  uint64_t idx = p.h1 % n;
  uint8_t fp = p.fp;
  uint8_t victim_slot = 0;

  for (uint16_t i = 0; i < max_iterations_; ++i) {
    // Evict the fingerprint at victim_slot in bucket idx and take its place.
    // Then jump to the evicted fingerprint's alternate bucket and try to place it there.
    // victim_slot cycles across slots to avoid getting stuck in displacement cycles.
    std::swap(sf[idx * slots_per_bucket_ + victim_slot], fp);
    idx = AltIndex(fp, idx) % n;

    for (uint8_t s = 0; s < slots_per_bucket_; ++s) {
      if (sf[idx * slots_per_bucket_ + s] == 0) {
        sf[idx * slots_per_bucket_ + s] = fp;
        return true;
      }
    }
    victim_slot = (victim_slot + 1) % slots_per_bucket_;
  }

  // Roll back all swaps to restore the SubFilter to its original state.
  for (uint16_t i = 0; i < max_iterations_; ++i) {
    victim_slot = (victim_slot + slots_per_bucket_ - 1) % slots_per_bucket_;
    idx = AltIndex(fp, idx) % n;
    std::swap(sf[idx * slots_per_bucket_ + victim_slot], fp);
  }

  return false;
}

namespace {

constexpr uint32_t kCfDumpVersion = 1;
// version(4) + slots_per_bucket(1) + max_iterations(2) + expansion(2) + num_buckets(8) +
// num_items(8) + num_deletes(8) + num_filters(8)
constexpr size_t kCfDumpHeaderSize = 4 + 1 + 2 + 2 + 8 + 8 + 8 + 8;

void AppendU8(std::string& out, uint8_t v) {
  out.push_back(static_cast<char>(v));
}

void AppendU16(std::string& out, uint16_t v) {
  char buf[sizeof(v)];
  absl::little_endian::Store16(buf, v);
  out.append(buf, sizeof(buf));
}

void AppendU32(std::string& out, uint32_t v) {
  char buf[sizeof(v)];
  absl::little_endian::Store32(buf, v);
  out.append(buf, sizeof(buf));
}

void AppendU64(std::string& out, uint64_t v) {
  char buf[sizeof(v)];
  absl::little_endian::Store64(buf, v);
  out.append(buf, sizeof(buf));
}

}  // namespace

const char* ToString(CFLoadResult res) {
  switch (res) {
    case CFLoadResult::kOk:
      return "ok";
    case CFLoadResult::kBadVersion:
      return "bad version";
    case CFLoadResult::kBadInput:
      return "bad input";
    case CFLoadResult::kOutOfRange:
      return "out of range";
  }
  return "unknown";
}

CFDumpIterator::CFDumpIterator(const CuckooFilter& cf, int64_t cursor) : cf_(cf), cursor_(cursor) {
  ResolveCursorToPos();
}

std::string CFDumpIterator::SerializeHeader() const {
  std::string out;
  out.reserve(kCfDumpHeaderSize);

  AppendU32(out, kCfDumpVersion);
  AppendU8(out, cf_.SlotsPerBucket());
  AppendU16(out, cf_.MaxIterations());
  AppendU16(out, cf_.Expansion());
  AppendU64(out, cf_.NumBuckets());
  AppendU64(out, cf_.NumItems());
  AppendU64(out, cf_.NumDeletes());
  AppendU64(out, cf_.NumFilters());

  return out;
}

void CFDumpIterator::ResolveCursorToPos() {
  if (cursor_ == 0) {
    filter_index_ = 0;
    byte_offset_ = 0;
    return;
  }

  size_t global_offset = cursor_ - 1;
  for (uint32_t i = 0; i < cf_.NumFilters(); ++i) {
    const size_t filter_size = cf_.FilterBytes(i).size();
    if (global_offset < filter_size) {
      filter_index_ = i;
      byte_offset_ = global_offset;
      return;
    }
    global_offset -= filter_size;
  }

  filter_index_ = cf_.NumFilters();
  byte_offset_ = 0;
}

CFChunk CFDumpIterator::Next() {
  // Matches upstream CF.SCANDUMP: an empty filter (nothing ever inserted) always reports
  // done, regardless of the cursor requested. There's nothing meaningful to reconstruct
  // from a filter with zero items, so we skip emitting even the header chunk.
  if (cf_.NumItems() == 0)
    return {0, {}};

  if (cursor_ == 0) {
    cursor_ = 1;
    return {cursor_, SerializeHeader()};
  }

  if (filter_index_ >= cf_.NumFilters())
    return {0, {}};

  const std::string_view filter_data = cf_.FilterBytes(filter_index_);
  const size_t chunk_len = std::min<size_t>(kMaxChunkSize, filter_data.size() - byte_offset_);
  std::string chunk(filter_data.substr(byte_offset_, chunk_len));

  byte_offset_ += chunk_len;
  cursor_ += chunk_len;

  if (byte_offset_ == filter_data.size()) {
    ++filter_index_;
    byte_offset_ = 0;
  }

  return {cursor_, std::move(chunk)};
}

nonstd::expected<CuckooFilter*, CFLoadResult> LoadCFHeader(std::string_view header_data,
                                                           std::pmr::memory_resource* mr) {
  using enum CFLoadResult;
  using nonstd::make_unexpected;

  if (header_data.size() != kCfDumpHeaderSize)
    return make_unexpected(kBadInput);

  const char* ptr = header_data.data();
  if (const uint32_t version = absl::little_endian::Load32(ptr); version != kCfDumpVersion)
    return make_unexpected(kBadVersion);
  ptr += 4;

  const uint8_t slots_per_bucket = static_cast<uint8_t>(*ptr);
  ptr += 1;
  const uint16_t max_iterations = absl::little_endian::Load16(ptr);
  ptr += 2;
  const uint16_t expansion = absl::little_endian::Load16(ptr);
  ptr += 2;
  const uint64_t num_buckets = absl::little_endian::Load64(ptr);
  ptr += 8;
  const uint64_t num_items = absl::little_endian::Load64(ptr);
  ptr += 8;
  const uint64_t num_deletes = absl::little_endian::Load64(ptr);
  ptr += 8;
  const uint64_t num_filters = absl::little_endian::Load64(ptr);

  if (slots_per_bucket == 0 || max_iterations == 0 || !IsPowerOfTwo(num_buckets) ||
      (expansion != 0 && !IsPowerOfTwo(expansion)) || num_filters == 0)
    return make_unexpected(kBadInput);

  CuckooFilter* cf = CompactObj::AllocateMR<CuckooFilter>(CuckooFilterOptions{}, mr);
  if (!cf->InitForChunkedLoad(slots_per_bucket, max_iterations, expansion, num_buckets, num_items,
                              num_deletes, num_filters)) {
    CompactObj::DeleteMR<CuckooFilter>(cf);
    return make_unexpected(kBadInput);
  }
  return cf;
}

CFLoadResult LoadCFChunk(int64_t cursor, std::string_view data, CuckooFilter* cf) {
  DCHECK(cf);

  const int64_t write_pos = cursor - static_cast<int64_t>(data.size());
  if (write_pos < 1)
    return CFLoadResult::kOutOfRange;

  size_t global_offset = write_pos - 1;
  for (uint32_t i = 0; i < cf->NumFilters(); ++i) {
    const size_t filter_size = cf->FilterBytes(i).size();
    if (global_offset < filter_size) {
      if (global_offset + data.size() > filter_size)
        return CFLoadResult::kOutOfRange;
      cf->WriteFilterBytes(i, global_offset, data);
      return CFLoadResult::kOk;
    }
    global_offset -= filter_size;
  }

  return CFLoadResult::kOutOfRange;
}

}  // namespace dfly
