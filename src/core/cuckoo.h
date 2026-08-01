// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <memory_resource>
#include <nonstd/expected.hpp>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace dfly {

struct CuckooFilterOptions {
  static constexpr uint8_t kDefaultSlotsPerBucket = 2;
  static constexpr uint16_t kDefaultMaxIterations = 20;
  static constexpr uint16_t kDefaultExpansion = 1;

  uint64_t capacity = 0;
  uint8_t slots_per_bucket = kDefaultSlotsPerBucket;
  uint16_t max_iterations = kDefaultMaxIterations;
  uint16_t expansion = kDefaultExpansion;
};

class CuckooFilter {
 public:
  using Options = CuckooFilterOptions;

  CuckooFilter(const Options& options, std::pmr::memory_resource* mr);

  // Inserts a pre-computed hash. Returns false only if the filter is full
  // and expansion is disabled (expansion_ == 0) or memory allocation fails.
  // Allows duplicate insertions — use InsertUnique to prevent them.
  bool Insert(uint64_t hash);

  // Inserts only if hash is not already present. Returns false if the item
  // already exists or the filter is full.
  bool InsertUnique(uint64_t hash);

  // Returns true if hash is present in the filter. May return false positives
  // but never false negatives.
  // TODO(kostas): SIMD for the inner bucket scan. Establish a baseline bench and then add SIMD.
  bool Exists(uint64_t hash) const;

  // Returns the number of fingerprint matches for hash across both candidate buckets and
  // all sub-filters. Each successful Insert of the same item occupies its own slot (Insert
  // never deduplicates), so this reflects how many times the item was added minus how many
  // times it was deleted. Like Exists, can overcount on fingerprint collisions.
  size_t Count(uint64_t hash) const;

  // Removes one occurrence of hash from the filter. Returns true if found and removed.
  // This is the key advantage over Bloom filters, which do not support deletion.
  bool Delete(uint64_t hash);

  static uint64_t Hash(std::string_view item);

  size_t NumItems() const {
    return num_items_;
  }

  // For tests. Returns the number of times an insertion found both candidate buckets full
  // and had to evict an existing fingerprint to its alternate bucket before the new
  // fingerprint could be placed.
  size_t NumKOInserts() const {
    return num_ko_inserts_;
  }

  // Returns approximate heap bytes used by this filter's SubFilter data.
  size_t MallocUsed() const;

  // Base bucket count from construction; never changes as the filter grows (each new
  // sub-filter scales its own bucket count by expansion_ instead).
  uint64_t NumBuckets() const {
    return num_buckets_;
  }

  size_t NumFilters() const {
    return filters_.size();
  }

  uint64_t NumDeletes() const {
    return num_deletes_;
  }

  uint8_t SlotsPerBucket() const {
    return slots_per_bucket_;
  }

  uint16_t MaxIterations() const {
    return max_iterations_;
  }

  // Already rounded up to the next power of two (or 0 if expansion is disabled).
  uint16_t Expansion() const {
    return expansion_;
  }

  // Returns the raw bytes of the idx'th sub-filter. For RDB serialization.
  std::string_view FilterBytes(size_t idx) const {
    const SubFilter& sf = filters_[idx];
    return {reinterpret_cast<const char*>(sf.data()), sf.size()};
  }

  struct SerializedDataView {
    uint8_t slots_per_bucket;
    uint16_t max_iterations;
    uint16_t expansion;
    uint64_t num_buckets;
    uint64_t num_items;
    uint64_t num_deletes;
    const std::vector<std::string>& filters;
  };

  // Restores complete internal state from previously-serialized data (RDB load).
  void Deserialize(const SerializedDataView& data);

  // Appends a single sub-filter from its raw bytes. For chunked RDB load (append mode).
  void AppendFilter(std::string_view blob);

  // Prepares an empty-shell filter to receive raw sub-filter bytes via WriteFilterBytes.
  // Allocates `num_filters` zero-filled sub-filters, sized deterministically from
  // num_buckets/expansion/slots_per_bucket (sub-filter i has num_buckets * expansion^i
  // buckets). Used by CF.LOADCHUNK's header phase (see CFDumpIterator below).
  // Returns false (leaving *this untouched) if the header's fields imply a sub-filter
  // count/size that overflows or could never be produced organically by AddNewSubFilter —
  // this is the caller-controlled header, so it must be validated the same way
  // AddNewSubFilter validates growth before allocating.
  bool InitForChunkedLoad(uint8_t slots_per_bucket, uint16_t max_iterations, uint16_t expansion,
                          uint64_t num_buckets, uint64_t num_items, uint64_t num_deletes,
                          uint64_t num_filters);

  // Writes raw bytes at `offset` into sub-filter `idx`. The caller (LoadCFChunk) guarantees
  // the range [offset, offset + data.size()) fits within that sub-filter. Used by
  // CF.LOADCHUNK's data phase to restore a filter dumped by CFDumpIterator. Clears IsLoading()
  // once every byte announced by InitForChunkedLoad has been written.
  void WriteFilterBytes(size_t idx, size_t offset, std::string_view data);

  // True from InitForChunkedLoad until every byte of every pre-allocated sub-filter has been
  // supplied via WriteFilterBytes. Queries/inserts must be rejected while true — the
  // sub-filters exist but are still zero-filled placeholders.
  bool IsLoading() const {
    return loading_;
  }

  // Reclaims space by moving items from newer sub-filters back into older ones, freeing the
  // newest sub-filter once it's been fully emptied. Only ever frees filters_.back(), one at
  // a time, working from the newest sub-filter down to (but not including) filters_[0].
  // If `cont` is false then the algorithm stops at the first sub-filter that can't be fully
  // emptied; If `cont` is true (CF.COMPACT), keeps trying older sub-filters regardless.
  void Compact(bool cont);

 private:
  using SubFilter = std::pmr::vector<uint8_t>;

  struct LookupParams {
    uint8_t fp;
    uint64_t h1;  // raw (unmodded) first candidate index
    uint64_t h2;  // raw (unmodded) alternate index
  };

  LookupParams LookupParamsFromHash(uint64_t hash) const;

  // Returns {h1 % num_buckets, h2 % num_buckets} for the given SubFilter.
  std::pair<uint64_t, uint64_t> BucketIndices(const SubFilter& sf, const LookupParams& p) const;

  uint64_t NumBuckets(const SubFilter& sf) const;

  // Appends a new SubFilter sized num_buckets_ * expansion_^filters_.size().
  // This is a Redis engineering choice to avoid rehashing on growth; the original
  // Fan et al. paper describes a single fixed-size filter only.
  bool AddNewSubFilter();

  // When both candidate buckets are full, evicts a fingerprint from h1, places ours
  // there, then tries to reinsert the evicted fingerprint into its own alternate bucket.
  // Repeats up to max_iterations_ times. On failure, rolls back all swaps.
  bool KOInsert(const LookupParams& p, SubFilter& sf);

  // Attempts to relocate every occupied slot in filters_[filter_idx] into some earlier
  // sub-filter. Returns true if every slot was relocated or already empty (i.e. this
  // sub-filter is now empty and safe to free if it's the last one).
  bool CompactSingleFilter(size_t filter_idx);

  // Tries to move the fingerprint located by the parameters into the
  // first earlier sub-filter (lowest index first) with room for it.
  // Returns true if the slot was already empty or the fingerprint was relocated
  // Returns false if no earlier sub-filter had room
  bool RelocateSlot(size_t filter_idx, uint64_t bucket_idx, uint8_t slot_idx);

  uint8_t slots_per_bucket_;
  uint16_t max_iterations_;
  uint16_t expansion_;

  uint64_t num_buckets_ = 0;
  uint64_t num_items_ = 0;
  uint64_t num_deletes_ = 0;
  uint64_t num_ko_inserts_ = 0;

  bool loading_ = false;
  uint64_t pending_load_bytes_ = 0;

  std::pmr::memory_resource* mr_;
  std::pmr::vector<SubFilter> filters_;
};

enum class CFLoadResult : uint8_t {
  kOk,
  kBadVersion,
  kBadInput,
  kOutOfRange,
};

const char* ToString(CFLoadResult res);

// Pair of values returned to a CF.SCANDUMP caller.
struct CFChunk {
  // 1: `data` is a header used to reconstruct the CuckooFilter shell itself (via LoadCFHeader).
  // >1: `data` is raw sub-filter bytes, to be written at the position implied by the cursor
  //     (via LoadCFChunk). A chunk never spans two sub-filters.
  // 0: iteration is complete. `data` is empty.
  int64_t cursor;
  std::string data;
};

// Streams the contents of a CuckooFilter to the caller in chunks of at most 16MiB. The first
// chunk is the filter's header/metadata; every following chunk carries raw sub-filter bytes.
// Sub-filter sizes are fully deterministic from the header (num_buckets * expansion^i *
// slots_per_bucket), so unlike SBFDumpIterator, no per-filter metadata needs to be embedded
// in the chunk stream itself.
class CFDumpIterator {
 public:
  static constexpr uint64_t kMaxChunkSize = 16 * 1024 * 1024;

  // cursor is the client-supplied position to resume from; 0 starts iteration from the start.
  CFDumpIterator(const CuckooFilter& cf, int64_t cursor);

  // Returns (next cursor, data up to the next cursor). Returns {0, ""} once fully consumed.
  CFChunk Next();

 private:
  std::string SerializeHeader() const;

  // Converts cursor_ to a sub-filter index and byte offset within it. O(n) in number of filters.
  void ResolveCursorToPos();

  const CuckooFilter& cf_;
  int64_t cursor_;
  uint32_t filter_index_ = 0;
  size_t byte_offset_ = 0;
};

// Creates a CuckooFilter shell from a dump header chunk (the chunk returned with cursor=1).
nonstd::expected<CuckooFilter*, CFLoadResult> LoadCFHeader(std::string_view header_data,
                                                           std::pmr::memory_resource* mr);

// Writes a data chunk (cursor > 1, as returned by CFDumpIterator) into a CuckooFilter shell
// previously created by LoadCFHeader.
CFLoadResult LoadCFChunk(int64_t cursor, std::string_view data, CuckooFilter* cf);

}  // namespace dfly
