// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/random/random.h>
#include <absl/types/span.h>

#include <algorithm>
#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iterator>
#include <limits>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/rapidhash.h"
#include "core/detail/stateless_allocator.h"
#include "core/oah_base.h"

extern "C" {
#include "redis/zmalloc.h"
}

namespace dfly {

// A compact set of strings with keys no longer than kMaxKeySize. Each table bucket owns a tightly
// packed chain of up to eight keys per block. The upper four bits of a link record whether its
// block holds 0..8 keys, or kChainedBlockTag when its full block points at a following block.
//
// Per-element size bytes use their two high bits for metadata:
//   bit 7: content is 7-bit ASCII packed (the empty key uses this bit as its zero-length sentinel)
//   bit 6: a four-byte absolute expiry precedes the content
//   bits 0-5: non-empty logical key length minus one
// This supports all logical key sizes from zero through 64 while retaining both metadata bits.
class MergeTable {
 private:
  struct StoredEntry;

 public:
  static constexpr size_t kMaxKeySize = 64;
  static constexpr uint8_t kBlockCapacity = 8;
  static constexpr uint8_t kChainedBlockTag = kBlockCapacity + 1;
  static constexpr size_t kMinBucketCount = 16;
  static constexpr uint32_t kMaxBatchLen = 32;

  // Keep this layout at 16 bytes: one tagged pointer and one byte-sized hash cache for the first
  // block. Chained blocks store their own hash cache in their allocation.
  struct Bucket {
    uint64_t pointer = 0;
    uint64_t hashes = 0;
  };
  static_assert(sizeof(Bucket) == 16);
  static_assert(alignof(Bucket) == alignof(uint64_t));

  class iterator {
   public:
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = std::string_view;
    using pointer = const std::string_view*;
    using reference = std::string_view;

    iterator() = default;

    reference operator*() const {
      assert(owner_ != nullptr);
      return DecodeEntry(block_, chained_, state_, pos_, decoded_);
    }

    pointer operator->() const {
      value_ = operator*();
      return &value_;
    }

    iterator& operator++() {
      assert(owner_ != nullptr);

      ++pos_;
      if (pos_ < LocalCount(state_))
        return *this;

      if (state_ == kChainedBlockTag) {
        const TaggedPtr next = ReadTagged(block_);
        block_ = RawPointer(next);
        state_ = State(next);
        chained_ = true;
        pos_ = 0;
        return *this;
      }

      ++bucket_;
      AdvanceToNextBucket();
      return *this;
    }

    iterator operator++(int) {
      iterator copy = *this;
      ++*this;
      return copy;
    }

    bool operator==(const iterator& other) const {
      if (owner_ == nullptr || other.owner_ == nullptr)
        return owner_ == other.owner_;

      assert(owner_ == other.owner_);
      return bucket_ == other.bucket_ && block_ == other.block_ && pos_ == other.pos_;
    }

    bool operator!=(const iterator& other) const {
      return !(*this == other);
    }

    explicit operator bool() const {
      return owner_ != nullptr;
    }

    bool HasExpiry() const {
      assert(owner_ != nullptr);
      return EntryHasExpiry(block_, chained_, state_, pos_);
    }

    uint32_t ExpiryTime() const {
      assert(owner_ != nullptr);
      return EntryExpiryTime(block_, chained_, state_, pos_);
    }

    // ttl_sec is relative to the table's abstract time, matching OAHSet.
    void SetExpiryTime(uint32_t ttl_sec) {
      assert(owner_ != nullptr);
      const std::string key(operator*());
      if (owner_->UpdateExpiry(key, ttl_sec)) {
        *this = owner_->Find(key);
      } else {
        *this = owner_->end();
      }
    }

   private:
    friend class MergeTable;

    iterator(MergeTable* owner, size_t bucket, const uint8_t* block, uint8_t state, bool chained,
             uint8_t pos)
        : owner_(owner),
          bucket_(bucket),
          block_(block),
          state_(state),
          chained_(chained),
          pos_(pos) {
    }

    void AdvanceToNextBucket() {
      while (bucket_ < owner_->buckets_.size()) {
        const TaggedPtr pointer = owner_->buckets_[bucket_].pointer;
        if (pointer != 0) {
          block_ = RawPointer(pointer);
          state_ = State(pointer);
          chained_ = false;
          pos_ = 0;
          return;
        }
        ++bucket_;
      }

      owner_ = nullptr;
      block_ = nullptr;
    }

    MergeTable* owner_ = nullptr;
    size_t bucket_ = 0;
    const uint8_t* block_ = nullptr;
    uint8_t state_ = 0;
    bool chained_ = false;
    uint8_t pos_ = 0;
    mutable char decoded_[kMaxKeySize];
    mutable std::string_view value_;
  };

  MergeTable() = default;
  MergeTable(const MergeTable&) = delete;
  MergeTable& operator=(const MergeTable&) = delete;

  ~MergeTable() {
    FreeAllBuckets();
  }

  // Adds key if it is absent. Keys longer than kMaxKeySize are rejected.
  bool Add(std::string_view key, uint32_t ttl_sec = kNoExpiry) {
    if (!IsSupportedKey(key))
      return false;

    const QueryKey query(key);
    const uint64_t hash = HashContent(query.content);
    if (!buckets_.empty() && FindQuery(query, hash) != end())
      return false;

    if (buckets_.empty())
      GrowCapacity(kMinBucketCount);

    // The table has eight physical slots per bucket. A resize is only triggered for a new key,
    // never for a duplicate at the capacity boundary.
    if (size_ == Capacity())
      GrowCapacity(BucketCount() * 2);

    Bucket& bucket = buckets_[BucketIndex(hash)];
    AppendKnownUnique(&bucket, query.content, query.Metadata(ttl_sec != kNoExpiry),
                      EntryTTL(ttl_sec), Fingerprint(hash));
    ++size_;
    expiration_used_ |= ttl_sec != kNoExpiry;
    return true;
  }

  // keepttl=true leaves a duplicate's existing TTL unchanged. With keepttl=false, a supplied TTL
  // updates every duplicate while newly inserted keys always receive ttl_sec.
  unsigned AddMany(absl::Span<std::string_view> keys, uint32_t ttl_sec = kNoExpiry,
                   bool keepttl = true) {
    const size_t supported = std::count_if(
        keys.begin(), keys.end(), [](std::string_view key) { return IsSupportedKey(key); });
    Reserve(size_ + supported);

    unsigned added = 0;
    const bool has_ttl = ttl_sec != kNoExpiry;
    for (std::string_view key : keys) {
      if (Add(key, ttl_sec)) {
        ++added;
      } else if (has_ttl && !keepttl) {
        UpdateExpiry(key, ttl_sec);
      }
    }
    return added;
  }

  bool Erase(std::string_view key) {
    if (!IsSupportedKey(key) || buckets_.empty())
      return false;

    const QueryKey query(key);
    const uint64_t hash = HashContent(query.content);
    if (FindQuery(query, hash) == end())
      return false;

    Bucket& bucket = buckets_[BucketIndex(hash)];
    std::vector<EntryCopy> retained;
    retained.reserve(CountEntries(bucket) - 1);
    bool removed = false;
    VisitBucket(bucket, [&](const StoredEntry& entry, uint8_t fingerprint) {
      if (!removed && Matches(entry, query)) {
        removed = true;
      } else {
        retained.emplace_back(entry, fingerprint);
      }
    });
    assert(removed);

    RebuildBucket(&bucket, retained);
    --size_;
    return true;
  }

  iterator Find(std::string_view key) {
    if (!IsSupportedKey(key) || buckets_.empty())
      return end();

    const QueryKey query(key);
    return FindQuery(query, HashContent(query.content));
  }

  iterator Find(std::string_view key) const {
    return const_cast<MergeTable*>(this)->Find(key);
  }

  bool Contains(std::string_view key) const {
    return Find(key) != end();
  }

  iterator begin() {
    CollectExpired();
    for (size_t bucket = 0; bucket < buckets_.size(); ++bucket) {
      const TaggedPtr pointer = buckets_[bucket].pointer;
      if (pointer != 0)
        return iterator(this, bucket, RawPointer(pointer), State(pointer), false, 0);
    }
    return end();
  }

  iterator begin() const {
    return const_cast<MergeTable*>(this)->begin();
  }

  iterator end() {
    return iterator{};
  }

  iterator end() const {
    return iterator{};
  }

  // Calls cb for the contents of one non-empty bucket and returns the next cursor. Bucket ids use
  // the high hash bits, so the cursor remains valid when the table grows or shrinks between calls.
  // Views are valid only during cb.
  using ItemCb = std::function<void(std::string_view)>;
  uint32_t Scan(uint32_t cursor, const ItemCb& cb) {
    if (buckets_.empty())
      return 0;

    const uint32_t bucket_log = BucketLog();
    const uint32_t cursor_shift = 32 - bucket_log;
    size_t bucket = static_cast<uint64_t>(cursor) >> cursor_shift;
    for (; bucket < buckets_.size(); ++bucket) {
      if (expiration_used_)
        PruneExpiredInBucket(&buckets_[bucket]);
      if (buckets_[bucket].pointer == 0)
        continue;

      char decoded[kMaxKeySize];
      VisitBucket(buckets_[bucket],
                  [&](const StoredEntry& entry, uint8_t) { cb(Decode(entry, decoded)); });
      return bucket + 1 == buckets_.size() ? 0
                                           : static_cast<uint32_t>((bucket + 1) << cursor_shift);
    }
    return 0;
  }

  uint32_t Scan(uint32_t cursor, const ItemCb& cb) const {
    return const_cast<MergeTable*>(this)->Scan(cursor, cb);
  }

  iterator GetRandomMember() {
    CollectExpired();
    if (size_ == 0)
      return end();

    static thread_local absl::InsecureBitGen generator;
    const size_t start = absl::Uniform<size_t>(generator, 0, buckets_.size());
    for (size_t offset = 0; offset < buckets_.size(); ++offset) {
      const size_t bucket = (start + offset) & (buckets_.size() - 1);
      const TaggedPtr pointer = buckets_[bucket].pointer;
      if (pointer != 0)
        return iterator(this, bucket, RawPointer(pointer), State(pointer), false, 0);
    }

    return end();
  }

  iterator GetRandomMember() const {
    return const_cast<MergeTable*>(this)->GetRandomMember();
  }

  // Ensures room for count members without a table resize.
  void Reserve(size_t count) {
    if (count == 0)
      return;
    GrowCapacity((count + kBlockCapacity - 1) / kBlockCapacity);
  }

  // Shrinks to at most the requested number of buckets, while retaining enough buckets for all
  // current members at the fixed eight-member load threshold.
  void Shrink(size_t bucket_count) {
    CollectExpired();
    if (buckets_.empty())
      return;

    const size_t required = (size_ + kBlockCapacity - 1) / kBlockCapacity;
    const size_t requested = std::max({bucket_count, required, kMinBucketCount});
    const size_t target = std::bit_ceil(requested);
    if (target >= buckets_.size())
      return;

    Rehash(target);
  }

  void Clear() {
    FreeAllBuckets();
    buckets_.clear();
    size_ = 0;
    expiration_used_ = false;
    assert(block_alloc_used_ == 0);
  }

  // Clears the requested portion of the physical slot capacity and returns the next cursor. Whole
  // packed buckets are reclaimed at once, while the cursor remains in the [0, Capacity()] domain
  // used by OAHSet::ClearStep. The bucket array stays allocated for asynchronous deletion.
  uint32_t ClearStep(uint32_t start, uint32_t count) {
    const size_t total = Capacity();
    const size_t begin = std::min(static_cast<size_t>(start), total);
    const size_t end = std::min(total, begin + count);
    if (begin == end)
      return static_cast<uint32_t>(end);

    const size_t first_bucket = begin / kBlockCapacity;
    const size_t last_bucket =
        std::min(buckets_.size(), (end + kBlockCapacity - 1) / kBlockCapacity);
    for (size_t bucket = first_bucket; bucket < last_bucket; ++bucket) {
      const size_t removed = CountEntries(buckets_[bucket]);
      FreeBucket(&buckets_[bucket]);
      assert(removed <= size_);
      size_ -= removed;
    }
    if (size_ == 0)
      expiration_used_ = false;
    return static_cast<uint32_t>(end);
  }

  void Fill(MergeTable* other) const {
    assert(other != nullptr);
    assert(other != this);
    assert(other->size_ == 0);

    auto* source = const_cast<MergeTable*>(this);
    source->CollectExpired();
    other->Reserve(source->size_);
    other->set_time(source->time_now());
    other->expiration_used_ = false;

    for (const Bucket& bucket : source->buckets_) {
      VisitBucket(bucket, [&](const StoredEntry& entry, uint8_t fingerprint) {
        const std::string_view content = entry.Content();
        const uint64_t hash = HashContent(content);
        Bucket& target = other->buckets_[other->BucketIndex(hash)];
        other->AppendKnownUnique(&target, content, entry.metadata, entry.ExpiryTime(), fingerprint);
        ++other->size_;
        other->expiration_used_ |= entry.HasExpiry();
      });
    }
  }

  size_t UpperBoundSize() const {
    return size_;
  }

  size_t SizeSlow() {
    CollectExpired();
    return size_;
  }

  size_t SizeSlow() const {
    return const_cast<MergeTable*>(this)->SizeSlow();
  }

  bool Empty() const {
    return size_ == 0;
  }

  size_t BucketCount() const {
    return buckets_.size();
  }

  size_t Capacity() const {
    return buckets_.size() * kBlockCapacity;
  }

  // set_time uses the same abstract-time contract as OAHSet. A zero value disables expiration
  // collection, which is useful while serializing a snapshot.
  void set_time(uint32_t value) {
    time_now_ = value;
  }

  uint32_t time_now() const {
    return time_now_;
  }

  bool ExpirationUsed() const {
    return expiration_used_;
  }

  // Object memory is the allocator-reported usable size of all packed bucket blocks. Set memory
  // is the fixed 16-byte bucket array, matching OAHSet's accounting convention.
  size_t ObjMallocUsed() const {
    return block_alloc_used_;
  }

  size_t SetMallocUsed() const {
    return buckets_.capacity() * sizeof(Bucket);
  }

  size_t MallocUsed() const {
    return ObjMallocUsed() + SetMallocUsed();
  }

  // Hashes the same physical representation used by the table: long 7-bit ASCII keys are packed
  // before hashing, while all other keys are hashed verbatim.
  static uint64_t Hash(std::string_view key) {
    const ASCIIStr packed(key);
    return HashContent(packed.content());
  }

 private:
  using TaggedPtr = uint64_t;
  using Buckets = std::vector<Bucket, StatelessAllocator<Bucket>>;

  static constexpr uint64_t kPointerMask = (uint64_t{1} << 60) - 1;
  static constexpr uint64_t kMetadataMask = ~kPointerMask;
  static constexpr uint8_t kMetadataShift = 60;
  static constexpr uint8_t kAsciiBit = uint8_t{1} << 7;
  static constexpr uint8_t kExpiryBit = uint8_t{1} << 6;
  static constexpr uint8_t kLengthMask = kExpiryBit - 1;
  static constexpr uint32_t kNoExpiry = std::numeric_limits<uint32_t>::max();
  static_assert(sizeof(uintptr_t) == sizeof(uint64_t));

  struct TaggedSlot {
    Bucket* bucket = nullptr;
    uint8_t* block = nullptr;

    TaggedPtr Get() const {
      return block == nullptr ? bucket->pointer : ReadTagged(block);
    }

    void Set(TaggedPtr pointer) const {
      if (block == nullptr) {
        bucket->pointer = pointer;
      } else {
        WriteTagged(block, pointer);
      }
    }
  };

  struct QueryKey {
    explicit QueryKey(std::string_view key)
        : packed(key),
          content(packed.content()),
          logical_length(static_cast<uint8_t>(packed.len())),
          ascii_packed(packed.encoded()) {
      assert(key.size() <= kMaxKeySize);
    }

    uint8_t Metadata(bool has_expiry) const {
      return MakeMetadata(logical_length, ascii_packed, has_expiry);
    }

    ASCIIStr packed;
    std::string_view content;
    uint8_t logical_length;
    bool ascii_packed;
  };

  struct StoredEntry {
    const uint8_t* payload;
    uint8_t metadata;

    bool HasExpiry() const {
      return HasExpiryMetadata(metadata);
    }

    bool IsAsciiPacked() const {
      return IsAsciiPackedMetadata(metadata);
    }

    uint8_t LogicalLength() const {
      return MetadataLength(metadata);
    }

    size_t ContentBytes() const {
      return MergeTable::ContentBytes(metadata);
    }

    size_t PayloadBytes() const {
      return EntryPayloadBytes(metadata);
    }

    uint32_t ExpiryTime() const {
      uint32_t expiry = kNoExpiry;
      if (HasExpiry())
        std::memcpy(&expiry, payload, sizeof(expiry));
      return expiry;
    }

    std::string_view Content() const {
      const char* content =
          reinterpret_cast<const char*>(payload + (HasExpiry() ? sizeof(uint32_t) : 0));
      return {content, ContentBytes()};
    }
  };

  struct EntryCopy {
    EntryCopy(const StoredEntry& entry, uint8_t entry_fingerprint)
        : content(entry.Content()),
          metadata(entry.metadata),
          expiry(entry.ExpiryTime()),
          fingerprint(entry_fingerprint) {
    }

    std::string content;
    uint8_t metadata;
    uint32_t expiry;
    uint8_t fingerprint;
  };

  static bool IsSupportedKey(std::string_view key) {
    return key.size() <= kMaxKeySize;
  }

  static uint8_t MakeMetadata(uint8_t logical_length, bool ascii_packed, bool has_expiry) {
    assert(logical_length <= kMaxKeySize);
    // The one otherwise-ambiguous zero length code represents a raw one-byte key. Use the ASCII
    // flag as an empty-key sentinel; the ASCII codec never packs a one-byte key.
    uint8_t metadata = logical_length == 0 ? kAsciiBit : logical_length - 1;
    if (ascii_packed)
      metadata |= kAsciiBit;
    if (has_expiry)
      metadata |= kExpiryBit;
    return metadata;
  }

  static uint8_t MetadataLength(uint8_t metadata) {
    const uint8_t length_code = metadata & kLengthMask;
    return length_code == 0 && (metadata & kAsciiBit) ? 0 : length_code + 1;
  }

  static bool HasExpiryMetadata(uint8_t metadata) {
    return metadata & kExpiryBit;
  }

  static bool IsAsciiPackedMetadata(uint8_t metadata) {
    return (metadata & kAsciiBit) && MetadataLength(metadata) != 0;
  }

  static size_t ContentBytes(uint8_t metadata) {
    const uint8_t logical_length = MetadataLength(metadata);
    return IsAsciiPackedMetadata(metadata) ? detail::binpacked_len(logical_length) : logical_length;
  }

  static size_t EntryPayloadBytes(uint8_t metadata) {
    return (HasExpiryMetadata(metadata) ? sizeof(uint32_t) : 0) + ContentBytes(metadata);
  }

  static uint64_t HashContent(std::string_view content) {
    constexpr uint64_t kHashSeed = 24061983;
    const char* data = content.empty() ? "" : content.data();
    return rapidhashMicro_withSeed(data, content.size(), kHashSeed);
  }

  static uint8_t Fingerprint(uint64_t hash) {
    return static_cast<uint8_t>(hash);
  }

  uint32_t EntryTTL(uint32_t ttl_sec) const {
    return ttl_sec == kNoExpiry ? kNoExpiry : time_now_ + ttl_sec;
  }

  bool IsExpired(const StoredEntry& entry) const {
    return time_now_ != 0 && entry.HasExpiry() && entry.ExpiryTime() <= time_now_;
  }

  size_t BucketIndex(uint64_t hash) const {
    assert(!buckets_.empty());
    const uint32_t bucket_log = BucketLog();
    return bucket_log == 0 ? 0 : hash >> (64 - bucket_log);
  }

  uint32_t BucketLog() const {
    assert(!buckets_.empty());
    assert(std::has_single_bit(buckets_.size()));
    return std::bit_width(buckets_.size()) - 1;
  }

  static uint8_t State(TaggedPtr pointer) {
    const uint8_t state = static_cast<uint8_t>(pointer >> kMetadataShift);
    assert(state <= kChainedBlockTag);
    return state;
  }

  static uint8_t* RawPointer(TaggedPtr pointer) {
    return reinterpret_cast<uint8_t*>(static_cast<uintptr_t>(pointer & kPointerMask));
  }

  static TaggedPtr MakeTagged(const void* pointer, uint8_t state) {
    assert(state <= kChainedBlockTag);
    const uint64_t raw = reinterpret_cast<uintptr_t>(pointer);
    assert((raw & kMetadataMask) == 0);
    assert(pointer != nullptr || state == 0);
    return raw | (uint64_t{state} << kMetadataShift);
  }

  static TaggedPtr ReadTagged(const uint8_t* source) {
    TaggedPtr result;
    std::memcpy(&result, source, sizeof(result));
    return result;
  }

  static void WriteTagged(uint8_t* dest, TaggedPtr pointer) {
    std::memcpy(dest, &pointer, sizeof(pointer));
  }

  static uint8_t LocalCount(uint8_t state) {
    assert(state > 0 && state <= kChainedBlockTag);
    return state == kChainedBlockTag ? kBlockCapacity : state;
  }

  // A full block stores a next link before all other block-local fields. Chained blocks always
  // store their one-byte hash cache; roots use Bucket::hashes instead.
  static size_t PrefixBytes(bool chained, uint8_t state) {
    return (state == kChainedBlockTag ? sizeof(TaggedPtr) : 0) + (chained ? sizeof(uint64_t) : 0);
  }

  static const uint8_t* Sizes(const uint8_t* block, bool chained, uint8_t state) {
    return block + PrefixBytes(chained, state);
  }

  static uint8_t* Sizes(uint8_t* block, bool chained, uint8_t state) {
    return block + PrefixBytes(chained, state);
  }

  static size_t DataBytes(const uint8_t* block, bool chained, uint8_t state) {
    const uint8_t* sizes = Sizes(block, chained, state);
    size_t bytes = 0;
    for (uint8_t i = 0; i < LocalCount(state); ++i)
      bytes += EntryPayloadBytes(sizes[i]);
    return bytes;
  }

  static size_t BlockBytes(const uint8_t* block, bool chained, uint8_t state) {
    return PrefixBytes(chained, state) + LocalCount(state) + DataBytes(block, chained, state);
  }

  static StoredEntry BlockEntry(const uint8_t* block, bool chained, uint8_t state, uint8_t index) {
    const uint8_t count = LocalCount(state);
    assert(index < count);
    const uint8_t* sizes = Sizes(block, chained, state);
    size_t offset = 0;
    for (uint8_t i = 0; i < index; ++i)
      offset += EntryPayloadBytes(sizes[i]);
    return {sizes + count + offset, sizes[index]};
  }

  static std::string_view Decode(const StoredEntry& entry, char* decoded) {
    if (!entry.IsAsciiPacked())
      return {entry.Content().data(), entry.LogicalLength()};

    detail::ascii_unpack(reinterpret_cast<const uint8_t*>(entry.Content().data()),
                         entry.LogicalLength(), decoded);
    return {decoded, entry.LogicalLength()};
  }

  static std::string_view DecodeEntry(const uint8_t* block, bool chained, uint8_t state,
                                      uint8_t index, char* decoded) {
    return Decode(BlockEntry(block, chained, state, index), decoded);
  }

  static bool EntryHasExpiry(const uint8_t* block, bool chained, uint8_t state, uint8_t index) {
    return BlockEntry(block, chained, state, index).HasExpiry();
  }

  static uint32_t EntryExpiryTime(const uint8_t* block, bool chained, uint8_t state,
                                  uint8_t index) {
    return BlockEntry(block, chained, state, index).ExpiryTime();
  }

  static const uint8_t* Hashes(const Bucket& bucket, const uint8_t* block, bool chained,
                               uint8_t state) {
    if (!chained)
      return reinterpret_cast<const uint8_t*>(&bucket.hashes);
    return block + (state == kChainedBlockTag ? sizeof(TaggedPtr) : 0);
  }

  static uint8_t* Hashes(Bucket& bucket, uint8_t* block, bool chained, uint8_t state) {
    if (!chained)
      return reinterpret_cast<uint8_t*>(&bucket.hashes);
    return block + (state == kChainedBlockTag ? sizeof(TaggedPtr) : 0);
  }

  static bool Matches(const StoredEntry& entry, const QueryKey& query) {
    if (entry.IsAsciiPacked() != query.ascii_packed ||
        entry.LogicalLength() != query.logical_length) {
      return false;
    }
    return entry.Content() == query.content;
  }

  template <typename Fn> static void VisitBucket(const Bucket& bucket, Fn&& fn) {
    TaggedPtr pointer = bucket.pointer;
    if (pointer == 0)
      return;

    const uint8_t* block = RawPointer(pointer);
    uint8_t state = State(pointer);
    bool chained = false;
    while (true) {
      const uint8_t count = LocalCount(state);
      const uint8_t* hashes = Hashes(bucket, block, chained, state);
      for (uint8_t i = 0; i < count; ++i)
        fn(BlockEntry(block, chained, state, i), hashes[i]);

      if (state != kChainedBlockTag)
        return;

      pointer = ReadTagged(block);
      block = RawPointer(pointer);
      state = State(pointer);
      chained = true;
    }
  }

  static size_t CountEntries(const Bucket& bucket) {
    size_t count = 0;
    VisitBucket(bucket, [&](const StoredEntry&, uint8_t) { ++count; });
    return count;
  }

  uint8_t* AllocateBlock(size_t bytes) {
    auto* block = static_cast<uint8_t*>(zmalloc(bytes));
    block_alloc_used_ += zmalloc_usable_size(block);
    return block;
  }

  uint8_t* ReallocateBlock(uint8_t* block, size_t bytes) {
    const size_t old_size = zmalloc_usable_size(block);
    auto* resized = static_cast<uint8_t*>(zrealloc(block, bytes));
    const size_t new_size = zmalloc_usable_size(resized);
    if (new_size >= old_size) {
      block_alloc_used_ += new_size - old_size;
    } else {
      block_alloc_used_ -= old_size - new_size;
    }
    return resized;
  }

  void FreeBlock(uint8_t* block) {
    block_alloc_used_ -= zmalloc_usable_size(block);
    zfree(block);
  }

  static void WritePayload(uint8_t* dest, std::string_view content, uint8_t metadata,
                           uint32_t expiry) {
    if (HasExpiryMetadata(metadata)) {
      std::memcpy(dest, &expiry, sizeof(expiry));
      dest += sizeof(expiry);
    }
    if (!content.empty())
      std::memcpy(dest, content.data(), content.size());
  }

  uint8_t* AllocateChainedBlock(std::string_view content, uint8_t metadata, uint32_t expiry,
                                uint8_t fingerprint) {
    // Partial chained blocks start with their hash cache. Their count lives in the incoming
    // tagged pointer, so they acquire a next pointer only when promoted from eight to nine keys.
    uint8_t* block = AllocateBlock(sizeof(uint64_t) + 1 + EntryPayloadBytes(metadata));
    std::memset(block, 0, sizeof(uint64_t));
    block[0] = fingerprint;
    block[sizeof(uint64_t)] = metadata;
    WritePayload(block + sizeof(uint64_t) + 1, content, metadata, expiry);
    return block;
  }

  void AppendToPartialBlock(const TaggedSlot& slot, Bucket* bucket, uint8_t* block, bool chained,
                            uint8_t state, std::string_view content, uint8_t metadata,
                            uint32_t expiry, uint8_t fingerprint) {
    assert(state > 0 && state < kBlockCapacity);

    const size_t prefix = PrefixBytes(chained, state);
    const size_t data_bytes = DataBytes(block, chained, state);
    const size_t old_bytes = prefix + state + data_bytes;
    block = ReallocateBlock(block, old_bytes + 1 + EntryPayloadBytes(metadata));

    uint8_t* sizes = Sizes(block, chained, state);
    std::memmove(sizes + state + 1, sizes + state, data_bytes);
    sizes[state] = metadata;
    WritePayload(sizes + state + 1 + data_bytes, content, metadata, expiry);
    Hashes(*bucket, block, chained, state)[state] = fingerprint;
    slot.Set(MakeTagged(block, state + 1));
  }

  void PromoteFullBlock(const TaggedSlot& slot, uint8_t* block, bool chained,
                        std::string_view content, uint8_t metadata, uint32_t expiry,
                        uint8_t fingerprint) {
    assert(State(slot.Get()) == kBlockCapacity);

    const size_t old_bytes = BlockBytes(block, chained, kBlockCapacity);
    block = ReallocateBlock(block, old_bytes + sizeof(TaggedPtr));
    std::memmove(block + sizeof(TaggedPtr), block, old_bytes);

    uint8_t* child = AllocateChainedBlock(content, metadata, expiry, fingerprint);
    WriteTagged(block, MakeTagged(child, 1));
    slot.Set(MakeTagged(block, kChainedBlockTag));
  }

  void AppendKnownUnique(Bucket* bucket, std::string_view content, uint8_t metadata,
                         uint32_t expiry, uint8_t fingerprint) {
    if (bucket->pointer == 0) {
      uint8_t* block = AllocateBlock(1 + EntryPayloadBytes(metadata));
      block[0] = metadata;
      WritePayload(block + 1, content, metadata, expiry);
      bucket->pointer = MakeTagged(block, 1);
      bucket->hashes = fingerprint;
      return;
    }

    TaggedSlot slot{bucket, nullptr};
    TaggedPtr pointer = slot.Get();
    uint8_t* block = RawPointer(pointer);
    uint8_t state = State(pointer);
    bool chained = false;

    while (state == kChainedBlockTag) {
      slot = TaggedSlot{nullptr, block};
      pointer = slot.Get();
      block = RawPointer(pointer);
      state = State(pointer);
      chained = true;
    }

    if (state < kBlockCapacity) {
      AppendToPartialBlock(slot, bucket, block, chained, state, content, metadata, expiry,
                           fingerprint);
    } else {
      PromoteFullBlock(slot, block, chained, content, metadata, expiry, fingerprint);
    }
  }

  void RebuildBucket(Bucket* bucket, const std::vector<EntryCopy>& retained) {
    FreeBucket(bucket);
    for (const EntryCopy& entry : retained) {
      AppendKnownUnique(bucket, entry.content, entry.metadata, entry.expiry, entry.fingerprint);
    }
  }

  bool PruneExpiredInBucket(Bucket* bucket) {
    if (!expiration_used_ || time_now_ == 0 || bucket->pointer == 0)
      return false;

    bool has_expired = false;
    VisitBucket(*bucket,
                [&](const StoredEntry& entry, uint8_t) { has_expired |= IsExpired(entry); });
    if (!has_expired)
      return false;

    std::vector<EntryCopy> retained;
    retained.reserve(CountEntries(*bucket));
    size_t removed = 0;
    VisitBucket(*bucket, [&](const StoredEntry& entry, uint8_t fingerprint) {
      if (IsExpired(entry)) {
        ++removed;
      } else {
        retained.emplace_back(entry, fingerprint);
      }
    });
    assert(removed <= size_);
    RebuildBucket(bucket, retained);
    size_ -= removed;
    return true;
  }

  void CollectExpired() {
    if (!expiration_used_ || time_now_ == 0)
      return;
    for (Bucket& bucket : buckets_)
      PruneExpiredInBucket(&bucket);
  }

  iterator FindQuery(const QueryKey& query, uint64_t hash) {
    if (buckets_.empty())
      return end();

    const size_t bucket_index = BucketIndex(hash);
    Bucket& bucket = buckets_[bucket_index];
    if (expiration_used_)
      PruneExpiredInBucket(&bucket);

    TaggedPtr pointer = bucket.pointer;
    if (pointer == 0)
      return end();

    const uint8_t fingerprint = Fingerprint(hash);
    const uint8_t* block = RawPointer(pointer);
    uint8_t state = State(pointer);
    bool chained = false;
    while (true) {
      const uint8_t count = LocalCount(state);
      const uint8_t* hashes = Hashes(bucket, block, chained, state);
      for (uint8_t i = 0; i < count; ++i) {
        if (hashes[i] == fingerprint && Matches(BlockEntry(block, chained, state, i), query))
          return iterator(this, bucket_index, block, state, chained, i);
      }

      if (state != kChainedBlockTag)
        return end();

      pointer = ReadTagged(block);
      block = RawPointer(pointer);
      state = State(pointer);
      chained = true;
    }
  }

  bool UpdateExpiry(std::string_view key, uint32_t ttl_sec) {
    if (!IsSupportedKey(key) || buckets_.empty())
      return false;

    const QueryKey query(key);
    const uint64_t hash = HashContent(query.content);
    if (FindQuery(query, hash) == end())
      return false;

    Bucket& bucket = buckets_[BucketIndex(hash)];
    std::vector<EntryCopy> entries;
    entries.reserve(CountEntries(bucket));
    bool updated = false;
    VisitBucket(bucket, [&](const StoredEntry& entry, uint8_t fingerprint) {
      entries.emplace_back(entry, fingerprint);
      EntryCopy& copy = entries.back();
      if (!updated && Matches(entry, query)) {
        copy.metadata |= kExpiryBit;
        copy.expiry = EntryTTL(ttl_sec);
        updated = true;
      }
    });
    assert(updated);
    RebuildBucket(&bucket, entries);
    expiration_used_ = true;
    return true;
  }

  void FreeBucket(Bucket* bucket) {
    TaggedPtr pointer = bucket->pointer;
    while (pointer != 0) {
      uint8_t* block = RawPointer(pointer);
      const uint8_t state = State(pointer);
      const TaggedPtr next = state == kChainedBlockTag ? ReadTagged(block) : 0;
      FreeBlock(block);
      pointer = next;
    }

    bucket->pointer = 0;
    bucket->hashes = 0;
  }

  void FreeAllBuckets() {
    for (Bucket& bucket : buckets_)
      FreeBucket(&bucket);
  }

  void GrowCapacity(size_t requested_bucket_count) {
    const size_t target = std::bit_ceil(std::max(kMinBucketCount, requested_bucket_count));
    if (buckets_.empty() && size_ == 0) {
      buckets_.resize(target);
    } else if (target > buckets_.size()) {
      Rehash(target);
    }
  }

  void Rehash(size_t bucket_count) {
    assert(std::has_single_bit(bucket_count));
    Buckets old;
    old.swap(buckets_);
    buckets_.resize(bucket_count);

    for (const Bucket& bucket : old) {
      VisitBucket(bucket, [&](const StoredEntry& entry, uint8_t fingerprint) {
        const std::string_view content = entry.Content();
        const uint64_t hash = HashContent(content);
        AppendKnownUnique(&buckets_[BucketIndex(hash)], content, entry.metadata, entry.ExpiryTime(),
                          fingerprint);
      });
    }
    for (Bucket& bucket : old)
      FreeBucket(&bucket);
  }

  size_t size_ = 0;
  size_t block_alloc_used_ = 0;
  uint32_t time_now_ = 0;
  bool expiration_used_ = false;
  Buckets buckets_;
};

}  // namespace dfly
