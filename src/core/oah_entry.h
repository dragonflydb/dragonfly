// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cassert>
#include <cstring>
#include <string_view>

#include "base/hash.h"
#include "base/logging.h"

extern "C" {
#include "redis/zmalloc.h"
}

namespace dfly {

#define PREFETCH_READ(x) __builtin_prefetch(x, 0, 1)
#define FORCE_INLINE __attribute__((always_inline))

// TODO add allocator support
template <class T> class PtrVector {
  static constexpr size_t kVectorBit = 1ULL << 54;     // first 3 bits aren't used by pointer
  static constexpr size_t kTagMask = (4095ULL << 52);  // we reserve 12 high bits and 3 low bits

  static constexpr size_t kLogSizeShift = 56;
  static constexpr size_t kLogSizeMask = 0xFFULL;
  static constexpr size_t kLogSizeShiftedMask = kLogSizeMask << kLogSizeShift;

 public:
  static PtrVector FromLogSize(uint64_t log_size) {
    return PtrVector(log_size);
  }

  T* begin() const {
    return &Raw()[0];
  }

  T* end() const {
    return &Raw()[Size()];
  }

  PtrVector(PtrVector&& other) {
    uptr_ = other.uptr_;
    other.uptr_ = 0;
  }

  ~PtrVector() {
    Clear();
  }

  size_t LogSize() const {
    return (uptr_ >> kLogSizeShift) & kLogSizeMask;
  }

  size_t Size() const {
    return 1 << LogSize();
  }

  uint64_t Release() {
    uint64_t res = uptr_;
    uptr_ = 0;
    return res;
  }

  void ResizeLog(uint64_t new_log_size) {
    auto new_ptr = reinterpret_cast<T*>(zmalloc(sizeof(T) << new_log_size));
    size_t new_size = 1 << new_log_size;
    const size_t size = std::min(Size(), new_size);
    for (size_t i = 0; i < size; ++i) {
      new (new_ptr + i) T(std::move(Raw()[i]));
    }
    for (size_t i = size; i < new_size; ++i) {
      new (new_ptr + i) T();
    }
    Clear();
    uptr_ = reinterpret_cast<uint64_t>(new_ptr);
    SetLogSize(new_log_size);
  }

  T& operator[](size_t idx) {
    return Raw()[idx];
  }

  const T& operator[](size_t idx) const {
    return Raw()[idx];
  }

  T* Raw() const {
    return (T*)(uptr_ & ~kTagMask);
  }

 private:
  void Clear() {
    const size_t size = Size();
    T* raw = Raw();
    if (!raw)
      return;
    for (size_t i = 0; i < size; ++i) {
      if (raw[i])
        raw[i].~T();
    }

    zfree(Raw());
    uptr_ = 0;
  }

  // because of log_size I prefer to hide it
  PtrVector(uint64_t log_size) {
    DCHECK(log_size <= 32);
    uptr_ = reinterpret_cast<uint64_t>(zmalloc(sizeof(T) << log_size));
    const uint64_t size = 1 << log_size;
    for (uint64_t i = 0; i < size; ++i) {
      new (reinterpret_cast<T*>(uptr_) + i) T();
    }
    SetLogSize(log_size);
  }

  void SetLogSize(uint64_t log_size) {
    uptr_ = (uptr_ & ~kLogSizeShiftedMask) | kVectorBit | (uint64_t(log_size) << kLogSizeShift);
  }

  uint64_t uptr_ = 0;
};

static uint64_t Hash(std::string_view str) {
  constexpr XXH64_hash_t kHashSeed = 24061983;
  return XXH3_64bits_withSeed(str.data(), str.size(), kHashSeed);
}

static uint32_t BucketId(uint64_t hash, uint32_t capacity_log) {
  assert(capacity_log > 0);
  return hash >> (64 - capacity_log);
}
// doesn't possess memory, it should be created and release manually
class OAHEntry {
  // we can assume that high 12 bits of user address space
  // can be used for tagging. At most 52 bits of address are reserved for
  // some configurations, and usually it's 48 bits.
  // https://docs.kernel.org/arch/arm64/memory.html
  // first 3 bits aren't used by pointer
  static constexpr size_t kExpiryBit = 1ULL << 52;

  // if bit is set the string length field is 1 byte instead of 4
  static constexpr size_t kSsoBit = 1ULL << 53;
  static constexpr size_t kVectorBit = 1ULL << 54;

  // extended hash allows us to reduce keys comparisons
  static constexpr size_t kExtHashShift = 56;
  static constexpr uint32_t kExtHashSize = 8;
  static constexpr size_t kExtHashMask = 0xFFULL;
  static constexpr size_t kExtHashShiftedMask = kExtHashMask << kExtHashShift;

  static constexpr size_t kTagMask = (4095ULL << 52);  // we reserve 12 high bits and 3 low.

 public:
  OAHEntry() = default;

  OAHEntry(std::string_view key, uint32_t expiry = UINT32_MAX) {
    uint32_t key_size = key.size();

    uint32_t expiry_size = (expiry != UINT32_MAX) * sizeof(expiry);

    uint32_t key_len_field_size = key_size <= std::numeric_limits<uint8_t>::max() ? 1 : 4;

    auto size = key_len_field_size + key_size + expiry_size;

    auto* expiry_pos = (char*)zmalloc(size);
    data_ = reinterpret_cast<uint64_t>(expiry_pos);
    if (expiry_size) {
      SetExpiryBit(true);
      std::memcpy(expiry_pos, &expiry, sizeof(expiry));
    }

    auto* key_size_pos = expiry_pos + expiry_size;
    if (key_len_field_size == 1) {
      SetSsoBit();
      uint8_t sso_key_size = key_size;
      std::memcpy(key_size_pos, &sso_key_size, key_len_field_size);
    } else {
      std::memcpy(key_size_pos, &key_size, key_len_field_size);
    }

    auto* key_pos = key_size_pos + key_len_field_size;
    std::memcpy(key_pos, key.data(), key_size);
  }

  // TODO add initializer list constructor
  OAHEntry(PtrVector<OAHEntry>&& vec) {
    data_ = vec.Release() | kVectorBit;
  }

  OAHEntry(const OAHEntry& e) = delete;
  OAHEntry(OAHEntry&& e) {
    data_ = e.data_;
    e.data_ = 0;
  }

  // consider manual removing, we waste a lot of time to check nullptr
  ~OAHEntry() {
    Clear();
  }

  OAHEntry& operator=(const OAHEntry& e) = delete;
  OAHEntry& operator=(OAHEntry&& e) {
    std::swap(data_, e.data_);
    return *this;
  }

  FORCE_INLINE bool Empty() const {
    return data_ == 0;
  }

  FORCE_INLINE operator bool() const {
    return !Empty();
  }

  bool IsVector() const {
    return (data_ & kVectorBit) != 0;
  }

  PtrVector<OAHEntry>& AsVector() {
    static_assert(sizeof(PtrVector<OAHEntry>) == sizeof(uint64_t));
    return *reinterpret_cast<PtrVector<OAHEntry>*>(&data_);
  }

  std::string_view Key() const {
    DCHECK(!IsVector());
    return {GetKeyData(), GetKeySize()};
  }

  FORCE_INLINE bool HasExpiry() const {
    return (data_ & kExpiryBit) != 0;
  }

  // returns the expiry time of the current entry or UINT32_MAX if no expiry is set.
  FORCE_INLINE uint32_t GetExpiry() const {
    std::uint32_t res = UINT32_MAX;
    if (HasExpiry()) {
      DCHECK(!IsVector());
      std::memcpy(&res, Raw(), sizeof(res));
    }
    return res;
  }

  // TODO consider another option to implement iterator
  OAHEntry* operator->() {
    return this;
  }

  uint64_t GetHash() const {
    return (data_ & kExtHashShiftedMask) >> kExtHashShift;
  }

  bool CheckBucketAffiliation(uint32_t bucket_id, uint32_t capacity_log, uint32_t shift_log) {
    DCHECK(!IsVector());
    if (Empty())
      return false;
    uint32_t bucket_id_hash_part = capacity_log > shift_log ? shift_log : capacity_log;
    uint32_t bucket_mask = (1 << bucket_id_hash_part) - 1;
    bucket_id &= bucket_mask;
    auto stored_hash = GetHash();
    if (!stored_hash) {
      stored_hash = SetHash(Hash(Key()), capacity_log, shift_log);
    }
    uint32_t stored_bucket_id = stored_hash >> (kExtHashSize - bucket_id_hash_part);
    return bucket_id == stored_bucket_id;
  }

  static uint64_t CalcExtHash(uint64_t hash, uint32_t capacity_log, uint32_t shift_log) {
    const uint32_t start_hash_bit = capacity_log > shift_log ? capacity_log - shift_log : 0;
    const uint32_t ext_hash_shift = 64 - start_hash_bit - kExtHashSize;
    const uint64_t ext_hash = (hash >> ext_hash_shift) & kExtHashMask;
    return ext_hash;
  }

  bool CheckExtendedHash(const uint64_t ext_hash, uint32_t capacity_log, uint32_t shift_log) {
    if (Empty())
      return false;
    auto stored_hash = GetHash();
    if (!stored_hash && !IsVector()) {
      stored_hash = SetHash(Hash(Key()), capacity_log, shift_log);
    }
    return stored_hash == ext_hash;
  }

  // shift_log identify which bucket the element belongs to
  uint64_t SetHash(uint64_t hash, uint32_t capacity_log, uint32_t shift_log) {
    DCHECK(data_);
    DCHECK(!IsVector());
    const uint64_t result_hash = CalcExtHash(hash, capacity_log, shift_log);
    const uint64_t ext_hash = result_hash << kExtHashShift;
    data_ = (data_ & ~kExtHashShiftedMask) | ext_hash;
    return result_hash;
  }

  void ClearHash() {
    data_ &= ~kExtHashShiftedMask;
  }

  // return new bucket_id
  uint32_t Rehash(uint32_t current_bucket_id, uint32_t prev_capacity_log, uint32_t new_capacity_log,
                  uint32_t shift_log) {
    DCHECK(!IsVector());
    auto stored_hash = GetHash();

    const uint32_t logs_diff = new_capacity_log - prev_capacity_log;
    const uint32_t prev_significant_bits =
        prev_capacity_log > shift_log ? shift_log : prev_capacity_log;
    const uint32_t needed_hash_bits = prev_significant_bits + logs_diff;

    if (!stored_hash || needed_hash_bits > kExtHashSize) {
      auto hash = Hash(Key());
      SetHash(hash, new_capacity_log, shift_log);
      return BucketId(hash, new_capacity_log);
    }

    const uint32_t real_bucket_end = stored_hash >> (kExtHashSize - prev_significant_bits);
    const uint32_t prev_shift_mask = (1 << prev_significant_bits) - 1;
    const uint32_t curr_shift = (current_bucket_id - real_bucket_end) & prev_shift_mask;
    const uint32_t prev_bucket_mask = (1 << prev_capacity_log) - 1;
    const uint32_t base_bucket_id = (current_bucket_id - curr_shift) & prev_bucket_mask;

    const uint32_t last_bits_mask = (1 << logs_diff) - 1;
    const uint32_t stored_hash_shift = kExtHashSize - needed_hash_bits;
    const uint32_t last_bits = (stored_hash >> stored_hash_shift) & last_bits_mask;
    const uint32_t new_bucket_id = (base_bucket_id << logs_diff) | last_bits;

    ClearHash();  // the cache is invalid after rehash operation

    DCHECK_EQ(BucketId(Hash(Key()), new_capacity_log), new_bucket_id);

    return new_bucket_id;
  }

  void SetExpiry(uint32_t at_sec) {
    DCHECK(!IsVector());
    if (HasExpiry()) {
      auto* expiry_pos = Raw();
      std::memcpy(expiry_pos, &at_sec, sizeof(at_sec));
    } else {
      *this = OAHEntry(Key(), at_sec);
    }
  }

  // TODO refactor, because it's inefficient
  std::optional<uint32_t> Find(std::string_view str, uint64_t ext_hash, uint32_t capacity_log,
                               uint32_t shift_log, uint32_t* set_size, uint32_t time_now = 0) {
    if (!Empty()) {
      if (!IsVector()) {
        ExpireIfNeeded(time_now, set_size);
        return CheckExtendedHash(ext_hash, capacity_log, shift_log) && Key() == str
                   ? 0
                   : std::optional<uint32_t>();
      }
      auto& vec = AsVector();
      for (size_t i = 0, size = vec.Size(); i < size; ++i) {
        vec[i].ExpireIfNeeded(time_now, set_size);
        if (vec[i].CheckExtendedHash(ext_hash, capacity_log, shift_log) && vec[i].Key() == str) {
          return i;
        }
      }
    }
    return std::nullopt;
  }

  FORCE_INLINE void ExpireIfNeeded(uint32_t time_now, uint32_t* set_size) {
    DCHECK(!IsVector());
    if (GetExpiry() <= time_now) {
      Clear();
      --*set_size;
    }
  }

  // TODO refactor, because it's inefficient
  uint32_t Insert(OAHEntry&& e) {
    if (Empty()) {
      *this = std::move(e);
      return 0;
    } else if (!IsVector()) {
      OAHEntry tmp(PtrVector<OAHEntry>::FromLogSize(1));
      auto& arr = tmp.AsVector();
      arr[0] = std::move(*this);
      arr[1] = std::move(e);
      *this = std::move(tmp);
      return 1;
    } else {
      auto& arr = AsVector();
      size_t i = 0;
      for (; i < arr.Size(); ++i) {
        if (!arr[i]) {
          arr[i] = std::move(e);
          return i;
        }
      }
      auto new_pos = arr.Size();
      arr.ResizeLog(arr.LogSize() + 1);
      arr[new_pos] = (std::move(e));
      return new_pos;
    }
  }

  uint32_t ElementsNum() {
    if (Empty()) {
      return 0;
    } else if (!IsVector()) {
      return 1;
    }
    return AsVector().Size();
  }

  // TODO remove, it is inefficient
  inline OAHEntry& operator[](uint32_t pos) {
    DCHECK(!Empty());
    if (!IsVector()) {
      DCHECK(pos == 0);
      return *this;
    } else {
      auto& arr = AsVector();
      DCHECK(pos < arr.Size());
      return arr[pos];
    }
  }

  OAHEntry Remove(uint32_t pos) {
    if (Empty()) {
      // I'm not sure that this scenario should be check at all
      DCHECK(pos == 0);
      return OAHEntry();
    } else if (!IsVector()) {
      DCHECK(pos == 0);
      return std::move(*this);
    } else {
      auto& arr = AsVector();
      DCHECK(pos < arr.Size());
      return std::move(arr[pos]);
    }
  }

  OAHEntry Pop() {
    if (IsVector()) {
      auto& arr = AsVector();
      for (auto& e : arr) {
        if (e)
          return std::move(e);
      }
      return {};
    }
    return std::move(*this);
  }

  FORCE_INLINE char* Raw() const {
    return (char*)(data_ & ~kTagMask);
  }

 protected:
  void Clear() {
    // TODO add optimization to avoid destructor calls during vector allocator
    if (!data_)
      return;

    if (IsVector()) {
      AsVector().~PtrVector<OAHEntry>();
    } else {
      zfree(Raw());
    }
    data_ = 0;
  }

  const char* GetKeyData() const {
    uint32_t key_field_size = HasSso() ? 1 : 4;
    return Raw() + GetExpirySize() + key_field_size;
  }

  uint32_t GetKeySize() const {
    if (HasSso()) {
      uint8_t size = 0;
      std::memcpy(&size, Raw() + GetExpirySize(), sizeof(size));
      return size;
    }
    uint32_t size = 0;
    std::memcpy(&size, Raw() + GetExpirySize(), sizeof(size));
    return size;
  }

  void SetExpiryBit(bool b) {
    if (b)
      data_ |= kExpiryBit;
    else
      data_ &= ~kExpiryBit;
  }

  void SetVectorBit() {
    data_ |= kVectorBit;
  }

  void SetSsoBit() {
    data_ |= kSsoBit;
  }

  bool HasSso() const {
    return (data_ & kSsoBit) != 0;
  }

  size_t Size() {
    size_t key_field_size = HasSso() ? 1 : 4;
    size_t expiry_field_size = HasExpiry() ? 4 : 0;
    return expiry_field_size + key_field_size + GetKeySize();
  }

  std::uint32_t GetExpirySize() const {
    return HasExpiry() ? sizeof(std::uint32_t) : 0;
  }

  // memory daya layout [Expiry, key_size, key]
  uint64_t data_ = 0;
};

}  // namespace dfly
