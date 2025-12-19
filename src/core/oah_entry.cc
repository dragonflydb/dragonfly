// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/oah_entry.h"

#include "base/hash.h"
#include "base/logging.h"

namespace dfly {

OAHEntry::OAHEntry(std::string_view key, uint32_t expiry) {
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

// returns the expiry time of the current entry or UINT32_MAX if no expiry is set.
uint32_t OAHEntry::GetExpiry() const {
  std::uint32_t res = UINT32_MAX;
  if (HasExpiry()) {
    assert(!IsVector());
    std::memcpy(&res, Raw(), sizeof(res));
  }
  return res;
}

bool OAHEntry::CheckBucketAffiliation(uint32_t bucket_id, uint32_t capacity_log,
                                      uint32_t shift_log) {
  assert(!IsVector());
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

uint64_t OAHEntry::CalcExtHash(uint64_t hash, uint32_t capacity_log, uint32_t shift_log) {
  const uint32_t start_hash_bit = capacity_log > shift_log ? capacity_log - shift_log : 0;
  const uint32_t ext_hash_shift = 64 - start_hash_bit - kExtHashSize;
  const uint64_t ext_hash = (hash >> ext_hash_shift) & kExtHashMask;
  return ext_hash;
}

bool OAHEntry::CheckNoCollisions(const uint64_t ext_hash) {
  auto stored_hash = GetHash();
  return ((stored_hash != ext_hash) & (stored_hash != 0)) | (Empty());
}

bool OAHEntry::CheckExtendedHash(const uint64_t ext_hash, uint32_t capacity_log,
                                 uint32_t shift_log) {
  auto stored_hash = GetHash();
  if (!stored_hash) {
    if (IsEntry()) {
      stored_hash = SetHash(Hash(Key()), capacity_log, shift_log);
    } else {
      return false;
    }
  }
  return stored_hash == ext_hash;
}

// shift_log identify which bucket the element belongs to
uint64_t OAHEntry::SetHash(uint64_t hash, uint32_t capacity_log, uint32_t shift_log) {
  assert(data_);
  assert(!IsVector());
  const uint64_t result_hash = CalcExtHash(hash, capacity_log, shift_log);
  const uint64_t ext_hash = result_hash << kExtHashShift;
  data_ = (data_ & ~kExtHashShiftedMask) | ext_hash;
  return result_hash;
}

// return new bucket_id
uint32_t OAHEntry::Rehash(uint32_t current_bucket_id, uint32_t prev_capacity_log,
                          uint32_t new_capacity_log, uint32_t shift_log) {
  assert(!IsVector());
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

  const uint32_t expected_bucket_id = BucketId(Hash(Key()), new_capacity_log);
  assert(expected_bucket_id == new_bucket_id);

  return new_bucket_id;
}

void OAHEntry::SetExpiry(uint32_t at_sec) {
  assert(!IsVector());
  if (HasExpiry()) {
    auto* expiry_pos = Raw();
    std::memcpy(expiry_pos, &at_sec, sizeof(at_sec));
  } else {
    *this = OAHEntry(Key(), at_sec);
  }
}

// TODO refactor, because it's inefficient
std::optional<uint32_t> OAHEntry::Find(std::string_view str, uint64_t ext_hash,
                                       uint32_t capacity_log, uint32_t shift_log,
                                       uint32_t* set_size, uint32_t time_now) {
  if (IsEntry()) {
    ExpireIfNeeded(time_now, set_size);
    return CheckExtendedHash(ext_hash, capacity_log, shift_log) && Key() == str
               ? 0
               : std::optional<uint32_t>();
  }
  if (IsVector()) {
    auto& vec = AsVector();
    auto raw_arr = vec.Raw();
    for (size_t i = 0, size = vec.Size(); i < size; ++i) {
      raw_arr[i].ExpireIfNeeded(time_now, set_size);
      if (raw_arr[i].CheckExtendedHash(ext_hash, capacity_log, shift_log) &&
          raw_arr[i].Key() == str) {
        return i;
      }
    }
  }

  return std::nullopt;
}

void OAHEntry::ExpireIfNeeded(uint32_t time_now, uint32_t* set_size) {
  assert(!IsVector());
  if (GetExpiry() <= time_now) {
    Clear();
    --*set_size;
  }
}

// TODO refactor, because it's inefficient
uint32_t OAHEntry::Insert(OAHEntry&& e) {
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

uint32_t OAHEntry::ElementsNum() {
  if (Empty()) {
    return 0;
  } else if (!IsVector()) {
    return 1;
  }
  return AsVector().Size();
}

// TODO remove, it is inefficient
OAHEntry& OAHEntry::operator[](uint32_t pos) {
  assert(!Empty());
  if (!IsVector()) {
    assert(pos == 0);
    return *this;
  } else {
    auto& arr = AsVector();
    assert(pos < arr.Size());
    return arr[pos];
  }
}

OAHEntry OAHEntry::Remove(uint32_t pos) {
  if (Empty()) {
    // I'm not sure that this scenario should be check at all
    assert(pos == 0);
    return OAHEntry();
  } else if (!IsVector()) {
    assert(pos == 0);
    return std::move(*this);
  } else {
    auto& arr = AsVector();
    assert(pos < arr.Size());
    return std::move(arr[pos]);
  }
}

OAHEntry OAHEntry::Pop() {
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

void OAHEntry::Clear() {
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

uint32_t OAHEntry::GetKeySize() const {
  if (HasSso()) {
    uint8_t size = 0;
    std::memcpy(&size, Raw() + GetExpirySize(), sizeof(size));
    return size;
  }
  uint32_t size = 0;
  std::memcpy(&size, Raw() + GetExpirySize(), sizeof(size));
  return size;
}

void OAHEntry::SetExpiryBit(bool b) {
  if (b)
    data_ |= kExpiryBit;
  else
    data_ &= ~kExpiryBit;
}

size_t OAHEntry::Size() {
  size_t key_field_size = HasSso() ? 1 : 4;
  size_t expiry_field_size = HasExpiry() ? 4 : 0;
  return expiry_field_size + key_field_size + GetKeySize();
}

}  // namespace dfly
