// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <bit>
#include <cassert>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "core/oah_base.h"
#include "core/oah_pair.h"
#include "core/oah_table.h"

namespace dfly {

// OAHMap - open-addressing hash map. Adds the map-specific insertion path (key + value with
// replace/exchange semantics) on top of OAHTable. Mirrors StringMap's surface.
class OAHMap : public OAHTable<OAHPair> {
 public:
  // Returns true if added, false if an existing field was updated.
  bool AddOrUpdate(std::string_view field, std::string_view value, uint32_t ttl_sec = UINT32_MAX,
                   bool keepttl = false) {
    TaggedPtr new_tp = MakePair(field, value, ComputeTtl(field, ttl_sec, keepttl));
    return AddPairImpl(field, new_tp, /*replace=*/true, nullptr);
  }

  // Returns false (no update) if the field already exists.
  bool AddOrSkip(std::string_view field, std::string_view value, uint32_t ttl_sec = UINT32_MAX) {
    return AddPairImpl(field, MakePair(field, value, ttl_sec), /*replace=*/false, nullptr);
  }

  // Like AddOrUpdate but on update returns the previous entry (RAII-owned; freed on destruction);
  // empty if a new field was added.
  OwnedOAHPair AddOrExchange(std::string_view field, std::string_view value,
                             uint32_t ttl_sec = UINT32_MAX, bool keepttl = false) {
    TaggedPtr new_tp = MakePair(field, value, ComputeTtl(field, ttl_sec, keepttl));
    TaggedPtr old_tp = 0;
    AddPairImpl(field, new_tp, /*replace=*/true, &old_tp);
    return OwnedOAHPair(old_tp);
  }

  std::optional<std::string_view> GetValue(std::string_view field) {
    auto it = Find(field);
    return it == end() ? std::nullopt : std::optional{(*it).Value()};
  }

  // Removes `field` and returns it RAII-owned (freed on destruction), or empty if absent.
  OwnedOAHPair Extract(std::string_view field) {
    if (entries_.empty())
      return {};
    const uint64_t hash = Hash(field);
    const uint32_t bid = BucketId(hash, capacity_log_);
    const uint64_t ext_hash = CalcExtHash(hash, capacity_log_);
    const LaneMasks masks = ProbeWindowShifted(&entries_[bid], ext_hash << oah::kExtHashShift);

    TaggedPtr* matched = nullptr;
    TaggedPtr* base = entries_.data();
    for (uint32_t cand_bits = masks.candidates; cand_bits; cand_bits &= cand_bits - 1) {
      TaggedPtr* cell = &base[bid + std::countr_zero(cand_bits)];
      if (OAHPair(*cell).Key() == field) {
        matched = cell;
        break;
      }
    }
    const uint32_t ext_bid = GetExtensionPoint(bid);
    bool in_vector = false;
    if (!matched && At(ext_bid).IsVector()) {
      matched = ProbeExtensionVector(ext_bid, field, ext_hash);
      in_vector = matched != nullptr;
    }
    if (!matched)
      return {};

    OAHPair victim(*matched);
    const bool expired = IsExpired(victim);
    --size_;
    obj_alloc_used_ -= victim.AllocSize();
    OwnedOAHPair result(victim.Release());

    if (in_vector) {  // reclaim the vector if the extract emptied it
      OAHPtr<OAHPair> bucket = At(ext_bid);
      auto vec = bucket.AsVector();
      if (vec.Empty()) {
        ptr_vectors_alloc_used_ -= vec.AllocSize();
        bucket.Clear();
      }
    }

    if (expired)  // already-expired target => report absent (like Redis); result frees the blob
      return {};
    return result;
  }

  // Selects up to `count` unique live pairs via single-pass threshold sampling (Algorithm S),
  // mirroring StringMap::RandomPairsUnique. Only the picked pairs are copied.
  void RandomPairsUnique(unsigned count, std::vector<std::string>& keys,
                         std::vector<std::string>& vals, bool with_value) {
    unsigned total = SizeSlow();
    if (count > total)
      count = total;

    keys.reserve(keys.size() + count);
    if (with_value)
      vals.reserve(vals.size() + count);

    static thread_local absl::InsecureBitGen rng;
    unsigned index = 0, remaining = count;
    for (auto it = begin(), it_end = end(); remaining && it != it_end; ++it, ++index) {
      double threshold = double(remaining) / (total - index);
      if (absl::Uniform(rng, 0.0, 1.0) <= threshold) {
        OAHPair e = *it;
        keys.emplace_back(e.Key());
        if (with_value)
          vals.emplace_back(e.Value());
        --remaining;
      }
    }
  }

 private:
  uint32_t ComputeTtl(std::string_view field, uint32_t ttl_sec, bool keepttl) {
    if (keepttl) {
      auto it = Find(field);
      if (it != end() && it.HasExpiry() && it.ExpiryTime() > time_now_)
        return it.ExpiryTime() - time_now_;
    }
    return ttl_sec;
  }

  // Creates a pair blob for field/value with a relative ttl, flagging expiry use.
  TaggedPtr MakePair(std::string_view field, std::string_view value, uint32_t ttl) {
    if (ttl != UINT32_MAX)
      expiration_used_ = true;
    return OAHPair::Create(field, value, EntryTTL(ttl));
  }

  // Map insertion core. On a live duplicate: replace=false destroys new_tp; replace=true swaps it
  // in, returning the old TaggedPtr via *old_out (or destroying it when null). Returns true only on
  // a fresh insert.
  bool AddPairImpl(std::string_view field, TaggedPtr new_tp, bool replace, TaggedPtr* old_out) {
    if (size_ >= entries_.size()) [[unlikely]] {
      Reserve(BucketCount() * 2);
    }
    assert(Capacity() >= kDisplacementSize);

    uint64_t hash = Hash(field);
    auto bucket_id = BucketId(hash, capacity_log_);
    oah::PrefetchRead(entries_.data() + bucket_id);

    const uint64_t ext_hash = CalcExtHash(hash, capacity_log_);
    const uint64_t shifted_ext_hash = ext_hash << oah::kExtHashShift;
    OAHPair(new_tp).SetShiftedExtHash(shifted_ext_hash);
    const size_t entry_alloc_size = OAHPair(new_tp).AllocSize();

    const uint32_t ext_bid = GetExtensionPoint(bucket_id);
    oah::PrefetchRead(At(ext_bid).Raw());

    const LaneMasks masks = ProbeWindowShifted(&entries_[bucket_id], shifted_ext_hash);

    TaggedPtr* matched = nullptr;
    TaggedPtr* base = entries_.data();
    for (uint32_t cand_bits = masks.candidates; cand_bits; cand_bits &= cand_bits - 1) {
      TaggedPtr* cell = &base[bucket_id + std::countr_zero(cand_bits)];
      if (OAHPair(*cell).Key() == field) {
        matched = cell;
        break;
      }
    }
    if (!matched && At(ext_bid).IsVector())
      matched = ProbeExtensionVector(ext_bid, field, ext_hash);

    if (matched) {
      OAHPair dup(*matched);
      ExpireIfNeeded(dup);
      if (!dup.Empty()) {
        if (!replace) {
          OAHPair::Destroy(new_tp);
          return false;
        }
        obj_alloc_used_ -= dup.AllocSize();
        TaggedPtr old_tp = dup.Release();
        if (old_out)
          *old_out = old_tp;
        else
          OAHPair::Destroy(old_tp);
        obj_alloc_used_ += entry_alloc_size;
        *matched = new_tp;
        return false;  // updated, not newly added
      }
      // Reuse the just-reaped expired cell in place.
      obj_alloc_used_ += entry_alloc_size;
      ++size_;
      *matched = new_tp;
      return true;
    }

    obj_alloc_used_ += entry_alloc_size;
    ++size_;
    if (masks.empties) {
      At(bucket_id + std::countr_zero(masks.empties)).Assign(new_tp);
    } else {
      ptr_vectors_alloc_used_ += At(ext_bid).InsertNonEmpty(new_tp);  // window full
    }
    return true;
  }
};

}  // namespace dfly
