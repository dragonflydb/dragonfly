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
    return AddOrUpdateImpl({.key = ASCIIStr(field), .value = value, .ttl_sec = ttl_sec}, keepttl);
  }

  // Returns false (no update) if the field already exists.
  bool AddOrSkip(std::string_view field, std::string_view value, uint32_t ttl_sec = UINT32_MAX) {
    return AddPairImpl<AddMode::kSkip>(
        {.key = ASCIIStr(field), .value = value, .ttl_sec = ttl_sec});
  }

  // Like AddOrUpdate but on update returns the previous entry (RAII-owned; freed on destruction);
  // empty if a new field was added.
  OwnedOAHPair AddOrExchange(std::string_view field, std::string_view value,
                             uint32_t ttl_sec = UINT32_MAX, bool keepttl = false) {
    TaggedPtr previous = 0;
    AddOrUpdateImpl({.key = ASCIIStr(field), .value = value, .ttl_sec = ttl_sec}, keepttl,
                    &previous);
    return OwnedOAHPair(previous);
  }

  std::optional<std::string_view> GetValue(std::string_view field) {
    auto it = Find(field);
    return it == end() ? std::nullopt : std::optional{(*it).Value()};
  }

  // Removes `field` and returns it RAII-owned (freed on destruction), or empty if absent.
  OwnedOAHPair Extract(std::string_view field) {
    if (entries_.empty())
      return {};
    const ASCIIStr key(field);
    const uint64_t hash = Hash(key.content());
    const uint32_t bid = BucketId(hash, capacity_log_);
    const uint64_t ext_hash = CalcExtHash(hash, capacity_log_);
    const LaneMasks masks = ProbeWindowShifted(&entries_[bid], ext_hash << oah::kExtHashShift);

    TaggedPtr* matched = nullptr;
    TaggedPtr* base = entries_.data();
    for (uint32_t cand_bits = masks.candidates; cand_bits; cand_bits &= cand_bits - 1) {
      TaggedPtr* cell = &base[bid + std::countr_zero(cand_bits)];
      if (OAHPair(*cell).KeyMatches(key.content(), key.len())) {
        matched = cell;
        break;
      }
    }
    const uint32_t ext_bid = GetExtensionPoint(bid);
    bool in_vector = false;
    if (!matched && At(ext_bid).IsVector()) {
      matched = ProbeExtensionVector(ext_bid, key.content(), key.len(), ext_hash);
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
                         std::vector<std::string>& vals, bool with_value);

 private:
  enum class AddMode { kReplace, kKeepTtl, kSkip };

  struct AddParams {
    const ASCIIStr key;
    const std::string_view value;
    const uint32_t ttl_sec;
  };

  bool AddOrUpdateImpl(const AddParams& params, bool keepttl, TaggedPtr* previous_out = nullptr) {
    if (keepttl)
      return AddPairImpl<AddMode::kKeepTtl>(params, previous_out);
    return AddPairImpl<AddMode::kReplace>(params, previous_out);
  }

  TaggedPtr MakePair(const AddParams& params, uint32_t ttl_sec, uint64_t shifted_ext_hash) {
    if (ttl_sec != UINT32_MAX)
      expiration_used_ = true;
    TaggedPtr ptr =
        OAHPair::Create(params.key.content(), params.key.len(), params.value, EntryTTL(ttl_sec));
    OAHPair pair(ptr);
    pair.SetShiftedExtHash(shifted_ext_hash);
    return ptr;
  }

  // Map insertion core. Normal replacements retain the preallocated hot path, while duplicate
  // skips and KEEPTTL replacements defer allocation until this probe resolves the existing entry.
  template <AddMode kMode>
  bool AddPairImpl(const AddParams& params, TaggedPtr* previous_out = nullptr) {
    TaggedPtr new_pair = 0;
    size_t new_pair_alloc_size = 0;

    TryGrow();
    assert(Capacity() >= kDisplacementSize);

    uint64_t hash = Hash(params.key.content());
    auto bucket_id = BucketId(hash, capacity_log_);
    oah::PrefetchRead(entries_.data() + bucket_id);

    const uint64_t ext_hash = CalcExtHash(hash, capacity_log_);
    const uint64_t shifted_ext_hash = ext_hash << oah::kExtHashShift;

    if constexpr (kMode == AddMode::kReplace) {
      new_pair = MakePair(params, params.ttl_sec, shifted_ext_hash);
      new_pair_alloc_size = OAHPair(new_pair).AllocSize();
    }

    const uint32_t ext_bid = GetExtensionPoint(bucket_id);
    oah::PrefetchRead(At(ext_bid).Raw());

    const LaneMasks masks = ProbeWindowShifted(&entries_[bucket_id], shifted_ext_hash);

    TaggedPtr* matched = nullptr;
    TaggedPtr* base = entries_.data();
    for (uint32_t cand_bits = masks.candidates; cand_bits; cand_bits &= cand_bits - 1) {
      TaggedPtr* cell = &base[bucket_id + std::countr_zero(cand_bits)];
      if (OAHPair(*cell).KeyMatches(params.key.content(), params.key.len())) {
        matched = cell;
        break;
      }
    }
    if (!matched && At(ext_bid).IsVector())
      matched = ProbeExtensionVector(ext_bid, params.key.content(), params.key.len(), ext_hash);

    if (matched) {
      OAHPair dup(*matched);
      ExpireIfNeeded(dup);
      if (!dup.Empty()) {
        if constexpr (kMode == AddMode::kSkip) {
          return false;
        } else {
          if constexpr (kMode == AddMode::kKeepTtl) {
            uint32_t ttl = params.ttl_sec;
            if (dup.HasExpiry())
              ttl = dup.GetExpiry() - time_now_;
            new_pair = MakePair(params, ttl, shifted_ext_hash);
            new_pair_alloc_size = OAHPair(new_pair).AllocSize();
          }

          obj_alloc_used_ -= dup.AllocSize();
          TaggedPtr previous = dup.Release();
          if (previous_out)
            *previous_out = previous;
          else
            OAHPair::Destroy(previous);
          obj_alloc_used_ += new_pair_alloc_size;
          *matched = new_pair;
          return false;
        }
      }
    }

    if constexpr (kMode != AddMode::kReplace) {
      new_pair = MakePair(params, params.ttl_sec, shifted_ext_hash);
      new_pair_alloc_size = OAHPair(new_pair).AllocSize();
    }

    obj_alloc_used_ += new_pair_alloc_size;
    ++size_;
    if (matched) {  // reuse the just-reaped expired cell
      *matched = new_pair;
    } else if (masks.empties) {
      At(bucket_id + std::countr_zero(masks.empties)).Assign(new_pair);
    } else {
      ptr_vectors_alloc_used_ += At(ext_bid).InsertNonEmpty(new_pair);  // window full
    }
    return true;
  }
};

}  // namespace dfly
