// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/types/span.h>

#include <bit>
#include <cassert>
#include <string_view>

#include "core/oah_base.h"
#include "core/oah_entry.h"
#include "core/oah_table.h"
#include "core/string_set.h"

namespace dfly {

// OAHSet - open-addressing hash set. Adds the set-specific insertion path on top of OAHTable's
// shared machinery; AddImpl is a separately optimized hot path, independent of the map's.
class OAHSet : public OAHTable<OAHEntry> {
 public:
  // Inserts `str` (optional TTL); returns false if already present.
  bool Add(std::string_view str, uint32_t ttl_sec = UINT32_MAX) {
    const ASCIIStr key(str);
    return AddImpl(key.content(), key.len(), ttl_sec);
  }

  // keepttl=true: existing entries are left alone (current/legacy behavior).
  // keepttl=false: when ttl_sec is set, existing entries' expiry is updated to ttl_sec.
  unsigned AddMany(absl::Span<std::string_view> span, uint32_t ttl_sec = UINT32_MAX,
                   bool keepttl = true) {
    Reserve(span.size());
    unsigned res = 0;
    const bool has_ttl = ttl_sec != UINT32_MAX;
    for (auto& s : span) {
      const ASCIIStr key(s);
      if (AddImpl(key.content(), key.len(), ttl_sec)) {
        ++res;
      } else if (has_ttl && !keepttl) {
        auto it = Find(key.content(), key.len());
        if (it != end())
          it.SetExpiryTime(ttl_sec);
      }
    }
    return res;
  }

  // TODO: Consider using chunks for this as in StringSet
  void Fill(OAHSet* other) {
    assert(other->entries_.empty());
    other->Reserve(UpperBoundSize());
    other->set_time(time_now());
    for (auto it = begin(), it_end = end(); it != it_end; ++it) {
      // Copy the stored (already-encoded) content verbatim -- no decode/re-encode round-trip.
      const oah::key::Stored s = (*it).StoredKey();
      const uint32_t ttl = it.HasExpiry() ? it.ExpiryTime() - time_now() : UINT32_MAX;
      other->AddImpl({s.content, s.header.content_size}, s.header.len, ttl);
    }
  }

 private:
  bool AddImpl(std::string_view content, uint32_t len, uint32_t ttl_sec) {
    if (size_ >= entries_.size()) [[unlikely]] {
      Reserve(BucketCount() * 2);
    }
    assert(Capacity() >= kDisplacementSize);

    uint64_t hash = Hash(content);
    auto bucket_id = BucketId(hash, capacity_log_);
    oah::PrefetchRead(entries_.data() + bucket_id);

    const uint64_t ext_hash = CalcExtHash(hash, capacity_log_);
    const uint64_t shifted_ext_hash = ext_hash << oah::kExtHashShift;

    const ssize_t mem_before = zmalloc_used_memory_tl;
    TaggedPtr entry_tagged_ptr = OAHEntry::Create(content, len, EntryTTL(ttl_sec));
    OAHEntry(entry_tagged_ptr).SetShiftedExtHash(shifted_ext_hash);  // reuse the shifted value
    if (ttl_sec != UINT32_MAX)
      expiration_used_ = true;
    const size_t entry_alloc_size = zmalloc_used_memory_tl - mem_before;

    const uint32_t ext_bid = GetExtensionPoint(bucket_id);
    oah::PrefetchRead(At(ext_bid).Raw());

    const LaneMasks masks = ProbeWindowShifted(&entries_[bucket_id], shifted_ext_hash);

    // Add/Find/Erase each inline their own probe (measured faster than a shared helper). Add needs
    // only the matched cell, for the duplicate check and slot reuse.
    TaggedPtr* matched = nullptr;
    TaggedPtr* base = entries_.data();
    for (uint32_t cand_bits = masks.candidates; cand_bits; cand_bits &= cand_bits - 1) {
      TaggedPtr* cell = &base[bucket_id + std::countr_zero(cand_bits)];
      if (OAHEntry(*cell).KeyMatches(content, len)) {
        matched = cell;
        break;
      }
    }
    if (!matched && At(ext_bid).IsVector())
      matched = ProbeExtensionVector(ext_bid, content, len, ext_hash);

    if (matched) {
      OAHEntry dup(*matched);
      ExpireIfNeeded(dup);  // reap an already-expired duplicate so its cell can be reused
      if (!dup.Empty()) {
        OAHEntry::Destroy(entry_tagged_ptr);  // live duplicate
        return false;
      }
      // Reaped an expired duplicate: its cell is empty, so reuse it in place.
      obj_alloc_used_ += entry_alloc_size;
      ++size_;
      *matched = entry_tagged_ptr;
      return true;
    }

    obj_alloc_used_ += entry_alloc_size;
    ++size_;
    if (masks.empties) {
      At(bucket_id + std::countr_zero(masks.empties)).Assign(entry_tagged_ptr);
    } else {
      ptr_vectors_alloc_used_ += At(ext_bid).InsertNonEmpty(entry_tagged_ptr);  // window full
    }
    return true;
  }
};

// Snapshot of --use_oah_set captured once at startup.
inline bool g_use_oah_set = false;

// Dispatches a generic lambda over the runtime-selected set type (StringSet or
// OAHSet) backing kEncodingStrMap2 SETs; both expose the same surface.
template <typename Fn> auto VisitSet(void* ptr, Fn&& fn) {
  return g_use_oah_set ? fn(static_cast<OAHSet*>(ptr)) : fn(static_cast<StringSet*>(ptr));
}

// Current member from either iterator type. OAHSet returns a small owning/view object: it
// references raw entry bytes directly and owns decoded ASCII bytes inline. Callers keep that object
// alive while using GetKeyView(), so decoded views remain valid across nested calls and fiber
// yields.
inline std::string_view Key(StringSet::iterator it) {
  sds s = *it;
  return {s, sdslen(s)};
}

inline oah::key::Decoded Key(OAHSet::iterator it) {
  return OAHSet::DecodeKey(*it);
}

inline std::string_view GetKeyView(std::string_view key) {
  return key;
}

inline std::string_view GetKeyView(sds key) {
  return {key, sdslen(key)};
}

inline std::string_view GetKeyView(const oah::key::Decoded& key) {
  return key.view();
}

inline std::string_view GetKeyView(oah::key::Decoded&&) = delete;
inline std::string_view GetKeyView(const oah::key::Decoded&&) = delete;

}  // namespace dfly
