// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "core/json/detail/interned_string.h"

namespace {
constexpr auto kLoadFactorToShrinkPool = 0.2;

thread_local dfly::InternedStringStats tl_stats;

}  // namespace

namespace dfly::detail {

InternedString& InternedString::operator=(InternedString other) {
  swap(other);
  return *this;
}

void InternedString::ResetPool() {
  InternedBlobPool& pool = GetPoolRef();
  for (InternedBlobHandle handle : pool) {
    InternedBlobHandle::Destroy(handle);
  }
  pool.clear();

  // Pool hits and misses are not reset, they are monotonically increasing counters
  // TODO reset these two fields in config resetstats
  tl_stats.pool_bytes = 0;
  tl_stats.pool_entries = 0;
  tl_stats.pool_table_bytes = 0;
  tl_stats.live_references = 0;
}

InternedBlobHandle InternedString::Intern(const std::string_view sv) {
  if (sv.empty())
    return {};

  tl_stats.live_references += 1;
  InternedBlobPool& pool_ref = GetPoolRef();
  if (const auto it = pool_ref.find(sv); it != pool_ref.end()) {
    tl_stats.hits++;
    InternedBlobHandle blob = *it;
    blob.IncrRefCount();
    return blob;
  }

  InternedBlobHandle handle = InternedBlobHandle::Create(sv);
  pool_ref.emplace(handle);
  tl_stats.pool_entries++;
  tl_stats.pool_bytes += handle.MemUsed();
  tl_stats.misses++;
  return handle;
}

void InternedString::Acquire() {  // NOLINT
  if (!entry_)
    return;

  tl_stats.live_references += 1;
  entry_.IncrRefCount();
}

void InternedString::Release() {
  if (!entry_)
    return;

  entry_.DecrRefCount();
  tl_stats.live_references -= 1;

  if (entry_.RefCount() == 0) {
    InternedBlobPool& pool_ref = GetPoolRef();
    pool_ref.erase(entry_);
    tl_stats.pool_entries--;
    tl_stats.pool_bytes -= entry_.MemUsed();
    InternedBlobHandle::Destroy(entry_);

    // When pool is underutilized, shrink it by swapping.
    if (const auto load_factor = pool_ref.load_factor();
        ABSL_PREDICT_FALSE(load_factor > 0 && load_factor < kLoadFactorToShrinkPool)) {
      // The LHS of swap is a new pool constructed from the original pool reference. The RHS is the
      // original pool. After the swap, the temporary is destroyed. Note that this is not a strict
      // shrink. The new pool internally allocates enough capacity so that the load factor is around
      // 0.8. So the capacity after swap is still larger than size, but the load factor is improved.
      InternedBlobPool(pool_ref).swap(pool_ref);
    }
  }
}

InternedBlobPool& InternedString::GetPoolRef() {
  // Note on lifetimes: this pool is thread local and depends on the thread local memory resource
  // defined in the stateless allocator in src/core/detail/stateless_allocator.h. Since there is no
  // well-defined order of destruction, this pool must be manually reset before the memory resource
  // destruction.
  thread_local InternedBlobPool pool;
  return pool;
}

}  // namespace dfly::detail

namespace dfly {

InternedStringStats& InternedStringStats::operator+=(const InternedStringStats& other) {
  pool_entries += other.pool_entries;
  pool_bytes += other.pool_bytes;
  hits += other.hits;
  misses += other.misses;
  pool_table_bytes += other.pool_table_bytes;
  live_references += other.live_references;
  return *this;
}

InternedStringStats GetInternedStringStats() {
  tl_stats.pool_table_bytes =
      detail::InternedString::GetPoolRef().capacity() * (sizeof(detail::InternedBlobHandle) + 1);
  return tl_stats;
}

}  // namespace dfly
