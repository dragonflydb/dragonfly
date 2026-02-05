// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "core/json/detail/interned_string.h"

namespace {
constexpr auto kLoadFactorToShrinkPool = 0.2;
}

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
}

InternedBlobHandle InternedString::Intern(const std::string_view sv) {
  if (sv.empty())
    return {};

  InternedBlobPool& pool_ref = GetPoolRef();
  if (const auto it = pool_ref.find(sv); it != pool_ref.end()) {
    InternedBlobHandle blob = *it;
    blob.IncrRefCount();
    return blob;
  }

  InternedBlobHandle handle = InternedBlobHandle::Create(sv);
  pool_ref.emplace(handle);
  return handle;
}

void InternedString::Acquire() {  // NOLINT
  if (!entry_)
    return;

  entry_.IncrRefCount();
}

void InternedString::Release() {
  if (!entry_)
    return;

  entry_.DecrRefCount();

  if (entry_.RefCount() == 0) {
    InternedBlobPool& pool_ref = GetPoolRef();
    pool_ref.erase(entry_);
    InternedBlobHandle::Destroy(entry_);

    if (const auto load_factor = pool_ref.load_factor();
        ABSL_PREDICT_FALSE(load_factor > 0 && load_factor < kLoadFactorToShrinkPool)) {
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
