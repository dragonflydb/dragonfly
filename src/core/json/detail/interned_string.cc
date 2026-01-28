// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "core/json/detail/interned_string.h"

#include <glog/logging.h>

namespace dfly::detail {

InternedString::InternedString() {
}

InternedString::InternedString(const std::string_view sv) : entry_(Intern(sv)) {
}

InternedString::InternedString(const InternedString& other) {
  entry_ = other.entry_;
  Acquire();
}

InternedString::InternedString(InternedString&& other) noexcept : entry_(other.entry_) {
  other.entry_ = nullptr;
}

InternedString& InternedString::operator=(const InternedString& other) {
  if (this != &other) {
    Release();
    entry_ = other.entry_;
    Acquire();
  }
  return *this;
}

InternedString& InternedString::operator=(InternedString&& other) noexcept {
  if (this != &other) {
    Release();
    entry_ = other.entry_;
    other.entry_ = nullptr;
  }
  return *this;
}

InternedString::~InternedString() {
  Release();
}

InternedString::operator std::string_view() const {
  return entry_ ? entry_->View() : std::string_view{};
}

const char* InternedString::data() const {
  return entry_ ? entry_->Data() : "";
}

const char* InternedString::c_str() const {
  return data();
}

void InternedString::swap(InternedString& other) noexcept {
  std::swap(entry_, other.entry_);
}

size_t InternedString::length() const {
  return entry_ ? entry_->Size() : 0;
}

size_t InternedString::size() const {
  return length();
}

int InternedString::compare(const InternedString& other) const {
  return std::string_view{*this}.compare(std::string_view{other});
}

int InternedString::compare(std::string_view other) const {
  return std::string_view{*this}.compare(other);
}

bool InternedString::operator==(const InternedString& other) const {
  // Compare pointers since we store them in the same pool
  return entry_ == other.entry_;
}

bool InternedString::operator!=(const InternedString& other) const {
  return entry_ != other.entry_;
}

bool InternedString::operator<(const InternedString& other) const {
  return compare(other) < 0;
}
void InternedString::shrink_to_fit() {  // NOLINT (must be non-const to align with jsoncons usage)
  DCHECK(entry_ != nullptr);
}

void InternedString::ResetPool() {
  InternedBlobPool& pool = GetPoolRef();
  StatelessAllocator<InternedBlob> alloc;
  for (InternedBlob* blob : pool) {
    blob->~InternedBlob();
    alloc.deallocate(blob, 1);
  }
  pool.clear();
}

InternedBlob* InternedString::Intern(const std::string_view sv) {
  if (sv.empty())
    return nullptr;

  auto& pool_ref = GetPoolRef();
  if (auto it = pool_ref.find(sv); it != pool_ref.end()) {
    InternedBlob* blob = *it;
    blob->IncrRefCount();
    return blob;
  }

  void* ptr = StatelessAllocator<InternedBlob>{}.allocate(1);
  auto blob = new (ptr) InternedBlob(sv);
  auto [new_elem, _] = pool_ref.emplace(blob);
  return *new_elem;
}

void InternedString::Acquire() {  // NOLINT
  if (entry_) {
    entry_->IncrRefCount();
  }
}

void InternedString::Release() {
  if (!entry_)
    return;

  entry_->DecrRefCount();

  if (entry_->RefCount() == 0) {
    GetPoolRef().erase(entry_);
    entry_->~InternedBlob();
    StatelessAllocator<InternedBlob>{}.deallocate(entry_, 1);
    entry_ = nullptr;
  }
}

InternedBlobPool& InternedString::GetPoolRef() {
  thread_local InternedBlobPool pool;
  return pool;
}

size_t InternedString::MemUsed() const {
  return entry_ ? entry_->MemUsed() : 0;
}

}  // namespace dfly::detail
