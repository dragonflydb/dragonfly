// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/string_stats.h"

#include <absl/strings/str_cat.h>

#include "base/logging.h"

namespace {

void MakeHLL(HllBufferPtr* p) {
  p->size = getDenseHllSize();
  p->hll = new uint8_t[p->size];
  CHECK_EQ(0, createDenseHll(*p));
}

}  // namespace

namespace dfly {

using namespace container_utils;

UniqueStrings::UniqueStrings() {
  MakeHLL(&counter_);
}

UniqueStrings::UniqueStrings(UniqueStrings&& other) noexcept
    : total_count{other.total_count}, total_bytes{other.total_bytes}, counter_{other.counter_} {
  other.counter_ = HllBufferPtr{};
}

UniqueStrings& UniqueStrings::operator=(UniqueStrings&& other) noexcept {
  if (this == &other) {
    return *this;
  }

  delete[] counter_.hll;
  counter_ = other.counter_;
  total_count = other.total_count;
  total_bytes = other.total_bytes;
  other.counter_ = HllBufferPtr{};
  return *this;
}

void UniqueStrings::AddHMap(const PrimeValue& pv) {
  // Only adds the keys of a map
  IterateMap(pv, [&](const ContainerEntry& k, const auto&) { return AddString(k); });
}

void UniqueStrings::AddSet(const PrimeValue& pv) {
  IterateSet(pv, [&](const ContainerEntry& e) { return AddString(e); });
}

void UniqueStrings::AddList(const PrimeValue& pv) {
  IterateList(pv, [&](const ContainerEntry& e) { return AddString(e); });
}

void UniqueStrings::AddZSet(const PrimeValue& pv) {
  IterateSortedSet(pv, [&](const ContainerEntry& e, auto) { return AddString(e); });
}

void UniqueStrings::Add(UniqueStrings&& other) {
  total_count += other.total_count;
  total_bytes += other.total_bytes;
  HllBufferPtr inputs[2] = {other.counter_, counter_};
  CHECK_EQ(0, pfmerge(inputs, 2, counter_));
}

std::string UniqueStrings::ToString(std::string_view label) const {
  if (total_count == 0)
    return {};
  std::string result;
  absl::StrAppend(&result, label, ":\n");
  absl::StrAppend(&result, "  total strings: ", total_count, "\n");
  absl::StrAppend(&result, "  unique strings: ", UniqueCount(), "\n");
  absl::StrAppend(&result, "  total bytes: ", total_bytes, "\n");
  absl::StrAppend(&result, "  average length: ", AverageLength(), "\n");
  absl::StrAppend(&result, "  estimated savings: ", ByteSavingsOnDedup(), " bytes\n");
  return result;
}

bool UniqueStrings::AddString(const ContainerEntry& e) {  // NOLINT must always return true
  // Count both strings and ints, because ints might be used as keys and will benefit from
  // deduplication just like strings.
  if (e.IsString()) {
    CHECK_NE(-1, pfadd_dense(counter_, reinterpret_cast<const unsigned char*>(e.data()), e.size()));
    ++total_count;
    total_bytes += e.size();
  } else {
    char buf[absl::numbers_internal::kFastToBufferSize];
    const char* end = absl::numbers_internal::FastIntToBuffer(e.as_long(), buf);
    const auto size = end - buf;
    const int result = pfadd_dense(counter_, reinterpret_cast<const unsigned char*>(buf), size);
    CHECK_NE(-1, result);
    ++total_count;
    total_bytes += size;
  }
  return true;
}

uint64_t UniqueStrings::ByteSavingsOnDedup() const {
  const auto uniques = UniqueCount();
  const auto diff = total_count > uniques ? total_count - uniques : 0;
  return diff * AverageLength();
}

}  // namespace dfly
