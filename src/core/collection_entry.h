// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/strings/str_cat.h>

#include <cstddef>
#include <string>
#include <string_view>

namespace dfly {

// Stores either:
// - A single long long value (longval) when value = nullptr
// - A single char* (value) when value != nullptr
struct CollectionEntry {
  CollectionEntry(const char* value, size_t length) : value_{value}, length_{length} {
  }
  explicit CollectionEntry(long long longval) : value_{nullptr}, longval_{longval} {
  }

  CollectionEntry& operator=(const CollectionEntry&) = default;

  std::string ToString() const {
    if (value_)
      return {value_, length_};
    else
      return absl::StrCat(longval_);
  }

  bool IsString() const {
    return value_ != nullptr;
  }

  bool is_int() const {
    return value_ == nullptr;
  }

  const char* data() const {
    return value_;
  }

  size_t size() const {
    return length_;
  }

  long long as_long() const {
    return longval_;
  }

  // Assumes value is not null.
  std::string_view view() const {
    return {value_, length_};
  }

  // compatibility method
  std::string to_string() const {
    return ToString();
  }

  // compatibility method
  long long ival() const {
    return longval_;
  }

  bool operator==(std::string_view sv) const;
  friend bool operator==(std::string_view sv, const CollectionEntry& entry) {
    return entry == sv;
  }

 private:
  const char* value_;
  union {
    size_t length_;
    long long longval_;
  };
};

inline bool CollectionEntry::operator==(std::string_view sv) const {
  if (value_ == nullptr) {
    char buf[absl::numbers_internal::kFastToBufferSize];
    char* end = absl::numbers_internal::FastIntToBuffer(longval_, buf);
    return sv == std::string_view(buf, end - buf);
  }
  return view() == sv;
}

}  // namespace dfly
