// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/base.h"

#include <absl/strings/numbers.h>

namespace dfly::search {

std::string_view QueryParams::operator[](std::string_view name) const {
  if (auto it = params.find(name); it != params.end())
    return it->second;
  return "";
}

std::string& QueryParams::operator[](std::string_view k) {
  return params[k];
}

WrappedStrPtr::WrappedStrPtr(const PMR_NS::string& s)
    : ptr{std::make_unique<char[]>(s.size() + 1)} {
  std::strcpy(ptr.get(), s.c_str());
}

WrappedStrPtr::WrappedStrPtr(const std::string& s) : ptr{std::make_unique<char[]>(s.size() + 1)} {
  std::strcpy(ptr.get(), s.c_str());
}

bool WrappedStrPtr::operator<(const WrappedStrPtr& other) const {
  return std::strcmp(ptr.get(), other.ptr.get()) < 0;
}

bool WrappedStrPtr::operator>=(const WrappedStrPtr& other) const {
  return !operator<(other);
}

WrappedStrPtr::operator std::string_view() const {
  return std::string_view{ptr.get(), std::strlen(ptr.get())};
}

std::optional<double> ParseNumericField(std::string_view value) {
  double value_as_double;
  if (absl::SimpleAtod(value, &value_as_double))
    return value_as_double;
  return std::nullopt;
}

}  // namespace dfly::search
