// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/search/base.h"

namespace dfly::search {

std::string_view QueryParams::operator[](std::string_view name) const {
  if (auto it = params.find(name); it != params.end())
    return it->second;
  return "";
}

std::string& QueryParams::operator[](std::string_view k) {
  return params[k];
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

}  // namespace dfly::search
