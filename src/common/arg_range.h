// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/types/span.h>

#include <string_view>
#include <variant>

#include "base/iterator.h"

namespace cmn {

using ArgSlice = absl::Span<const std::string_view>;
using OwnedArgSlice = absl::Span<const std::string>;

inline std::string_view ToSV(std::string_view slice) {
  return slice;
}

inline std::string_view ToSV(const std::string& slice) {
  return slice;
}

inline std::string_view ToSV(std::string&& slice) = delete;

constexpr auto kToSV = [](auto&& v) { return ToSV(std::forward<decltype(v)>(v)); };

struct ArgRange {
  ArgRange(ArgRange&&) = default;
  ArgRange(const ArgRange&) = default;
  ArgRange(ArgRange& range) : ArgRange((const ArgRange&)range) {
  }

  template <typename T, std::enable_if_t<!std::is_same_v<ArgRange, T>, bool> = true>
  ArgRange(T&& span) : span(std::forward<T>(span)) {  // NOLINT google-explicit-constructor)
  }

  size_t Size() const {
    return std::visit([](const auto& span) { return span.size(); }, span);
  }

  auto Range() const {
    return base::it::Wrap(kToSV, span);
  }

  auto begin() const {
    return Range().first;
  }

  auto end() const {
    return Range().second;
  }

  std::string_view operator[](size_t idx) const {
    return std::visit([idx](const auto& span) -> std::string_view { return span[idx]; }, span);
  }

  std::variant<ArgSlice, OwnedArgSlice> span;
};

}  // namespace cmn
