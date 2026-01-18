// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/types/span.h>

#include <string_view>
#include <variant>

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

  template <typename T, std::enable_if_t<!std::is_same_v<std::decay_t<T>, ArgRange>, bool> = true>
  ArgRange(T&& span) : span(std::forward<T>(span)) {  // NOLINT google-explicit-constructor)
  }

  size_t Size() const {
    return std::visit([](const auto& span) { return span.size(); }, span);
  }

  auto view() const {
    return std::views::iota(size_t{0}, Size()) |
           std::views::transform([this](size_t i) { return (*this)[i]; });
  }

  std::string_view operator[](size_t idx) const {
    return std::visit([idx](const auto& span) -> std::string_view { return span[idx]; }, span);
  }

  std::variant<ArgSlice, OwnedArgSlice> span;
};

}  // namespace cmn
