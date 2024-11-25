// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string>
#include <string_view>
#include <variant>

namespace dfly {

class StringOrView {
 public:
  static StringOrView FromString(std::string s) {
    StringOrView sov;
    sov.val_ = std::move(s);
    return sov;
  }

  static StringOrView FromView(std::string_view sv) {
    StringOrView sov;
    sov.val_ = sv;
    return sov;
  }

  StringOrView() = default;
  StringOrView(const StringOrView& o) = default;
  StringOrView(StringOrView&& o) = default;
  StringOrView& operator=(const StringOrView& o) = default;
  StringOrView& operator=(StringOrView&& o) = default;

  bool operator==(const StringOrView& o) const {
    return *this == o.view();
  }

  bool operator==(std::string_view o) const {
    return view() == o;
  }

  bool operator!=(const StringOrView& o) const {
    return *this != o.view();
  }

  bool operator!=(std::string_view o) const {
    return !(*this == o);
  }

  std::string_view view() const {
    return visit([](const auto& s) -> std::string_view { return s; }, val_);
  }

  friend std::ostream& operator<<(std::ostream& o, const StringOrView& key) {
    return o << key.view();
  }

  // Make hashable
  template <typename H> friend H AbslHashValue(H h, const StringOrView& c) {
    return H::combine(std::move(h), c.view());
  }

  // If the key is backed by a string_view, replace it with a string with the same value
  void MakeOwned() {
    if (std::holds_alternative<std::string_view>(val_))
      val_ = std::string{std::get<std::string_view>(val_)};
  }

  bool empty() const {
    return visit([](const auto& s) { return s.empty(); }, val_);
  }

 private:
  std::variant<std::string_view, std::string> val_;
};

}  // namespace dfly
