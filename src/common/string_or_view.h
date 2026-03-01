// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string>
#include <string_view>
#include <variant>

namespace cmn {

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

  auto operator<=>(std::string_view o) const {
    return view() <=> o;
  }

  auto operator<=>(const StringOrView& o) const {
    return *this <=> o.view();
  }

  std::string_view view() const {
    return visit([](const auto& s) -> std::string_view { return s; }, val_);
  }

  bool empty() const {
    return visit([](const auto& s) { return s.empty(); }, val_);
  }

  friend std::ostream& operator<<(std::ostream& o, const StringOrView& key) {
    return o << key.view();
  }

  template <typename H> friend H AbslHashValue(H h, const StringOrView& c) {
    return H::combine(std::move(h), c.view());
  }

  // If the key is backed by a string_view, replace it with a string with the same value
  void MakeOwned() {
    if (std::holds_alternative<std::string_view>(val_))
      val_ = std::string{std::get<std::string_view>(val_)};
  }

  // Move out of value as string
  std::string Take() && {
    MakeOwned();
    return std::move(std::get<std::string>(val_));
  }

  std::string* GetMutable() {
    MakeOwned();
    return &std::get<std::string>(val_);
  }

 private:
  std::variant<std::string_view, std::string> val_;
};

}  // namespace cmn
