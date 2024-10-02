// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/strings/match.h>
#include <absl/strings/numbers.h>

#include <optional>
#include <string_view>
#include <utility>

#include "facade/facade_types.h"

namespace facade {

// Helper class for numerical range restriction during parsing
template <auto min, auto max> struct FInt {
  decltype(min) value = {};
  operator decltype(min)() {
    return value;
  }

  static_assert(std::is_same_v<decltype(min), decltype(max)>, "inconsistent types");
  static constexpr auto kMin = min;
  static constexpr auto kMax = max;
};

template <class T> constexpr bool is_fint = false;

template <auto min, auto max> constexpr bool is_fint<FInt<min, max>> = true;

// Utility class for easily parsing command options from argument lists.
struct CmdArgParser {
  enum ErrorType {
    OUT_OF_BOUNDS,
    SHORT_OPT_TAIL,
    INVALID_INT,
    INVALID_CASES,
    INVALID_NEXT,
    UNPROCESSED
  };

  struct ErrorInfo {
    ErrorType type;
    size_t index;

    ErrorReply MakeReply() const;
  };

 public:
  CmdArgParser(CmdArgList args) : args_{args} {
  }

  // Debug asserts sure error was consumed
  ~CmdArgParser();

  // Get next value without consuming it
  std::string_view Peek() {
    return SafeSV(cur_i_);
  }

  // Consume next value
  template <class T = std::string_view, class... Ts> auto Next() {
    if (cur_i_ + sizeof...(Ts) >= args_.size()) {
      Report(OUT_OF_BOUNDS, cur_i_);
      return std::conditional_t<sizeof...(Ts) == 0, T, std::tuple<T, Ts...>>();
    }

    if constexpr (sizeof...(Ts) == 0) {
      auto idx = cur_i_++;
      return Convert<T>(idx);
    } else {
      std::tuple<T, Ts...> res;
      NextImpl<0>(&res);
      cur_i_ += sizeof...(Ts) + 1;
      return res;
    }
  }

  // returns next value if exists or default value
  template <class T = std::string_view> auto NextOrDefault(T default_value = {}) {
    return HasNext() ? Next<T>() : default_value;
  }

  // check next value ignoring case and consume it
  void ExpectTag(std::string_view tag);

  // Consume next value
  template <class... Cases> auto MapNext(Cases&&... cases) {
    if (cur_i_ >= args_.size()) {
      Report(OUT_OF_BOUNDS, cur_i_);
      return typename decltype(MapImpl(std::string_view(),
                                       std::forward<Cases>(cases)...))::value_type{};
    }

    auto idx = cur_i_++;
    auto res = MapImpl(SafeSV(idx), std::forward<Cases>(cases)...);
    if (!res) {
      Report(INVALID_CASES, idx);
      return typename decltype(res)::value_type{};
    }
    return *res;
  }

  // Consume next value if can map it and return mapped result or return nullopt
  template <class... Cases>
  auto TryMapNext(Cases&&... cases)
      -> std::optional<std::tuple_element_t<1, std::tuple<Cases...>>> {
    if (cur_i_ >= args_.size()) {
      return std::nullopt;
    }

    auto res = MapImpl(SafeSV(cur_i_), std::forward<Cases>(cases)...);
    cur_i_ = res ? cur_i_ + 1 : cur_i_;
    return res;
  }

  // Check if the next value is equal to a specific tag. If equal, its consumed.
  template <class... Args> bool Check(std::string_view tag, Args*... args) {
    if (cur_i_ + sizeof...(Args) >= args_.size())
      return false;

    std::string_view arg = SafeSV(cur_i_);
    if (!absl::EqualsIgnoreCase(arg, tag))
      return false;

    ((*args = Convert<Args>(++cur_i_)), ...);

    ++cur_i_;

    return true;
  }

  // Skip specified number of arguments
  CmdArgParser& Skip(size_t n) {
    if (cur_i_ + n > args_.size()) {
      Report(OUT_OF_BOUNDS, cur_i_);
    } else {
      cur_i_ += n;
    }
    return *this;
  }

  bool Finalize() {
    if (HasNext()) {
      Report(UNPROCESSED, cur_i_);
      return false;
    }
    return !HasError();
  }

  // Return remaining arguments
  CmdArgList Tail() const {
    return args_.subspan(cur_i_);
  }

  // Return true if arguments are left and no errors occured
  bool HasNext() {
    return cur_i_ < args_.size() && !error_;
  }

  bool HasError() {
    return error_.has_value();
  }

  // Get optional error if occured
  std::optional<ErrorInfo> Error() {
    return std::exchange(error_, {});
  }

  bool HasAtLeast(size_t i) const {
    return cur_i_ + i <= args_.size() && !error_;
  }

 private:
  template <class T, class... Cases>
  std::optional<std::decay_t<T>> MapImpl(std::string_view arg, std::string_view tag, T&& value,
                                         Cases&&... cases) {
    if (absl::EqualsIgnoreCase(arg, tag))
      return std::forward<T>(value);

    if constexpr (sizeof...(cases) > 0)
      return MapImpl(arg, cases...);

    return std::nullopt;
  }

  template <size_t shift, class Tuple> void NextImpl(Tuple* t) {
    std::get<shift>(*t) = Convert<std::tuple_element_t<shift, Tuple>>(cur_i_ + shift);
    if constexpr (constexpr auto next = shift + 1; next < std::tuple_size_v<Tuple>)
      NextImpl<next>(t);
  }

  template <class T> T Convert(size_t idx) {
    static_assert(
        std::is_arithmetic_v<T> || std::is_constructible_v<T, std::string_view> || is_fint<T>,
        "incorrect type");
    if constexpr (std::is_arithmetic_v<T>) {
      return Num<T>(idx);
    } else if constexpr (std::is_constructible_v<T, std::string_view>) {
      return static_cast<T>(SafeSV(idx));
    } else if constexpr (is_fint<T>) {
      return {ConvertFInt<T::kMin, T::kMax>(idx)};
    }
  }

  template <auto min, auto max> FInt<min, max> ConvertFInt(size_t idx) {
    auto res = Num<decltype(min)>(idx);
    if (res < min || res > max) {
      Report(INVALID_INT, idx);
      return {};
    }
    return {res};
  }

  std::string_view SafeSV(size_t i) const {
    using namespace std::literals::string_view_literals;
    if (i >= args_.size())
      return ""sv;
    return args_[i].empty() ? ""sv : ToSV(args_[i]);
  }

  void Report(ErrorType type, size_t idx) {
    if (!error_) {
      error_ = {type, idx};
      cur_i_ = args_.size();
    }
  }

  template <typename T> T Num(size_t idx) {
    auto arg = SafeSV(idx);
    T out;
    if constexpr (std::is_same_v<T, float>) {
      if (absl::SimpleAtof(arg, &out))
        return out;
    } else if constexpr (std::is_same_v<T, double>) {
      if (absl::SimpleAtod(arg, &out))
        return out;
    } else if constexpr (std::is_integral_v<T> && sizeof(T) >= sizeof(int32_t)) {
      if (absl::SimpleAtoi(arg, &out))
        return out;
    } else if constexpr (std::is_integral_v<T> && sizeof(T) < sizeof(int32_t)) {
      int32_t tmp;
      if (absl::SimpleAtoi(arg, &tmp)) {
        out = tmp;  // out can not store the whole tmp
        if (tmp == out)
          return out;
      }
    }

    Report(INVALID_INT, idx);
    return {};
  }

 private:
  size_t cur_i_ = 0;
  CmdArgList args_;

  std::optional<ErrorInfo> error_;
};

}  // namespace facade
