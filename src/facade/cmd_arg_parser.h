// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <optional>
#include <string_view>
#include <utility>

#include "facade/facade_types.h"

namespace facade {

// Utility class for easily parsing command options from argument lists.
struct CmdArgParser {
  enum ErrorType { OUT_OF_BOUNDS, SHORT_OPT_TAIL, INVALID_INT, INVALID_CASES, INVALID_NEXT };

  struct CheckProxy {
    explicit operator bool() const;

    // Expect the tag to be followed by a number of arguments.
    // Reports an error if the tag is matched but the condition is not met.
    CheckProxy& ExpectTail(size_t tail) {
      expect_tail_ = tail;
      return *this;
    }

    // Call ToUpper on the next value after the flag and its expected tail.
    CheckProxy& NextUpper() {
      next_upper_ = true;
      return *this;
    }

    CheckProxy& IgnoreCase() {
      ignore_case_ = true;
      return *this;
    }

   private:
    friend struct CmdArgParser;

    CheckProxy(CmdArgParser* parser, std::string_view tag, size_t idx)
        : parser_{parser}, tag_{tag}, idx_{idx} {
    }

    CmdArgParser* parser_;
    std::string_view tag_;
    size_t idx_;
    size_t expect_tail_ = 0;
    bool next_upper_ = false;
    bool ignore_case_ = false;
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

  // check next value ignoring case and consume it
  void ExpectTag(std::string_view tag);

  // Consume next value
  template <class... Cases> auto Switch(Cases&&... cases) {
    if (cur_i_ >= args_.size())
      Report(OUT_OF_BOUNDS, cur_i_);

    auto idx = cur_i_++;
    auto res = SwitchImpl(SafeSV(idx), std::forward<Cases>(cases)...);
    if (!res) {
      Report(INVALID_CASES, idx);
      return typename decltype(res)::value_type{};
    }
    return *res;
  }

  // Check if the next value if equal to a specific tag. If equal, its consumed.
  CheckProxy Check(std::string_view tag) {
    return CheckProxy(this, tag, cur_i_);
  }

  // Skip specified number of arguments
  CmdArgParser& Skip(size_t n) {
    cur_i_ += n;
    return *this;
  }

  // In-place convert the next argument to uppercase
  CmdArgParser& ToUpper() {
    if (cur_i_ < args_.size())
      ToUpper(cur_i_);
    return *this;
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
  std::optional<std::decay_t<T>> SwitchImpl(std::string_view arg, std::string_view tag, T&& value,
                                            Cases&&... cases) {
    if (arg == tag)
      return std::forward<T>(value);

    if constexpr (sizeof...(cases) > 0)
      return SwitchImpl(arg, cases...);

    return std::nullopt;
  }

  template <size_t shift, class Tuple> void NextImpl(Tuple* t) {
    std::get<shift>(*t) = Convert<std::tuple_element_t<shift, Tuple>>(cur_i_ + shift);
    if constexpr (constexpr auto next = shift + 1; next < std::tuple_size_v<Tuple>)
      NextImpl<next>(t);
  }

  template <class T> T Convert(size_t idx) {
    static_assert(std::is_arithmetic_v<T> || std::is_constructible_v<T, std::string_view>,
                  "incorrect type");
    if constexpr (std::is_arithmetic_v<T>) {
      return Num<T>(idx);
    } else if constexpr (std::is_constructible_v<T, std::string_view>) {
      return static_cast<T>(SafeSV(idx));
    }
  }

  std::string_view SafeSV(size_t i) const {
    if (i >= args_.size())
      return "";
    return ToSV(args_[i]);
  }

  void Report(ErrorType type, size_t idx) {
    if (!error_)
      error_ = {type, idx};
  }

  template <typename T> T Num(size_t idx);

  void ToUpper(size_t i);

 private:
  size_t cur_i_ = 0;
  CmdArgList args_;

  std::optional<ErrorInfo> error_;
};

}  // namespace facade
