// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/strings/match.h>
#include <absl/strings/numbers.h>

#include <optional>
#include <string_view>
#include <tuple>
#include <utility>

#include "facade/facade_types.h"

namespace facade {

// CmdArgParser — utility for parsing command option lists.
//
// Reading individual args:
//   CmdArgParser parser(args);
//   auto key = parser.Next<string_view>();                      // read one arg by type
//   auto [src, dst] = parser.Next<string_view, string_view>();  // read several at once (tuple)
//   auto db = parser.Next<FInt<0, 15>>();                       // range-restricted int
//                                                               // (INVALID_INT if out of range)
//   auto count = parser.NextOrDefault<size_t>(10);              // read optional with default
//
// Tag matching:
//   parser.ExpectTag("LOAD");                                   // required literal keyword
//   if (parser.Check("NX")) { ... }                             // consume tag only if matched
//   auto mode = parser.MapNext("EX", Mode::EX, "PX", Mode::PX); // tag -> enum mapping
//   auto maybe_mode = parser.TryMapNext("ASC", Dir::ASC,        // like MapNext but returns
//                                       "DESC", Dir::DESC);     // nullopt (no error) on miss
//
// Bulk named options with Apply():
//   parser.Apply(
//       Exist("WITHSCORES", &params.with_scores),  // tag present -> sets bool true
//       Tag("LIMIT", &offset, &limit),             // tag -> reads following args
//       Tag("COUNT", &optional_count),             // std::optional<T>* is supported
//       Tag("GET", [&](CmdArgParser* p) {          // lambda handler for custom parsing
//         patterns.push_back(p->Next<string_view>());
//       }));
//
// Navigating manually:
//   if (parser.HasNext()) { ... }                               // is there another arg?
//   if (parser.HasAtLeast(3)) { ... }                           // at least N args remain?
//   auto peek = parser.Peek();                                  // look at next without consuming
//                                                               //   (useful for error messages)
//   parser.Skip(n);                                             // advance n args
//   CmdArgList rest = parser.Tail();                            // remaining args (e.g. k/v pairs)
//
// Apply stops at the first unmatched arg without reporting an error. Use ApplyOrSkip() instead
// to silently ignore unknown tags, or call Finalize() afterwards to require all args were
// consumed (reports UNPROCESSED otherwise). Error surfacing:
//   if (parser.HasError()) { ... }                              // any error so far?
//   if (!parser.Finalize())                                     // common end-of-parse check
//     return cmd_cntx->SendError(parser.TakeError().MakeReply());

// Numerical range restriction used with Next<FInt<lo, hi>>().
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

template <class T> constexpr bool is_optional = false;

template <class U> constexpr bool is_optional<std::optional<U>> = true;

struct CmdArgParser {
  enum ErrorType {
    NO_ERROR,
    OUT_OF_BOUNDS,
    SHORT_OPT_TAIL,
    INVALID_INT,
    INVALID_FLOAT,
    INVALID_CASES,
    INVALID_NEXT,
    UNPROCESSED,
    CUSTOM_ERROR  // should be the last one
  };

  struct ErrorInfo {
    int type = NO_ERROR;
    size_t index = 0;

    operator bool() const {
      return type != ErrorType::NO_ERROR;
    }
    ErrorReply MakeReply() const;
  };

 public:
  CmdArgParser(ArgSlice args) : args_{args} {
  }

  // DCHECKs that any error was consumed.
  ~CmdArgParser();

  std::string_view Peek() {
    return SafeSV(cur_i_);
  }

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

  template <class T = std::string_view> auto NextOrDefault(T default_value = {}) {
    return HasNext() ? Next<T>() : default_value;
  }

  // Consumes the next arg; reports INVALID_NEXT if it doesn't match (case-insensitive).
  void ExpectTag(std::string_view tag);

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

  // Same as MapNext, but returns nullopt (no error) if no case matches.
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

  // If the next arg matches `tag`, consume it and the following args-into-pointers; else no-op.
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

  // Greedily matches remaining args against the options. See the file header for usage.
  template <class... Opts> void Apply(Opts... opts) {
    while (HasNext() && (opts.TryApply(this) || ...)) {
    }
  }

  // Like Apply, but silently skips unmatched args (one at a time) instead of stopping. Use when
  // unknown tags should be ignored rather than reported. Prefer Apply + Finalize when strictness
  // is desired.
  template <class... Opts> void ApplyOrSkip(Opts... opts) {
    while (HasNext()) {
      if (!(opts.TryApply(this) || ...))
        Skip(1);
    }
  }

  CmdArgParser& Skip(size_t n) {
    if (cur_i_ + n > args_.size()) {
      Report(OUT_OF_BOUNDS, cur_i_);
    } else {
      cur_i_ += n;
    }
    return *this;
  }

  // Requires no leftover args and no prior errors. Reports UNPROCESSED if args remain.
  bool Finalize() {
    if (HasNext()) {
      Report(UNPROCESSED, cur_i_);
      return false;
    }
    return !HasError();
  }

  ArgSlice Tail() const {
    return args_.subspan(cur_i_);
  }

  bool HasNext() {
    return cur_i_ < args_.size() && !error_;
  }

  bool HasError() const {
    return bool(error_);
  }

  ErrorInfo TakeError();

  bool HasAtLeast(size_t i) const {
    return !error_ && i <= args_.size() - cur_i_;
  }

  size_t GetCurrentIndex() const {
    return cur_i_;
  }

  // Reports a custom error (error_type >= CUSTOM_ERROR) at the previously-consumed index
  // (or 0 if called before any arg was consumed).
  void Report(int error_type) {
    Report(error_type, cur_i_ > 0 ? cur_i_ - 1 : 0);
  }

 private:
  void Report(int error_type, size_t idx) {
    if (!error_) {
      error_ = {error_type, idx};
      cur_i_ = args_.size();
    }
  }

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
    static_assert(std::is_arithmetic_v<T> || std::is_constructible_v<T, std::string_view> ||
                      is_fint<T> || is_optional<T>,
                  "incorrect type");
    if constexpr (is_optional<T>) {
      return T{Convert<typename T::value_type>(idx)};
    } else if constexpr (std::is_arithmetic_v<T>) {
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

    if constexpr (std::is_floating_point_v<T>) {
      Report(INVALID_FLOAT, idx);
    } else {
      Report(INVALID_INT, idx);
    }
    return {};
  }

 private:
  size_t cur_i_ = 0;
  ArgSlice args_;

  ErrorInfo error_;
};

// Option types used with CmdArgParser::Apply. See the file header for usage.
namespace detail {

struct ExistOpt {
  std::string_view tag;
  bool* field;

  bool TryApply(CmdArgParser* parser) const {
    if (parser->Check(tag)) {
      *field = true;
      return true;
    }
    return false;
  }
};

template <class... Args> struct TagOpt {
  std::string_view tag;
  std::tuple<Args*...> args;

  bool TryApply(CmdArgParser* parser) const {
    // Match the tag first; then read each field via Next<>(). This ensures a matched tag is
    // always consumed — a missing trailing value surfaces OUT_OF_BOUNDS rather than looking like
    // "tag didn't match" (which ApplyOrSkip would silently skip past).
    if (!parser->Check(tag))
      return false;
    std::apply(
        [&](auto*... ptrs) {
          (((*ptrs) = parser->template Next<std::remove_pointer_t<decltype(ptrs)>>()), ...);
        },
        args);
    return true;
  }
};

template <class Func> struct LambdaOpt {
  std::string_view tag;
  Func func;

  bool TryApply(CmdArgParser* parser) const {
    if (parser->Check(tag)) {
      func(parser);
      return true;
    }
    return false;
  }
};

}  // namespace detail

inline detail::ExistOpt Exist(std::string_view tag, bool* field) {
  return {tag, field};
}

template <class... Args> detail::TagOpt<Args...> Tag(std::string_view tag, Args*... args) {
  return detail::TagOpt<Args...>{tag, std::make_tuple(args...)};
}

// Overload for custom parsing: the callable receives a CmdArgParser* after the tag matches.
template <class Func, std::enable_if_t<std::is_invocable_v<Func, CmdArgParser*>, int> = 0>
detail::LambdaOpt<Func> Tag(std::string_view tag, Func func) {
  return {tag, std::move(func)};
}

}  // namespace facade
