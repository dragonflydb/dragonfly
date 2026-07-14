// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/strings/match.h>
#include <absl/strings/numbers.h>

#include <array>
#include <cassert>
#include <cmath>
#include <concepts>
#include <optional>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>

#include "facade/facade_types.h"

namespace facade {

// CmdArgParser — utility for parsing command option lists.
//
// Ideology: parse everything, then check the error ONCE at the end. The first error is latched and
// every later read becomes a no-op, so intermediate `if (HasError())` guards are unnecessary. Fold
// value checks into the read via Next<FInt<...>>() / Next<Validated<...>>() instead of post-parse
// `if` blocks.
//
// Reading individual args:
//   CmdArgParser parser(args);
//   auto key = parser.Next<string_view>();                      // read one arg by type
//   auto [src, dst] = parser.Next<string_view, string_view>();  // read several at once (tuple)
//   auto db = parser.Next<FInt<0, 15>>();                       // range-restricted int
//                                                               // (INVALID_INT if out of range)
//   auto f  = parser.Next<FInt<1, 99>>("bad f");                // FInt with a custom out-of-range
//                                                               // / non-integer error message
//   auto s  = parser.Next<Validated<double, NotNan<kMsg>>>();   // parse + custom-error rule check
//   auto u  = parser.Next(ParseUnit);                           // token parser fn: (sv, err) -> T
//   auto n  = parser.Next(Number<int>);                         // full parser fn: (parser) -> T
//   auto count = parser.NextOrDefault<size_t>(10);              // read optional with default
//   auto conv  = parser.NextOrDefault(ParseUnit, 1.0);          // optional arg via a parser fn
//   Range fields = parser.NextRange();                         // [N, e1..eN] counted list
//   Range pairs  = parser.NextRange(2);                        // [N, f1,v1,..] N field/value pairs
//   Range fs     = parser.NextRange(1, mismatch, true);        // consume-all variant
//   Range rest   = parser.RemainingRange();                    // all remaining args, no count
//   Range items  = parser.RemainingRange("need >=1 item");     // ...erroring if none remain
//
// Tag matching:
//   parser.ExpectTag("LOAD");                                   // required literal keyword
//   if (parser.Check("NX")) { ... }                             // consume tag only if matched
//   if (parser.Check("COUNT", &count)) { ... }                 // ...also read args (check errors)
//   auto mode = parser.MapNext("EX", Mode::EX, "PX", Mode::PX); // tag -> enum mapping
//   auto maybe_mode = parser.TryMapNext("ASC", Dir::ASC,        // like MapNext but returns
//                                       "DESC", Dir::DESC);     // nullopt (no error) on miss
//
// Compile-time grammar for CmdArgParser: a `static constexpr` grammar built from consteval
// factories (Compile/Args/Options/OneOf/Flag/Exist/Field/Map/Choice/Action/If) bound to members of
// a target struct T. Nothing is built per call; Apply keeps only a tiny stack state. The factories
// live in `facade`; Exist/OneOf overload the runtime combinators.
//
//   struct P { std::string_view key; uint16_t flags = 0; uint32_t mc = 0; int64_t ttl = 0; };
//   enum : uint16_t { kNx = 1, kXx = 2 };
//   static constexpr auto kGrammar = Compile(
//       Args(&P::key),
//       Options(OneOf("NX and XX are incompatible",
//                     Flag("NX", &P::flags, kNx), Flag("XX", &P::flags, kXx)),
//               Action("EX", +[](CmdArgParser* p, P* o) { o->ttl = p->Next<int64_t>(); }),
//               Field("MCFLAGS", &P::mc)));
//   P o;
//   kGrammar.Apply(&parser, &o);
//   if (!parser.Finalize()) ...
//
// Options stops at the first unmatched argument; pair the grammar with Finalize() to reject
// leftovers. The legacy parser.Apply(Exist(...), Tag(...), Map(...), OneOf(...), If(...)) runtime
// combinators remain until their callers migrate.
//
// Navigating manually:
//   if (parser.HasNext()) { ... }                               // is there another arg?
//   if (parser.HasAtLeast(3)) { ... }                           // at least N args remain?
//   auto peek = parser.Peek();                                  // look at next without consuming
//   parser.Skip(n);                                             // advance n args
//   size_t start = parser.UnparsedStart();                       // index of first unparsed arg
//   ParsedArgs rest = parser.UnparsedArgs();                     // view of remaining args
//
// Error surfacing (at the end of parse):
//   if (!parser.Finalize())                                     // also reports UNPROCESSED on
//     return cmd_cntx->SendError(parser.TakeError().MakeReply()); // trailing args
//   parser.Finalize("Unsupported option: ");                    // report "<prefix><leftover arg>"
//   // or: if (parser.HasError()) ...
//   parser.ReportCustom("bad option");                          // inject a custom error (no-op if
//                                                               // one is already set)

// Result of a validation rule: `failed` marks rejection. A non-empty `msg` is reported verbatim;
// an empty one leaves the generic type error (INVALID_INT / INVALID_FLOAT), chosen by the caller.
struct RuleError {
  bool failed = false;
  std::string_view msg = {};
};

template <class T>
concept as_vnum = requires(T t) {
  static_cast<decltype(t.value)>(t);
  { T::validate(t.value) } -> std::same_as<RuleError>;
};

struct CmdArgParser;

// A parser callable for Next(fn), taking either fn(CmdArgParser*) (drives the parser, may consume
// several args) or fn(std::string_view, RuleError&) (converts the next arg, sets err on failure).
template <class F>
concept ParserFn =
    std::is_invocable_v<F, CmdArgParser*> || std::is_invocable_v<F, std::string_view, RuleError&>;

// Numeric conversion core shared by Num and Number; false if `arg` isn't a round-trippable T.
template <class T> bool TryParseNum(std::string_view arg, T* out) {
  if constexpr (std::is_same_v<T, float>) {
    return absl::SimpleAtof(arg, out);
  } else if constexpr (std::is_same_v<T, double>) {
    return absl::SimpleAtod(arg, out);
  } else if constexpr (std::is_integral_v<T> && sizeof(T) >= sizeof(int32_t)) {
    return absl::SimpleAtoi(arg, out);
  } else {
    static_assert(std::is_integral_v<T> && sizeof(T) < sizeof(int32_t));
    int32_t tmp;
    if (!absl::SimpleAtoi(arg, &tmp))
      return false;
    *out = static_cast<T>(tmp);
    return tmp == *out;  // reject values that don't fit T
  }
}

// Base for validated numbers: holds the parsed value and converts back to it.
template <class T> struct VNum {
  T value = {};
  operator T() const {
    return value;
  }
};

// Validation rules for Next<Validated<T, Rules...>>(): free functions `RuleError rule(T)`. Reusable
// ones take the message as a reference-to-constexpr NTTP.

// Out of [min, max] -> generic type error (FInt is the idiomatic spelling).
template <auto min, auto max> RuleError InRange(decltype(min) v) {
  static_assert(std::is_same_v<decltype(min), decltype(max)>, "inconsistent types");
  return {v < min || v > max, {}};
}

// Out of [min, max] -> custom Msg. Integer bounds only (float NTTPs need clang 18+).
template <auto min, auto max, const auto& Msg> RuleError Bounded(decltype(min) v) {
  static_assert(std::is_same_v<decltype(min), decltype(max)>, "inconsistent types");
  return {!(v >= min && v <= max), Msg};
}

// v < 0 -> custom Msg; NaN is accepted (matches a plain `v < 0` guard).
template <const auto& Msg, class V> RuleError NonNegative(V v) {
  return {v < 0, Msg};
}

// Rejects NaN but accepts +/-inf.
template <const auto& Msg, std::floating_point V> RuleError NotNan(V v) {
  return {std::isnan(v), Msg};
}

template <const auto& Msg, std::floating_point V> RuleError Finite(V v) {
  return {!std::isfinite(v), Msg};
}

template <auto Bad, const auto& Msg> RuleError NotEq(decltype(Bad) v) {
  return {v == Bad, Msg};
}

// Accepts a value only if every rule accepts it; first rejection wins.
template <class T, RuleError (*... Rules)(T)> struct Validated : VNum<T> {
  static RuleError validate(T v) {
    RuleError e;
    (void)((e = Rules(v)).failed || ...);
    return e;
  }
};

template <auto min, auto max> using FInt = Validated<decltype(min), InRange<min, max>>;

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
    CUSTOM_ERROR  // keep last
  };

  struct ErrorInfo {
    int type = NO_ERROR;
    size_t index = 0;
    std::string custom_msg;

    operator bool() const {
      return type != NO_ERROR;
    }
    ErrorReply MakeReply() const;
  };

  // Bounded view over the first `count` args of a ParsedArgs, returned by NextRange()/
  // RemainingRange(). A terminal Range (count covers the whole tail) converts to ParsedArgs.
  class Range {
   public:
    Range() = default;
    explicit Range(ParsedArgs args) : args_{args}, count_{args.size()} {
    }

    class iterator {
     public:
      using iterator_category = std::input_iterator_tag;
      using value_type = std::string_view;
      using difference_type = ptrdiff_t;
      using pointer = const std::string_view*;
      using reference = std::string_view;

      iterator(const ParsedArgs* args, size_t index) : args_{args}, index_{index} {
      }
      std::string_view operator*() const {
        return (*args_)[index_];
      }
      iterator& operator++() {
        ++index_;
        return *this;
      }
      iterator operator++(int) {
        iterator copy = *this;
        ++index_;
        return copy;
      }
      bool operator==(const iterator& o) const {
        return index_ == o.index_;
      }
      bool operator!=(const iterator& o) const {
        return index_ != o.index_;
      }

     private:
      const ParsedArgs* args_;
      size_t index_;
    };

    iterator begin() const {
      return {&args_, 0};
    }
    iterator end() const {
      return {&args_, count_};
    }
    std::string_view operator[](size_t i) const {
      return args_[i];
    }
    size_t size() const {
      return count_;
    }
    bool empty() const {
      return count_ == 0;
    }

    // Valid only for a terminal Range.
    operator ParsedArgs() const {
      assert(count_ == args_.size());
      return args_;
    }

   private:
    friend struct CmdArgParser;
    Range(ParsedArgs args, size_t count) : args_{args}, count_{count} {
    }
    ParsedArgs args_;
    size_t count_ = 0;
  };

 public:
  explicit CmdArgParser(const cmn::BackedArguments& bargs, uint32_t offset = 0)
      : args_{bargs, offset}, size_{args_.size()} {
  }

  explicit CmdArgParser(const ParsedArgs& args) : args_{args}, size_{args_.size()} {
  }

  CmdArgParser(const ParsedArgs& args, uint32_t offset)
      : args_{args.Tail(offset)}, size_{args_.size()} {
  }

  // Asserts that any error was consumed.
  ~CmdArgParser() {
    assert(!error_ && "Parsing error occured but not checked");
  }

  // Returns the arg `ahead` positions past the cursor without consuming it (empty if out of range).
  std::string_view Peek(size_t ahead = 0) {
    return SafeSV(cur_i_ + ahead);
  }

  std::string_view CurrentUnchecked() const {
    return SVAt(cur_i_);
  }

  template <class T = std::string_view, class... Ts> auto Next() {
    if (cur_i_ + sizeof...(Ts) >= size_) {
      ReportCode(OUT_OF_BOUNDS, cur_i_);
      return std::conditional_t<sizeof...(Ts) == 0, decltype(Convert<T>(0)),
                                std::tuple<T, Ts...>>();
    }

    if constexpr (sizeof...(Ts) == 0) {
      auto idx = cur_i_++;
      return Convert<T>(idx);
    } else {
      std::tuple<T, Ts...> res;
      const size_t base = cur_i_;
      // Report() moves cur_i_ on failure, so every conversion uses the captured base.
      cur_i_ = base + sizeof...(Ts) + 1;
      NextImpl<0>(&res, base);
      return res;
    }
  }

  // Runs a parser callable (see ParserFn): a fn(CmdArgParser*) drives the parser; a
  // fn(std::string_view, RuleError&) converts the next arg, its RuleError becoming a report.
  template <class F>
  requires ParserFn<F>
  auto Next(F&& fn) {
    if constexpr (std::is_invocable_v<F, CmdArgParser*>) {
      return std::forward<F>(fn)(this);
    } else {
      using R = std::invoke_result_t<F, std::string_view, RuleError&>;
      static_assert(std::is_default_constructible_v<R> && !std::is_reference_v<R>,
                    "token parser must return a default-constructible value type");
      if (cur_i_ >= size_) {
        Report(OUT_OF_BOUNDS, cur_i_);
        return R{};
      }
      size_t idx = cur_i_++;
      RuleError e;
      R val = std::forward<F>(fn)(SVAt(idx), e);
      if (e.failed)
        Report(e.msg.empty() ? INVALID_CASES : CUSTOM_ERROR, idx, std::string{e.msg});
      return e.failed ? R{} : val;
    }
  }

  // Like Next<T>(), but replaces any read failure (bad value, missing arg, ...) with a caller-
  // supplied CUSTOM_ERROR message. A rule's own message from a Validated<T, Rules...> passes
  // through unchanged, so parser.Next<Timeout>(kNotAFloat) reports kNotAFloat for a non-float but
  // keeps each rule's out-of-range / negative message.
  template <class T = std::string_view> auto Next(std::string_view err_msg) {
    bool prior = bool(error_);
    auto val = Next<T>();
    if (!prior && !err_msg.empty() && error_ && error_.type != CUSTOM_ERROR) {
      error_.type = CUSTOM_ERROR;
      error_.custom_msg = std::string{err_msg};
    }
    return val;
  }

  // Reads a counted list [count, e1..e(count*group)] into a bounded Range (group=2 reads count
  // field/value pairs). A wrong number of args reports `size_err`; an invalid/zero count reports
  // `count_err`, or `size_err` when `count_err` is empty. With `consume_all` the Range must cover
  // ALL remaining args. An empty message keeps the generic error (INVALID_INT / INVALID_CASES).
  Range NextRange(size_t group = 1, std::string_view size_err = {}, bool consume_all = false,
                  std::string_view count_err = {}) {
    uint32_t count = Next<FInt<1u, UINT32_MAX>>(count_err.empty() ? size_err : count_err);
    ParsedArgs rest = args_.Tail(cur_i_);
    size_t need = size_t(count) * group;
    if (!error_ && (consume_all ? rest.size() != need : rest.size() < need)) {
      if (size_err.empty())
        Report(INVALID_CASES, cur_i_ - 1);
      else
        ReportCustom(std::string{size_err});
    }
    if (error_)
      return {};
    cur_i_ += need;
    return Range{rest, need};
  }

  // Consumes and returns all remaining args as a terminal Range, no leading count. If `empty_err`
  // is provided and no args remain, reports it as a custom error.
  Range RemainingRange(std::string_view empty_err = {}) {
    Range r{args_.Tail(cur_i_)};
    cur_i_ = size_;
    if (!empty_err.empty() && r.empty())
      ReportCustom(std::string{empty_err});
    return r;
  }

  template <class T = std::string_view> auto NextOrDefault(T default_value = {}) {
    return HasNext() ? Next<T>() : default_value;
  }

  // Runs a parser callable (see ParserFn) if an arg remains, else returns default_value.
  template <class F, class D>
  requires ParserFn<F>
  auto NextOrDefault(F&& fn, D default_value) {
    return HasNext() ? Next(std::forward<F>(fn)) : default_value;
  }

  // Consumes the next arg; reports INVALID_NEXT if it doesn't match (case-insensitive).
  void ExpectTag(std::string_view tag);

  // Same as ExpectTag, but reports a caller-supplied error message instead of the generic one.
  void ExpectTag(std::string_view tag, std::string error_msg);

  // Consumes the next arg; if it begins with `prefix`, returns the suffix without it. Otherwise
  // reports a custom error and returns an empty view. Use for "@field" / "$param" style positional
  // arguments.
  std::string_view ExpectStartsWith(std::string_view prefix, std::string error_msg);

  // Consumes the next arg as integer T, allowing an optional leading `prefix` (sets *prefixed when
  // present). Reports INVALID_INT if the remaining text isn't an integer. Use for offsets like
  // BITFIELD's "#index" form.
  template <class T> T NextWithPrefix(std::string_view prefix, bool* prefixed) {
    static_assert(std::is_integral_v<T>);
    if (cur_i_ >= size_) {
      Report(OUT_OF_BOUNDS, cur_i_);
      return {};
    }
    size_t idx = cur_i_++;
    std::string_view val = SVAt(idx);
    *prefixed = absl::StartsWith(val, prefix);
    if (*prefixed)
      val.remove_prefix(prefix.size());
    T out{};
    if (!absl::SimpleAtoi(val, &out)) {
      Report(INVALID_INT, idx);
      return {};
    }
    return out;
  }

  template <class... Cases> auto MapNext(Cases&&... cases) {
    if (cur_i_ >= size_) {
      Report(OUT_OF_BOUNDS, cur_i_);
      return typename decltype(MapImpl(std::string_view(),
                                       std::forward<Cases>(cases)...))::value_type{};
    }

    auto idx = cur_i_++;
    auto res = MapImpl(SVAt(idx), std::forward<Cases>(cases)...);
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
    if (cur_i_ >= size_) {
      return std::nullopt;
    }

    auto res = MapImpl(SVAt(cur_i_), std::forward<Cases>(cases)...);
    cur_i_ = res ? cur_i_ + 1 : cur_i_;
    return res;
  }

  // Consumes `tag` if next and reads the following args-into-pointers; no-op otherwise. The result
  // is the tag match only: a bad/missing value still returns true but latches an error (check it).
  template <class... Args> bool Check(std::string_view tag, Args*... args) {
    if (cur_i_ >= size_)
      return false;

    if (!absl::EqualsIgnoreCase(SVAt(cur_i_), tag))
      return false;

    ++cur_i_;
    ((*args = Next<Args>()), ...);

    return true;
  }

  // TODO: remove — superseded by the compile-time cap grammar (Compile/Options/...). Greedily
  // matches remaining args against the options, stopping at the first unmatched arg.
  template <class... Opts> void Apply(Opts... opts) {
    while (HasNext() && (opts.TryApply(this) || ...)) {
    }
  }

  // TODO: remove. Like Apply, but silently skips unmatched args one at a time instead of stopping.
  template <class... Opts> void ApplyOrSkip(Opts... opts) {
    while (HasNext()) {
      if (!(opts.TryApply(this) || ...))
        Skip(1);
    }
  }

  CmdArgParser& Skip(size_t n) {
    if (cur_i_ + n > size_) {
      Report(OUT_OF_BOUNDS, cur_i_);
    } else {
      cur_i_ += n;
    }
    return *this;
  }

  void AdvanceUnchecked() {
    ++cur_i_;
  }

  // Requires all args consumed and no prior error. If args remain, reports the generic UNPROCESSED
  // (syntax) error, or "<unexpected_prefix><first leftover arg>" when a prefix is given (built only
  // on failure). Returns true only if everything was consumed without error.
  bool Finalize(std::string_view unexpected_prefix = {}) {
    if (HasNext()) {
      if (unexpected_prefix.empty()) {
        Report(UNPROCESSED, cur_i_);
      } else {
        std::string msg{unexpected_prefix};
        msg.append(Peek());
        Report(CUSTOM_ERROR, cur_i_, std::move(msg));
      }
      return false;
    }
    return !HasError();
  }

  size_t UnparsedStart() const {
    return cur_i_;
  }

  ParsedArgs UnparsedArgs() const {
    return args_.Tail(cur_i_);
  }

  bool HasNext() {
    return cur_i_ < size_ && !error_;
  }

  bool InBounds() const {
    return cur_i_ < size_;
  }

  bool HasError() const {
    return bool(error_);
  }

  ErrorInfo TakeError();

  bool HasAtLeast(size_t i) const {
    return !error_ && i <= size_ - cur_i_;
  }

  // Reports a custom error (error_type >= CUSTOM_ERROR) at the previously-consumed index
  // (or 0 if called before any arg was consumed).
  void Report(int error_type) {
    Report(error_type, cur_i_ > 0 ? cur_i_ - 1 : 0, {});
  }

  // Reports a custom error with a caller-supplied message. The message is surfaced by
  // ErrorInfo::MakeReply() instead of the generic kSyntaxErr text.
  void ReportCustom(std::string msg) {
    Report(CUSTOM_ERROR, cur_i_ > 0 ? cur_i_ - 1 : 0, std::move(msg));
  }

 private:
  void Report(int error_type, size_t idx, std::string msg = {}) {
    if (!error_) {
      error_ = {error_type, idx, std::move(msg)};
      cur_i_ = size_;
    }
  }

  void ReportCode(int error_type, size_t idx) {
    if (!error_) {
      error_.type = error_type;
      error_.index = idx;
      cur_i_ = size_;
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

  template <size_t shift, class Tuple> void NextImpl(Tuple* t, size_t base) {
    std::get<shift>(*t) = Convert<std::tuple_element_t<shift, Tuple>>(base + shift);
    if constexpr (constexpr auto next = shift + 1; next < std::tuple_size_v<Tuple>)
      NextImpl<next>(t, base);
  }

  template <class T> T Convert(size_t idx) {
    static_assert(std::is_arithmetic_v<T> || std::is_constructible_v<T, std::string_view> ||
                      as_vnum<T> || is_optional<T>,
                  "incorrect type");
    if constexpr (is_optional<T>) {
      return T{Convert<typename T::value_type>(idx)};
    } else if constexpr (std::is_arithmetic_v<T>) {
      return Num<T>(idx);
    } else if constexpr (std::is_constructible_v<T, std::string_view>) {
      return static_cast<T>(SVAt(idx));
    } else if constexpr (as_vnum<T>) {
      using U = decltype(T::value);
      U val{};
      if (!TryParseNum(SVAt(idx), &val)) {
        ReportCode(std::is_floating_point_v<U> ? INVALID_FLOAT : INVALID_INT, idx);
        return {};
      }
      if (RuleError e = T::validate(val); e.failed) {
        if (e.msg.empty())
          ReportCode(std::is_floating_point_v<U> ? INVALID_FLOAT : INVALID_INT, idx);
        else
          Report(CUSTOM_ERROR, idx, std::string{e.msg});
        return {};
      }
      return T{val};
    }
  }

  // Preserve a non-null data() for empty arguments (#3627).
  std::string_view SVAt(size_t i) const {
    std::string_view sv = args_[i];
    return sv.empty() ? std::string_view{""} : sv;
  }

  std::string_view SafeSV(size_t i) const {
    return i >= size_ ? std::string_view{""} : SVAt(i);
  }

  template <typename T> T Num(size_t idx) {
    T out{};
    if (TryParseNum(SVAt(idx), &out))
      return out;
    ReportCode(std::is_floating_point_v<T> ? INVALID_FLOAT : INVALID_INT, idx);
    return {};
  }

 private:
  size_t cur_i_ = 0;
  ParsedArgs args_;
  size_t size_ = 0;

  ErrorInfo error_;
};

// Default parser callable for arithmetic types: Next(Number<int>) behaves like Next<int>().
template <class T> T Number(CmdArgParser* parser) {
  return parser->Next<T>();
}

// TODO: remove — the runtime combinator family (the options below and their Exist/Tag/Map/If/OneOf
// factories) is superseded by the compile-time cap grammar; kept until the last parser.Apply
// callers migrate.
namespace detail {

// CRTP base for Apply() options: a fluent .Err(msg) surfaced by ReportErr() on failure, else the
// generic INVALID_CASES error.
template <class Derived> struct OptBase {
  std::string err = {};

  Derived&& Err(std::string msg) && {
    err = std::move(msg);
    return std::move(static_cast<Derived&>(*this));
  }

 protected:
  void ReportErr(CmdArgParser* parser) const {
    if (err.empty())
      parser->Report(CmdArgParser::INVALID_CASES);
    else
      parser->ReportCustom(err);
  }
};

struct ExistOpt : OptBase<ExistOpt> {
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

template <class... Args> struct TagOpt : OptBase<TagOpt<Args...>> {
  std::string_view tag;
  std::tuple<Args*...> args;

  bool TryApply(CmdArgParser* parser) const {
    // Match the tag first, then read fields via Next<>() — so a missing value surfaces
    // OUT_OF_BOUNDS instead of being swallowed by ApplyOrSkip as "no match".
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

template <class Func> struct LambdaOpt : OptBase<LambdaOpt<Func>> {
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

template <class T, class... Cases> struct MapOpt : OptBase<MapOpt<T, Cases...>> {
  static_assert(sizeof...(Cases) % 2 == 0, "Map expects alternating tag/value pairs");

  T* field;
  std::tuple<Cases...> cases;

  bool TryApply(CmdArgParser* parser) const {
    return TryMatch<0>(parser);
  }

 private:
  template <size_t I> bool TryMatch(CmdArgParser* parser) const {
    if constexpr (I >= sizeof...(Cases)) {
      return false;
    } else if (parser->Check(std::get<I>(cases))) {
      *field = std::get<I + 1>(cases);
      return true;
    } else {
      return TryMatch<I + 2>(parser);
    }
  }
};

template <class Inner> struct IfOpt : OptBase<IfOpt<Inner>> {
  bool cond;
  Inner inner;

  bool TryApply(CmdArgParser* parser) const {
    return cond && inner.TryApply(parser);
  }
};

template <class... Opts> struct OneOfOpt : OptBase<OneOfOpt<Opts...>> {
  std::tuple<Opts...> opts;
  mutable bool matched = false;

  bool TryApply(CmdArgParser* parser) const {
    bool any = std::apply([&](auto&... os) { return (os.TryApply(parser) || ...); }, opts);
    if (!any)
      return false;
    if (matched)
      this->ReportErr(parser);
    matched = true;
    return true;
  }
};

// Outer tag consumes one arg, then the inner option must match the next; otherwise reports
// .Err(msg) or INVALID_CASES.
template <class Inner> struct TagNestedOpt : OptBase<TagNestedOpt<Inner>> {
  std::string_view tag;
  Inner inner;

  bool TryApply(CmdArgParser* parser) const {
    if (!parser->Check(tag))
      return false;
    if (!inner.TryApply(parser))
      this->ReportErr(parser);
    return true;
  }
};

// Concept matching any of the Apply options (has a TryApply(CmdArgParser*) method).
template <class T>
concept ParseOption = requires(const T& t, CmdArgParser* p) {
  { t.TryApply(p) } -> std::same_as<bool>;
};

}  // namespace detail

namespace cap_detail {

struct NoState {};

template <class T, class... M>
void ReadMembers(CmdArgParser* p, T* o, const std::tuple<M T::*...>& fields) {
  std::apply(
      [&](auto... field) {
        if constexpr (sizeof...(M) == 1)
          ((o->*field = p->Next<M>()), ...);
        else
          std::tie(o->*field...) = p->Next<M...>();
      },
      fields);
}

inline bool TagMatch(std::string_view cur, std::string_view tag) {
  if (cur.size() != tag.size())
    return false;
  // TODO: Dispatch on compile-time tag metadata while preserving Abseil's comparison semantics.
  return absl::EqualsIgnoreCase(cur, tag);
}

template <class Derived, class T> struct TaggedRule {
  using Target = T;
  using State = NoState;

  consteval explicit TaggedRule(std::string_view tag) : tag_(tag) {
  }

  bool Consume(CmdArgParser* p, std::string_view cur, T* o, State&) const {
    if (!TagMatch(cur, tag_))
      return false;
    p->AdvanceUnchecked();
    static_cast<const Derived*>(this)->OnMatch(p, o);
    return true;
  }

 private:
  std::string_view tag_;
};

template <class T, class M> struct Flag : TaggedRule<Flag<T, M>, T> {
  consteval Flag(const char* tag, M T::*field, M value)
      : TaggedRule<Flag<T, M>, T>(tag), field_(field), value_(value) {
  }

  void OnMatch(CmdArgParser*, T* o) const {
    o->*field_ |= value_;
  }

 private:
  M T::*field_;
  M value_;
};

template <class T> struct Exist : TaggedRule<Exist<T>, T> {
  consteval Exist(const char* tag, bool T::*field) : TaggedRule<Exist<T>, T>(tag), field_(field) {
  }

  void OnMatch(CmdArgParser*, T* o) const {
    o->*field_ = true;
  }

 private:
  bool T::*field_;
};

template <class T, class... M> struct Field : TaggedRule<Field<T, M...>, T> {
  consteval Field(const char* tag, M T::*... fields)
      : TaggedRule<Field<T, M...>, T>(tag), fields_(fields...) {
  }

  void OnMatch(CmdArgParser* p, T* o) const {
    ReadMembers(p, o, fields_);
  }

 private:
  std::tuple<M T::*...> fields_;
};

template <class T> struct Action : TaggedRule<Action<T>, T> {
  consteval Action(const char* tag, void (*fn)(CmdArgParser*, T*))
      : TaggedRule<Action<T>, T>(tag), fn_(fn) {
  }

  void OnMatch(CmdArgParser* p, T* o) const {
    fn_(p, o);
  }

 private:
  void (*fn_)(CmdArgParser*, T*);
};

template <class T, class M, size_t N> struct Map {
  using Target = T;
  using State = NoState;
  consteval Map(M T::*field, std::array<std::string_view, N> tags, std::array<M, N> values)
      : field_(field), tags_(tags), values_(values) {
  }
  bool Consume(CmdArgParser* p, std::string_view cur, T* o, State&) const {
    for (size_t i = 0; i < N; ++i) {
      if (TagMatch(cur, tags_[i])) {
        p->AdvanceUnchecked();
        o->*field_ = values_[i];
        return true;
      }
    }
    return false;
  }

 private:
  M T::*field_;
  std::array<std::string_view, N> tags_;
  std::array<M, N> values_;
};

template <class T, class M, size_t N> struct Choice : TaggedRule<Choice<T, M, N>, T> {
  consteval Choice(std::string_view tag, M T::*field, std::array<std::string_view, N> keys,
                   std::array<M, N> values)
      : TaggedRule<Choice<T, M, N>, T>(tag), field_(field), keys_(keys), values_(values) {
  }

  void OnMatch(CmdArgParser* p, T* o) const {
    std::string_view val = p->template Next<std::string_view>();
    for (size_t i = 0; i < N; ++i) {
      if (TagMatch(val, keys_[i])) {
        o->*field_ = values_[i];
        return;
      }
    }
    p->Report(CmdArgParser::INVALID_CASES);
  }

 private:
  M T::*field_;
  std::array<std::string_view, N> keys_;
  std::array<M, N> values_;
};

template <class T, class Inner> struct If {
  using Target = T;
  using State = typename Inner::State;
  consteval If(bool T::*cond, bool want, Inner inner) : cond_(cond), want_(want), inner_(inner) {
  }
  bool Consume(CmdArgParser* p, std::string_view cur, T* o, State& st) const {
    return (o->*cond_ == want_) && inner_.Consume(p, cur, o, st);
  }

 private:
  bool T::*cond_;
  bool want_;
  Inner inner_;
};

template <class T, class... M> struct Args {
  using Target = T;
  using State = NoState;
  consteval explicit Args(M T::*... fields) : fields_(fields...) {
  }
  void Consume(CmdArgParser* p, T* o, State&) const {
    ReadMembers(p, o, fields_);
  }

 private:
  std::tuple<M T::*...> fields_;
};

template <class T, class... Alts> struct OneOf {
  using Target = T;
  struct State {
    signed char matched = -1;
  };
  consteval OneOf(std::string_view err, Alts... alts) : err_(err), alts_(alts...) {
  }
  bool Consume(CmdArgParser* p, std::string_view cur, T* o, State& st) const {
    return TryAt<0>(p, cur, o, st);
  }

 private:
  template <size_t I> bool TryAt(CmdArgParser* p, std::string_view cur, T* o, State& st) const {
    if constexpr (I >= sizeof...(Alts)) {
      return false;
    } else {
      typename std::tuple_element_t<I, std::tuple<Alts...>>::State alt_st{};
      if (std::get<I>(alts_).Consume(p, cur, o, alt_st)) {
        if (st.matched >= 0 && st.matched != static_cast<signed char>(I))
          ReportConflict(p);
        st.matched = static_cast<signed char>(I);
        return true;
      }
      return TryAt<I + 1>(p, cur, o, st);
    }
  }
  void ReportConflict(CmdArgParser* p) const {
    if (err_.empty())
      p->Report(CmdArgParser::INVALID_CASES);
    else
      p->ReportCustom(std::string{err_});
  }
  std::string_view err_;
  std::tuple<Alts...> alts_;
};

template <class T, class... Rules> struct Options {
  using Target = T;
  using State = std::tuple<typename Rules::State...>;
  consteval explicit Options(Rules... rules) : rules_(rules...) {
  }
  void Consume(CmdArgParser* p, T* o, State& st) const {
    while (p->InBounds()) {
      std::string_view cur = p->CurrentUnchecked();
      if (!Match(p, cur, o, st, std::index_sequence_for<Rules...>{}))
        break;
    }
  }

 private:
  template <size_t... I>
  bool Match(CmdArgParser* p, std::string_view cur, T* o, State& st,
             std::index_sequence<I...>) const {
    return (std::get<I>(rules_).Consume(p, cur, o, std::get<I>(st)) || ...);
  }
  std::tuple<Rules...> rules_;
};

template <class T, class... Elems> struct Grammar {
  using Target = T;
  consteval explicit Grammar(Elems... elems) : elems_(elems...) {
  }
  void Apply(CmdArgParser* p, T* o) const {
    std::tuple<typename Elems::State...> st{};
    Run(p, o, st, std::index_sequence_for<Elems...>{});
  }

 private:
  template <size_t... I>
  void Run(CmdArgParser* p, T* o, std::tuple<typename Elems::State...>& st,
           std::index_sequence<I...>) const {
    (std::get<I>(elems_).Consume(p, o, std::get<I>(st)), ...);
  }
  std::tuple<Elems...> elems_;
};

}  // namespace cap_detail

template <class T, class M>
consteval auto Flag(const char* tag, M T::*field, std::type_identity_t<M> value) {
  return cap_detail::Flag<T, M>{tag, field, value};
}
template <class T> consteval auto Exist(const char* tag, bool T::*field) {
  return cap_detail::Exist<T>{tag, field};
}
template <class T, class... M> consteval auto Field(const char* tag, M T::*... fields) {
  static_assert(sizeof...(M) > 0, "Field expects at least one member");
  return cap_detail::Field<T, M...>{tag, fields...};
}
template <class T> consteval auto Action(const char* tag, void (*fn)(CmdArgParser*, T*)) {
  return cap_detail::Action<T>{tag, fn};
}
template <class T, class M, class... Cs> consteval auto Map(M T::*field, Cs... cs) {
  static_assert(sizeof...(Cs) % 2 == 0, "Map expects alternating tag/value pairs");
  constexpr size_t N = sizeof...(Cs) / 2;
  std::array<std::string_view, N> tags{};
  std::array<M, N> values{};
  auto cases = std::tuple{cs...};
  [&]<size_t... I>(std::index_sequence<I...>) {
    ((tags[I] = std::get<2 * I>(cases), values[I] = std::get<2 * I + 1>(cases)), ...);
  }
  (std::make_index_sequence<N>{});
  return cap_detail::Map<T, M, N>{field, tags, values};
}
template <class T, class M, class... Cs>
consteval auto Choice(const char* tag, M T::*field, Cs... cs) {
  static_assert(sizeof...(Cs) % 2 == 0, "Choice expects alternating key/value pairs");
  constexpr size_t N = sizeof...(Cs) / 2;
  std::array<std::string_view, N> keys{};
  std::array<M, N> values{};
  auto cases = std::tuple{cs...};
  [&]<size_t... I>(std::index_sequence<I...>) {
    ((keys[I] = std::get<2 * I>(cases), values[I] = std::get<2 * I + 1>(cases)), ...);
  }
  (std::make_index_sequence<N>{});
  return cap_detail::Choice<T, M, N>{tag, field, keys, values};
}
template <class T, class Inner> consteval auto If(bool T::*cond, Inner inner) {
  return cap_detail::If<T, Inner>{cond, true, inner};
}
template <class T, class Inner> consteval auto IfNot(bool T::*cond, Inner inner) {
  return cap_detail::If<T, Inner>{cond, false, inner};
}
template <class T, class... M> consteval auto Args(M T::*... fields) {
  static_assert(sizeof...(M) > 0, "Args expects at least one member");
  return cap_detail::Args<T, M...>{fields...};
}
template <class... Alts> consteval auto OneOf(std::string_view err, Alts... alts) {
  using T = typename std::tuple_element_t<0, std::tuple<Alts...>>::Target;
  return cap_detail::OneOf<T, Alts...>{err, alts...};
}
template <class... Rules> consteval auto Options(Rules... rules) {
  using T = typename std::tuple_element_t<0, std::tuple<Rules...>>::Target;
  return cap_detail::Options<T, Rules...>{rules...};
}
template <class... Elems> consteval auto Compile(Elems... elems) {
  using T = typename std::tuple_element_t<0, std::tuple<Elems...>>::Target;
  return cap_detail::Grammar<T, Elems...>{elems...};
}

// TODO: remove — runtime combinator factories, superseded by the consteval cap factories above.
inline detail::ExistOpt Exist(std::string_view tag, bool* field) {
  return {{}, tag, field};
}

template <class... Args> detail::TagOpt<Args...> Tag(std::string_view tag, Args*... args) {
  return {{}, tag, std::make_tuple(args...)};
}

template <class Func>
requires std::is_invocable_v<Func, CmdArgParser*> detail::LambdaOpt<Func> Tag(std::string_view tag,
                                                                              Func func) {
  return {{}, tag, std::move(func)};
}

template <detail::ParseOption Inner>
detail::TagNestedOpt<Inner> Tag(std::string_view tag, Inner inner) {
  return {{}, tag, std::move(inner)};
}

template <class T, class... Cases> detail::MapOpt<T, Cases...> Map(T* field, Cases... cases) {
  return {{}, field, std::make_tuple(std::move(cases)...)};
}

template <class Inner> detail::IfOpt<Inner> If(bool cond, Inner inner) {
  return {{}, cond, std::move(inner)};
}

template <detail::ParseOption... Opts> detail::OneOfOpt<Opts...> OneOf(Opts... opts) {
  return {{}, {std::move(opts)...}};
}

}  // namespace facade
