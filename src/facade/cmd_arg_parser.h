// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/strings/ascii.h>
#include <absl/strings/match.h>

#include <optional>
#include <string_view>

#include "base/logging.h"
#include "facade/error.h"
#include "facade/facade_types.h"

namespace facade {

// Utility class for easily parsing command options from argument lists.
struct CmdArgParser {
  enum ErrorType {
    OUT_OF_BOUNDS,
    SHORT_OPT_TAIL,
    INVALID_INT,
    INVALID_CASES,
  };

  struct NextProxy;

  template <typename T> struct CaseProxy {
    operator T() {
      if (!value_)
        parser_->Report(INVALID_CASES, idx_);
      return value_.value_or(T{});
    }

    CaseProxy Case(std::string_view tag, T value) {
      std::string_view arg = parser_->SafeSV(idx_);
      if (arg == tag)
        value_ = std::move(value);
      return *this;
    }

   private:
    friend struct NextProxy;

    CaseProxy(CmdArgParser* parser, size_t idx) : parser_{parser}, idx_{idx} {
    }

    CmdArgParser* parser_;
    size_t idx_;
    std::optional<T> value_;
  };

  struct NextProxy {
    operator std::string_view() {
      return parser_->SafeSV(idx_);
    }

    operator std::string() {
      return std::string{operator std::string_view()};
    }

    template <typename T> T Int() {
      T out;
      if (absl::SimpleAtoi(operator std::string_view(), &out))
        return out;
      parser_->Report(INVALID_INT, idx_);
      return T{0};
    }

    // Detect value based on cases.
    // Returns default if the argument is not present among the cases list,
    // and reports an error.
    template <typename T> auto Case(std::string_view tag, T value) {
      return CaseProxy<T>{parser_, idx_}.Case(tag, value);
    }

   private:
    friend struct CmdArgParser;

    NextProxy(CmdArgParser* parser, size_t idx) : parser_{parser}, idx_{idx} {
    }

    CmdArgParser* parser_;
    size_t idx_;
  };

  struct CheckProxy {
    explicit operator bool() const {
      if (idx_ >= parser_->args_.size())
        return false;

      std::string_view arg = parser_->SafeSV(idx_);
      if ((!ignore_case_ && arg != tag_) || (ignore_case_ && !absl::EqualsIgnoreCase(arg, tag_)))
        return false;

      if (idx_ + expect_tail_ >= parser_->args_.size()) {
        parser_->Report(SHORT_OPT_TAIL, idx_);
        return false;
      }

      parser_->cur_i_++;

      if (size_t uidx = idx_ + expect_tail_ + 1; next_upper_ && uidx < parser_->args_.size())
        parser_->ToUpper(uidx);

      return true;
    }

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

    ErrorReply MakeReply() {
      switch (type) {
        case INVALID_INT:
          return ErrorReply{kInvalidIntErr};
        default:
          return ErrorReply{kSyntaxErr};
      };
      return ErrorReply{kSyntaxErr};
    }
  };

 public:
  CmdArgParser(CmdArgList args) : args_{args} {
  }

  ~CmdArgParser() {
    DCHECK(!error_.has_value()) << "Parsing error occured but not checked";
  }

  // Get next value without consuming it
  NextProxy Peek() {
    return NextProxy(this, cur_i_);
  }

  // Consume next value
  NextProxy Next() {
    if (cur_i_ >= args_.size())
      Report(OUT_OF_BOUNDS, cur_i_);
    return NextProxy{this, cur_i_++};
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

  // Get optional error if occured
  std::optional<ErrorInfo> Error() {
    auto out = std::move(error_);
    error_.reset();
    return out;
  }

 private:
  std::string_view SafeSV(size_t i) const {
    if (i >= args_.size())
      return "";
    return ToSV(args_[i]);
  }

  void Report(ErrorType type, size_t idx) {
    if (!error_)
      error_ = {type, idx};
  }

  void ToUpper(size_t i) {
    for (auto& c : args_[i])
      c = absl::ascii_toupper(c);
  }

 private:
  size_t cur_i_ = 0;
  CmdArgList args_;

  std::optional<ErrorInfo> error_;
};

}  // namespace facade
