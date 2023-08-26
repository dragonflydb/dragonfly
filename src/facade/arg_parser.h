// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/strings/match.h>

#include <string_view>

#include "facade/error.h"
#include "facade/facade_types.h"

namespace facade {

struct ArgumentParser {
  struct NextProxy;
  friend struct NextProxy;
  struct CheckProxy;
  friend struct CheckProxy;

  enum ErrorType {
    OUT_OF_BOUNDS,
    SHORT_OPT_TAIL,
    INVALID_INT,
    INVALID_CASES,
  };

  struct NextProxy {
    operator std::string_view() {
      return ToSV(parser_->args_[idx_]);
    }

    operator std::string() {
      return std::string{operator std::string_view()};
    }

    template <typename T> operator T() {
      T out;
      if (absl::SimpleAtoi(operator std::string_view(), &out))
        return out;
      parser_->Report(INVALID_INT, idx_);
      return T{0};
    }

    template <typename T> T Cases(std::initializer_list<std::pair<std::string_view, T>> values) {
      std::string_view arg = operator std::string_view();
      for (const auto& [tag, value] : values) {
        if (absl::EqualsIgnoreCase(tag, arg))
          return value;
      }
      parser_->Report(INVALID_CASES, idx_);
      return T{};
    }

   private:
    friend struct ArgumentParser;

    NextProxy(ArgumentParser* parser, size_t idx) : parser_{parser}, idx_{idx} {
    }

    ArgumentParser* parser_;
    size_t idx_;
  };

  struct CheckProxy {
    operator bool() {
      if (idx_ >= parser_->args_.size())
        return false;

      std::string_view arg = ToSV(parser_->args_[idx_]);
      if (arg != tag_)
        return false;

      if (idx_ + expect_tail_ >= parser_->args_.size()) {
        parser_->Report(SHORT_OPT_TAIL, idx_);
        return false;
      }

      parser_->cur_i_++;

      if (next_upper_)
        parser_->ToUpper(idx_ + expect_tail_ + 1);

      return true;
    }

    CheckProxy& ExpectTail(size_t tail) {
      expect_tail_ = tail;
      return *this;
    }

    CheckProxy& NextUpper() {
      next_upper_ = true;
      return *this;
    }

   private:
    friend struct ArgumentParser;

    CheckProxy(ArgumentParser* parser, std::string_view tag, size_t idx)
        : parser_{parser}, tag_{tag}, idx_{idx} {
    }

    ArgumentParser* parser_;
    std::string_view tag_;
    size_t idx_;
    size_t expect_tail_ = 0;
    bool next_upper_ = false;
  };

  struct ErrorInfo {
    ErrorType type;
    size_t index;

    ErrorReply MakeReply() {
      return ErrorReply{kSyntaxErr};  // switch here
    }
  };

 public:
  ArgumentParser(CmdArgList args) : args_{args} {
  }

  NextProxy Peek() {
    return Next(0);
  }

  NextProxy Next(size_t step = 1) {
    if (cur_i_ >= args_.size())
      Report(OUT_OF_BOUNDS, cur_i_);
    cur_i_ += step;
    return NextProxy{this, cur_i_ - step};
  }

  CheckProxy Check(std::string_view tag) {
    return CheckProxy(this, tag, cur_i_);
  }

  void Skip(size_t n) {
    cur_i_ += n;
  }

  ArgumentParser& ToUpper() {
    if (cur_i_ < args_.size())
      ToUpper(cur_i_);
    return *this;
  }

  bool Ok() {
    return cur_i_ <= args_.size() && !error_;
  }

  std::optional<ErrorInfo> Error() {
    return std::move(error_);
  }

 private:
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
