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
      T out = 0;
      if (!absl::SimpleAtoi(operator std::string_view(), &out))
        parser_->Report(INVALID_INT, idx_);
      return out;
    }

    template <typename T> T Cases(const std::vector<std::pair<std::string_view, T>>& values) {
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
      DCHECK(next_upper_ == false || expect_tail_ == 0);

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
        parser_->ToUpper();

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

  NextProxy Next() {
    if (cur_i_ >= args_.size())
      Report(OUT_OF_BOUNDS, cur_i_);
    return NextProxy{this, cur_i_++};
  }

  CheckProxy Check(std::string_view tag) {
    return CheckProxy(this, tag, cur_i_);
  }

  void Skip(size_t n) {
    cur_i_ += n;
  }

  ArgumentParser& ToUpper() {
    if (cur_i_ >= args_.size())
      return *this;

    for (auto& c : args_[cur_i_])
      c = absl::ascii_toupper(c);

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
    if (error_)
      return;
    error_ = {type, idx};
  }

 private:
  size_t cur_i_ = 0;
  CmdArgList args_;

  std::optional<ErrorInfo> error_;
};

}  // namespace facade
