// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/cmd_arg_parser.h"

#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>

#include "base/logging.h"
#include "facade/error.h"

namespace facade {

CmdArgParser::CheckProxy::operator bool() const {
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

void CmdArgParser::ExpectTag(std::string_view tag) {
  if (cur_i_ >= args_.size()) {
    Report(OUT_OF_BOUNDS, cur_i_);
    return;
  }

  auto idx = cur_i_++;
  auto val = ToSV(args_[idx]);
  if (!absl::EqualsIgnoreCase(val, tag)) {
    Report(INVALID_NEXT, idx);
  }
}

template <typename T> T CmdArgParser::Num(size_t idx) {
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

template float CmdArgParser::Num<float>(size_t);
template double CmdArgParser::Num<double>(size_t);
template uint64_t CmdArgParser::Num<uint64_t>(size_t);
template int64_t CmdArgParser::Num<int64_t>(size_t);
template uint32_t CmdArgParser::Num<uint32_t>(size_t);
template int32_t CmdArgParser::Num<int32_t>(size_t);
template uint16_t CmdArgParser::Num<uint16_t>(size_t);
template int16_t CmdArgParser::Num<int16_t>(size_t);

ErrorReply CmdArgParser::ErrorInfo::MakeReply() const {
  switch (type) {
    case INVALID_INT:
      return ErrorReply{kInvalidIntErr};
    default:
      return ErrorReply{kSyntaxErr};
  };
  return ErrorReply{kSyntaxErr};
}

CmdArgParser::~CmdArgParser() {
  DCHECK(!error_.has_value()) << "Parsing error occured but not checked";
}

void CmdArgParser::ToUpper(size_t i) {
  for (auto& c : args_[i])
    c = absl::ascii_toupper(c);
}

}  // namespace facade
