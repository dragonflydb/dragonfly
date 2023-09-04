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

template <typename T> T CmdArgParser::NextProxy::Int() {
  T out;
  if (absl::SimpleAtoi(operator std::string_view(), &out))
    return out;
  parser_->Report(INVALID_INT, idx_);
  return T{0};
}

template uint64_t CmdArgParser::NextProxy::Int<uint64_t>();
template int64_t CmdArgParser::NextProxy::Int<int64_t>();

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
