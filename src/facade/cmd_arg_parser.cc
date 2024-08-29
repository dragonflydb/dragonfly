// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/cmd_arg_parser.h"

#include <absl/strings/ascii.h>

#include "base/logging.h"
#include "facade/error.h"

namespace facade {

CmdArgParser::CheckProxy::operator bool() const {
  if (idx_ >= parser_->args_.size())
    return false;

  std::string_view arg = parser_->SafeSV(idx_);
  if (!absl::EqualsIgnoreCase(arg, tag_))
    return false;

  if (idx_ + expect_tail_ >= parser_->args_.size()) {
    parser_->Report(SHORT_OPT_TAIL, idx_);
    return false;
  }

  parser_->cur_i_++;

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
