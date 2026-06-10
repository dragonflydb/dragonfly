// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "facade/cmd_arg_parser.h"

#include <absl/strings/ascii.h>

#include "base/logging.h"
#include "facade/error.h"

namespace facade {

void CmdArgParser::ExpectTag(std::string_view tag) {
  if (cur_i_ >= args_.size()) {
    Report(OUT_OF_BOUNDS, cur_i_);
    return;
  }

  auto idx = cur_i_++;
  auto val = args_[idx];
  if (!absl::EqualsIgnoreCase(val, tag)) {
    Report(INVALID_NEXT, idx);
  }
}

void CmdArgParser::ExpectTag(std::string_view tag, std::string error_msg) {
  if (cur_i_ >= args_.size()) {
    Report(CUSTOM_ERROR, cur_i_, std::move(error_msg));
    return;
  }

  auto idx = cur_i_++;
  if (!absl::EqualsIgnoreCase(args_[idx], tag))
    Report(CUSTOM_ERROR, idx, std::move(error_msg));
}

std::string_view CmdArgParser::ExpectStartsWith(std::string_view prefix, std::string error_msg) {
  if (cur_i_ >= args_.size()) {
    Report(CUSTOM_ERROR, cur_i_, std::move(error_msg));
    return {};
  }

  auto idx = cur_i_++;
  auto val = args_[idx];
  if (!absl::StartsWith(val, prefix)) {
    Report(CUSTOM_ERROR, idx, std::move(error_msg));
    return {};
  }
  val.remove_prefix(prefix.size());
  return val;
}

CmdArgParser::ErrorInfo CmdArgParser::TakeError() {
  return std::exchange(error_, {});
}

ErrorReply CmdArgParser::ErrorInfo::MakeReply() const {
  DCHECK(operator bool());
  if (!custom_msg.empty())
    return ErrorReply{std::string{custom_msg}, kSyntaxErrType};
  switch (type) {
    case INVALID_INT:
      return ErrorReply{kInvalidIntErr};
    case INVALID_FLOAT:
      return ErrorReply{kInvalidFloatErr};
    default:
      return ErrorReply{kSyntaxErr};
  };
  return ErrorReply{kSyntaxErr};
}

CmdArgParser::~CmdArgParser() {
  DCHECK(!error_) << "Parsing error occured but not checked";
  // TODO DCHECK(!HasNext()) << "Not all args were processed";
}

}  // namespace facade
