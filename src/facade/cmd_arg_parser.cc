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
  // TODO DCHECK(!HasNext()) << "Not all args were processed";
}

}  // namespace facade
