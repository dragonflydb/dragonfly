// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/scan_options.h"

#include "core/compact_object.h"
#include "core/glob_matcher.h"
#include "facade/cmd_arg_parser.h"

namespace dfly {

ScanOpts::~ScanOpts() {
}

OpResult<ScanOpts> ScanOpts::TryFrom(CmdArgList args, bool allow_novalues) {
  ScanOpts scan_opts;
  facade::CmdArgParser parser(args);

  while (parser.HasNext()) {
    std::string_view pattern;
    std::string_view type_str;

    if (parser.Check("NOVALUES")) {
      if (!allow_novalues) {
        return facade::OpStatus::SYNTAX_ERR;
      }
      scan_opts.novalues = true;
    } else if (parser.Check("COUNT", &scan_opts.limit)) {
      if (scan_opts.limit == 0)
        scan_opts.limit = 1;
    } else if (parser.Check("MATCH", &pattern)) {
      if (pattern != "*")
        scan_opts.matcher.reset(new GlobMatcher{pattern, true});
    } else if (parser.Check("TYPE", &type_str)) {
      CompactObjType obj_type = ObjTypeFromString(type_str);
      if (obj_type == kInvalidCompactObjType) {
        return facade::OpStatus::SYNTAX_ERR;
      }
      scan_opts.type_filter = obj_type;
    } else if (parser.Check("BUCKET", &scan_opts.bucket_id)) {
      // no-op
    } else if (parser.Check("ATTR")) {
      scan_opts.mask =
          parser.MapNext("v", ScanOpts::Mask::Volatile, "p", ScanOpts::Mask::Permanent, "a",
                         ScanOpts::Mask::Accessed, "u", ScanOpts::Mask::Untouched);
    } else if (parser.Check("MINMSZ", &scan_opts.min_malloc_size)) {
      // no-op
    } else
      return facade::OpStatus::SYNTAX_ERR;
  }  // while

  // Check for parsing errors (e.g. missing values or invalid integers)
  if (auto err = parser.TakeError()) {
    if (err.type == facade::CmdArgParser::INVALID_INT) {
      return facade::OpStatus::INVALID_INT;
    }
    return facade::OpStatus::SYNTAX_ERR;
  }

  return scan_opts;
}

bool ScanOpts::Matches(std::string_view val_name) const {
  return !matcher || matcher->Matches(val_name);
}

}  // namespace dfly
