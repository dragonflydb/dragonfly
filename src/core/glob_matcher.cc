// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/glob_matcher.h"

#define PCRE2_CODE_UNIT_WIDTH 8
#include <pcre2.h>

#include "base/logging.h"

namespace dfly {
using namespace std;

string GlobMatcher::Glob2Regex(string_view glob) {
  string regex;
  regex.reserve(glob.size());
  bool in_group = false;

  for (size_t i = 0; i < glob.size(); i++) {
    char c = glob[i];
    if (in_group) {
      regex.push_back(c);
      if (c == ']')
        in_group = false;
      continue;
    }

    switch (c) {
      case '*':
        regex.append(".*");
        break;
      case '?':
        regex.append(".");
        break;
      case '.':
      case '(':
      case ')':
      case '{':
      case '}':
      case '^':
      case '$':
      case '+':
      case '|':
        regex.push_back('\\');
        regex.push_back(c);
        break;
      case '\\':
        regex.push_back('\\');
        if (i + 1 < glob.size()) {
          regex.push_back(glob[++i]);
        }
        break;
      case '[':
        regex.push_back('[');
        if (i + 1 < glob.size()) {
          in_group = true;
        }
        break;
      default:
        regex.push_back(c);
        break;
    }
  }
  return regex;
}

GlobMatcher::GlobMatcher(string_view pattern, bool case_sensitive)
    : case_sensitive_(case_sensitive) {
  if (!pattern.empty()) {
    starts_with_star_ = pattern.front() == '*';
    pattern.remove_prefix(starts_with_star_);

    if (!pattern.empty()) {
      ends_with_star_ = pattern.back() == '*';
      pattern.remove_suffix(ends_with_star_);
    }
  }

  empty_pattern_ = pattern.empty();
  // string regex;
#if 1
  string regex("(?");  // dotall mode
  if (!case_sensitive) {
    regex.push_back('i');
  }
  regex.push_back(')');
#endif
  regex.append(Glob2Regex(pattern));
  int errnum;
  PCRE2_SIZE erroffset;
  pcre2_code_8* re =
      pcre2_compile_8((PCRE2_SPTR)regex.c_str(), regex.size(), 0, &errnum, &erroffset, nullptr);
  if (re) {
    CHECK_EQ(0, pcre2_jit_compile_8(re, PCRE2_JIT_COMPLETE));
  } else {
    LOG(WARNING) << "Failed to compile regex: " << regex;
  }
  matcher_ = re;
}

GlobMatcher::~GlobMatcher() {
  pcre2_code_free((pcre2_code_8*)matcher_);
}

bool GlobMatcher::Matches(std::string_view str) const {
  if (!matcher_)
    return false;

  pcre2_code_8* re = (pcre2_code_8*)matcher_;
  pcre2_match_data* match_data = pcre2_match_data_create_from_pattern_8(re, NULL);
  int rc = pcre2_jit_match_8(re, (PCRE2_SPTR)str.data(), str.size(), 0,
                             PCRE2_ANCHORED | PCRE2_ENDANCHORED, match_data, NULL);
  pcre2_match_data_free(match_data);
  return rc > 0;
}

}  // namespace dfly
