// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/glob_matcher.h"

#include <absl/strings/ascii.h>

#include "base/logging.h"

namespace dfly {
using namespace std;

string GlobMatcher::Glob2Regex(string_view glob) {
  string regex;
  regex.reserve(glob.size());
  size_t in_group = 0;

  for (size_t i = 0; i < glob.size(); i++) {
    char c = glob[i];
    if (in_group > 0) {
      if (c == ']') {
        if (i == in_group + 1) {
          if (glob[in_group] == '^') {  // [^
            regex.pop_back();
            regex.back() = '.';
            in_group = 0;
            continue;
          }
        }
        in_group = 0;
      }
      regex.push_back(c);
      if (c == '\\') {
        regex.push_back(c);  // escape it.
      }
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
        if (i + 1 < glob.size()) {
          ++i;
        }
        if (absl::ascii_ispunct(glob[i])) {
          regex.push_back('\\');
        }
        regex.push_back(glob[i]);
        break;
      case '[':
        regex.push_back('[');
        if (i + 1 < glob.size()) {
          in_group = i + 1;
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
  string regex("(?s");  // dotall mode
  if (!case_sensitive) {
    regex.push_back('i');
  }
  regex.push_back(')');
  regex.append(Glob2Regex(pattern));
  matcher_.pattern(regex);
}

bool GlobMatcher::Matches(std::string_view str) const {
  DCHECK(!matcher_.pattern().empty());

  matcher_.input(reflex::Input(str.data(), str.size()));

  bool use_find = starts_with_star_ || ends_with_star_;
  if (!use_find) {
    return matcher_.matches() > 0;
  }

  if (empty_pattern_) {
    return !str.empty();
  }

  bool found = matcher_.find() > 0;
  if (!found) {
    return false;
  }

  if (!ends_with_star_ && matcher_.last() != str.size()) {
    return false;
  }
  if (!starts_with_star_ && matcher_.first() != 0) {
    return false;
  }

  return true;
}

}  // namespace dfly
