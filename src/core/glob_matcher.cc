// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/glob_matcher.h"

#include <absl/strings/ascii.h>

#include "base/logging.h"

namespace dfly {
using namespace std;

/* Glob-style pattern matching taken from Redis. */
int stringmatchlen(const char* pattern, int patternLen, const char* string, int stringLen,
                   int nocase) {
  while (patternLen && stringLen) {
    switch (pattern[0]) {
      case '*':
        while (patternLen && pattern[1] == '*') {
          pattern++;
          patternLen--;
        }
        if (patternLen == 1)
          return 1; /* match */
        while (stringLen) {
          if (stringmatchlen(pattern + 1, patternLen - 1, string, stringLen, nocase))
            return 1; /* match */
          string++;
          stringLen--;
        }
        return 0; /* no match */
        break;
      case '?':
        string++;
        stringLen--;
        break;
      case '[': {
        int neg, match;

        pattern++;
        patternLen--;
        neg = pattern[0] == '^';
        if (neg) {
          pattern++;
          patternLen--;
        }
        match = 0;
        while (1) {
          if (pattern[0] == '\\' && patternLen >= 2) {
            pattern++;
            patternLen--;
            if (pattern[0] == string[0])
              match = 1;
          } else if (pattern[0] == ']') {
            break;
          } else if (patternLen == 0) {
            pattern--;
            patternLen++;
            break;
          } else if (patternLen >= 3 && pattern[1] == '-') {
            int start = pattern[0];
            int end = pattern[2];
            int c = string[0];
            if (start > end) {
              int t = start;
              start = end;
              end = t;
            }
            if (nocase) {
              start = tolower(start);
              end = tolower(end);
              c = tolower(c);
            }
            pattern += 2;
            patternLen -= 2;
            if (c >= start && c <= end)
              match = 1;
          } else {
            if (!nocase) {
              if (pattern[0] == string[0])
                match = 1;
            } else {
              if (tolower((int)pattern[0]) == tolower((int)string[0]))
                match = 1;
            }
          }
          pattern++;
          patternLen--;
        }
        if (neg)
          match = !match;
        if (!match)
          return 0; /* no match */
        string++;
        stringLen--;
        break;
      }
      case '\\':
        if (patternLen >= 2) {
          pattern++;
          patternLen--;
        }
        /* fall through */
      default:
        if (!nocase) {
          if (pattern[0] != string[0])
            return 0; /* no match */
        } else {
          if (tolower((int)pattern[0]) != tolower((int)string[0]))
            return 0; /* no match */
        }
        string++;
        stringLen--;
        break;
    }
    pattern++;
    patternLen--;
    if (stringLen == 0) {
      while (*pattern == '*') {
        pattern++;
        patternLen--;
      }
      break;
    }
  }
  if (patternLen == 0 && stringLen == 0)
    return 1;
  return 0;
}

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
#ifdef FIX_PERFORMANCE_MATCHING
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
#else
  glob_ = pattern;
#endif
}

bool GlobMatcher::Matches(std::string_view str) const {
#ifdef FIX_PERFORMANCE_MATCHING
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
#else
  return stringmatchlen(glob_.data(), glob_.size(), str.data(), str.size(), !case_sensitive_);
#endif
}

}  // namespace dfly
