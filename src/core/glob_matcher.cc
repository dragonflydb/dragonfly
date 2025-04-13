// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/glob_matcher.h"

#include <absl/strings/ascii.h>

#include "base/logging.h"

namespace dfly {
using namespace std;

/* Glob-style pattern matching taken from Redis. */
static int stringmatchlen_impl(const char* pattern, int patternLen, const char* string,
                               int stringLen, int nocase, int* skipLongerMatches) {
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
          if (stringmatchlen_impl(pattern + 1, patternLen - 1, string, stringLen, nocase,
                                  skipLongerMatches))
            return 1; /* match */
          if (*skipLongerMatches)
            return 0; /* no match */
          string++;
          stringLen--;
        }

        /* There was no match for the rest of the pattern starting
         * from anywhere in the rest of the string. If there were
         * any '*' earlier in the pattern, we can terminate the
         * search early without trying to match them to longer
         * substrings. This is because a longer match for the
         * earlier part of the pattern would require the rest of the
         * pattern to match starting later in the string, and we
         * have just determined that there is no match for the rest
         * of the pattern starting from anywhere in the current
         * string. */
        *skipLongerMatches = 1;
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

int stringmatchlen(const char* pattern, int patternLen, const char* string, int stringLen,
                   int nocase) {
  int skipLongerMatches = 0;
  return stringmatchlen_impl(pattern, patternLen, string, stringLen, nocase, &skipLongerMatches);
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
    : glob_(pattern), case_sensitive_(case_sensitive) {
#ifdef REFLEX_PERFORMANCE
  if (!pattern.empty()) {
    starts_with_star_ = pattern.front() == '*';
    pattern.remove_prefix(starts_with_star_);

    if (!pattern.empty()) {
      ends_with_star_ = pattern.back() == '*';
      pattern.remove_suffix(ends_with_star_);
    }
  }

  string regex("(?s");  // dotall mode
  if (!case_sensitive) {
    regex.push_back('i');
  }
  regex.push_back(')');
  regex.append(Glob2Regex(pattern));
  matcher_.pattern(regex);
#elif USE_PCRE2
  string regex("(?s");  // dotall mode
  if (!case_sensitive) {
    regex.push_back('i');
  }
  regex.push_back(')');
  regex.append(Glob2Regex(pattern));

  int errnum;
  PCRE2_SIZE erroffset;
  re_ = pcre2_compile((PCRE2_SPTR)regex.c_str(), regex.size(), 0, &errnum, &erroffset, nullptr);
  if (re_) {
    CHECK_EQ(0, pcre2_jit_compile(re_, PCRE2_JIT_COMPLETE));
    match_data_ = pcre2_match_data_create_from_pattern(re_, NULL);
  }
#endif
}

bool GlobMatcher::Matches(std::string_view str) const {
#ifdef REFLEX_PERFORMANCE
  if (str.size() < 16) {
    return stringmatchlen(glob_.data(), glob_.size(), str.data(), str.size(), !case_sensitive_);
  }
  if (glob_.empty()) {
    return true;
  }

  DCHECK(!matcher_.pattern().empty());

  matcher_.input(reflex::Input(str.data(), str.size()));

  bool use_find = starts_with_star_ || ends_with_star_;
  if (!use_find) {
    return matcher_.matches() > 0;
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
#elif USE_PCRE2
  if (!re_ || str.size() < 16) {
    return stringmatchlen(glob_.data(), glob_.size(), str.data(), str.size(), !case_sensitive_);
  }

  if (glob_.empty()) {
    return true;
  }

  int rc = pcre2_jit_match(re_, (PCRE2_SPTR)str.data(), str.size(), 0, 0, match_data_, NULL);
  return rc > 0;

#else
  return stringmatchlen(glob_.data(), glob_.size(), str.data(), str.size(), !case_sensitive_);
#endif
}

GlobMatcher::~GlobMatcher() {
#ifdef REFLEX_PERFORMANCE
#elif USE_PCRE2
  if (re_) {
    pcre2_code_free(re_);
    pcre2_match_data_free(match_data_);
  }
#endif
}

}  // namespace dfly
