// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/glob_matcher.h"

#include <absl/strings/ascii.h>

#include <algorithm>
#include <cstdint>
#include <utility>

#include "base/logging.h"

namespace dfly {
using namespace std;

namespace {

// Glob syntax, the upstream dialect, matched byte by byte:
//   *      any run of bytes, including none
//   ?      exactly one byte
//   [...]  one byte from the set; a leading ^ negates it. A member is a byte, a range lo-hi or
//          \x for the literal byte x. ] ^ - are ordinary bytes outside a class.
//   \x     the literal byte x; a trailing lone \ is a literal backslash
//   x      any other byte matches itself
// Kept for compatibility with upstream:
//   - the empty string matches only the empty pattern, so "*" does not match ""
//   - reversed range bounds are ordered first and lowered afterwards, so "[a-Z]" is "[Z-a]" and
//     matches nothing case-insensitively
//   - an unclosed class runs to the end of the pattern; "[]" matches nothing; "[^]" is "?"
//   - escaped members of a class are compared case-sensitively even when case-insensitive
// Ranges compare unsigned byte values. Upstream promotes plain char, so for bytes >= 0x80 its
// result depends on whether char is signed; this does not.

template <bool kCaseSensitive> bool EqualChars(char a, char b) {
  if constexpr (kCaseSensitive)
    return a == b;
  else
    return absl::ascii_tolower(a) == absl::ascii_tolower(b);
}

constexpr bool IsMeta(char c) {
  return c == '*' || c == '?' || c == '[' || c == '\\';
}

string_view SkipStars(string_view pattern) {
  while (!pattern.empty() && pattern.front() == '*')
    pattern.remove_prefix(1);
  return pattern;
}

template <bool kCaseSensitive> bool InRange(uint8_t lo, uint8_t hi, uint8_t c) {
  const auto [low, high] = minmax({lo, hi});  // The list overload returns values, not references.
  if constexpr (kCaseSensitive) {
    return low <= c && c <= high;
  } else {
    const uint8_t lo_folded = absl::ascii_tolower(low), hi_folded = absl::ascii_tolower(high),
                  c_folded = absl::ascii_tolower(c);
    return lo_folded <= c_folded && c_folded <= hi_folded;
  }
}

// Consumes the class that follows '[', matched or not. An unclosed class runs to the pattern end.
template <bool kCaseSensitive> bool MatchClass(string_view* pattern, char c) {
  string_view items = *pattern;
  const bool negate = !items.empty() && items.front() == '^';
  if (negate)
    items.remove_prefix(1);

  bool match = false;
  while (!items.empty() && items.front() != ']') {
    const bool is_escape = items.size() >= 2 && items[0] == '\\';
    const bool is_range = !is_escape && items.size() >= 3 && items[1] == '-';
    if (is_escape) {
      match |= items[1] == c;  // Escaped members are always case sensitive.
      items.remove_prefix(2);
    } else if (is_range) {
      match |= InRange<kCaseSensitive>(items[0], items[2], c);
      items.remove_prefix(3);
    } else {
      match |= EqualChars<kCaseSensitive>(items[0], c);
      items.remove_prefix(1);
    }
  }
  if (!items.empty())
    items.remove_prefix(1);
  *pattern = items;
  return match != negate;
}

// Consumes the non-'*' token at the front, matched or not: the caller resets pattern on mismatch.
template <bool kCaseSensitive> bool MatchToken(string_view* pattern, char c) {
  DCHECK(!pattern->empty());
  char token = pattern->front();
  pattern->remove_prefix(1);
  switch (token) {
    case '?':
      return true;
    case '[':
      return MatchClass<kCaseSensitive>(pattern, c);
    case '\\':
      if (!pattern->empty()) {  // A trailing lone backslash is a literal.
        token = pattern->front();
        pattern->remove_prefix(1);
      }
      [[fallthrough]];
    default:
      return EqualChars<kCaseSensitive>(token, c);
  }
}

// Iterative on purpose: the recursive upstream matcher overflows small fiber stacks.
template <bool kCaseSensitive> bool MatchGlobImpl(string_view pattern, string_view str) {
  if (str.empty())  // "*" does not match an empty string.
    return pattern.empty();

  // Fast path for the common "prefix*" shape: leading literal bytes have to match in place.
  while (!pattern.empty() && !str.empty() && !IsMeta(pattern.front())) {
    if (!EqualChars<kCaseSensitive>(pattern.front(), str.front()))
      return false;
    pattern.remove_prefix(1);
    str.remove_prefix(1);
  }

  string_view star_pattern;   // pattern after the last '*', empty until a '*' is seen
  string_view star_str;       // where the last '*' currently stops absorbing
  bool star_literal = false;  // star_pattern starts with a literal byte that find() can look for

  while (!str.empty()) {
    if (!pattern.empty() && pattern.front() == '*') {
      pattern = SkipStars(pattern);
      if (pattern.empty())
        return true;
      star_pattern = pattern;
      star_str = str;
      star_literal = kCaseSensitive && !IsMeta(pattern.front());
    } else if (!pattern.empty() && MatchToken<kCaseSensitive>(&pattern, str.front())) {
      str.remove_prefix(1);
    } else if (star_pattern.empty()) {
      return false;
    } else {
      // Only the last '*' is ever retried: it absorbs up to where its next token can match.
      star_str.remove_prefix(1);
      if (star_literal && !star_str.empty() && star_str.front() != star_pattern.front()) {
        const size_t pos = star_str.find(star_pattern.front());
        if (pos == string_view::npos)
          return false;
        star_str.remove_prefix(pos);
      }
      pattern = star_pattern;
      str = star_str;
    }
  }
  return SkipStars(pattern).empty();
}

}  // namespace

bool GlobMatcher::MatchGlob(string_view pattern, string_view str, bool case_sensitive) {
  return case_sensitive ? MatchGlobImpl<true>(pattern, str) : MatchGlobImpl<false>(pattern, str);
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
        if (i + 1 < glob.size() && glob[i + 1] == ']') {
          ++i;
          regex.push_back(']');
        } else {
          regex.push_back('\\');  // escape the backslash
        }
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
      ends_with_star_ =
          (pattern.back() == '*') && (pattern.size() == 1 || pattern[pattern.size() - 2] != '\\');
      pattern.remove_suffix(ends_with_star_);
    }
  }

  string regex("(?s");  // dotall mode
  if (!case_sensitive) {
    regex.push_back('i');
  }
  regex.push_back(')');
  if (pattern.empty()) {
    regex.append(Glob2Regex("*"));
  } else {
    regex.append(Glob2Regex(pattern));
  }
  matcher_.pattern(regex);
#elif defined(USE_PCRE2)
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
    return MatchGlob(glob_, str, case_sensitive_);
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
#elif defined(USE_PCRE2)
  if (!re_ || str.size() < 16) {
    return MatchGlob(glob_, str, case_sensitive_);
  }

  if (glob_.empty()) {
    return true;
  }

  int rc = pcre2_jit_match(re_, (PCRE2_SPTR)str.data(), str.size(), 0, 0, match_data_, NULL);
  return rc > 0;

#else
  return MatchGlob(glob_, str, case_sensitive_);
#endif
}

GlobMatcher::~GlobMatcher() {
#ifdef REFLEX_PERFORMANCE
#elif defined(USE_PCRE2)
  if (re_) {
    pcre2_code_free(re_);
    pcre2_match_data_free(match_data_);
  }
#endif
}

}  // namespace dfly
