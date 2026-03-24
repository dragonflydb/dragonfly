// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/search/filter_functions.h"

#include <absl/container/flat_hash_map.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <ctime>
#include <numbers>
#include <string>
#include <variant>

namespace dfly::aggregate {

namespace {

// Type helpers

std::optional<double> AsDouble(const Value& v) {
  if (std::holds_alternative<double>(v))
    return std::get<double>(v);
  if (std::holds_alternative<std::string>(v)) {
    double d = 0.0;
    if (absl::SimpleAtod(std::get<std::string>(v), &d))
      return d;
  }
  return std::nullopt;
}

const std::string* AsStr(const Value& v) {
  return std::get_if<std::string>(&v);
}

std::optional<struct tm> ToTm(const Value& v) {
  auto d = AsDouble(v);
  if (!d)
    return std::nullopt;
  // Guard against NaN / +/-Inf / values outside time_t range before casting.
  // time_t is typically int64_t; use the same safe bounds as FnFormat '%d'.
  if (!std::isfinite(*d))
    return std::nullopt;
  constexpr double kTimeMin = -9223372036854775808.0;  // -(2^63), exact
  constexpr double kTimeMax = 9223372036854774784.0;   // 2^63 - 1024
  time_t ts = static_cast<time_t>(std::clamp(*d, kTimeMin, kTimeMax));
  struct tm t {};
  gmtime_r(&ts, &t);
  return t;
}

// String functions

Value FnLower(FuncArgs args) {
  if (args.size() != 1)
    return {};
  const auto* s = AsStr(args[0]);
  if (!s)
    return {};
  std::string result(*s);
  absl::AsciiStrToLower(&result);
  return result;
}

Value FnUpper(FuncArgs args) {
  if (args.size() != 1)
    return {};
  const auto* s = AsStr(args[0]);
  if (!s)
    return {};
  std::string result(*s);
  absl::AsciiStrToUpper(&result);
  return result;
}

Value FnSubstr(FuncArgs args) {
  if (args.size() != 3)
    return {};
  const auto* s = AsStr(args[0]);
  auto offset = AsDouble(args[1]);
  auto len = AsDouble(args[2]);
  if (!s || !offset || !len)
    return {};
  // Non-finite offset/len: propagate null (same pattern as the rest of the codebase).
  if (!std::isfinite(*offset) || !std::isfinite(*len))
    return {};
  // Clamp to [0, s->size()] before casting to avoid UB for large doubles (> SIZE_MAX).
  double safe_max = static_cast<double>(s->size());
  size_t off = static_cast<size_t>(std::clamp(*offset, 0.0, safe_max));
  size_t length = static_cast<size_t>(std::clamp(*len, 0.0, safe_max));
  if (off >= s->size())
    return std::string{};
  return std::string(s->substr(off, length));
}

Value FnFormat(FuncArgs args) {
  if (args.empty())
    return {};
  const auto* fmt = AsStr(args[0]);
  if (!fmt)
    return {};

  std::string out;
  size_t arg_idx = 1;
  for (size_t i = 0; i < fmt->size(); ++i) {
    if ((*fmt)[i] == '%' && i + 1 < fmt->size()) {
      char spec = (*fmt)[i + 1];
      if (spec == '%') {
        out += '%';
        ++i;
        continue;
      }
      if (arg_idx >= args.size()) {
        out += (*fmt)[i];
        continue;
      }
      if (spec == 's') {
        if (const auto* sv = AsStr(args[arg_idx]))
          out += *sv;
        else if (auto d = AsDouble(args[arg_idx]))
          out += absl::StrCat(*d);
        ++arg_idx;
        ++i;
        continue;
      }
      if (spec == 'd') {
        if (auto d = AsDouble(args[arg_idx])) {
          if (std::isfinite(*d)) {
            // LLONG_MAX (2^63-1) is not exactly representable as double; it rounds up
            // to 2^63, which overflows on cast to long long (UB). Clamp to the largest
            // double that safely casts: 2^63-1024 = 9223372036854774784.
            constexpr double kLLMin = -9223372036854775808.0;  // -(2^63), exact
            constexpr double kLLMax = 9223372036854774784.0;   // 2^63 - 1024
            out += absl::StrCat(static_cast<long long>(std::clamp(*d, kLLMin, kLLMax)));
          }
          // Non-finite (NaN / +/-Inf): emit nothing, consistent with null propagation.
        }
        ++arg_idx;
        ++i;
        continue;
      }
      if (spec == 'f') {
        if (auto d = AsDouble(args[arg_idx])) {
          if (std::isfinite(*d)) {
            char buf[64];
            std::snprintf(buf, sizeof(buf), "%f", *d);
            out += buf;
          }
          // Non-finite (NaN / +/-Inf): emit nothing, consistent with %d.
        }
        ++arg_idx;
        ++i;
        continue;
      }
      // Unknown specifier: emit %X literally, advance past spec char, don't consume an arg.
      out += (*fmt)[i];  // emit '%'
      out += spec;
      ++i;
      continue;
    }
    out += (*fmt)[i];
  }
  return out;
}

// split(s [, sep [, strip]]) -- splits s by sep (default ",") and returns the
// parts rejoined with "," as a normalised comma-separated string.
// strip (third argument, any truthy value) trims whitespace from each part.
// Value does not support arrays, so the caller uses contains() / startswith()
// on the joined result.
Value FnSplit(FuncArgs args) {
  if (args.empty())
    return {};
  const auto* s = AsStr(args[0]);
  if (!s)
    return {};

  std::string sep = ",";
  if (args.size() >= 2) {
    const auto* sep_arg = AsStr(args[1]);
    if (!sep_arg)
      return {};  // non-string separator (null or number) -> propagate null
    sep = *sep_arg;
  }

  // Empty separator: return the original string unchanged to avoid splitting into characters.
  if (sep.empty())
    return *s;

  bool strip = false;
  if (args.size() >= 3) {
    if (auto d = AsDouble(args[2]))
      strip = (!std::isnan(*d) && *d != 0.0);
    else if (const auto* sv = AsStr(args[2]))
      strip = !sv->empty();
  }

  std::vector<std::string_view> parts = absl::StrSplit(*s, sep);
  std::string result;
  bool first = true;
  for (auto part : parts) {
    if (!first)
      result += ',';
    if (strip) {
      result += absl::StripAsciiWhitespace(part);
    } else {
      result += part;
    }
    first = false;
  }
  return result;
}

// matched_terms() -- in the aggregation pipeline there is no FT.SEARCH query
// context available, so this always returns an empty string.
Value FnMatchedTerms(FuncArgs /*args*/) {
  return std::string{};
}

Value FnToNumber(FuncArgs args) {
  if (args.size() != 1)
    return {};
  if (auto d = AsDouble(args[0]))
    return *d;
  return {};
}

Value FnToStr(FuncArgs args) {
  if (args.size() != 1)
    return {};
  if (const auto* s = AsStr(args[0]))
    return *s;
  if (auto d = AsDouble(args[0]))
    return absl::StrCat(*d);
  return {};  // null input -> propagate null, consistent with to_number()
}

Value FnExists(FuncArgs args) {
  if (args.size() != 1)
    return {};
  return std::holds_alternative<std::monostate>(args[0]) ? 0.0 : 1.0;
}

Value FnStartsWith(FuncArgs args) {
  if (args.size() != 2)
    return {};
  const auto* s = AsStr(args[0]);
  const auto* prefix = AsStr(args[1]);
  if (!s || !prefix)
    return {};
  return absl::StartsWith(*s, *prefix) ? 1.0 : 0.0;
}

Value FnContains(FuncArgs args) {
  if (args.size() != 2)
    return {};
  const auto* s = AsStr(args[0]);
  const auto* sub = AsStr(args[1]);
  if (!s || !sub)
    return {};
  return absl::StrContains(*s, *sub) ? 1.0 : 0.0;
}

Value FnStrLen(FuncArgs args) {
  if (args.size() != 1)
    return {};
  const auto* s = AsStr(args[0]);
  if (!s)
    return {};
  return static_cast<double>(s->size());
}

// Math functions (single numeric argument)

double MathAbs(double x) {
  return std::abs(x);
}

double MathFloor(double x) {
  return std::floor(x);
}

double MathCeil(double x) {
  return std::ceil(x);
}

double MathSqrt(double x) {
  return std::sqrt(x);
}

double MathLog(double x) {
  return std::log(x);
}

double MathLog2(double x) {
  return std::log2(x);
}

double MathLog10(double x) {
  return std::log10(x);
}

double MathExp(double x) {
  return std::exp(x);
}

template <double (*Fn)(double)> Value MathFunc1(FuncArgs args) {
  if (args.size() != 1)
    return {};
  auto d = AsDouble(args[0]);
  if (!d)
    return {};
  return Fn(*d);
}

// Date/time functions

Value FnHour(FuncArgs args) {
  if (args.size() != 1)
    return {};
  auto t = ToTm(args[0]);
  if (!t)
    return {};
  return static_cast<double>(t->tm_hour);
}

Value FnMinute(FuncArgs args) {
  if (args.size() != 1)
    return {};
  auto t = ToTm(args[0]);
  if (!t)
    return {};
  return static_cast<double>(t->tm_min);
}

Value FnDay(FuncArgs args) {
  if (args.size() != 1)
    return {};
  auto t = ToTm(args[0]);
  if (!t)
    return {};
  return static_cast<double>(t->tm_mday);  // 1-31
}

Value FnMonth(FuncArgs args) {
  if (args.size() != 1)
    return {};
  auto t = ToTm(args[0]);
  if (!t)
    return {};
  return static_cast<double>(t->tm_mon + 1);  // 1-12
}

Value FnYear(FuncArgs args) {
  if (args.size() != 1)
    return {};
  auto t = ToTm(args[0]);
  if (!t)
    return {};
  return static_cast<double>(t->tm_year + 1900);
}

Value FnDayOfWeek(FuncArgs args) {
  if (args.size() != 1)
    return {};
  auto t = ToTm(args[0]);
  if (!t)
    return {};
  return static_cast<double>(t->tm_wday);  // 0=Sunday
}

Value FnDayOfYear(FuncArgs args) {
  if (args.size() != 1)
    return {};
  auto t = ToTm(args[0]);
  if (!t)
    return {};
  return static_cast<double>(t->tm_yday + 1);  // 1-based
}

Value FnTimefmt(FuncArgs args) {
  if (args.empty())
    return {};
  auto t = ToTm(args[0]);
  if (!t)
    return {};

  std::string fmt = "%Y-%m-%dT%H:%M:%S";
  if (args.size() >= 2) {
    const auto* fmt_arg = AsStr(args[1]);
    if (!fmt_arg)
      return {};  // non-string format (null or number) -> propagate null
    fmt = *fmt_arg;
  }

  // Prepend a sentinel so we can distinguish "buffer too small" (returns 0) from
  // "format produced empty output" (returns 1 for the sentinel alone).
  fmt.insert(fmt.begin(), ' ');
  for (size_t cap = 256; cap <= (1u << 16); cap *= 2) {
    std::string buf(cap, '\0');
    size_t n = strftime(buf.data(), cap, fmt.c_str(), &*t);
    if (n > 0) {
      buf.erase(0, 1);
      buf.resize(n - 1);
      return buf;
    }
  }
  return std::string{};
}

Value FnParseTime(FuncArgs args) {
  if (args.size() != 2)
    return {};
  const auto* s = AsStr(args[0]);
  const auto* fmt = AsStr(args[1]);
  if (!s || !fmt)
    return {};

  struct tm t {};
  // strptime returns a pointer to the first unprocessed character, or NULL on failure.
  // We require the entire string to be consumed (no trailing garbage).
  const char* end = strptime(s->c_str(), fmt->c_str(), &t);
  if (!end || *end != '\0')
    return {};
  return static_cast<double>(timegm(&t));
}

// Geo function

// Haversine formula: distance between two (lon, lat) points in km.
double HaversineKm(double lat1, double lon1, double lat2, double lon2) {
  constexpr double kR = 6371.0;
  auto toRad = [](double deg) { return deg * std::numbers::pi / 180.0; };
  double dlat = toRad(lat2 - lat1);
  double dlon = toRad(lon2 - lon1);
  double a = std::sin(dlat / 2) * std::sin(dlat / 2) + std::cos(toRad(lat1)) *
                                                           std::cos(toRad(lat2)) *
                                                           std::sin(dlon / 2) * std::sin(dlon / 2);
  // Clamp to [0, 1] before sqrt to guard against floating-point rounding that
  // can push `a` slightly above 1.0 for near-antipodal points, which would
  // produce NaN.
  a = std::clamp(a, 0.0, 1.0);
  return kR * 2.0 * std::atan2(std::sqrt(a), std::sqrt(1.0 - a));
}

// geodistance(p1, p2 [, unit]) -> numeric distance.
// Note: RediSearch documents a 4th "fmt" argument for the output string format,
// but this implementation returns a numeric distance directly -- fmt is unused.
Value FnGeoDistance(FuncArgs args) {
  if (args.size() < 2)
    return {};

  // Points are "lon,lat" strings.
  auto ParsePoint = [](const Value& v) -> std::optional<std::pair<double, double>> {
    const auto* s = std::get_if<std::string>(&v);
    if (!s)
      return std::nullopt;
    std::vector<std::string_view> pts = absl::StrSplit(*s, ',');
    if (pts.size() != 2)
      return std::nullopt;
    double lon = 0.0, lat = 0.0;
    if (!absl::SimpleAtod(pts[0], &lon) || !absl::SimpleAtod(pts[1], &lat))
      return std::nullopt;
    return std::make_pair(lon, lat);
  };

  auto p1 = ParsePoint(args[0]);
  auto p2 = ParsePoint(args[1]);
  if (!p1 || !p2)
    return {};

  double dist_km = HaversineKm(p1->second, p1->first, p2->second, p2->first);

  std::string unit = "km";
  if (args.size() >= 3) {
    const auto* u = AsStr(args[2]);
    if (!u)
      return {};  // non-string unit (null or number) -> propagate null
    unit = *u;
    absl::AsciiStrToLower(&unit);
  }

  double result = dist_km;
  if (unit == "km")
    result = dist_km;
  else if (unit == "m")
    result = dist_km * 1000.0;
  else if (unit == "mi")
    result = dist_km / 1.609344;
  else if (unit == "ft")
    result = dist_km * (1000.0 / 0.3048);
  else
    return {};  // unknown unit -> propagate null

  return result;
}

// Registry

const absl::flat_hash_map<std::string, FuncImpl>& Registry() {
  static const absl::flat_hash_map<std::string, FuncImpl> kRegistry = {
      // String
      {"lower", FnLower},
      {"upper", FnUpper},
      {"substr", FnSubstr},
      {"format", FnFormat},
      {"split", FnSplit},
      {"matched_terms", FnMatchedTerms},
      {"to_number", FnToNumber},
      {"to_str", FnToStr},
      {"exists", FnExists},
      {"startswith", FnStartsWith},
      {"contains", FnContains},
      {"strlen", FnStrLen},
      // Math
      {"abs", MathFunc1<MathAbs>},
      {"floor", MathFunc1<MathFloor>},
      {"ceil", MathFunc1<MathCeil>},
      {"sqrt", MathFunc1<MathSqrt>},
      {"log", MathFunc1<MathLog>},
      {"log2", MathFunc1<MathLog2>},
      {"log10", MathFunc1<MathLog10>},
      {"exp", MathFunc1<MathExp>},
      // Date/time
      {"hour", FnHour},
      {"minute", FnMinute},
      {"day", FnDay},
      {"month", FnMonth},
      {"monthofyear", FnMonth},  // alias
      {"dayofmonth", FnDay},     // alias
      {"dayofweek", FnDayOfWeek},
      {"dayofyear", FnDayOfYear},
      {"year", FnYear},
      {"timefmt", FnTimefmt},
      {"parsetime", FnParseTime},
      // Geo
      {"geodistance", FnGeoDistance},
  };
  return kRegistry;
}

}  // namespace

const FuncImpl* FindFilterFunction(std::string_view name) {
  // The parser lowercases all identifiers (IDENT rule calls ToLower), so name
  // is already lowercase here -- no extra allocation needed.
  const auto& reg = Registry();
  auto it = reg.find(name);
  return it != reg.end() ? &it->second : nullptr;
}

}  // namespace dfly::aggregate
