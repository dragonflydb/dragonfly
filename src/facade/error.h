// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string>
#include <string_view>

namespace facade {

std::string WrongNumArgsError(std::string_view cmd);
std::string ConfigSetFailed(std::string_view config_name);
std::string InvalidExpireTime(std::string_view cmd);
std::string UnknownSubCmd(std::string_view subcmd, std::string_view cmd);

inline constexpr char kSyntaxErr[] = "syntax error";
inline constexpr char kWrongTypeErr[] =
    "-WRONGTYPE Operation against a key holding the wrong kind of value";
inline constexpr char kWrongJsonTypeErr[] = "-WRONGTYPE wrong JSON type of path value";
inline constexpr char kKeyNotFoundErr[] = "no such key";
inline constexpr char kInvalidIntErr[] = "value is not an integer or out of range";
inline constexpr char kInvalidFloatErr[] = "value is not a valid float";
inline constexpr char kUintErr[] = "value is out of range, must be positive";
inline constexpr char kIncrOverflow[] = "increment or decrement would overflow";
inline constexpr char kDbIndOutOfRangeErr[] = "DB index is out of range";
inline constexpr char kInvalidDbIndErr[] = "invalid DB index";
inline constexpr char kScriptNotFound[] = "-NOSCRIPT No matching script. Please use EVAL.";
inline constexpr char kAuthRejected[] =
    "-WRONGPASS invalid username-password pair or user is disabled.";
inline constexpr char kExpiryOutOfRange[] = "expiry is out of range";
inline constexpr char kIndexOutOfRange[] = "index out of range";
inline constexpr char kOutOfMemory[] = "Out of memory";
inline constexpr char kInvalidNumericResult[] = "result is not a number";
inline constexpr char kClusterNotConfigured[] = "Cluster is not yet configured";
inline constexpr char kLoadingErr[] = "-LOADING Dragonfly is loading the dataset in memory";
inline constexpr char kUndeclaredKeyErr[] = "script tried accessing undeclared key";
inline constexpr char kInvalidDumpValueErr[] = "DUMP payload version or checksum are wrong";
inline constexpr char kInvalidJsonPathErr[] = "invalid JSON path";
inline constexpr char kJsonParseError[] = "failed to parse JSON";
inline constexpr char kNanOrInfDuringIncr[] = "increment would produce NaN or Infinity";
inline constexpr char kCrossSlotError[] = "-CROSSSLOT Keys in request don't hash to the same slot";

inline constexpr char kSyntaxErrType[] = "syntax_error";
inline constexpr char kScriptErrType[] = "script_error";
inline constexpr char kConfigErrType[] = "config_error";
inline constexpr char kSearchErrType[] = "search_error";
inline constexpr char kWrongTypeErrType[] = "wrong_type";
inline constexpr char kRestrictDenied[] = "restrict_denied";

}  // namespace facade
