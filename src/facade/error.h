// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <atomic>
#include <string>

namespace facade {

std::string WrongNumArgsError(std::string_view cmd);
std::string InvalidExpireTime(std::string_view cmd);
std::string UnknownSubCmd(std::string_view subcmd, std::string_view cmd);


extern const char kSyntaxErr[];
extern const char kWrongTypeErr[];
extern const char kKeyNotFoundErr[];
extern const char kInvalidIntErr[];
extern const char kInvalidFloatErr[];
extern const char kUintErr[];
extern const char kIncrOverflow[];
extern const char kDbIndOutOfRangeErr[];
extern const char kInvalidDbIndErr[];
extern const char kScriptNotFound[];
extern const char kAuthRejected[];
extern const char kExpiryOutOfRange[];
extern const char kSyntaxErrType[];
extern const char kScriptErrType[];
extern const char kIndexOutOfRange[];
extern const char kOutOfMemory[];
extern const char kInvalidNumericResult[];

}  // namespace dfly
