// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <memory>
#include <memory_resource>
#include <optional>
#include <string_view>

// Note about this file - once we have the issue with jsonpath in jsoncons resolved
// we would add the implementation for the allocator here as well. Right now this
// file is a little bit empty, but for external "users" such as json_family they
// should include this when creating JSON object from string that we're getting
// from the commands.
namespace jsoncons {
struct sorted_policy;
template <typename CharT, typename Policy, typename Allocator> class basic_json;
}  // namespace jsoncons

// the last one in object.h is OBJ_STREAM and it is 6,
// this will add enough place for Redis types to grow
constexpr unsigned OBJ_JSON = 15;

namespace dfly {

// This is temporary, there is an issue right now with jsoncons about using jsonpath
// with custom allocator. once it would resolved, we would change this to use custom allocator
// that allocate memory from mimalloc
using JsonType = jsoncons::basic_json<char, jsoncons::sorted_policy, std::allocator<char>>;

// Build a json object from string. If the string is not legal json, will return nullopt
std::optional<JsonType> JsonFromString(std::string_view input);

}  // namespace dfly
