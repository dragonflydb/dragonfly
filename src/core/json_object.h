// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>
#include <memory>
#include <optional>
#include <string_view>

namespace dfly {

// This is temporary, there is an issue right now with jsoncons about using jsonpath
// with custom allocator. once it would resolved, we would change this to use custom allocator
// that allocate memory from mimalloc
using JsonType = jsoncons::pmr::json;

// Build a json object from string. If the string is not legal json, will return nullopt
std::optional<JsonType> JsonFromString(std::string_view input);

inline auto MakeJsonPathExpr(std::string_view path, std::error_code& ec)
    -> jsoncons::jsonpath::jsonpath_expression<JsonType> {
  return jsoncons::jsonpath::make_expression<JsonType, std::allocator<char>>(
      jsoncons::allocator_set<JsonType::allocator_type, std::allocator<char>>(), path, ec);
}

}  // namespace dfly
