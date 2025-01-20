// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <version>  // for __cpp_lib_to_chars macro.

// std::from_chars is available in C++17 if __cpp_lib_to_chars is defined.
#if __cpp_lib_to_chars >= 201611L
#define JSONCONS_HAS_STD_FROM_CHARS 1
#endif

#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>
#include <memory>
#include <optional>
#include <string_view>

#include "base/pmr/memory_resource.h"

namespace dfly {

using JsonType = jsoncons::pmr::json;

// Build a json object from string. If the string is not legal json, will return nullopt
std::optional<JsonType> JsonFromString(std::string_view input, PMR_NS::memory_resource* mr);

inline auto MakeJsonPathExpr(std::string_view path, std::error_code& ec)
    -> jsoncons::jsonpath::jsonpath_expression<JsonType> {
  return jsoncons::jsonpath::make_expression<JsonType, std::allocator<char>>(
      jsoncons::allocator_set<JsonType::allocator_type, std::allocator<char>>(), path, ec);
}

}  // namespace dfly
