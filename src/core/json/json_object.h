// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

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
