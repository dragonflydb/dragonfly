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

using TmpJson = jsoncons::json;
using JsonType = jsoncons::pmr::json;

// A helper type to use in template functions which are expected to work with both TmpJson
// and JsonType
template <typename Allocator>
using JsonWithAllocator = jsoncons::basic_json<char, jsoncons::sorted_policy, Allocator>;

// Parses string into JSON. Any allocatons are done using the std allocator. This method should be
// used for generic JSON parsing, in particular, it should not be used to parse objects which will
// be stored in the db, as the backing storage is not managed by mimalloc.
std::optional<TmpJson> JsonFromString(std::string_view input);

// Parses string into JSON, using mimalloc heap for allocations. This method should only be used on
// shards where mimalloc heap is initialized.
std::optional<JsonType> JsonFromString(std::string_view input, PMR_NS::memory_resource* mr);

// Deep copy a JSON object, by first serializing it to a string and then deserializing the string.
// The operation is intended to help during defragmentation, by copying into a page reserved for
// malloc.
JsonType DeepCopyJSON(const JsonType* j, PMR_NS::memory_resource* mr);

template <typename Json = JsonType>
auto MakeJsonPathExpr(std::string_view path, std::error_code& ec)
    -> jsoncons::jsonpath::jsonpath_expression<Json> {
  using result_allocator_t = typename Json::allocator_type;
  using temp_allocator_t = std::allocator<char>;
  using allocator_set_t = jsoncons::allocator_set<result_allocator_t, temp_allocator_t>;
  return jsoncons::jsonpath::make_expression<Json, temp_allocator_t>(allocator_set_t(), path, ec);
}

}  // namespace dfly
