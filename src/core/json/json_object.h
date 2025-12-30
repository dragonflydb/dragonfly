// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <version>  // for __cpp_lib_to_chars macro.

#include "core/detail/stateless_allocator.h"

// std::from_chars is available in C++17 if __cpp_lib_to_chars is defined.
#if __cpp_lib_to_chars >= 201611L
#define JSONCONS_HAS_STD_FROM_CHARS 1
#endif

#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/jsonpath.hpp>
#include <memory>
#include <optional>
#include <string_view>

namespace dfly {
class PageUsage;

using TmpJson = jsoncons::json;
using JsonType = jsoncons::basic_json<char, jsoncons::sorted_policy, StatelessAllocator<char>>;

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
std::optional<JsonType> ParseJsonUsingShardHeap(std::string_view input);

// Defragments the given json object by traversing its tree structure non-recursively, examining
// nodes and defragmenting as needed. Returns true if any object within the node was reallocated
bool Defragment(JsonType& j, PageUsage* page_usage);

template <typename Json = JsonType>
auto MakeJsonPathExpr(std::string_view path, std::error_code& ec)
    -> jsoncons::jsonpath::jsonpath_expression<Json> {
  using ResultAllocT = typename Json::allocator_type;
  using TmpAllocT = std::allocator<char>;
  using AllocSetT = jsoncons::allocator_set<ResultAllocT, TmpAllocT>;
  return jsoncons::jsonpath::make_expression<Json, TmpAllocT>(AllocSetT(), path, ec);
}

}  // namespace dfly
