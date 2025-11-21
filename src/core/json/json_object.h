// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <glog/logging.h>

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

namespace detail {

inline thread_local PMR_NS::memory_resource* tl_mr = nullptr;

template <typename T> class StatelessJsonAllocator {
 public:
  using value_type = T;
  using size_type = std::size_t;
  using difference_type = std::ptrdiff_t;
  using is_always_equal = std::true_type;

  template <typename U> StatelessJsonAllocator(const StatelessJsonAllocator<U>&) noexcept {
  }

  StatelessJsonAllocator() noexcept {
    DCHECK_NE(tl_mr, nullptr) << "json allocator created without backing memory resource";
  };

  static value_type* allocate(size_type n) {
    void* ptr = tl_mr->allocate(n * sizeof(value_type), alignof(value_type));
    return static_cast<value_type*>(ptr);
  }

  static void deallocate(value_type* ptr, size_type n) noexcept {
    tl_mr->deallocate(ptr, n * sizeof(value_type), alignof(value_type));
  }

  static PMR_NS::memory_resource* resource() {
    return tl_mr;
  }
};

template <typename T, typename U>
bool operator==(const StatelessJsonAllocator<T>&, const StatelessJsonAllocator<U>&) noexcept {
  return true;
}

template <typename T, typename U>
bool operator!=(const StatelessJsonAllocator<T>&, const StatelessJsonAllocator<U>&) noexcept {
  return false;
}

}  // namespace detail

void InitTLJsonHeap(PMR_NS::memory_resource* mr);

using TmpJson = jsoncons::json;
using JsonType =
    jsoncons::basic_json<char, jsoncons::sorted_policy, detail::StatelessJsonAllocator<char>>;

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

// Deep copy a JSON object, by first serializing it to a string and then deserializing the string.
// The operation is intended to help during defragmentation, by copying into a page reserved for
// malloc.
JsonType DeepCopyJSON(const JsonType* j);

template <typename Json = JsonType>
auto MakeJsonPathExpr(std::string_view path, std::error_code& ec)
    -> jsoncons::jsonpath::jsonpath_expression<Json> {
  using ResultAllocT = typename Json::allocator_type;
  using TmpAllocT = std::allocator<char>;
  using AllocSetT = jsoncons::allocator_set<ResultAllocT, TmpAllocT>;
  return jsoncons::jsonpath::make_expression<Json, TmpAllocT>(AllocSetT(), path, ec);
}

}  // namespace dfly
