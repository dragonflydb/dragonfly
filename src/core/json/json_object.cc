// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/json/json_object.h"

#include <stack>

#include "base/logging.h"
#include "core/page_usage/page_usage_stats.h"

using namespace jsoncons;

namespace {

template <typename T>
std::optional<T> ParseWithDecoder(std::string_view input, json_decoder<T>&& decoder) {
  std::error_code ec;
  auto JsonErrorHandler = [](json_errc ec, const ser_context&) {
    VLOG(1) << "Error while decode JSON: " << make_error_code(ec).message();
    return false;
  };

  // The maximum allowed JSON nesting depth is 64.
  // The limit was reduced from 256 to 64. This change is reasonable, as most documents contain
  // no more than 20-30 levels of nesting. In the test case, over 128 levels were used, causing
  // the parser to enter a long stall due to excessive resource consumption. Even a limit of 128
  // does not mitigate the issue. A limit of 64 is a sensible compromise.
  // See https://github.com/dragonflydb/dragonfly/issues/5028
  const uint32_t json_nesting_depth_limit = 64;

  /* The maximum possible JSON nesting depth is either the specified json_nesting_depth_limit or
     half of the input size. Since nesting a JSON object requires at least 2 characters. */
  auto parser_options = json_options{}.max_nesting_depth(
      std::min(json_nesting_depth_limit, uint32_t(input.size() / 2)));

  json_parser parser(parser_options, JsonErrorHandler);

  parser.update(input);
  parser.finish_parse(decoder, ec);

  if (!ec && decoder.is_valid()) {
    return decoder.get_result();
  }
  return std::nullopt;
}

using namespace dfly;

// The following two functions allocate a string-based object by copying data to a fresh memory
// page. Then the move-assignment operator swaps it with the input node (swap_l_r in jsoncons), and
// the temporary is destroyed at the end of the scope.
bool DefragmentByteString(JsonType& j, PageUsage* page_usage) {
  const auto& byte_storage = j.cast<JsonType::byte_string_storage>();
  if (byte_storage.length() == 0 ||
      !page_usage->IsPageForObjectUnderUtilized(const_cast<uint8_t*>(byte_storage.data())))
    return false;

  const byte_string_view bsv{byte_storage.data(), byte_storage.length()};
  if (j.tag() == semantic_tag::ext) {
    j = JsonType(byte_string_arg, bsv, j.ext_tag(), byte_storage.get_allocator());
    return true;
  }

  j = JsonType(byte_string_arg, bsv, j.tag(), byte_storage.get_allocator());
  return true;
}

bool DefragmentLongString(JsonType& j, PageUsage* page_usage) {
  const auto& str_storage = j.cast<JsonType::long_string_storage>();
  if (str_storage.length() == 0 ||
      !page_usage->IsPageForObjectUnderUtilized(const_cast<char*>(str_storage.data())))
    return false;

  JsonType::string_view_type svt{str_storage.data(), str_storage.length()};
  j = JsonType(svt, j.tag(), str_storage.get_allocator());
  return true;
}

// Allocates a new json object of type json_object_arg, with fresh memory allocation for its
// contained vector of key value pairs. Then moves members from j to this new object. Finally j is
// swapped with the new object.
bool DefragmentJsonObject(JsonType& j, PageUsage* page_usage) {
  auto& object = j.cast<JsonType::object_storage>().value();
  if (object.empty() || !page_usage->IsPageForObjectUnderUtilized(&*object.begin()))
    return false;

  // Creates a fresh object and reserves space for the underlying vector.
  JsonType new_node{json_object_arg, j.tag(), object.get_allocator()};
  new_node.reserve(object.size());

  for (auto& member : object) {
    // The member values are JsonType themselves, they just wrap pointers to actual storage.
    // Their move invokes the move ctor in jsoncons, which will move the value wrappers to new_node,
    // and leave the original in `j` holding references to `null_storage` type, see
    // `uninitialized_move_a` in jsoncons. The member key (a string) is not moved but copied into
    // new_node members.
    new_node.try_emplace(member.key(), std::move(member.value()));
  }

  // Invokes move assignment. A swap is performed, and new_node now holds null_storage
  // references instead of `j`. It will be destroyed on leaving scope, cleaning up its memory.
  j = std::move(new_node);
  return true;
}

// Same as DefragmentJsonObject except uses an array object. The contained members are moved
// similarly, and on exit the old node is destroyed.
bool DefragmentJsonArray(JsonType& j, PageUsage* page_usage) {
  auto& array = j.cast<JsonType::array_storage>().value();
  if (array.empty() || !page_usage->IsPageForObjectUnderUtilized(&*array.begin()))
    return false;

  JsonType new_node{json_array_arg, j.tag(), array.get_allocator()};
  new_node.reserve(array.size());

  for (JsonType& member : array) {
    new_node.push_back(std::move(member));
  }

  j = std::move(new_node);
  return true;
}

}  // namespace

namespace dfly {

std::optional<TmpJson> JsonFromString(std::string_view input) {
  return ParseWithDecoder(input, json_decoder<TmpJson>{});
}

optional<JsonType> ParseJsonUsingShardHeap(string_view input) {
  return ParseWithDecoder(input, json_decoder<JsonType>{StatelessAllocator<char>{}});
}

bool Defragment(JsonType& j, PageUsage* page_usage) {
  bool did_defragment = false;
  // stack-based traversal inspired from jsoncons::basic_json::compute_memory_size
  std::stack<JsonType*> stack;
  stack.push(&j);

  while (!stack.empty()) {
    JsonType* current = stack.top();
    stack.pop();

    const json_storage_kind storage_kind = current->storage_kind();
    switch (storage_kind) {
      case json_storage_kind::byte_str:
        did_defragment |= DefragmentByteString(*current, page_usage);
        break;
      case json_storage_kind::long_str:
        did_defragment |= DefragmentLongString(*current, page_usage);
        break;
      case json_storage_kind::object: {
        did_defragment |= DefragmentJsonObject(*current, page_usage);
        auto& object = current->cast<JsonType::object_storage>().value();
        for (auto& member : object) {
          stack.push(&member.value());
        }
        break;
      }
      case json_storage_kind::array: {
        did_defragment |= DefragmentJsonArray(*current, page_usage);
        auto& array = current->cast<JsonType::array_storage>().value();
        for (auto& member : array) {
          stack.push(&member);
        }
        break;
      }
      default:
        DCHECK(is_trivial_storage(storage_kind))
            << "unexpected non trivial storage type:" << storage_kind;
        break;
    }
  }
  return did_defragment;
}

}  // namespace dfly
