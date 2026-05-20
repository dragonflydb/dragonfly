// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <memory>
#include <string>
#include <variant>

#include "base/function2.hpp"
#include "facade/facade_types.h"

namespace facade {

class SinkReplyBuilder;
namespace payload {

// SendError (msg, type)
using Error = std::unique_ptr<std::pair<std::string, std::string>>;
using Null = std::nullptr_t;  // SendNull or SendNullArray

struct CollectionPayload;
struct SimpleString : public std::string {};  // SendSimpleString
struct BulkString : public std::string {};    // SendBulkString

// Borrowed bulk string: the underlying bytes are owned by a stable source
// (e.g., a CompactObj raw payload) and must remain valid until the captured
// reply is applied to the sink. Used by SendBulkStringBorrowed to preserve
// zero-copy through the squashing capture/replay boundary. Same read-only
// lifetime contract as CompactObj::TryGetRawView().
struct BulkStringView {
  std::string_view view;
};

using Payload = std::variant<std::monostate, Null, Error, long, double, SimpleString, BulkString,
                             BulkStringView, std::unique_ptr<CollectionPayload>>;

#if defined(__linux__) && !defined(_LIBCPP_VERSION)
static_assert(sizeof(Payload) == 40);
#endif

struct CollectionPayload {
  CollectionPayload(unsigned _len, CollectionType _type) : len{_len}, type{_type} {
    arr.reserve(type == CollectionType::MAP ? len * 2 : len);
  }

  unsigned len;
  CollectionType type;
  std::vector<Payload> arr;
};

inline Error make_error(std::string_view msg, std::string_view type = "") {
  return std::make_unique<std::pair<std::string, std::string>>(msg, type);
}

inline Payload make_simple_or_noreply(std::string_view resp) {
  if (resp.empty())
    return std::monostate{};
  else
    return SimpleString{std::string(resp)};
}

}  // namespace payload
}  // namespace facade
