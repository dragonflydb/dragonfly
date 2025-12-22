// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <memory>
#include <string>
#include <variant>

#include "facade/facade_types.h"

namespace facade::payload {

// SendError (msg, type)
using Error = std::unique_ptr<std::pair<std::string, std::string>>;
using Null = std::nullptr_t;  // SendNull or SendNullArray

struct CollectionPayload;
struct SimpleString : public std::string {};  // SendSimpleString
struct BulkString : public std::string {};    // SendBulkString
struct StoredReply {
  bool ok = false;  // true for SendStored, false for SendSetSkipped
};

struct SingleGetReply {
  std::string value;
  uint64_t mc_ver = 0;
  uint32_t mc_flag = 0;
  bool is_found = false;
};

using MGetReply = std::unique_ptr<SingleGetReply[]>;

using Payload = std::variant<std::monostate, Null, Error, long, double, SimpleString, BulkString,
                             std::unique_ptr<CollectionPayload>, StoredReply, MGetReply>;

static_assert(sizeof(Payload) == 40);

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

};  // namespace facade::payload
