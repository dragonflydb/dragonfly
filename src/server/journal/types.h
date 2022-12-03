// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <string>
#include <variant>

#include "server/common.h"
#include "server/table.h"

namespace dfly {
namespace journal {

enum class OpCode : uint8_t {
  // 0-20 control opcodes
  NOOP = 0,

  // 21-50 commands
  SET = 21,
  DEL = 22,
  LPUSH = 23,
};

using KeyType = std::string_view;
using ValueType = std::variant<KeyType, const PrimeValue*>;
using ListType = ArgSlice;

using PldEmpty = std::tuple<>;
using PldKeyValue = std::tuple<KeyType, ValueType>;
using PldKeyList = std::tuple<KeyType, ListType>;

using Payload = std::variant<PldEmpty, ListType, PldKeyValue, PldKeyList>;

struct Entry {
  Entry() = default;

  template <typename... Ts>
  Entry(TxId txid, DbIndex dbid, OpCode code, Ts... ts)
      : txid{txid}, dbid{dbid}, code{code}, payload{} {
    if constexpr (sizeof...(Ts) > 1)
      payload = std::make_tuple(std::forward<Ts>(ts)...);
    else
      payload = std::get<0>(std::make_tuple(std::forward<Ts>(ts)...));
  }

  TxId txid;
  DbIndex dbid;

  OpCode code;
  Payload payload;
};

using ChangeCallback = std::function<void(const Entry&)>;

}  // namespace journal
}  // namespace dfly
