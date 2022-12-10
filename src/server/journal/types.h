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

enum class Op : uint8_t {
  NOOP = 0,
  SELECT = 1,
  COMMAND = 2,
};

// TODO: to pass all the attributes like ttl, stickiness etc.
struct Entry {
  using Payload = std::variant<std::monostate,
                               CmdArgList,                            // Full single key command.
                               std::pair<std::string_view, ArgSlice>  // Command + shard parts.
                               >;

  using OwnedPayload = std::optional<CmdArgVec>;

  Entry(TxId txid, DbIndex dbid, Payload pl)
      : txid{txid}, opcode{Op::COMMAND}, dbid{dbid}, payload{pl}, owned_payload{} {
  }

  Entry(journal::Op opcode, DbIndex dbid)
      : txid{0}, opcode{opcode}, dbid{dbid}, payload{}, owned_payload{} {
  }

  Entry(DbIndex dbid)
      : txid{0}, opcode{journal::Op::SELECT}, dbid{dbid}, payload{}, owned_payload{} {
  }

  TxId txid;
  Op opcode;
  DbIndex dbid;

  Payload payload;
  OwnedPayload owned_payload;
};

using ChangeCallback = std::function<void(const Entry&)>;

}  // namespace journal
}  // namespace dfly
