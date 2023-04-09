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
  SELECT = 6,
  EXPIRED = 9,
  COMMAND = 10,
  MULTI_COMMAND = 11,
  EXEC = 12,
};

struct EntryBase {
  TxId txid;
  Op opcode;
  DbIndex dbid;
  uint32_t shard_cnt;
};

// This struct represents a single journal entry.
// Those are either control instructions or commands.
struct Entry : public EntryBase {
  // Payload represents a non-owning view into a command executed on the shard.
  using Payload =
      std::variant<std::monostate,                           // No payload.
                   std::pair<std::string_view, CmdArgList>,  // Parts of a full command.
                   std::pair<std::string_view, ArgSlice>     // Command and its shard parts.
                   >;

  Entry(TxId txid, Op opcode, DbIndex dbid, uint32_t shard_cnt, Payload pl)
      : EntryBase{txid, opcode, dbid, shard_cnt}, payload{pl} {
  }

  Entry(journal::Op opcode, DbIndex dbid) : EntryBase{0, opcode, dbid, 0}, payload{} {
  }

  Entry(TxId txid, journal::Op opcode, DbIndex dbid, uint32_t shard_cnt)
      : EntryBase{txid, opcode, dbid, shard_cnt}, payload{} {
  }

  bool HasPayload() const {
    return !std::holds_alternative<std::monostate>(payload);
  }

  Payload payload;
};

struct ParsedEntry : public EntryBase {
  struct CmdData {
    std::unique_ptr<char[]> command_buf;
    CmdArgVec cmd_args;  // represents the parsed command.
  };
  CmdData cmd;
};

using ChangeCallback = std::function<void(const Entry&, bool await)>;

}  // namespace journal
}  // namespace dfly
