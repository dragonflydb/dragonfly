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
  COMMAND = 10,
};

struct EntryBase {
  TxId txid;
  Op opcode;
  DbIndex dbid;
};

// This struct represents a single journal entry.
// Those are either control instructions or commands.
struct Entry : public EntryBase {
  // Payload represents a non-owning view into a command executed on the shard.
  using Payload =
      std::variant<std::monostate,                        // No payload.
                   CmdArgList,                            // Parts of a full command.
                   std::pair<std::string_view, ArgSlice>  // Command and its shard parts.
                   >;

  Entry(TxId txid, DbIndex dbid, Payload pl)
      : EntryBase{txid, journal::Op::COMMAND, dbid}, payload{pl} {
  }

  Entry(journal::Op opcode, DbIndex dbid) : EntryBase{0, opcode, dbid}, payload{} {
  }

  Payload payload;
};

struct ParsedEntry : public EntryBase {
  // Payload represents the parsed command.
  using Payload = std::optional<CmdArgVec>;

  ParsedEntry() = default;

  ParsedEntry(journal::Op opcode, DbIndex dbid) : EntryBase{0, opcode, dbid}, payload{} {
  }

  ParsedEntry(TxId txid, DbIndex dbid, Payload pl)
      : EntryBase{txid, journal::Op::COMMAND, dbid}, payload{pl} {
  }

  Payload payload;
};

using ChangeCallback = std::function<void(const Entry&)>;

}  // namespace journal
}  // namespace dfly
