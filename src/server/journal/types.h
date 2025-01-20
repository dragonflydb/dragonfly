// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <optional>
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
  PING = 13,
  FIN = 14,
  LSN = 15
};

struct EntryBase {
  TxId txid;
  Op opcode;
  DbIndex dbid;
  uint32_t shard_cnt;  // This field is no longer used by the replica, but we continue to serialize
                       // and deserialize it to maintain backward compatibility.
  std::optional<SlotId> slot;
  LSN lsn{0};
};

// This struct represents a single journal entry.
// Those are either control instructions or commands.
struct Entry : public EntryBase {
  // Payload represents a non-owning view into a command executed on the shard.
  struct Payload {
    std::string_view cmd;
    std::variant<ShardArgs,  // Shard parts.
                 ArgSlice>   // Parts of a full command.
        args;

    Payload() = default;

    Payload(std::string_view c, const ShardArgs& a) : cmd(c), args(a) {
    }
    Payload(std::string_view c, ArgSlice a) : cmd(c), args(a) {
    }
  };

  Entry(TxId txid, Op opcode, DbIndex dbid, uint32_t shard_cnt, std::optional<SlotId> slot_id,
        Payload pl)
      : EntryBase{txid, opcode, dbid, shard_cnt, slot_id}, payload{pl} {
  }

  Entry(journal::Op opcode, DbIndex dbid, std::optional<SlotId> slot_id)
      : EntryBase{0, opcode, dbid, 0, slot_id, 0} {
  }

  Entry(journal::Op opcode, LSN lsn) : EntryBase{0, opcode, 0, 0, std::nullopt, lsn} {
  }

  Entry(TxId txid, journal::Op opcode, DbIndex dbid, uint32_t shard_cnt,
        std::optional<SlotId> slot_id)
      : EntryBase{txid, opcode, dbid, shard_cnt, slot_id, 0} {
  }

  bool HasPayload() const {
    return !payload.cmd.empty();
  }

  std::string ToString() const;

  Payload payload;
};

struct ParsedEntry : public EntryBase {
  struct CmdData {
    std::unique_ptr<uint8_t[]> command_buf;
    CmdArgVec cmd_args;  // represents the parsed command.
  };
  CmdData cmd;

  std::string ToString() const;
};

struct JournalItem {
  LSN lsn;
  Op opcode;
  std::string data;
  std::string_view cmd;
  std::optional<SlotId> slot;
};

using ChangeCallback = std::function<void(const JournalItem&, bool await)>;

}  // namespace journal
}  // namespace dfly
