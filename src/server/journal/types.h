// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <optional>
#include <string>
#include <variant>

#include "common/backed_args.h"
#include "server/common_types.h"
#include "server/table.h"

namespace dfly {
namespace journal {

enum class Op : uint8_t { SELECT = 6, EXPIRED = 9 /* sunset*/, COMMAND = 10, PING = 13, LSN = 15 };

struct EntryBase {
  TxId txid;
  Op opcode;
  DbIndex dbid;
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

  Entry(TxId txid, Op opcode, DbIndex dbid, std::optional<SlotId> slot_id, Payload pl)
      : EntryBase{txid, opcode, dbid, slot_id}, payload{std::move(pl)} {
  }

  Entry(journal::Op opcode, DbIndex dbid, std::optional<SlotId> slot_id)
      : EntryBase{0, opcode, dbid, slot_id, 0} {
  }

  Entry(journal::Op opcode, LSN lsn) : EntryBase{0, opcode, 0, std::nullopt, lsn} {
  }

  Entry(TxId txid, journal::Op opcode, DbIndex dbid, std::optional<SlotId> slot_id)
      : EntryBase{txid, opcode, dbid, slot_id, 0} {
  }

  bool HasPayload() const {
    return !payload.cmd.empty();
  }

  std::string ToString() const;

  Payload payload;
};

struct ParsedEntry : public EntryBase {
  using CmdData = cmn::BackedArguments;
  CmdData cmd;

  ParsedEntry(const ParsedEntry&) = delete;
  ParsedEntry() = default;

  std::string ToString() const;
};

struct JournalItem {
  LSN lsn;
  std::string data;
};

struct JournalChangeItem {
  JournalItem journal_item;

  std::string_view cmd;
  std::optional<SlotId> slot;
  // Replid of the upstream source this record was applied from. Empty for locally
  // originated writes. A JournalStreamer skips records whose source_replid equals
  // its target replica's replid (active-replication loop suppression).
  // Stored as std::string (not string_view) because the originating Transaction may
  // be destroyed before async streamer fibers iterate over this item.
  std::string source_replid;
};

struct JournalConsumerInterface {
  virtual ~JournalConsumerInterface() = default;

  // Receives a journal change for serializing
  virtual void ConsumeJournalChange(const JournalChangeItem& item) = 0;
  // Waits for writing the serialized data
  virtual void ThrottleIfNeeded() = 0;
};

}  // namespace journal
}  // namespace dfly
