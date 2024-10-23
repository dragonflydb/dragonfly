// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <optional>
#include <string>

#include "io/io.h"
#include "io/io_buf.h"
#include "server/common.h"
#include "server/journal/types.h"

namespace dfly {

// JournalWriter serializes journal entries to a sink.
// It automatically keeps track of the current database index.
class JournalWriter {
 public:
  JournalWriter(io::Sink* sink);

  // Write single entry to sink.
  void Write(const journal::Entry& entry);
  void Write(uint64_t v);  // Write packed unsigned integer.

 private:
  void Write(std::string_view sv);  // Write string.
  void Write(const journal::Entry::Payload& payload);

 private:
  io::Sink* sink_;
  std::optional<DbIndex> cur_dbid_{};
};

// JournalReader allows deserializing journal entries from a source.
// Like the writer, it automatically keeps track of the database index.
struct JournalReader {
 public:
  // Initialize start database index.
  JournalReader(io::Source* source, DbIndex dbid);

  // Overwrite current source and ensure there is no leftover from previous.
  void SetSource(io::Source* source);

  // Try reading entry from source.
  io::Result<journal::ParsedEntry> ReadEntry();

 private:
  // Read from source until buffer contains at least num bytes.
  std::error_code EnsureRead(size_t num);

  // Read unsigned integer in packed encoding.
  template <typename UT> io::Result<UT> ReadUInt();

  // Read and copy to buffer, return size.
  io::Result<size_t> ReadString(io::MutableBytes buffer);

  // Read argument array into string buffer.
  std::error_code ReadCommand(journal::ParsedEntry::CmdData* entry);

 private:
  io::Source* source_;
  base::IoBuf buf_;
  DbIndex dbid_;
};

}  // namespace dfly
