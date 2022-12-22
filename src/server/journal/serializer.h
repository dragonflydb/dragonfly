// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <optional>
#include <string>

#include "base/io_buf.h"
#include "io/io.h"
#include "server/common.h"
#include "server/journal/types.h"

namespace dfly {

// JournalWriter serializes journal entries to a sink.
// It automatically keeps track of the current database index.
class JournalWriter {
 public:
  // Initialize with sink and optional start database index. If no start index is set,
  // a SELECT will be issued before the first entry.
  JournalWriter(io::Sink* sink, std::optional<DbIndex> dbid = std::nullopt);

  // Write single entry.
  std::error_code Write(const journal::Entry& entry);

 private:
  std::error_code Write(uint64_t v);           // Write packed unsigned integer.
  std::error_code Write(std::string_view sv);  // Write string.
  std::error_code Write(CmdArgList args);
  std::error_code Write(std::pair<std::string_view, ArgSlice> args);

  std::error_code Write(std::monostate);  // Overload for empty std::variant

 private:
  io::Sink* sink_;
  std::optional<DbIndex> cur_dbid_;
};

// JournalReader allows deserializing journal entries from a source.
// Like the writer, it automatically keeps track of the database index.
struct JournalReader {
 public:
  // Initialize start database index.
  JournalReader(DbIndex dbid);

  // Overwrite current db index.
  void SetDb(DbIndex dbid);

  // Try reading entry from source.
  io::Result<journal::ParsedEntry> ReadEntry(io::Source* source);

 private:
  // TODO: Templated endian encoding to not repeat...?
  io::Result<uint8_t> ReadU8(io::Source* source);
  io::Result<uint16_t> ReadU16(io::Source* source);
  io::Result<uint32_t> ReadU32(io::Source* source);
  io::Result<uint64_t> ReadU64(io::Source* source);

  // Read string into internal buffer and return size.
  io::Result<size_t> ReadString(io::Source* source);

  // Read argument array into internal buffer and build slice.
  // TODO: Inline store span data inside buffer to avoid alloaction
  std::error_code Read(io::Source* source, CmdArgVec* vec);

 private:
  base::IoBuf buf_;
  DbIndex dbid_;
};

}  // namespace dfly
