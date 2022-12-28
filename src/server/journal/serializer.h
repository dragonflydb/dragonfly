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
  JournalWriter(std::optional<DbIndex> dbid = std::nullopt);

  // Write single entry.
  void Write(const journal::Entry& entry);

  std::error_code Flush(io::Sink* sink_);

  base::IoBuf& Accumulated();

  ///

  void Steal(base::IoBuf* other);

  unsigned BufSize() {
    return buf_.InputLen();
  }

  ///

 private:
  void Write(uint64_t v);           // Write packed unsigned integer.
  void Write(std::string_view sv);  // Write string.
  void Write(CmdArgList args);
  void Write(std::pair<std::string_view, ArgSlice> args);

  void Write(std::monostate);  // Overload for empty std::variant

 private:
  base::IoBuf buf_;
  std::optional<DbIndex> cur_dbid_;
};

// JournalReader allows deserializing journal entries from a source.
// Like the writer, it automatically keeps track of the database index.
struct JournalReader {
 public:
  // Initialize start database index.
  JournalReader(io::Source* source, DbIndex dbid);

  // Overwrite current db index.
  void SetDb(DbIndex dbid);

  // Switch to different source. Ensures there is no leftover from the previous source.
  void SetSource(io::Source* source);

  // Try reading entry from source. Invalidates the last parsed entry.
  io::Result<journal::ParsedEntry> ReadEntry();

 private:
  // Ensure internal buffer has at least num bytes.
  std::error_code EnsureRead(size_t num);

  // Read unsigned integer in packed encoding.
  template <typename UT> io::Result<UT> ReadUInt();

  // Read and append string to string buffer, return size.
  io::Result<size_t> ReadString();

  // Read argument array into internal buffer and build slice.
  std::error_code Read(CmdArgVec* vec);

 private:
  io::Source* source_;
  base::IoBuf buf_;

  // Slices of the last parsed entry point to this string.
  std::string str_buf_;

  DbIndex dbid_;
};

}  // namespace dfly
