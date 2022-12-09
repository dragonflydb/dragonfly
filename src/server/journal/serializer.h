#pragma once

#include <string>

#include "base/io_buf.h"
#include "io/io.h"
#include "server/common.h"
#include "server/journal/types.h"
#include "server/main_service.h"

namespace dfly {

struct JournalWriter {
  JournalWriter(io::Sink* sink);
  std::error_code Write(const journal::Entry& entry);

 private:
  std::error_code Write(uint8_t v);
  std::error_code Write(uint16_t v);
  std::error_code Write(uint64_t v);

  std::error_code Write(std::string_view sv);
  std::error_code Write(std::pair<std::string_view, ArgSlice> args);
  std::error_code Write(CmdArgList args);

  std::error_code Write(std::monostate);  // Overload for empty std::variant

 private:
  io::Sink* sink_;
};

struct JournalReader {
  JournalReader(io::Source* source);

  io::Result<journal::Entry> ReadEntry();

 private:
  // TODO: Templated endian encoding to not repeat...?
  io::Result<uint8_t> ReadU8();
  io::Result<uint16_t> ReadU16();
  io::Result<uint64_t> ReadU64();

  // Read string into internal buffer and return size.
  io::Result<size_t> ReadString();

  // Read argument array into internal buffer and build slice.
  // TODO: Inline store span data inside buffer to avoid alloaction
  std::error_code Read(CmdArgVec* vec);

 private:
  io::Source* source_;
  base::IoBuf buf_;
};

struct JournalExecutor {
  JournalExecutor(Service* service);
  void Execute(journal::Entry&& entry);

 private:
  Service* service_;
  DbIndex dbid_;
};

}  // namespace dfly