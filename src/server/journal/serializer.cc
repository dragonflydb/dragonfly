// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/serializer.h"

#include "base/io_buf.h"
#include "base/logging.h"
#include "io/io.h"
#include "server/common.h"
#include "server/error.h"
#include "server/journal/types.h"
#include "server/main_service.h"
#include "server/serializer_commons.h"
#include "server/transaction.h"

using namespace std;

namespace dfly {

JournalWriter::JournalWriter(io::Sink* sink, std::optional<DbIndex> dbid)
    : sink_{sink}, cur_dbid_{dbid} {
}

error_code JournalWriter::Write(uint64_t v) {
  uint8_t buf[10];
  unsigned len = WritePackedUInt(v, buf);
  return sink_->Write(io::Bytes{buf, len});
}

error_code JournalWriter::Write(std::string_view sv) {
  RETURN_ON_ERR(Write(sv.size()));
  return sink_->Write(io::Buffer(sv));
}

error_code JournalWriter::Write(CmdArgList args) {
  RETURN_ON_ERR(Write(args.size()));
  for (auto v : args)
    RETURN_ON_ERR(Write(facade::ToSV(v)));

  return std::error_code{};
}

error_code JournalWriter::Write(std::pair<std::string_view, ArgSlice> args) {
  auto [cmd, tail_args] = args;

  RETURN_ON_ERR(Write(1 + tail_args.size()));
  RETURN_ON_ERR(Write(cmd));
  for (auto v : tail_args)
    RETURN_ON_ERR(Write(v));

  return std::error_code{};
}

error_code JournalWriter::Write(std::monostate) {
  return std::error_code{};
}

error_code JournalWriter::Write(const journal::Entry& entry) {
  // Check if entry has a new db index and we need to emit a SELECT entry.
  if (entry.opcode != journal::Op::SELECT && (!cur_dbid_ || entry.dbid != *cur_dbid_)) {
    RETURN_ON_ERR(Write(journal::Entry{journal::Op::SELECT, entry.dbid}));
    cur_dbid_ = entry.dbid;
  }

  RETURN_ON_ERR(Write(uint8_t(entry.opcode)));

  switch (entry.opcode) {
    case journal::Op::SELECT:
      return Write(entry.dbid);
    case journal::Op::COMMAND:
      RETURN_ON_ERR(Write(entry.txid));
      return std::visit([this](const auto& payload) { return Write(payload); }, entry.payload);
    default:
      break;
  };
  return std::error_code{};
}

JournalReader::JournalReader(io::Source* source, DbIndex dbid)
    : source_{source}, buf_{}, dbid_{dbid} {
}

template <typename UT> io::Result<UT> ReadPackedUIntTyped(io::Source* source) {
  uint64_t v;
  SET_OR_UNEXPECT(ReadPackedUInt(source), v);
  if (v > std::numeric_limits<UT>::max())
    return make_unexpected(make_error_code(errc::result_out_of_range));
  return static_cast<UT>(v);
}

io::Result<uint8_t> JournalReader::ReadU8() {
  return ReadPackedUIntTyped<uint8_t>(source_);
}

io::Result<uint16_t> JournalReader::ReadU16() {
  return ReadPackedUIntTyped<uint16_t>(source_);
}

io::Result<uint64_t> JournalReader::ReadU64() {
  return ReadPackedUIntTyped<uint64_t>(source_);
}

io::Result<size_t> JournalReader::ReadString() {
  size_t size = 0;
  SET_OR_UNEXPECT(ReadU64(), size);

  buf_.EnsureCapacity(size);
  auto dest = buf_.AppendBuffer().first(size);
  uint64_t read = 0;
  SET_OR_UNEXPECT(source_->Read(dest), read);

  buf_.CommitWrite(read);
  if (read != size)
    return make_unexpected(std::make_error_code(std::errc::message_size));

  return size;
}

std::error_code JournalReader::Read(CmdArgVec* vec) {
  buf_.ConsumeInput(buf_.InputBuffer().size());

  size_t size = 0;
  SET_OR_RETURN(ReadU64(), size);

  vec->resize(size);
  for (auto& span : *vec) {
    size_t len;
    SET_OR_RETURN(ReadString(), len);
    span = MutableSlice{nullptr, len};
  }

  size_t offset = 0;
  for (auto& span : *vec) {
    size_t len = span.size();
    auto ptr = buf_.InputBuffer().subspan(offset).data();
    span = MutableSlice{reinterpret_cast<char*>(ptr), len};
    offset += len;
  }

  return std::error_code{};
}

io::Result<journal::ParsedEntry> JournalReader::ReadEntry() {
  uint8_t opcode;
  SET_OR_UNEXPECT(ReadU8(), opcode);

  journal::ParsedEntry entry{static_cast<journal::Op>(opcode), dbid_};

  switch (entry.opcode) {
    case journal::Op::COMMAND:
      SET_OR_UNEXPECT(ReadU64(), entry.txid);
      entry.payload = CmdArgVec{};
      if (auto ec = Read(&*entry.payload); ec)
        return make_unexpected(ec);
      break;
    case journal::Op::SELECT:
      SET_OR_UNEXPECT(ReadU16(), dbid_);
      return ReadEntry();
    default:
      break;
  };
  return entry;
}

}  // namespace dfly
