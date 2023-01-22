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

JournalWriter::JournalWriter(io::Sink* sink) : sink_{sink} {
}

void JournalWriter::Write(uint64_t v) {
  uint8_t buf[10];
  unsigned len = WritePackedUInt(v, buf);
  sink_->Write(io::Bytes{buf}.first(len));
}

void JournalWriter::Write(std::string_view sv) {
  Write(sv.size());
  sink_->Write(io::Buffer(sv));
}

void JournalWriter::Write(CmdArgList args) {
  Write(args.size());
  size_t cmd_size = 0;
  for (auto v : args) {
    cmd_size += v.size();
  }
  Write(cmd_size);
  for (auto v : args)
    Write(facade::ToSV(v));
}

void JournalWriter::Write(std::pair<std::string_view, ArgSlice> args) {
  auto [cmd, tail_args] = args;

  Write(1 + tail_args.size());

  size_t cmd_size = cmd.size();
  for (auto v : tail_args) {
    cmd_size += v.size();
  }
  Write(cmd_size);

  Write(cmd);
  for (auto v : tail_args)
    Write(v);
}

void JournalWriter::Write(std::monostate) {
}

void JournalWriter::Write(const journal::Entry& entry) {
  // Check if entry has a new db index and we need to emit a SELECT entry.
  if (entry.opcode != journal::Op::SELECT && (!cur_dbid_ || entry.dbid != *cur_dbid_)) {
    Write(journal::Entry{journal::Op::SELECT, entry.dbid});
    cur_dbid_ = entry.dbid;
  }

  Write(uint8_t(entry.opcode));

  switch (entry.opcode) {
    case journal::Op::SELECT:
      return Write(entry.dbid);
    case journal::Op::COMMAND:
    case journal::Op::EXPIRED:
    case journal::Op::MULTI_COMMAND:
    case journal::Op::EXEC:
      Write(entry.txid);
      Write(entry.shard_cnt);
      return std::visit([this](const auto& payload) { return Write(payload); }, entry.payload);
    default:
      break;
  };
}

JournalReader::JournalReader(io::Source* source, DbIndex dbid)
    : source_{source}, buf_{4_KB}, dbid_{dbid} {
}

void JournalReader::SetDb(DbIndex dbid) {
  dbid_ = dbid;
}

void JournalReader::SetSource(io::Source* source) {
  CHECK_EQ(buf_.InputLen(), 0ULL);
  source_ = source;
}

std::error_code JournalReader::EnsureRead(size_t num) {
  // Check if we already have enough.
  if (buf_.InputLen() >= num)
    return {};

  uint64_t remainder = num - buf_.InputLen();
  buf_.EnsureCapacity(remainder);

  // Try reading at least how much we need, but possibly more
  uint64_t read;
  SET_OR_RETURN(source_->ReadAtLeast(buf_.AppendBuffer(), remainder), read);
  CHECK(read >= remainder);

  buf_.CommitWrite(read);
  return {};
}

template <typename UT> io::Result<UT> JournalReader::ReadUInt() {
  // Determine type and number of following bytes.
  if (auto ec = EnsureRead(1); ec)
    return make_unexpected(ec);
  PackedUIntMeta meta{buf_.InputBuffer()[0]};
  buf_.ConsumeInput(1);

  if (auto ec = EnsureRead(meta.ByteSize()); ec)
    return make_unexpected(ec);

  // Read and check intenger.
  uint64_t res;
  SET_OR_UNEXPECT(ReadPackedUInt(meta, buf_.InputBuffer()), res);
  buf_.ConsumeInput(meta.ByteSize());

  if (res > std::numeric_limits<UT>::max())
    return make_unexpected(make_error_code(errc::result_out_of_range));
  return static_cast<UT>(res);
}

template io::Result<uint8_t> JournalReader::ReadUInt<uint8_t>();
template io::Result<uint16_t> JournalReader::ReadUInt<uint16_t>();
template io::Result<uint32_t> JournalReader::ReadUInt<uint32_t>();
template io::Result<uint64_t> JournalReader::ReadUInt<uint64_t>();

io::Result<size_t> JournalReader::ReadString(char* buffer) {
  size_t size = 0;
  SET_OR_UNEXPECT(ReadUInt<uint64_t>(), size);

  if (auto ec = EnsureRead(size); ec)
    return make_unexpected(ec);

  buf_.ReadAndConsume(size, buffer);

  return size;
}

std::error_code JournalReader::ReadCommand(journal::ParsedEntry::CmdData* data) {
  size_t num_strings = 0;
  SET_OR_RETURN(ReadUInt<uint64_t>(), num_strings);
  data->cmd_args.resize(num_strings);

  size_t cmd_size = 0;
  SET_OR_RETURN(ReadUInt<uint64_t>(), cmd_size);

  // Read all strings consecutively.
  data->command_buf = make_unique<char[]>(cmd_size);
  char* ptr = data->command_buf.get();
  for (auto& span : data->cmd_args) {
    size_t size;
    SET_OR_RETURN(ReadString(ptr), size);
    span = MutableSlice{ptr, size};
    ptr += size;
  }
  return std::error_code{};
}

io::Result<journal::ParsedEntry> JournalReader::ReadEntry() {
  uint8_t int_op;
  SET_OR_UNEXPECT(ReadUInt<uint8_t>(), int_op);
  journal::Op opcode = static_cast<journal::Op>(int_op);

  if (opcode == journal::Op::SELECT) {
    SET_OR_UNEXPECT(ReadUInt<uint16_t>(), dbid_);
    return ReadEntry();
  }

  journal::ParsedEntry entry;
  entry.dbid = dbid_;
  entry.opcode = opcode;

  SET_OR_UNEXPECT(ReadUInt<uint64_t>(), entry.txid);
  SET_OR_UNEXPECT(ReadUInt<uint32_t>(), entry.shard_cnt);

  if (opcode == journal::Op::EXEC) {
    return entry;
  }

  auto ec = ReadCommand(&entry.cmd);
  if (ec)
    return make_unexpected(ec);

  return entry;
}

}  // namespace dfly
