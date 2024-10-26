// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/serializer.h"

#include <system_error>

#include "base/logging.h"
#include "glog/logging.h"
#include "io/io.h"
#include "io/io_buf.h"
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

void JournalWriter::Write(const journal::Entry::Payload& payload) {
  if (payload.cmd.empty())
    return;

  size_t num_elems = 0, size = 0;
  for (string_view str : base::it::Wrap(facade::kToSV, payload.args)) {
    num_elems++;
    size += str.size();
  };

  Write(1 + num_elems);

  size_t cmd_size = payload.cmd.size() + size;
  Write(cmd_size);
  Write(payload.cmd);

  for (string_view str : base::it::Wrap(facade::kToSV, payload.args))
    this->Write(str);
}

void JournalWriter::Write(const journal::Entry& entry) {
  // Check if entry has a new db index and we need to emit a SELECT entry.
  if (entry.opcode != journal::Op::SELECT && entry.opcode != journal::Op::LSN &&
      entry.opcode != journal::Op::PING && (!cur_dbid_ || entry.dbid != *cur_dbid_)) {
    Write(journal::Entry{journal::Op::SELECT, entry.dbid, entry.slot});
    cur_dbid_ = entry.dbid;
  }

  VLOG(1) << "Writing entry " << entry.ToString();

  Write(uint8_t(entry.opcode));

  switch (entry.opcode) {
    case journal::Op::SELECT:
      return Write(entry.dbid);
    case journal::Op::LSN:
      return Write(entry.lsn);
    case journal::Op::PING:
      return;
    case journal::Op::COMMAND:
    case journal::Op::EXPIRED:
      Write(entry.txid);
      Write(entry.shard_cnt);
      Write(entry.payload);
      break;
    default:
      break;
  };
}

JournalReader::JournalReader(io::Source* source, DbIndex dbid)
    : source_{source}, buf_{4096}, dbid_{dbid} {
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

  // Happens on end of stream (for example, a too-small string buffer or a closed socket)
  if (read < remainder) {
    return make_error_code(errc::io_error);
  }

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

io::Result<size_t> JournalReader::ReadString(io::MutableBytes buffer) {
  size_t size = 0;
  SET_OR_UNEXPECT(ReadUInt<uint64_t>(), size);

  if (auto ec = EnsureRead(size); ec)
    return make_unexpected(ec);

  if (size > buffer.size())
    return make_unexpected(make_error_code(errc::bad_message));

  buf_.ReadAndConsume(size, buffer.data());

  return size;
}

std::error_code JournalReader::ReadCommand(journal::ParsedEntry::CmdData* data) {
  size_t num_strings = 0;
  SET_OR_RETURN(ReadUInt<uint64_t>(), num_strings);
  data->cmd_args.resize(num_strings);

  size_t cmd_size = 0;
  SET_OR_RETURN(ReadUInt<uint64_t>(), cmd_size);

  // Read all strings consecutively.
  data->command_buf = make_unique<uint8_t[]>(cmd_size);
  uint8_t* ptr = data->command_buf.get();
  for (auto& span : data->cmd_args) {
    size_t size;
    SET_OR_RETURN(ReadString({ptr, cmd_size}), size);
    DCHECK(size <= cmd_size);
    span = string_view{reinterpret_cast<char*>(ptr), size};
    ptr += size;
    cmd_size -= size;
  }
  return {};
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

  if (opcode == journal::Op::PING || opcode == journal::Op::FIN) {
    return entry;
  }

  if (opcode == journal::Op::LSN) {
    SET_OR_UNEXPECT(ReadUInt<uint64_t>(), entry.lsn);
    return entry;
  }

  SET_OR_UNEXPECT(ReadUInt<uint64_t>(), entry.txid);
  SET_OR_UNEXPECT(ReadUInt<uint32_t>(), entry.shard_cnt);

  VLOG(1) << "Read entry " << entry.ToString();

  auto ec = ReadCommand(&entry.cmd);
  if (ec)
    return make_unexpected(ec);

  return entry;
}

}  // namespace dfly
