// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/serializer.h"

#include <absl/base/internal/endian.h>

#include <system_error>

extern "C" {
#include "redis/rdb.h"
}

#include "base/logging.h"
#include "io/io.h"
#include "io/io_buf.h"
#include "server/error.h"
#include "server/journal/types.h"
#include "server/main_service.h"
#include "server/serializer_commons.h"
#include "server/transaction.h"

using namespace std;

namespace dfly {

namespace {

// Decodes an integer written by WritePackedUInt from [*pos, end) and advances *pos past it.
// Returns false if the integer is truncated or uses an encoding WritePackedUInt does not produce.
inline bool ParseBufferedUInt(const uint8_t** pos, const uint8_t* end, uint64_t* dest) {
  const uint8_t* p = *pos;
  if (p == end)
    return false;

  const uint8_t first = *p++;
  switch (first >> 6) {
    case RDB_6BITLEN:
      *dest = first & 0x3F;
      break;
    case RDB_14BITLEN:
      if (p == end)
        return false;
      *dest = (uint64_t(first & 0x3F) << 8) | *p++;
      break;
    default:
      if (first == RDB_32BITLEN && end - p >= 4) {
        *dest = absl::big_endian::Load32(p);
        p += 4;
      } else if (first == RDB_64BITLEN && end - p >= 8) {
        *dest = absl::big_endian::Load64(p);
        p += 8;
      } else {
        return false;
      }
  }

  *pos = p;
  return true;
}

}  // namespace

JournalWriter::JournalWriter(io::Sink* sink) : sink_{sink} {
}

void JournalWriter::Write(uint64_t v) {
  uint8_t buf[10];
  unsigned len = WritePackedUInt(v, buf);
  sink_->Write(io::Bytes{buf}.first(len));
}

void JournalWriter::Write(std::string_view sv) {
  Write(sv.size());
  if (!sv.empty())  // arguments can be empty strings
    sink_->Write(io::Buffer(sv));
}

void JournalWriter::Write(const journal::Entry::Payload& payload) {
  if (payload.cmd.empty())
    return;

  size_t num_elems = 0, size = 0;
  for (string_view str : base::it::Wrap(cmn::kToSV, payload.args)) {
    num_elems++;
    size += str.size();
  };

  Write(1 + num_elems);

  size_t cmd_size = payload.cmd.size() + size;
  Write(cmd_size);
  Write(payload.cmd);

  for (string_view str : base::it::Wrap(cmn::kToSV, payload.args))
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
      Write(entry.txid);
      Write(1u);  // deprecated field, kept for backward compatibility.
      Write(entry.payload);
      break;
    default:
      LOG(FATAL) << "Unknown journal opcode: " << static_cast<int>(entry.opcode);
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

std::error_code JournalReader::ReadString(io::MutableBytes buffer) {
  size_t size = buffer.size();
  uint64_t available = std::min(size, buf_.InputLen());
  uint64_t remainder = 0;

  if (available < size) {
    remainder = size - available;
  }

  buf_.ReadAndConsume(available, buffer.data());

  // If remainder of string is bigger than threshold - read and populate directly
  // output buffer otherwise use intermediate io_buf.
  bool is_short_remainder = remainder < (buf_.Capacity() / 2);

  auto remainder_buf_pos = buffer.data() + available;

  if (remainder) {
    if (is_short_remainder) {
      if (auto ec = EnsureRead(remainder); ec)
        return ec;
      buf_.ReadAndConsume(remainder, remainder_buf_pos);
    } else {
      uint64_t read;
      SET_OR_RETURN(source_->Read({remainder_buf_pos, remainder}), read);
      if (read < remainder) {
        return make_error_code(errc::io_error);
      }
    }
  }

  return {};
}

std::error_code JournalReader::ReadCommand(journal::ParsedEntry::CmdData* data) {
  size_t num_strings = 0;
  SET_OR_RETURN(ReadUInt<uint64_t>(), num_strings);

  size_t cmd_size = 0;
  SET_OR_RETURN(ReadUInt<uint64_t>(), cmd_size);

  data->Reserve(num_strings, cmd_size + num_strings /* +\0 char*/);

  // Read all strings consecutively.
  for (size_t i = 0; i < num_strings; ++i) {
    size_t size = 0;
    SET_OR_RETURN(ReadUInt<uint64_t>(), size);
    if (size > cmd_size) {  // corrupted entry
      return make_error_code(errc::io_error);
    }
    data->PushArg(size);
    uint8_t* ptr = reinterpret_cast<uint8_t*>(data->data(i));
    if (auto ec = ReadString({ptr, size}); ec)
      return ec;

    ptr[size] = '\0';  // null terminate

    cmd_size -= size;
  }

  return {};
}

bool JournalReader::TryReadBufferedCommand(journal::ParsedEntry* dest) {
  const io::Bytes input = buf_.InputBuffer();
  const uint8_t* pos = input.data();
  const uint8_t* const end = pos + input.size();

  uint64_t opcode = 0, dbid = dbid_;
  if (!ParseBufferedUInt(&pos, end, &opcode))
    return false;

  if (opcode == uint8_t(journal::Op::SELECT)) {
    if (!ParseBufferedUInt(&pos, end, &dbid) || dbid > numeric_limits<DbIndex>::max() ||
        !ParseBufferedUInt(&pos, end, &opcode)) {
      return false;
    }
  }

  uint64_t txid = 0, unused = 0, num_strings = 0, cmd_size = 0;
  if (opcode != uint8_t(journal::Op::COMMAND) || !ParseBufferedUInt(&pos, end, &txid) ||
      !ParseBufferedUInt(&pos, end, &unused) || unused > numeric_limits<uint32_t>::max() ||
      !ParseBufferedUInt(&pos, end, &num_strings) || !ParseBufferedUInt(&pos, end, &cmd_size)) {
    return false;
  }

  // Each argument has a size prefix of at least one byte, so a fully buffered command needs at
  // least num_strings + cmd_size bytes. This also bounds the reservation below.
  const uint64_t left = end - pos;
  if (num_strings > left || cmd_size > left - num_strings)
    return false;

  // On failure below, the generic path clears dest->cmd before parsing the entry again.
  journal::ParsedEntry::CmdData& cmd = dest->cmd;
  cmd.clear();
  cmd.Reserve(num_strings, cmd_size + num_strings /* +\0 char*/);
  for (uint64_t i = 0; i < num_strings; ++i) {
    uint64_t size = 0;
    if (!ParseBufferedUInt(&pos, end, &size) || size > cmd_size || size > uint64_t(end - pos))
      return false;

    cmd.PushArg(string_view{reinterpret_cast<const char*>(pos), size});
    pos += size;
    cmd_size -= size;
  }

  dbid_ = dbid;
  dest->dbid = dbid_;
  dest->opcode = journal::Op::COMMAND;
  dest->txid = txid;
  buf_.ConsumeInput(pos - input.data());

  VLOG(1) << "Read entry " << dest->ToString();
  return true;
}

std::error_code JournalReader::ReadEntry(journal::ParsedEntry* dest) {
  if (TryReadBufferedCommand(dest))
    return {};

  uint8_t int_op;
  SET_OR_RETURN(ReadUInt<uint8_t>(), int_op);
  journal::Op opcode = static_cast<journal::Op>(int_op);

  if (opcode == journal::Op::SELECT) {
    SET_OR_RETURN(ReadUInt<uint16_t>(), dbid_);
    return ReadEntry(dest);
  }

  dest->dbid = dbid_;
  dest->opcode = opcode;
  dest->cmd.clear();
  if (opcode == journal::Op::PING) {
    return {};
  }

  if (opcode == journal::Op::LSN) {
    SET_OR_RETURN(ReadUInt<uint64_t>(), dest->lsn);
    return {};
  }

  SET_OR_RETURN(ReadUInt<uint64_t>(), dest->txid);
  [[maybe_unused]] uint32_t unused;

  SET_OR_RETURN(ReadUInt<uint32_t>(), unused);

  VLOG(1) << "Read entry " << dest->ToString();

  return ReadCommand(&dest->cmd);
}

}  // namespace dfly
