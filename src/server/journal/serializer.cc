// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/journal/serializer.h"

#include <chrono>
#include <system_error>

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
  const size_t buffered_before = buf_.InputLen();
  const size_t capacity = buf_.Capacity();
  auto start = std::chrono::steady_clock::now();
  uint64_t read;
  SET_OR_RETURN(source_->ReadAtLeast(buf_.AppendBuffer(), remainder), read);
  auto elapsed = std::chrono::steady_clock::now() - start;
  ++last_read_stats_.source_read_calls;
  last_read_stats_.source_read_bytes += read;
  last_read_stats_.source_read_time += elapsed;
  if (elapsed > std::chrono::milliseconds{10}) {
    VLOG(1) << "Journal socket read: elapsed_ms="
            << std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count()
            << ", required_bytes=" << remainder << ", received_bytes=" << read
            << ", reader_buffered_before=" << buffered_before
            << ", reader_buffer_capacity=" << capacity;
  }

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
      const size_t buffered_before = buf_.InputLen();
      const size_t capacity = buf_.Capacity();
      auto start = std::chrono::steady_clock::now();
      uint64_t read;
      SET_OR_RETURN(source_->Read({remainder_buf_pos, remainder}), read);
      auto elapsed = std::chrono::steady_clock::now() - start;
      ++last_read_stats_.source_read_calls;
      last_read_stats_.source_read_bytes += read;
      last_read_stats_.source_read_time += elapsed;
      if (elapsed > std::chrono::milliseconds{10}) {
        VLOG(1) << "Journal direct payload read: elapsed_ms="
                << std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count()
                << ", required_bytes=" << remainder << ", received_bytes=" << read
                << ", reader_buffered_before=" << buffered_before
                << ", reader_buffer_capacity=" << capacity;
      }
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

std::error_code JournalReader::ReadEntry(journal::ParsedEntry* dest) {
  last_read_stats_ = {};
  uint8_t int_op;
  SET_OR_RETURN(ReadUInt<uint8_t>(), int_op);
  journal::Op opcode = static_cast<journal::Op>(int_op);

  if (opcode == journal::Op::SELECT) {
    SET_OR_RETURN(ReadUInt<uint16_t>(), dbid_);
    ReadStats select_read_stats = last_read_stats_;
    std::error_code ec = ReadEntry(dest);
    last_read_stats_.source_read_calls += select_read_stats.source_read_calls;
    last_read_stats_.source_read_bytes += select_read_stats.source_read_bytes;
    last_read_stats_.source_read_time += select_read_stats.source_read_time;
    return ec;
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
