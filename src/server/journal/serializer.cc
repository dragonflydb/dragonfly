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
#include "server/transaction.h"

using namespace std;

using nonstd::make_unexpected;

// TODO: Stolen from rdb, unite in common utils

#define SET_OR_RETURN(expr, dest)              \
  do {                                         \
    auto exp_val = (expr);                     \
    if (!exp_val) {                            \
      VLOG(1) << "Error while calling " #expr; \
      return exp_val.error();                  \
    }                                          \
    dest = exp_val.value();                    \
  } while (0)

#define SET_OR_UNEXPECT(expr, dest)            \
  {                                            \
    auto exp_res = (expr);                     \
    if (!exp_res)                              \
      return make_unexpected(exp_res.error()); \
    dest = std::move(exp_res.value());         \
  }

namespace dfly {

JournalWriter::JournalWriter(io::Sink* sink) : sink_{sink} {
}

error_code JournalWriter::Write(uint8_t v) {
  return sink_->Write(io::Bytes{&v, 1});
}

error_code JournalWriter::Write(uint16_t v) {
  uint8_t buf[2];
  absl::big_endian::Store16(&buf, v);
  return sink_->Write(io::Bytes{buf, sizeof(buf)});
}

error_code JournalWriter::Write(uint64_t v) {
  uint8_t buf[8];
  absl::big_endian::Store64(&buf, v);
  return sink_->Write(io::Bytes{buf, sizeof(buf)});
}

error_code JournalWriter::Write(std::string_view sv) {
  RETURN_ON_ERR(Write(sv.size()));
  return sink_->Write(io::Buffer(sv));
}

error_code JournalWriter::Write(std::pair<std::string_view, ArgSlice> args) {
  auto [cmd, tail_args] = args;

  RETURN_ON_ERR(Write(1 + tail_args.size()));
  RETURN_ON_ERR(Write(cmd));
  for (auto v : tail_args)
    RETURN_ON_ERR(Write(v));

  return std::error_code{};
}
error_code JournalWriter::Write(CmdArgList args) {
  RETURN_ON_ERR(Write(args.size()));
  for (auto v : args)
    RETURN_ON_ERR(Write(facade::ToSV(v)));

  return std::error_code{};
}

error_code JournalWriter::Write(std::monostate) {
  return std::error_code{};
}

error_code JournalWriter::Write(const journal::Entry& entry) {
  // VLOG(0) << "Writing " << entry.Print();

  RETURN_ON_ERR(Write(entry.txid));
  RETURN_ON_ERR(Write(uint8_t(entry.opcode)));

  if (entry.opcode == journal::Op::SELECT) {
    return Write(entry.dbid);
  } else if (entry.opcode == journal::Op::COMMAND) {
    auto cb = [this](const auto& payload) { return Write(payload); };
    return std::visit(cb, entry.payload);
  }

  return std::error_code{};
}

JournalReader::JournalReader(io::Source* source) : source_{source}, buf_{} {
}

io::Result<uint8_t> JournalReader::ReadU8() {
  uint8_t buf[1];
  size_t read;
  SET_OR_UNEXPECT(source_->Read(io::MutableBytes{buf}), read);
  if (read < 1)  // TODO: Custom errc namespace? Generic opcodes?
    return make_unexpected(std::make_error_code(std::errc::result_out_of_range));
  return buf[0];
}

io::Result<uint16_t> JournalReader::ReadU16() {
  uint8_t buf[2];
  size_t read;
  SET_OR_UNEXPECT(source_->Read(io::MutableBytes{buf}), read);
  if (read < 2)  // TODO: Custom errc namespace? Generic opcodes?
    return make_unexpected(std::make_error_code(std::errc::result_out_of_range));
  return absl::big_endian::Load16(buf);
}

io::Result<uint64_t> JournalReader::ReadU64() {
  uint8_t buf[8];
  size_t read;
  SET_OR_UNEXPECT(source_->Read(io::MutableBytes{buf}), read);
  if (read < 8)  // TODO: Custom errc namespace? Generic opcodes?
    return make_unexpected(std::make_error_code(std::errc::result_out_of_range));
  return absl::big_endian::Load64(buf);
}

io::Result<size_t> JournalReader::ReadString() {
  size_t size = 0;
  SET_OR_UNEXPECT(ReadU64(), size);

  buf_.Reserve(buf_.InputLen() + size);  // TODO: There should be a more intuitive function

  auto dest = buf_.AppendBuffer().first(size);
  auto res = source_->Read(dest);
  if (!res)
    return res;

  buf_.CommitWrite(*res);
  if (*res != size)
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

  std::string out;
  for (auto& span : *vec) {
    out += facade::ToSV(span);
    out += " ";
  }
  // VLOG(0) << "Read payload:" << out;

  return std::error_code{};
}

io::Result<journal::Entry> JournalReader::ReadEntry() {
  uint64_t txid;
  SET_OR_UNEXPECT(ReadU64(), txid);

  uint8_t opcode;
  SET_OR_UNEXPECT(ReadU8(), opcode);

  journal::Entry entry{static_cast<journal::Op>(opcode), txid};

  if (entry.opcode == journal::Op::COMMAND) {
    entry.owned_payload = CmdArgVec{};
    if (auto ec = Read(&*entry.owned_payload); ec)
      return make_unexpected(ec);
  } else {
    SET_OR_UNEXPECT(ReadU16(), entry.dbid);
  }

  return entry;
}

JournalExecutor::JournalExecutor(Service* service) : service_{service} {
}

void JournalExecutor::Execute(journal::Entry&& entry) {
  if (entry.owned_payload) {
    auto& payload = *entry.owned_payload;

    io::NullSink null_sink;
    ConnectionContext conn_context{&null_sink, nullptr};
    conn_context.is_replicating = true;
    conn_context.journal_emulated = true;
    // TODO: conn_context.dbid = dbid_;!!!

    // std::string printout;
    // for (auto arg: payload) printout += std::string{facade::ToSV(arg)} + " ";
    // VLOG(0) << "            EXECUTE: " << printout;

    auto span = CmdArgList{payload.data(), payload.size()};
    service_->DispatchCommand(span, &conn_context);
  } else if (entry.opcode == journal::Op::SELECT) {
    dbid_ = entry.dbid;
  }
}

}  // namespace dfly