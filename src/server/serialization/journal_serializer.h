#pragma once

#include "base/io_buf.h"
#include "base/pod_array.h"
#include "io/io.h"
#include "server/common.h"
#include "server/generic_family.h"
#include "server/journal/types.h"
#include "server/list_family.h"
#include "server/main_service.h"
#include "server/string_family.h"
#include "server/table.h"
#include "server/transaction.h"

namespace dfly {

// Simplest form possible of journal serialzier.
struct JournalWriter {
  JournalWriter(io::Sink* sink) : sink_{sink}, tmp_str_{} {
  }

  void Write(uint32_t i) {
    sink_->Write(io::Bytes{reinterpret_cast<uint8_t*>(&i), 4});
  }

  void Write(journal::KeyType key) {
    Write(key.size());
    sink_->Write(io::Buffer(key));
  }

  void Write(const PrimeValue* value) {
    Write(value->GetSlice(&tmp_str_));
  }

  void Write(journal::ValueType value) {
    std::visit([this](const auto& v) { Write(v); }, value);
  }

  void Write(const journal::ListType& list) {
    Write(list.size());
    for (auto sv : list)
      Write(sv);
  }

  template <typename... Ts> void Write(const std::tuple<Ts...>& tuple) {
    std::apply([this](const auto&... args) { (Write(args), ...); }, tuple);
  }

  void WritePayload(const journal::Payload& pl) {
    std::visit([this](const auto& value) { Write(value); }, pl);
  }

  void Serialize(const journal::Entry& e) {
    uint8_t code = uint8_t(e.code);
    sink_->Write(io::Bytes{&code, 1});
    WritePayload(e.payload);
  }

 private:
  io::Sink* sink_;
  std::string tmp_str_;
};

struct JournalUtility {
  // TODO inline copy

  static void Print(const journal::KeyType& value, std::string* out) {
    *out += value;
    out->push_back(' ');
  }

  static void Print(const journal::ValueType& value, std::string* out) {
    Print(*std::get_if<journal::KeyType>(&value), out);
  }

  static void Print(const journal::ListType& value, std::string* out) {
    for (auto sv : value)
      Print(sv, out);
  }

  template <typename... Ts> static void Print(const std::tuple<Ts...>& tuple, std::string* out) {
    std::apply([out](const auto&... args) { (Print(args, out), ...); }, tuple);
  }

  static void Print(const journal::Payload& pl, std::string* out) {
    *out += "t:" + std::to_string(pl.index()) + " ";
    std::visit([out](const auto& value) { Print(value, out); }, pl);
  }

  static std::string Print(const journal::Entry& e) {
    std::string out;
    out += "code:" + std::to_string(uint8_t(e.code)) + " ";
    Print(e.payload, &out);
    return out;
  }
};

struct JournalReader {
  JournalReader(io::Source* source) : source_{source} {
  }

  uint32_t ReadInt() {
    uint32_t out;
    auto ptr = reinterpret_cast<uint8_t*>(&out);
    source_->Read(io::MutableBytes{ptr, 4});
    return out;
  }

  journal::OpCode ReadOpcode() {
    journal::OpCode code;
    source_->Read(io::MutableBytes{reinterpret_cast<uint8_t*>(&code), 1});
    return code;
  }

  journal::Payload InitPayloadType(journal::OpCode code) {
    using namespace std::literals;
    switch (code) {
      case journal::OpCode::SET:
        return std::make_tuple(""sv, ""sv);
      case journal::OpCode::DEL:
        return ArgSlice{};
      case journal::OpCode::LPUSH:
        return std::make_tuple(""sv, ArgSlice{});
      default:
        return journal::Payload{};
    }
  }

  void EnsureCapacity(uint32_t size) {
    if (payload_buf_.AppendBuffer().size() < size) {
      payload_buf_.Reserve(payload_buf_.Capacity() + size);
    }
  }

  void Read(const journal::KeyType* key) {
    uint32_t size = ReadInt();
    EnsureCapacity(size);
    source_->Read(payload_buf_.AppendBuffer().first(size));

    payload_buf_.CommitWrite(size);
    PushSize(size);
  }

  void Read(journal::ValueType* value) {
    Read(std::get_if<journal::KeyType>(value));
  }

  void Read(journal::ListType* value) {
    uint32_t list_size = ReadInt();
    size_t slice_size = sizeof(std::string_view) * list_size;
    size_t align = 8;

    EnsureCapacity(slice_size + align);
    PushSize(slice_size);
    payload_buf_.CommitWrite(slice_size + align);

    for (uint32_t i = 0; i < list_size; i++) {
      Read((const journal::KeyType*)nullptr);
    }
  }

  template <typename... Ts> void Read(std::tuple<Ts...>* tuple) {
    std::apply([this](auto&... args) { (Read(&args), ...); }, *tuple);
  }

  void Read(journal::Payload* pl) {
    std::visit([this](auto& value) { Read(&value); }, *pl);
  }

  void PushSize(size_t size) {
    payload_sizes_.push_back(size);
  }

  uint32_t PopSize() {
    uint32_t back = payload_sizes_.back();
    payload_sizes_.pop_back();
    return back;
  }

  void Consume(journal::KeyType* key) {
    CHECK(key != nullptr);

    uint32_t size = PopSize();
    auto span = payload_buf_.InputBuffer().subspan(payload_consumed_, size);
    *key = facade::ToSV(span);
    payload_consumed_ += size;
  }

  void Consume(journal::ValueType* value) {
    Consume(std::get_if<journal::KeyType>(value));
  }

  void Consume(journal::ListType* value) {
    uint32_t slice_size = PopSize();
    absl::Span<std::string_view> slice;
    {
      auto byte_span = payload_buf_.InputBuffer().subspan(payload_consumed_);
      size_t align = (8 - uint64_t(byte_span.data()) % 8) % 8;

      auto* ptr = reinterpret_cast<std::string_view*>(byte_span.data() + align);

      slice = absl::Span<std::string_view>{ptr, slice_size / sizeof(std::string_view)};
      payload_consumed_ += slice_size + 8;
    }

    for (auto& sv : slice) {
      new (&sv) std::string_view{};
      Consume(&sv);
    }

    *value = slice;
  }

  template <typename... Ts> void Consume(std::tuple<Ts...>* tuple) {
    std::apply([this](auto&... args) { (Consume(&args), ...); }, *tuple);
  }

  void Consume(journal::Payload* pl) {
    std::visit([this](auto& value) { Consume(&value); }, *pl);
  }

  // TODO: pass buffer to allow in-place initializing in direct storage
  journal::Entry ReadEntry() {
    payload_buf_.ConsumeInput(payload_buf_.InputLen());
    payload_sizes_.clear();
    payload_consumed_ = 0;

    journal::Entry e;
    e.code = ReadOpcode();
    e.payload = InitPayloadType(e.code);

    Read(&e.payload);
    std::reverse(payload_sizes_.begin(), payload_sizes_.end());
    Consume(&e.payload);

    return e;
  }

 private:
  io::Source* source_;
  base::IoBuf payload_buf_{128, std::align_val_t{8}};
  std::vector<uint32_t> payload_sizes_;
  size_t payload_consumed_ = 0;
};

struct JournalExecutor {
  void Execute(const journal::PldEmpty, const journal::OpCode code) {
  }

  void Execute(const journal::PldKeyValue kv, const journal::OpCode code) {
    CHECK(code == journal::OpCode::SET);
    auto [key, val_v] = kv;
    auto val = *std::get_if<std::string_view>(&val_v);

    auto t = new Transaction{cmd_registry_->FindCmd("SET")};
    t->InitByKeys(0, ArgSlice{&val, 1});

    auto cb = [key = key, val = val](Transaction* t, EngineShard* shard) {
      SetCmd sg(t->GetOpArgs(shard));
      return sg.Set({}, key, val);
    };
    t->ScheduleSingleHop(std::move(cb));
  }

  void Execute(const journal::ListType list, const journal::OpCode code) {
    CHECK(code == journal::OpCode::DEL);
    auto t = new Transaction{cmd_registry_->FindCmd("DEL")};
    t->InitByKeys(0, list);

    auto cb = [](const Transaction* t, EngineShard* shard) {
      ArgSlice args = t->ShardArgsInShard(shard->shard_id());
      GenericFamily::OpDel(t->GetOpArgs(shard), args);
      return OpStatus::OK;
    };
    t->ScheduleSingleHop(std::move(cb));
  }

  void Execute(const journal::PldKeyList kl, const journal::OpCode code) {
    CHECK(code == journal::OpCode::LPUSH);
    auto [key, vals] = kl;

    auto t = new Transaction{cmd_registry_->FindCmd("LPUSH")};
    t->InitByKeys(0, ArgSlice{&key, 1});

    auto cb = [key = key, vals = vals](const Transaction* t, EngineShard* shard) {
      return ListFamily::OpPush(t->GetOpArgs(shard), key, ListDir::LEFT, false, vals);
    };
    t->ScheduleSingleHopT(std::move(cb));
  }

  void Execute(const journal::Entry& e) {
    std::visit([this, code = e.code](const auto& value) { Execute(value, code); }, e.payload);
  }

  const Service* cmd_registry_;
};

};  // namespace dfly