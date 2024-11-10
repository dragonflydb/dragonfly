// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/container/flat_hash_map.h>

#include <optional>
#include <string_view>

#include "facade/facade_types.h"
#include "facade/op_status.h"
#include "io/io.h"

namespace facade {

// Reply mode allows filtering replies.
enum class ReplyMode {
  NONE,      // No replies are recorded
  ONLY_ERR,  // Only errors are recorded
  FULL       // All replies are recorded
};

// Base class for all reply builders. Offer a simple high level interface for controlling output
// modes and sending basic response types.
class SinkReplyBuilder {
  struct GuardBase {
    bool prev;
    SinkReplyBuilder* rb;
  };

 public:
  constexpr static size_t kMaxInlineSize = 32;
  constexpr static size_t kMaxBufferSize = 8192;

  explicit SinkReplyBuilder(io::Sink* sink) : sink_(sink) {
  }

  virtual ~SinkReplyBuilder() = default;

  // USE WITH CARE! ReplyScope assumes that all string views in Send calls keep valid for the scopes
  // lifetime. This allows the builder to avoid copies by enqueueing long strings directly for
  // vectorized io.
  struct ReplyScope : GuardBase {
    explicit ReplyScope(SinkReplyBuilder* rb) : GuardBase{std::exchange(rb->scoped_, true), rb} {
    }

    ~ReplyScope();
  };

  // Aggregator reduces the number of raw send calls by copying data in an intermediate buffer.
  // Prefer ReplyScope if possible to additionally reduce the number of copies.
  struct ReplyAggregator : GuardBase {
    explicit ReplyAggregator(SinkReplyBuilder* rb)
        : GuardBase{std::exchange(rb->batched_, true), rb} {
    }

    ~ReplyAggregator();
  };

  void Flush(size_t expected_buffer_cap = 0);  // Send all accumulated data and reset to clear state

  std::error_code GetError() const {
    return ec_;
  }

  size_t UsedMemory() const {
    return buffer_.Capacity();
  }

  size_t RepliesRecorded() const {
    return replies_recorded_;
  }

  bool IsSendActive() const {
    return send_active_;
  }

  void SetBatchMode(bool b) {
    batched_ = b;
  }

  void CloseConnection();

  static const ReplyStats& GetThreadLocalStats() {
    return tl_facade_stats->reply_stats;
  }

 public:  // High level interface
  virtual Protocol GetProtocol() const {
    return Protocol::NONE;
  }

  virtual void SendLong(long val) = 0;
  virtual void SendSimpleString(std::string_view str) = 0;

  virtual void SendStored() = 0;
  virtual void SendSetSkipped() = 0;
  void SendOk() {
    SendSimpleString("OK");
  }

  virtual void SendError(std::string_view str, std::string_view type = {}) = 0;  // MC and Redis
  void SendError(OpStatus status);
  void SendError(ErrorReply error);
  virtual void SendProtocolError(std::string_view str) = 0;

  std::string ConsumeLastError() {
    return std::exchange(last_error_, {});
  }

 protected:
  template <typename... Ts>
  void WritePieces(Ts&&... pieces);     // Copy pieces into buffer and reference buffer
  void WriteRef(std::string_view str);  // Add iovec bypassing buffer

  void FinishScope();  // Called when scope ends
  void NextVec(std::string_view str);

  void Send();

 protected:
  size_t replies_recorded_ = 0;
  std::string last_error_;

 private:
  io::Sink* sink_;
  std::error_code ec_;

  bool send_active_ = false;  // set while Send() is suspended on socket write
  bool scoped_ = false, batched_ = false;

  size_t total_size_ = 0;  // sum of vec_ lengths
  base::IoBuf buffer_;     // backing buffer for pieces

  // Stores iovecs for a single writev call. Can reference either the buffer (WritePiece) or
  // external data (WriteRef). Validity is ensured by FinishScope that either flushes before ref
  // lifetime ends or copies refs to the buffer.
  absl::InlinedVector<iovec, 16> vecs_;
  size_t guaranteed_pieces_ = 0;  // length of prefix of vecs_ that are guaranteed to be pieces
};

class MCReplyBuilder : public SinkReplyBuilder {
 public:
  explicit MCReplyBuilder(::io::Sink* sink);

  ~MCReplyBuilder() override = default;

  Protocol GetProtocol() const final {
    return Protocol::MEMCACHE;
  }

  void SendError(std::string_view str, std::string_view type = std::string_view{}) final;

  void SendStored() final;
  void SendLong(long val) final;
  void SendSetSkipped() final;

  void SendClientError(std::string_view str);
  void SendNotFound();

  void SendValue(std::string_view key, std::string_view value, uint64_t mc_ver, uint32_t mc_flag);
  void SendSimpleString(std::string_view str) final;
  void SendProtocolError(std::string_view str) final;

  void SendRaw(std::string_view str);

  void SetNoreply(bool noreply) {
    noreply_ = noreply;
  }

  bool NoReply() const {
    return noreply_;
  }

 private:
  bool noreply_ = false;
};

// Redis reply builder interface for sending RESP data.
class RedisReplyBuilderBase : public SinkReplyBuilder {
 public:
  enum CollectionType { ARRAY, SET, MAP, PUSH };
  enum VerbatimFormat { TXT, MARKDOWN };

  explicit RedisReplyBuilderBase(io::Sink* sink) : SinkReplyBuilder(sink) {
  }

  ~RedisReplyBuilderBase() override = default;

  Protocol GetProtocol() const final {
    return Protocol::REDIS;
  }

  virtual void SendNull();

  void SendSimpleString(std::string_view str) override;
  virtual void SendBulkString(std::string_view str);  // RESP: Blob String

  void SendLong(long val) override;
  virtual void SendDouble(double val);  // RESP: Number

  virtual void SendNullArray();
  virtual void StartCollection(unsigned len, CollectionType ct);

  using SinkReplyBuilder::SendError;
  void SendError(std::string_view str, std::string_view type = {}) override;
  void SendProtocolError(std::string_view str) override;

  virtual void SendVerbatimString(std::string_view str, VerbatimFormat format = TXT);

  static char* FormatDouble(double d, char* dest, unsigned len);
  static std::string SerializeCommand(std::string_view command);

  bool IsResp3() const {
    return resp3_;
  }

  void SetResp3(bool resp3) {
    resp3_ = resp3;
  }

 private:
  bool resp3_ = false;
};

// Non essential redis reply builder functions implemented on top of the base resp protocol
class RedisReplyBuilder : public RedisReplyBuilderBase {
 public:
  using RedisReplyBuilderBase::CollectionType;

  RedisReplyBuilder(io::Sink* sink) : RedisReplyBuilderBase(sink) {
  }

  ~RedisReplyBuilder() override = default;

  void SendSimpleStrArr(const facade::ArgRange& strs);
  void SendBulkStrArr(const facade::ArgRange& strs, CollectionType ct = ARRAY);
  void SendScoredArray(absl::Span<const std::pair<std::string, double>> arr, bool with_scores);

  void SendStored() final;
  void SendSetSkipped() final;

  void StartArray(unsigned len);
  void SendEmptyArray();
};

}  // namespace facade
