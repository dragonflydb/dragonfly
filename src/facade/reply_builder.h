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

  void Flush();  // Send all accumulated data and reset to clear state

  std::error_code GetError() const {
    return ec_;
  }

  size_t UsedMemory() const {
    return buffer_.Capacity();
  }

  bool IsSendActive() {
    return send_active_;
  }

  void SetBatchMode(bool b) {
    batched_ = b;
  }

  void CloseConnection();

  static const ReplyStats& GetThreadLocalStats() {
    return tl_facade_stats->reply_stats;
  }

  static void ResetThreadLocalStats() {
    tl_facade_stats->reply_stats = {};
  }

 public:  // High level interface
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

 protected:
  void WriteI(std::string_view str) {
    str.size() > kMaxInlineSize ? WriteRef(str) : WritePiece(str);
  }

  // Constexpr arrays are assumed to be protocol control sequences, stash them as pieces
  template <size_t S> void WriteI(const char (&arr)[S]) {
    WritePiece(std::string_view{arr, S - 1});  // we assume null termination
  }

  template <typename... Args> void Write(Args&&... strs) {
    (WriteI(strs), ...);
  }

  void FinishScope();  // Called when scope ends

  char* ReservePiece(size_t size);        // Reserve size bytes from buffer
  void CommitPiece(size_t size);          // Mark size bytes from buffer as used
  void WritePiece(std::string_view str);  // Reserve + memcpy + Commit

  void WriteRef(std::string_view str);  // Add iovec bypassing buffer
  void NextVec(std::string_view str);

 private:
  io::Sink* sink_;
  std::error_code ec_;

  bool send_active_ = false;
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

  void SendError(std::string_view str, std::string_view type = std::string_view{}) final;

  void SendStored() final;
  void SendLong(long val) final;
  void SendSetSkipped() final;

  void SendClientError(std::string_view str);
  void SendNotFound();

  void SendValue(std::string_view key, std::string_view value, uint64_t mc_ver, uint32_t mc_flag);
  void SendSimpleString(std::string_view str) final;
  void SendProtocolError(std::string_view str) final;

  void SetNoreply(bool noreply) {
    noreply_ = noreply;
  }

  bool NoReply() const {
    return noreply_;
  }

 private:
  void SendSimplePiece(std::string&& str);  // Send simple string as piece (for short lived data)

  bool noreply_ = false;
};

// Redis reply builder interface for sending RESP data.
class RedisReplyBuilderBase : public SinkReplyBuilder {
 public:
  enum CollectionType { ARRAY, SET, MAP, PUSH };

  enum VerbatimFormat { TXT, MARKDOWN };

  explicit RedisReplyBuilderBase(io::Sink* sink) : SinkReplyBuilder(sink) {
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

  static char* FormatDouble(double d, char* dest, unsigned len);
  virtual void SendVerbatimString(std::string_view str, VerbatimFormat format = TXT);

  bool IsResp3() const {
    return resp3_;
  }

  void SetResp3(bool resp3) {
    resp3_ = resp3;
  }

 private:
  void WriteIntWithPrefix(char prefix, int64_t val);  // FastIntToBuffer directly into ReservePiece

  bool resp3_ = false;
};

// Non essential rediss reply builder functions implemented on top of the base resp protocol
class RedisReplyBuilder : public RedisReplyBuilderBase {
 public:
  RedisReplyBuilder(io::Sink* sink) : RedisReplyBuilderBase(sink) {
  }

  void SendSimpleStrArr(const facade::ArgRange& strs);
  void SendBulkStrArr(const facade::ArgRange& strs, CollectionType ct = ARRAY);
  void SendScoredArray(absl::Span<const std::pair<std::string, double>> arr, bool with_scores);

  void SendStored() final;
  void SendSetSkipped() final;

  void StartArray(unsigned len);
  void SendEmptyArray();

  static std::string SerializeCommand(std::string_view cmd);
};

}  // namespace facade
