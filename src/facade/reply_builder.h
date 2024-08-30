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

class SinkReplyBuilder {
 public:
  struct MGetStorage {
    MGetStorage* next = nullptr;
    char data[1];
  };

  struct GetResp {
    std::string key;  // TODO: to use backing storage to optimize this as well.
    std::string_view value;

    uint64_t mc_ver = 0;  // 0 means we do not output it (i.e has not been requested).
    uint32_t mc_flag = 0;

    GetResp() = default;
    GetResp(std::string_view val) : value(val) {
    }
  };

  struct MGetResponse {
    MGetStorage* storage_list = nullptr;  // backing storage of resp_arr values.
    std::vector<std::optional<GetResp>> resp_arr;

    MGetResponse() = default;

    MGetResponse(size_t size) : resp_arr(size) {
    }

    ~MGetResponse();

    MGetResponse(MGetResponse&& other) noexcept
        : storage_list(other.storage_list), resp_arr(std::move(other.resp_arr)) {
      other.storage_list = nullptr;
    }

    MGetResponse& operator=(MGetResponse&& other) noexcept {
      resp_arr = std::move(other.resp_arr);
      storage_list = other.storage_list;
      other.storage_list = nullptr;
      return *this;
    }
  };

  SinkReplyBuilder(const SinkReplyBuilder&) = delete;
  void operator=(const SinkReplyBuilder&) = delete;

  explicit SinkReplyBuilder(::io::Sink* sink);

  virtual ~SinkReplyBuilder() {
  }

  static MGetStorage* AllocMGetStorage(size_t size) {
    static_assert(alignof(MGetStorage) == 8);  // if this breaks we should fix the code below.
    char* buf = new char[size + sizeof(MGetStorage)];
    return new (buf) MGetStorage();
  }

  virtual void SendError(std::string_view str, std::string_view type = {}) = 0;  // MC and Redis
  virtual void SendError(OpStatus status);
  void SendError(ErrorReply error);

  virtual void SendStored() = 0;  // Reply for set commands.
  virtual void SendSetSkipped() = 0;

  virtual void SendMGetResponse(MGetResponse resp) = 0;

  virtual void SendLong(long val) = 0;
  virtual void SendSimpleString(std::string_view str) = 0;

  void SendOk() {
    SendSimpleString("OK");
  }

  virtual void SendProtocolError(std::string_view str) = 0;

  // In order to reduce interrupt rate we allow coalescing responses together using
  // Batch mode. It is controlled by Connection state machine because it makes sense only
  // when pipelined requests are arriving.
  virtual void SetBatchMode(bool batch);

  virtual void FlushBatch();

  // Used for QUIT - > should move to conn_context?
  virtual void CloseConnection();

  virtual std::error_code GetError() const {
    return ec_;
  }

  bool IsSendActive() const {
    return send_active_;  // BROKEN
  }

  struct ReplyAggregator {
    explicit ReplyAggregator(SinkReplyBuilder* builder) : builder_(builder) {
      // If the builder is already aggregating then don't aggregate again as
      // this will cause redundant sink writes (such as in a MULTI/EXEC).
      if (builder->should_aggregate_) {
        return;
      }
      builder_->StartAggregate();
      is_nested_ = false;
    }

    ~ReplyAggregator() {
      if (!is_nested_) {
        builder_->StopAggregate();
      }
    }

   private:
    SinkReplyBuilder* builder_;
    bool is_nested_ = true;
  };

  void ExpectReply();
  bool HasReplied() const {
    return true;  // WE break it for now
  }

  virtual size_t UsedMemory() const;

  static const ReplyStats& GetThreadLocalStats() {
    return tl_facade_stats->reply_stats;
  }

  static void ResetThreadLocalStats();

  virtual void StartAggregate();
  virtual void StopAggregate();

 protected:
  void SendRaw(std::string_view str);  // Sends raw without any formatting.

  void Send(const iovec* v, uint32_t len);

  std::string batch_;
  ::io::Sink* sink_;
  std::error_code ec_;

  bool should_batch_ : 1;

  // Similarly to batch mode but is controlled by at operation level.
  bool should_aggregate_ : 1;
  bool has_replied_ : 1;
  bool send_active_ : 1;
};

// Base class for all reply builders. Offer a simple high level interface for controlling output
// modes and sending basic response types.
class SinkReplyBuilder2 {
  struct GuardBase {
    bool prev;
    SinkReplyBuilder2* rb;
  };

 public:
  constexpr static size_t kMaxInlineSize = 32;
  constexpr static size_t kMaxBufferSize = 8192;

  explicit SinkReplyBuilder2(io::Sink* sink) : sink_(sink) {
  }

  virtual ~SinkReplyBuilder2() = default;

  // USE WITH CARE! ReplyScope assumes that all string views in Send calls keep valid for the scopes
  // lifetime. This allows the builder to avoid copies by enqueueing long strings directly for
  // vectorized io.
  struct ReplyScope : GuardBase {
    explicit ReplyScope(SinkReplyBuilder2* rb) : GuardBase{std::exchange(rb->scoped_, true), rb} {
    }

    ~ReplyScope();
  };

  // Aggregator reduces the number of raw send calls by copying data in an intermediate buffer.
  // Prefer ReplyScope if possible to additionally reduce the number of copies.
  struct ReplyAggregator : GuardBase {
    explicit ReplyAggregator(SinkReplyBuilder2* rb)
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
  template <typename... Ts>
  void WritePieces(Ts&&... pieces);     // Copy pieces into buffer and reference buffer
  void WriteRef(std::string_view str);  // Add iovec bypassing buffer

  void FinishScope();  // Called when scope ends
  void NextVec(std::string_view str);

  void Send();

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
  bool noreply_;

 public:
  MCReplyBuilder(::io::Sink* stream);

  using SinkReplyBuilder::SendRaw;

  void SendError(std::string_view str, std::string_view type = std::string_view{}) final;

  // void SendGetReply(std::string_view key, uint32_t flags, std::string_view value) final;
  void SendMGetResponse(MGetResponse resp) final;

  void SendStored() final;
  void SendLong(long val) final;
  void SendSetSkipped() final;

  void SendClientError(std::string_view str);
  void SendNotFound();
  void SendSimpleString(std::string_view str) final;
  void SendProtocolError(std::string_view str) final;

  void SetNoreply(bool noreply) {
    noreply_ = noreply;
  }

  bool NoReply() const;
};

class MCReplyBuilder2 : public SinkReplyBuilder2 {
 public:
  explicit MCReplyBuilder2(::io::Sink* sink);

  ~MCReplyBuilder2() override = default;

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
  bool noreply_ = false;
};

class RedisReplyBuilder : public SinkReplyBuilder {
 public:
  enum CollectionType { ARRAY, SET, MAP, PUSH };

  enum VerbatimFormat { TXT, MARKDOWN };

  using StrSpan = facade::ArgRange;

  RedisReplyBuilder(::io::Sink* stream);

  virtual void SetResp3(bool is_resp3);
  virtual bool IsResp3() const {
    return is_resp3_;
  }

  void SendError(std::string_view str, std::string_view type = {}) override;
  using SinkReplyBuilder::SendError;

  void SendMGetResponse(MGetResponse resp) override;

  void SendStored() override;
  void SendSetSkipped() override;
  void SendProtocolError(std::string_view str) override;

  virtual void SendNullArray();   // Send *-1
  virtual void SendEmptyArray();  // Send *0
  virtual void SendSimpleStrArr(StrSpan arr);
  virtual void SendStringArr(StrSpan arr, CollectionType type = ARRAY);

  virtual void SendNull();
  void SendLong(long val) override;
  virtual void SendDouble(double val);
  void SendSimpleString(std::string_view str) override;

  virtual void SendBulkString(std::string_view str);
  virtual void SendVerbatimString(std::string_view str, VerbatimFormat format = TXT);
  virtual void SendScoredArray(absl::Span<const std::pair<std::string, double>> arr,
                               bool with_scores);

  void StartArray(unsigned len);  // StartCollection(len, ARRAY)

  virtual void StartCollection(unsigned len, CollectionType type);

  static char* FormatDouble(double val, char* dest, unsigned dest_len);

 private:
  void SendStringArrInternal(size_t size, absl::FunctionRef<std::string_view(unsigned)> producer,
                             CollectionType type);

  bool is_resp3_ = false;
};

// Redis reply builder interface for sending RESP data.
class RedisReplyBuilder2Base : public SinkReplyBuilder2, public RedisReplyBuilder {
 public:
  using CollectionType = RedisReplyBuilder::CollectionType;
  using VerbatimFormat = RedisReplyBuilder::VerbatimFormat;

  explicit RedisReplyBuilder2Base(io::Sink* sink)
      : SinkReplyBuilder2(sink), RedisReplyBuilder(nullptr) {
  }

  ~RedisReplyBuilder2Base() override = default;

  void SendNull() override;

  void SendSimpleString(std::string_view str) override;
  void SendBulkString(std::string_view str) override;  // RESP: Blob String

  void SendLong(long val) override;
  void SendDouble(double val) override;  // RESP: Number

  void SendNullArray() override;
  void StartCollection(unsigned len, CollectionType ct) override;

  using SinkReplyBuilder2::SendError;
  void SendError(std::string_view str, std::string_view type = {}) override;
  void SendProtocolError(std::string_view str) override;

  static char* FormatDouble(double d, char* dest, unsigned len);
  virtual void SendVerbatimString(std::string_view str, VerbatimFormat format = TXT) override;

  bool IsResp3() const override {
    return resp3_;
  }

  // REMOVE THIS override
  void SetResp3(bool resp3) override {
    resp3_ = resp3;
  }

  // REMOVE THIS
  void SetBatchMode(bool mode) override {
    SinkReplyBuilder2::SetBatchMode(mode);
  }

  void StartAggregate() override {
    aggregators_.emplace_back(SinkReplyBuilder2::ReplyAggregator(this));
  }

  void StopAggregate() override {
    aggregators_.pop_back();
  }

  void FlushBatch() override {
    SinkReplyBuilder2::Flush();
  }

  // REMOVE THIS

  void CloseConnection() override {
    SinkReplyBuilder2::CloseConnection();
  }

  std::error_code GetError() const override {
    return SinkReplyBuilder2::GetError();
  }

 private:
  std::vector<SinkReplyBuilder2::ReplyAggregator> aggregators_;
  bool resp3_ = false;
};

// Non essential rediss reply builder functions implemented on top of the base resp protocol
class RedisReplyBuilder2 : public RedisReplyBuilder2Base {
 public:
  RedisReplyBuilder2(io::Sink* sink) : RedisReplyBuilder2Base(sink) {
  }

  ~RedisReplyBuilder2() override = default;

  void SendSimpleStrArr2(const facade::ArgRange& strs);

  void SendBulkStrArr(const facade::ArgRange& strs, CollectionType ct = ARRAY);
  void SendScoredArray(absl::Span<const std::pair<std::string, double>> arr,
                       bool with_scores) override;

  void SendSimpleStrArr(RedisReplyBuilder::StrSpan arr) override {
    SendSimpleStrArr2(arr);
  }
  void SendStringArr(RedisReplyBuilder::StrSpan arr, CollectionType type = ARRAY) override {
    SendBulkStrArr(arr, type);
  }

  void SendStored() final;
  void SendSetSkipped() final;

  void StartArray(unsigned len);
  void SendEmptyArray() override;

  // TODO: Remove
  void SendMGetResponse(SinkReplyBuilder::MGetResponse resp) override;

  static std::string SerializeCommmand(std::string_view cmd);
};

class ReqSerializer {
 public:
  explicit ReqSerializer(::io::Sink* stream) : sink_(stream) {
  }

  void SendCommand(std::string_view str);

  std::error_code ec() const {
    return ec_;
  }

 private:
  ::io::Sink* sink_;
  std::error_code ec_;
};

}  // namespace facade
