// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/container/flat_hash_map.h>

#include <optional>
#include <string_view>

#include "facade/op_status.h"
#include "io/io.h"

namespace facade {

class SinkReplyBuilder {
 public:
  struct ResponseValue {
    std::string key;
    std::string value;
    uint64_t mc_ver = 0;  // 0 means we do not output it (i.e has not been requested).
    uint32_t mc_flag = 0;
  };

  using OptResp = std::optional<ResponseValue>;

 public:
  SinkReplyBuilder(const SinkReplyBuilder&) = delete;
  void operator=(const SinkReplyBuilder&) = delete;

  SinkReplyBuilder(::io::Sink* sink);

  virtual ~SinkReplyBuilder() {
  }

  virtual void SendError(std::string_view str, std::string_view type = {}) = 0;  // MC and Redis

  virtual void SendStored() = 0;  // Reply for set commands.
  virtual void SendSetSkipped() = 0;

  virtual void SendMGetResponse(absl::Span<const OptResp>) = 0;

  virtual void SendLong(long val) = 0;
  virtual void SendSimpleString(std::string_view str) = 0;

  void SendOk() {
    SendSimpleString("OK");
  }

  // In order to reduce interrupt rate we allow coalescing responses together using
  // Batch mode. It is controlled by Connection state machine because it makes sense only
  // when pipelined requests are arriving.
  void SetBatchMode(bool batch) {
    should_batch_ = batch;
  }

  // Used for QUIT - > should move to conn_context?
  void CloseConnection();

  std::error_code GetError() const {
    return ec_;
  }

  size_t io_write_cnt() const {
    return io_write_cnt_;
  }

  size_t io_write_bytes() const {
    return io_write_bytes_;
  }

  void reset_io_stats() {
    io_write_cnt_ = 0;
    io_write_bytes_ = 0;
    err_count_.clear();
  }

  const absl::flat_hash_map<std::string, uint64_t>& err_count() const {
    return err_count_;
  }

 protected:
  void SendRaw(std::string_view str);  // Sends raw without any formatting.
  void SendRawVec(absl::Span<const std::string_view> msg_vec);

  void Send(const iovec* v, uint32_t len);

  std::string batch_;
  ::io::Sink* sink_;
  std::error_code ec_;

  size_t io_write_cnt_ = 0;
  size_t io_write_bytes_ = 0;
  absl::flat_hash_map<std::string, uint64_t> err_count_;

  bool should_batch_ = false;
};

class MCReplyBuilder : public SinkReplyBuilder {
 public:
  MCReplyBuilder(::io::Sink* stream);

  using SinkReplyBuilder::SendRaw;

  void SendError(std::string_view str, std::string_view type = std::string_view{}) final;

  // void SendGetReply(std::string_view key, uint32_t flags, std::string_view value) final;
  void SendMGetResponse(absl::Span<const OptResp>) final;

  void SendStored() final;
  void SendLong(long val) final;
  void SendSetSkipped() final;

  void SendClientError(std::string_view str);
  void SendNotFound();
  void SendSimpleString(std::string_view str) final;
};

class RedisReplyBuilder : public SinkReplyBuilder {
 public:
  enum CollectionType { ARRAY, SET, MAP, PUSH };

  using StrSpan = std::variant<absl::Span<const std::string>, absl::Span<const std::string_view>>;

  RedisReplyBuilder(::io::Sink* stream);

  void SetResp3(bool is_resp3);

  void SendError(std::string_view str, std::string_view type = {}) override;
  void SendMGetResponse(absl::Span<const OptResp>) override;

  void SendStored() override;
  void SendSetSkipped() override;
  virtual void SendError(OpStatus status);

  virtual void SendNullArray();   // Send *-1
  virtual void SendEmptyArray();  // Send *0
  virtual void SendSimpleStrArr(StrSpan arr);
  virtual void SendStringArr(StrSpan arr, CollectionType type = ARRAY);

  virtual void SendNull();
  void SendLong(long val) override;
  virtual void SendDouble(double val);
  void SendSimpleString(std::string_view str) override;

  virtual void SendBulkString(std::string_view str);
  virtual void SendScoredArray(const std::vector<std::pair<std::string, double>>& arr,
                               bool with_scores);

  void StartArray(unsigned len);  // StartCollection(len, ARRAY)
  virtual void StartCollection(unsigned len, CollectionType type);

  static char* FormatDouble(double val, char* dest, unsigned dest_len);

  // You normally should not call this - maps the status
  // into the string that would be sent
  static std::string_view StatusToMsg(OpStatus status);

 protected:
  struct WrappedStrSpan : public StrSpan {
    size_t Size() const;
    std::string_view operator[](size_t index) const;
  };

 private:
  void SendStringArrInternal(WrappedStrSpan arr, CollectionType type);

  const char* NullString();

  bool is_resp3_ = false;
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
