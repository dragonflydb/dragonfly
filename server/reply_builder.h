// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include <optional>
#include <string_view>

#include "core/op_status.h"
#include "io/sync_stream_interface.h"

namespace dfly {

class ReplyBuilderInterface {
 public:
  virtual ~ReplyBuilderInterface() {
  }

  // Reply for set commands.
  virtual void SendStored() = 0;

  // Common for both MC and Redis.
  virtual void SendError(std::string_view str) = 0;

  virtual std::error_code GetError() const = 0;

  struct ResponseValue {
    std::string_view key;
    std::string value;
    uint64_t mc_ver = 0;  // 0 means we do not output it (i.e has not been requested).
    uint32_t mc_flag = 0;
  };

  using OptResp = std::optional<ResponseValue>;

  virtual void SendMGetResponse(const OptResp* resp, uint32_t count) = 0;
  virtual void SendLong(long val) = 0;

  virtual void SendSetSkipped() = 0;
};

class SinkReplyBuilder : public ReplyBuilderInterface {
 public:
  SinkReplyBuilder(const SinkReplyBuilder&) = delete;
  void operator=(const SinkReplyBuilder&) = delete;

  SinkReplyBuilder(::io::Sink* sink);

  // In order to reduce interrupt rate we allow coalescing responses together using
  // Batch mode. It is controlled by Connection state machine because it makes sense only
  // when pipelined requests are arriving.
  void SetBatchMode(bool batch) {
    should_batch_ = batch;
  }

  // Used for QUIT - > should move to conn_context?
  void CloseConnection();

  std::error_code GetError() const override {
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
  }

  //! Sends a string as is without any formatting. raw should be encoded according to the protocol.
  void SendDirect(std::string_view str);

 protected:
  void Send(const iovec* v, uint32_t len);

  std::string batch_;
  ::io::Sink* sink_;
  std::error_code ec_;
  size_t io_write_cnt_ = 0;
  size_t io_write_bytes_ = 0;

  bool should_batch_ = false;
};

class MCReplyBuilder : public SinkReplyBuilder {
 public:
  MCReplyBuilder(::io::Sink* stream);

  void SendError(std::string_view str) final;
  // void SendGetReply(std::string_view key, uint32_t flags, std::string_view value) final;
  void SendMGetResponse(const OptResp* resp, uint32_t count) final;

  void SendStored() final;
  void SendLong(long val) final;
  void SendSetSkipped() final;

  void SendClientError(std::string_view str);
};

class RedisReplyBuilder : public SinkReplyBuilder {
 public:
  RedisReplyBuilder(::io::Sink* stream);

  void SendOk() {
    SendSimpleString("OK");
  }

  void SendError(std::string_view str) override;
  void SendMGetResponse(const OptResp* resp, uint32_t count) override;

  void SendStored() override;
  void SendLong(long val) override;
  void SendSetSkipped() override;

  void SendError(OpStatus status);
  virtual void SendSimpleString(std::string_view str);

  virtual void SendSimpleStrArr(const std::string_view* arr, uint32_t count);
  virtual void SendNullArray();

  virtual void SendStringArr(absl::Span<const std::string_view> arr);
  virtual void SendNull();

  virtual void SendDouble(double val);

  virtual void SendBulkString(std::string_view str);

  virtual void StartArray(unsigned len);

 private:
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

}  // namespace dfly
