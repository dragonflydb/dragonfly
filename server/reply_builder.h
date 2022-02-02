// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include <optional>
#include <string_view>

#include "core/op_status.h"
#include "io/sync_stream_interface.h"
// #include "server/common_types.h"

namespace dfly {

class BaseSerializer {
 public:
  explicit BaseSerializer(::io::Sink* sink);
  virtual ~BaseSerializer() {
  }

 private:
  ::io::Sink* sink_;
  std::error_code ec_;
  std::string batch_;

  bool should_batch_ = false;
};

class ReplyBuilderInterface {
 public:
  virtual ~ReplyBuilderInterface() {
  }

  // Reply for set commands.
  virtual void SendStored() = 0;

  // Common for both MC and Redis.
  virtual void SendError(std::string_view str) = 0;

  virtual std::error_code GetError() const = 0;

  virtual void SendGetNotFound() = 0;
  virtual void SendGetReply(std::string_view key, uint32_t flags, std::string_view value) = 0;
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

 protected:
  //! Sends a string as is without any formatting. raw should be encoded according to the protocol.
  void SendDirect(std::string_view str);

  void Send(const iovec* v, uint32_t len);

  ::io::Sink* sink_;
  std::error_code ec_;
  std::string batch_;

  bool should_batch_ = false;
};

class MCReplyBuilder : public SinkReplyBuilder {
 public:
  MCReplyBuilder(::io::Sink* stream);

  void SendError(std::string_view str) final;
  void SendGetReply(std::string_view key, uint32_t flags, std::string_view value) final;

  // memcache does not print keys that are not found.
  void SendGetNotFound() final {
  }

  void SendStored() final;

  void SendClientError(std::string_view str);
};

class RedisReplyBuilder : public SinkReplyBuilder {
 public:
  RedisReplyBuilder(::io::Sink* stream);

  void SendOk() {
    SendSimpleString("OK");
  }

  void SendError(std::string_view str) override;
  void SendGetReply(std::string_view key, uint32_t flags, std::string_view value) override;
  void SendGetNotFound() override;
  void SendStored() override;

  void SendError(OpStatus status);
  virtual void SendSimpleString(std::string_view str);

  using StrOrNil = std::optional<std::string_view>;
  virtual void SendMGetResponse(const StrOrNil* arr, uint32_t count);
  virtual void SendSimpleStrArr(const std::string_view* arr, uint32_t count);
  virtual void SendNullArray();

  virtual void SendStringArr(absl::Span<const std::string_view> arr);
  virtual void SendNull();

  virtual void SendLong(long val);
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
