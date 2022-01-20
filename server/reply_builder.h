// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include <optional>
#include <string_view>

#include "core/op_status.h"
#include "io/sync_stream_interface.h"
#include "server/common_types.h"

namespace dfly {

class BaseSerializer {
 public:
  explicit BaseSerializer(::io::Sink* sink);
  virtual ~BaseSerializer() {
  }

  std::error_code ec() const {
    return ec_;
  }

  void CloseConnection() {
    if (!ec_)
      ec_ = std::make_error_code(std::errc::connection_aborted);
  }

  // In order to reduce interrupt rate we allow coalescing responses together using
  // Batch mode. It is controlled by Connection state machine because it makes sense only
  // when pipelined requests are arriving.
  void SetBatchMode(bool batch) {
    should_batch_ = batch;
  }

  //! Sends a string as is without any formatting. raw should be encoded according to the protocol.
  void SendDirect(std::string_view str);

  void Send(const iovec* v, uint32_t len);

 private:
  ::io::Sink* sink_;
  std::error_code ec_;
  std::string batch_;

  bool should_batch_ = false;
};

class RespSerializer : public BaseSerializer {
 public:
  RespSerializer(::io::Sink* sink) : BaseSerializer(sink) {
  }

  //! See https://redis.io/topics/protocol
  void SendSimpleString(std::string_view str);
  void SendNull();

  /// aka "$6\r\nfoobar\r\n"
  void SendBulkString(std::string_view str);
};

class MemcacheSerializer : public BaseSerializer {
 public:
  explicit MemcacheSerializer(::io::Sink* sink) : BaseSerializer(sink) {
  }

  void SendStored();
  void SendError();
};

class ReplyBuilder {
 public:
  ReplyBuilder(Protocol protocol, ::io::Sink* stream);

  void SendStored();

  void SendError(std::string_view str);
  void SendError(OpStatus status);

  void SendOk() {
    as_resp()->SendSimpleString("OK");
  }

  std::error_code ec() const {
    return serializer_->ec();
  }

  void SendMCClientError(std::string_view str);
  void EndMultilineReply();

  void SendSimpleRespString(std::string_view str) {
    as_resp()->SendSimpleString(str);
  }

  void SendGetReply(std::string_view key, uint32_t flags, std::string_view value);
  void SendGetNotFound();

  using StrOrNil = std::optional<std::string_view>;
  void SendMGetResponse(const StrOrNil* arr, uint32_t count);

  void SetBatchMode(bool mode) {
    serializer_->SetBatchMode(mode);
  }

  // Resp specific.
  // This one is prefixed with + and with clrf added automatically to each item..
  void SendSimpleStrArr(const std::string_view* arr, uint32_t count);
  void SendNullArray();

  void SendStringArr(absl::Span<const std::string_view> arr);

  void SendNull() {
    as_resp()->SendNull();
  }

  void SendLong(long val);

  void SendBulkString(std::string_view str) {
    as_resp()->SendBulkString(str);
  }

  void SendRespBlob(std::string_view str) {
    as_resp()->SendDirect(str);
  }

  void CloseConnection() {
    serializer_->CloseConnection();
  }

 private:
  RespSerializer* as_resp() {
    return static_cast<RespSerializer*>(serializer_.get());
  }
  MemcacheSerializer* as_mc() {
    return static_cast<MemcacheSerializer*>(serializer_.get());
  }

  std::unique_ptr<BaseSerializer> serializer_;
  Protocol protocol_;
};

}  // namespace dfly
