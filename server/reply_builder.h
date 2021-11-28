// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include <string_view>

#include "io/sync_stream_interface.h"
#include "server/dfly_protocol.h"
#include "server/op_status.h"

namespace dfly {

class BaseSerializer {
 public:
  explicit BaseSerializer(::io::Sink* sink);

  std::error_code ec() const {
    return ec_;
  }

  void CloseConnection() {
    if (!ec_)
      ec_ = std::make_error_code(std::errc::connection_aborted);
  }

  //! Sends a string as is without any formatting. raw should be encoded according to the protocol.
  void SendDirect(std::string_view str);

  ::io::Sink* sink() {
    return sink_;
  }

  void Send(const iovec* v, uint32_t len);

 private:
  ::io::Sink* sink_;
  std::error_code ec_;
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

  void SendRespBlob(std::string_view str) {
    as_resp()->SendDirect(str);
  }

  void SendGetReply(std::string_view key, uint32_t flags, std::string_view value);
  void SendGetNotFound();

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
