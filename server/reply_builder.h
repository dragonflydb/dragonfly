// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <string_view>
#include <optional>

#include "io/sync_stream_interface.h"

#include "server/op_status.h"

namespace dfly {

class RespSerializer {
 public:
  explicit RespSerializer(::io::Sink* sink);

  std::error_code ec() const {
    return ec_;
  }

  void CloseConnection() {
    if (!ec_)
      ec_ = std::make_error_code(std::errc::connection_aborted);
  }

  //! Sends a string as is without any formatting. raw should be RESP-encoded.
  void SendDirect(std::string_view str);

  ::io::Sink* sink() { return sink_; }

 protected:
  void Send(const iovec* v, uint32_t len);

  ::io::Sink* sink_;

 private:
  std::error_code ec_;
};

class ReplyBuilder : public RespSerializer {
 public:
  explicit ReplyBuilder(::io::Sink* stream) : RespSerializer(stream) {
  }

  /// aka "$6\r\nfoobar\r\n"
  void SendBulkString(std::string_view str);

  void SendNull();

  void SendOk() {
    return SendSimpleString("OK");
  }

  void SendError(std::string_view str);
  void SendError(OpStatus status);

  //! See https://redis.io/topics/protocol
  void SendSimpleString(std::string_view str);

private:
};

}  // namespace dfly
