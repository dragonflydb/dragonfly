// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/protocol_client.h"

extern "C" {
#include "redis/rdb.h"
}

#include <absl/cleanup/cleanup.h>
#include <absl/flags/flag.h>
#include <absl/functional/bind_front.h>
#include <absl/strings/escaping.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/strip.h>

#include <boost/asio/ip/tcp.hpp>

#include "base/logging.h"
#include "facade/dragonfly_connection.h"
#include "facade/redis_parser.h"
#include "server/error.h"
#include "server/journal/executor.h"
#include "server/journal/serializer.h"
#include "server/main_service.h"
#include "server/rdb_load.h"
#include "strings/human_readable.h"

ABSL_FLAG(std::string, masterauth, "", "password for authentication with master");

#define RETURN_ON_BAD_RESPONSE(x)                                                               \
  do {                                                                                          \
    if (!(x)) {                                                                                 \
      LOG(ERROR) << "Bad response to \"" << last_cmd_ << "\": \"" << absl::CEscape(last_resp_); \
      return std::make_error_code(errc::bad_message);                                           \
    }                                                                                           \
  } while (false)

namespace dfly {

using namespace std;
using namespace util;
using namespace boost::asio;
using namespace facade;
using absl::GetFlag;
using absl::StrCat;

namespace {

int ResolveDns(std::string_view host, char* dest) {
  struct addrinfo hints, *servinfo;

  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;
  hints.ai_flags = AI_ALL;

  int res = getaddrinfo(host.data(), NULL, &hints, &servinfo);
  if (res != 0)
    return res;

  static_assert(INET_ADDRSTRLEN < INET6_ADDRSTRLEN);

  // If possible, we want to use an IPv4 address.
  char ipv4_addr[INET6_ADDRSTRLEN];
  bool found_ipv4 = false;
  char ipv6_addr[INET6_ADDRSTRLEN];
  bool found_ipv6 = false;

  for (addrinfo* p = servinfo; p != NULL; p = p->ai_next) {
    CHECK(p->ai_family == AF_INET || p->ai_family == AF_INET6);
    if (p->ai_family == AF_INET && !found_ipv4) {
      struct sockaddr_in* ipv4 = (struct sockaddr_in*)p->ai_addr;
      CHECK(nullptr !=
            inet_ntop(p->ai_family, (void*)&ipv4->sin_addr, ipv4_addr, INET6_ADDRSTRLEN));
      found_ipv4 = true;
      break;
    } else if (!found_ipv6) {
      struct sockaddr_in6* ipv6 = (struct sockaddr_in6*)p->ai_addr;
      CHECK(nullptr !=
            inet_ntop(p->ai_family, (void*)&ipv6->sin6_addr, ipv6_addr, INET6_ADDRSTRLEN));
      found_ipv6 = true;
    }
  }

  CHECK(found_ipv4 || found_ipv6);
  memcpy(dest, found_ipv4 ? ipv4_addr : ipv6_addr, INET6_ADDRSTRLEN);

  freeaddrinfo(servinfo);

  return 0;
}

error_code Recv(FiberSocketBase* input, base::IoBuf* dest) {
  auto buf = dest->AppendBuffer();
  io::Result<size_t> exp_size = input->Recv(buf);
  if (!exp_size)
    return exp_size.error();

  dest->CommitWrite(*exp_size);

  return error_code{};
}

}  // namespace

std::string ProtocolClient::ServerContext::Description() const {
  return absl::StrCat(host, ":", port);
}

ProtocolClient::ProtocolClient(string host, uint16_t port) {
  server_context_.host = std::move(host);
  server_context_.port = port;
}

ProtocolClient::~ProtocolClient() {
  if (sock_) {
    auto ec = sock_->Close();
    LOG_IF(ERROR, ec) << "Error closing socket " << ec;
  }
}

error_code ProtocolClient::ResolveMasterDns() {
  char ip_addr[INET6_ADDRSTRLEN];
  int resolve_res = ResolveDns(server_context_.host, ip_addr);
  if (resolve_res != 0) {
    LOG(ERROR) << "Dns error " << gai_strerror(resolve_res) << ", host: " << server_context_.host;
    return make_error_code(errc::host_unreachable);
  }
  LOG(INFO) << "Resetting endpoint! " << ip_addr << ", " << server_context_.port;
  server_context_.endpoint = {ip::make_address(ip_addr), server_context_.port};

  return error_code{};
}

error_code ProtocolClient::ConnectAndAuth(std::chrono::milliseconds connect_timeout_ms,
                                          Context* cntx) {
  ProactorBase* mythread = ProactorBase::me();
  CHECK(mythread);
  {
    unique_lock lk(sock_mu_);
    // The context closes sock_. So if the context error handler has already
    // run we must not create a new socket. sock_mu_ syncs between the two
    // functions.
    if (!cntx->IsCancelled()) {
      sock_.reset(mythread->CreateSocket());
      serializer_.reset(new ReqSerializer(sock_.get()));
    } else {
      return cntx->GetError();
    }
  }

  // We set this timeout because this call blocks other REPLICAOF commands. We don't need it for the
  // rest of the sync.
  {
    uint32_t timeout = sock_->timeout();
    sock_->set_timeout(connect_timeout_ms.count());
    LOG(WARNING) << server_context_.Description();
    RETURN_ON_ERR(sock_->Connect(server_context_.endpoint));
    sock_->set_timeout(timeout);
  }

  /* These may help but require additional field testing to learn.
   int yes = 1;
   CHECK_EQ(0, setsockopt(sock_->native_handle(), IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes)));
   CHECK_EQ(0, setsockopt(sock_->native_handle(), SOL_SOCKET, SO_KEEPALIVE, &yes, sizeof(yes)));

   int intv = 15;
   CHECK_EQ(0, setsockopt(sock_->native_handle(), IPPROTO_TCP, TCP_KEEPIDLE, &intv, sizeof(intv)));

   intv /= 3;
   CHECK_EQ(0, setsockopt(sock_->native_handle(), IPPROTO_TCP, TCP_KEEPINTVL, &intv, sizeof(intv)));

   intv = 3;
   CHECK_EQ(0, setsockopt(sock_->native_handle(), IPPROTO_TCP, TCP_KEEPCNT, &intv, sizeof(intv)));
  */
  auto masterauth = absl::GetFlag(FLAGS_masterauth);
  if (!masterauth.empty()) {
    ResetParser(false);
    RETURN_ON_ERR(SendCommandAndReadResponse(StrCat("AUTH ", masterauth)));
    RETURN_ON_BAD_RESPONSE(CheckRespIsSimpleReply("OK"));
  }
  return error_code{};
}

void ProtocolClient::CloseSocket() {
  unique_lock lk(sock_mu_);
  if (sock_) {
    sock_->proactor()->Await([this] {
      if (sock_->IsOpen()) {
        auto ec = sock_->Shutdown(SHUT_RDWR);
        LOG_IF(ERROR, ec) << "Could not shutdown socket " << ec;
      }
    });
  }
}

void ProtocolClient::DefaultErrorHandler(const GenericError& err) {
  CloseSocket();
}

io::Result<size_t> ProtocolClient::ReadRespReply(base::IoBuf* buffer, bool copy_msg) {
  DCHECK(parser_);

  error_code ec;
  if (!buffer) {
    buffer = &resp_buf_;
    buffer->Clear();
  }
  last_resp_ = "";

  size_t processed_bytes = 0;

  RedisParser::Result result = RedisParser::OK;
  while (!ec) {
    uint32_t consumed;
    if (buffer->InputLen() == 0 || result == RedisParser::INPUT_PENDING) {
      io::MutableBytes buf = buffer->AppendBuffer();
      io::Result<size_t> size_res = sock_->Recv(buf);
      if (!size_res) {
        LOG(ERROR) << "Socket error " << size_res.error();
        return nonstd::make_unexpected(size_res.error());
      }

      VLOG(2) << "Read master response of " << *size_res << " bytes";

      UpdateIoTime();
      buffer->CommitWrite(*size_res);
    }

    result = parser_->Parse(buffer->InputBuffer(), &consumed, &resp_args_);
    if (copy_msg)
      last_resp_ += std::string_view((char*)buffer->InputBuffer().data(), consumed);
    buffer->ConsumeInput(consumed);
    processed_bytes += consumed;

    if (result == RedisParser::OK && !resp_args_.empty()) {
      return processed_bytes;  // success path
    }

    if (result != RedisParser::INPUT_PENDING) {
      LOG(ERROR) << "Invalid parser status " << result << " for response " << last_resp_;
      return nonstd::make_unexpected(std::make_error_code(std::errc::bad_message));
    }
  }

  return nonstd::make_unexpected(ec);
}

error_code ProtocolClient::ReadLine(base::IoBuf* io_buf, string_view* line) {
  size_t eol_pos;
  std::string_view input_str = ToSV(io_buf->InputBuffer());

  // consume whitespace.
  while (true) {
    auto it = find_if_not(input_str.begin(), input_str.end(), absl::ascii_isspace);
    size_t ws_len = it - input_str.begin();
    io_buf->ConsumeInput(ws_len);
    input_str = ToSV(io_buf->InputBuffer());
    if (!input_str.empty())
      break;
    RETURN_ON_ERR(Recv(sock_.get(), io_buf));
    input_str = ToSV(io_buf->InputBuffer());
  };

  // find eol.
  while (true) {
    eol_pos = input_str.find('\n');

    if (eol_pos != std::string_view::npos) {
      DCHECK_GT(eol_pos, 0u);  // can not be 0 because then would be consumed as a whitespace.
      if (input_str[eol_pos - 1] != '\r') {
        break;
      }
      *line = input_str.substr(0, eol_pos - 1);
      return error_code{};
    }

    RETURN_ON_ERR(Recv(sock_.get(), io_buf));
    input_str = ToSV(io_buf->InputBuffer());
  }

  LOG(ERROR) << "Bad replication header: " << input_str;
  return std::make_error_code(std::errc::illegal_byte_sequence);
}

bool ProtocolClient::CheckRespIsSimpleReply(string_view reply) const {
  return resp_args_.size() == 1 && resp_args_.front().type == RespExpr::STRING &&
         ToSV(resp_args_.front().GetBuf()) == reply;
}

bool ProtocolClient::CheckRespFirstTypes(initializer_list<RespExpr::Type> types) const {
  unsigned i = 0;
  for (RespExpr::Type type : types) {
    if (i >= resp_args_.size() || resp_args_[i].type != type)
      return false;
    ++i;
  }
  return true;
}

error_code ProtocolClient::SendCommand(string_view command) {
  serializer_->SendCommand(command);
  error_code ec = serializer_->ec();
  if (!ec) {
    UpdateIoTime();
  }
  return ec;
}

error_code ProtocolClient::SendCommandAndReadResponse(string_view command, base::IoBuf* buffer) {
  last_cmd_ = command;
  if (auto ec = SendCommand(command); ec)
    return ec;
  auto response_res = ReadRespReply(buffer);
  return response_res.has_value() ? error_code{} : response_res.error();
}

void ProtocolClient::ResetParser(bool server_mode) {
  parser_.reset(new RedisParser(server_mode));
}

uint64_t ProtocolClient::LastIoTime() const {
  return last_io_time_;
}

void ProtocolClient::UpdateIoTime() {
  last_io_time_ = Proactor()->GetMonotonicTimeNs();
}


}  // namespace dfly
