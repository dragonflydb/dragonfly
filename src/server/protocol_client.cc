// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/protocol_client.h"

#include "facade/tls_helpers.h"

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
#include <string>

#include "base/logging.h"
#include "facade/dragonfly_connection.h"
#include "facade/redis_parser.h"
#include "facade/socket_utils.h"
#include "server/error.h"
#include "server/journal/executor.h"
#include "server/journal/serializer.h"
#include "server/main_service.h"
#include "server/rdb_load.h"
#include "strings/human_readable.h"
#include "util/fibers/dns_resolve.h"

#ifdef DFLY_USE_SSL
#include "util/tls/tls_socket.h"
#endif

ABSL_FLAG(std::string, masteruser, "", "username for authentication with master");
ABSL_FLAG(std::string, masterauth, "", "password for authentication with master");
ABSL_FLAG(bool, tls_replication, false, "Enable TLS on replication");

ABSL_DECLARE_FLAG(std::string, tls_cert_file);
ABSL_DECLARE_FLAG(std::string, tls_key_file);
ABSL_DECLARE_FLAG(std::string, tls_ca_cert_file);
ABSL_DECLARE_FLAG(std::string, tls_ca_cert_dir);
ABSL_DECLARE_FLAG(int32_t, port);
ABSL_DECLARE_FLAG(uint16_t, admin_port);

namespace dfly {

using namespace std;
using namespace util;
using namespace boost::asio;
using namespace facade;
using absl::GetFlag;
using absl::StrCat;

error_code ProtocolClient::Recv(FiberSocketBase* input, base::IoBuf* dest) {
  auto buf = dest->AppendBuffer();
  io::Result<size_t> exp_size = input->Recv(buf);
  if (!exp_size) {
    LOG(WARNING) << "Socket error " << exp_size.error();
    return exp_size.error();
  }

  if (*exp_size == 0) {
    VLOG(1) << "Connection closed by peer";
    return make_error_code(errc::connection_aborted);
  }

  TouchIoTime();

  dest->CommitWrite(*exp_size);
  return error_code{};
}

std::string ProtocolClient::ServerContext::Description() const {
  return absl::StrCat(host, ":", port);
}

void ValidateClientTlsFlags() {
  if (!GetFlag(FLAGS_tls_replication)) {
    return;
  }

  bool has_auth = false;

  if (!GetFlag(FLAGS_tls_key_file).empty()) {
    if (GetFlag(FLAGS_tls_cert_file).empty()) {
      LOG(ERROR) << "tls_cert_file flag should be set";
      exit(1);
    }
    has_auth = true;
  }

  if (!GetFlag(FLAGS_masterauth).empty())
    has_auth = true;

  if (!has_auth) {
    LOG(ERROR) << "No authentication method configured!";
    exit(1);
  }
}

void ProtocolClient::MaybeInitSslCtx() {
#ifdef DFLY_USE_SSL
  if (GetFlag(FLAGS_tls_replication)) {
    ssl_ctx_ = CreateSslCntx(facade::TlsContextRole::CLIENT);
  }
#endif
}

ProtocolClient::ProtocolClient(string host, uint16_t port) {
  server_context_.host = std::move(host);
  server_context_.port = port;
#ifdef DFLY_USE_SSL
  MaybeInitSslCtx();
#endif
}
ProtocolClient::ProtocolClient(ServerContext context) : server_context_(std::move(context)) {
#ifdef DFLY_USE_SSL
  MaybeInitSslCtx();
#endif
}

ProtocolClient::~ProtocolClient() {
  exec_st_.JoinErrorHandler();

#ifdef DFLY_USE_SSL
  if (ssl_ctx_) {
    SSL_CTX_free(ssl_ctx_);
  }
#endif
}

error_code ProtocolClient::ResolveHostDns() {
  char ip_addr[INET6_ADDRSTRLEN];

  // IPv6 address can be enclosed in square brackets.
  // https://www.rfc-editor.org/rfc/rfc2732#section-2
  // We need to remove the brackets before resolving the DNS.
  // Enclosed IPv6 addresses can't be resolved by the DNS resolver.
  std::string host = server_context_.host;
  if (!host.empty() && host.front() == '[' && host.back() == ']') {
    host = host.substr(1, host.size() - 2);
  }

  auto ec = util::fb2::DnsResolve(host, 0, ip_addr, ProactorBase::me());
  if (ec) {
    LOG(ERROR) << "Dns error " << ec << ", host: " << server_context_.host;
    return make_error_code(errc::host_unreachable);
  }

  LOG_IF(INFO, std::string(ip_addr) != server_context_.host)
      << "Resolved endpoint " << server_context_.Description() << " to " << ip_addr << ":"
      << server_context_.port;
  server_context_.endpoint = {ip::make_address(ip_addr), server_context_.port};

  return error_code{};
}

error_code ProtocolClient::ConnectAndAuth(std::chrono::milliseconds connect_timeout_ms,
                                          ExecutionState* cntx) {
  ProactorBase* mythread = ProactorBase::me();
  CHECK(mythread);
  {
    unique_lock lk(sock_mu_);
    // The context closes sock_. So if the context error handler has already
    // run we must not create a new socket. sock_mu_ syncs between the two
    // functions.
    if (cntx->IsRunning()) {
      if (sock_) {
        LOG_IF(WARNING, sock_->Close()) << "Error closing socket";
        sock_.reset(nullptr);
      }

      if (ssl_ctx_) {
        auto tls_sock = std::make_unique<tls::TlsSocket>(mythread->CreateSocket());
        tls_sock->InitSSL(ssl_ctx_);
        sock_ = std::move(tls_sock);
      } else {
        sock_.reset(mythread->CreateSocket());
      }
    } else {
      return cntx->GetError();
    }
  }

  // We set this timeout because this call blocks other REPLICAOF commands. We don't need it for the
  // rest of the sync.
  {
    uint32_t timeout = sock_->timeout();
    sock_->set_timeout(connect_timeout_ms.count());
    RETURN_ON_ERR(sock_->Connect(server_context_.endpoint));
    sock_->set_timeout(timeout);
  }

  // For idle connections we enable TCP keepalive to prevent disconnects.
  int yes = 1;
  if (setsockopt(sock_->native_handle(), SOL_SOCKET, SO_KEEPALIVE, &yes, sizeof(yes)) == 0) {
    int intv = 300;
#ifdef __APPLE__
    setsockopt(sock_->native_handle(), IPPROTO_TCP, TCP_KEEPALIVE, &intv, sizeof(intv));
#else
    setsockopt(sock_->native_handle(), IPPROTO_TCP, TCP_KEEPIDLE, &intv, sizeof(intv));
#endif

    intv /= 3;
    setsockopt(sock_->native_handle(), IPPROTO_TCP, TCP_KEEPINTVL, &intv, sizeof(intv));

    intv = 3;
    setsockopt(sock_->native_handle(), IPPROTO_TCP, TCP_KEEPCNT, &intv, sizeof(intv));
  }

  // CHECK_EQ(0, setsockopt(sock_->native_handle(), IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes)));

  auto masterauth = GetFlag(FLAGS_masterauth);
  auto masteruser = GetFlag(FLAGS_masteruser);
  ResetParser(RedisParser::Mode::CLIENT);
  if (!masterauth.empty()) {
    auto cmd = masteruser.empty() ? StrCat("AUTH ", masterauth)
                                  : StrCat("AUTH ", masteruser, " ", masterauth);
    RETURN_ON_ERR(SendCommandAndReadResponse(cmd));
    last_cmd_ = "AUTH";  // Make sure the password is not printed to logs
    PC_RETURN_ON_BAD_RESPONSE(CheckRespIsSimpleReply("OK"));
  }
  return error_code{};
}

void ProtocolClient::ShutdownSocketImpl(bool should_close) {
  unique_lock lk(sock_mu_);
  if (sock_) {
    sock_->proactor()->Await([this, should_close] {
      if (sock_->IsOpen()) {
        auto ec = sock_->Shutdown(SHUT_RDWR);
        LOG_IF(ERROR, ec) << "Could not shutdown socket " << ec;
      }
      if (should_close) {
        auto ec = sock_->Close();  // Quietly close.
        LOG_IF(WARNING, ec) << "Error closing socket " << ec << "/" << ec.message();
      }
    });
  }
}

void ProtocolClient::CloseSocket() {
  return ShutdownSocketImpl(true);
}

void ProtocolClient::ShutdownSocket() {
  return ShutdownSocketImpl(false);
}

void ProtocolClient::DefaultErrorHandler(const GenericError& err) {
  LOG(WARNING) << "Socket error: " << err.Format() << " in " << server_context_.Description()
               << ", socket info: " << GetSocketInfo(sock_ ? sock_->native_handle() : -1);
  ShutdownSocket();
}

io::Result<ProtocolClient::ReadRespRes> ProtocolClient::ReadRespReply(base::IoBuf* buffer,
                                                                      bool copy_msg) {
  DCHECK(parser_);

  error_code ec;
  if (!buffer) {
    buffer = &resp_buf_;
    buffer->Clear();
  }
  last_resp_ = "";

  uint32_t processed_bytes = 0;

  RedisParser::Result result = RedisParser::OK;
  while (!ec) {
    uint32_t consumed;
    if (buffer->InputLen() == 0 || result == RedisParser::INPUT_PENDING) {
      DCHECK_GT(buffer->AppendLen(), 0u);

      ec = Recv(sock_.get(), buffer);
      if (ec) {
        return nonstd::make_unexpected(ec);
      }
    }

    result = parser_->Parse(buffer->InputBuffer(), &consumed, &resp_args_);
    processed_bytes += consumed;
    if (copy_msg)
      last_resp_ +=
          std::string_view(reinterpret_cast<char*>(buffer->InputBuffer().data()), consumed);

    if (result == RedisParser::OK) {
      return ReadRespRes{processed_bytes, consumed};  // success path
    }

    buffer->ConsumeInput(consumed);

    if (result != RedisParser::INPUT_PENDING) {
      LOG(ERROR) << "Invalid parser status " << result << " for response " << last_resp_;
      return nonstd::make_unexpected(std::make_error_code(std::errc::bad_message));
    }

    // We need to read more data. Check that we have enough space.
    if (buffer->AppendLen() < 64u) {
      buffer->EnsureCapacity(buffer->Capacity() * 2);
    }
  }

  return nonstd::make_unexpected(ec);
}

io::Result<ProtocolClient::ReadRespRes> ProtocolClient::ReadRespReply(uint32_t timeout) {
  auto prev_timeout = sock_->timeout();
  sock_->set_timeout(timeout);
  auto res = ReadRespReply();
  sock_->set_timeout(prev_timeout);
  return res;
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

bool ProtocolClient::CheckRespSimpleError(string_view error) const {
  return resp_args_.size() == 1 && resp_args_.front().type == RespExpr::ERROR &&
         ToSV(resp_args_.front().GetBuf()) == error;
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
  string formatted_command = RedisReplyBuilderBase::SerializeCommand(command);
  DCHECK(sock_->proactor() == ProactorBase::me());
  auto ec = sock_->Write(io::Buffer(formatted_command));
  if (!ec)
    TouchIoTime();
  return ec;
}

error_code ProtocolClient::SendCommandAndReadResponse(string_view command) {
  last_cmd_ = command;
  if (auto ec = SendCommand(command); ec)
    return ec;
  auto response_res = ReadRespReply();
  return response_res.has_value() ? error_code{} : response_res.error();
}

void ProtocolClient::ResetParser(RedisParser::Mode mode) {
  // We accept any length for the parser because it has been approved by the master.
  parser_.reset(new RedisParser(mode));
}

uint64_t ProtocolClient::LastIoTime() const {
  return last_io_time_.load(std::memory_order_relaxed);
}

void ProtocolClient::TouchIoTime() {
  last_io_time_.store(Proactor()->GetMonotonicTimeNs(), std::memory_order_relaxed);
}

bool ProtocolClient::IsConnectedToItself() const {
  if (!(sock_ && sock_->IsOpen())) {
    return false;
  }

  const int fd = sock_->native_handle();

  struct sockaddr_storage addr;
  socklen_t len = sizeof(addr);

  if (int res = getsockname(fd, (struct sockaddr*)&addr, &len); res != 0) {
    // reject it
    LOG(WARNING) << "getsockname failed " << res;
    return true;
  }

  auto get_addr = [](const sockaddr_storage* addr, socklen_t len) {
    char ipstr[INET6_ADDRSTRLEN];
    if (addr->ss_family == AF_INET) {
      struct sockaddr_in* s = (struct sockaddr_in*)addr;
      inet_ntop(AF_INET, &s->sin_addr, ipstr, sizeof(ipstr));
      return std::make_pair(std::string{ipstr}, ntohs(s->sin_port));
    }
    // addr->ss_family == AF_INET6
    struct sockaddr_in6* s = (struct sockaddr_in6*)addr;
    return std::make_pair(std::string{ipstr}, ntohs(s->sin6_port));
  };

  // port won't work with get_addr
  auto [source, _] = get_addr(&addr, len);
  size_t main_port = absl::GetFlag(FLAGS_port);
  size_t admin_port = absl::GetFlag(FLAGS_admin_port);

  struct sockaddr_storage peer_addr;
  socklen_t peer_len = sizeof(peer_addr);

  if (int res = getpeername(fd, (struct sockaddr*)&peer_addr, &peer_len); res != 0) {
    LOG(WARNING) << "getsockname failed " << res;
    return true;
  }

  auto [peer, peer_port] = get_addr(&peer_addr, peer_len);
  return source == peer && (peer_port == main_port || peer_port == admin_port);
}

}  // namespace dfly
