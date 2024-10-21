// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/protocol_client.h"

#include "facade/tls_error.h"

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

namespace dfly {

using namespace std;
using namespace util;
using namespace boost::asio;
using namespace facade;
using absl::GetFlag;
using absl::StrCat;

namespace {

#ifdef DFLY_USE_SSL

static ProtocolClient::SSL_CTX* CreateSslClientCntx() {
  ProtocolClient::SSL_CTX* ctx = SSL_CTX_new(TLS_client_method());
  const auto& tls_key_file = GetFlag(FLAGS_tls_key_file);
  unsigned mask = SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT;

  // Load client certificate if given.
  if (!tls_key_file.empty()) {
    DFLY_SSL_CHECK(1 == SSL_CTX_use_PrivateKey_file(ctx, tls_key_file.c_str(), SSL_FILETYPE_PEM));
    // We checked that the flag is non empty in ValidateClientTlsFlags.
    const auto& tls_cert_file = GetFlag(FLAGS_tls_cert_file);

    DFLY_SSL_CHECK(1 == SSL_CTX_use_certificate_chain_file(ctx, tls_cert_file.c_str()));
  }

  // Load custom certificate validation if given.
  const auto& tls_ca_cert_file = GetFlag(FLAGS_tls_ca_cert_file);
  const auto& tls_ca_cert_dir = GetFlag(FLAGS_tls_ca_cert_dir);

  const auto* file = tls_ca_cert_file.empty() ? nullptr : tls_ca_cert_file.data();
  const auto* dir = tls_ca_cert_dir.empty() ? nullptr : tls_ca_cert_dir.data();
  if (file || dir) {
    DFLY_SSL_CHECK(1 == SSL_CTX_load_verify_locations(ctx, file, dir));
  } else {
    DFLY_SSL_CHECK(1 == SSL_CTX_set_default_verify_paths(ctx));
  }

  DFLY_SSL_CHECK(1 == SSL_CTX_set_cipher_list(ctx, "DEFAULT"));
  SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);

  SSL_CTX_set_options(ctx, SSL_OP_DONT_INSERT_EMPTY_FRAGMENTS);

  SSL_CTX_set_verify(ctx, mask, NULL);

  DFLY_SSL_CHECK(1 == SSL_CTX_set_dh_auto(ctx, 1));
  return ctx;
}
#endif

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

void ValidateClientTlsFlags() {
  if (!absl::GetFlag(FLAGS_tls_replication)) {
    return;
  }

  bool has_auth = false;

  if (!absl::GetFlag(FLAGS_tls_key_file).empty()) {
    if (absl::GetFlag(FLAGS_tls_cert_file).empty()) {
      LOG(ERROR) << "tls_cert_file flag should be set";
      exit(1);
    }
    has_auth = true;
  }

  if (!absl::GetFlag(FLAGS_masterauth).empty())
    has_auth = true;

  if (!has_auth) {
    LOG(ERROR) << "No authentication method configured!";
    exit(1);
  }
}

void ProtocolClient::MaybeInitSslCtx() {
  if (absl::GetFlag(FLAGS_tls_replication)) {
    ssl_ctx_ = CreateSslClientCntx();
  }
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
  cntx_.JoinErrorHandler();

  // FIXME: We should close the socket explictly outside of the destructor. This currently
  // breaks test_cancel_replication_immediately.
  if (sock_) {
    std::error_code ec;
    sock_->proactor()->Await([this, &ec]() { ec = sock_->Close(); });
    LOG_IF(ERROR, ec) << "Error closing socket " << ec;
  }
#ifdef DFLY_USE_SSL
  if (ssl_ctx_) {
    SSL_CTX_free(ssl_ctx_);
  }
#endif
}

error_code ProtocolClient::ResolveHostDns() {
  char ip_addr[INET6_ADDRSTRLEN];
  auto ec = util::fb2::DnsResolve(server_context_.host, 0, ip_addr, ProactorBase::me());
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
                                          Context* cntx) {
  ProactorBase* mythread = ProactorBase::me();
  CHECK(mythread);
  {
    unique_lock lk(sock_mu_);
    // The context closes sock_. So if the context error handler has already
    // run we must not create a new socket. sock_mu_ syncs between the two
    // functions.
    if (!cntx->IsCancelled()) {
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
  auto masteruser = absl::GetFlag(FLAGS_masteruser);
  ResetParser(false);
  if (!masterauth.empty()) {
    auto cmd = masteruser.empty() ? StrCat("AUTH ", masterauth)
                                  : StrCat("AUTH ", masteruser, " ", masterauth);
    RETURN_ON_ERR(SendCommandAndReadResponse(cmd));
    last_cmd_ = "AUTH";  // Make sure the password is not printed to logs
    PC_RETURN_ON_BAD_RESPONSE(CheckRespIsSimpleReply("OK"));
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
      io::MutableBytes buf = buffer->AppendBuffer();
      io::Result<size_t> size_res = sock_->Recv(buf);
      if (!size_res) {
        LOG(ERROR) << "Socket error " << size_res.error();
        return nonstd::make_unexpected(size_res.error());
      }

      VLOG(2) << "Read master response of " << *size_res << " bytes";

      TouchIoTime();
      buffer->CommitWrite(*size_res);
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

void ProtocolClient::ResetParser(bool server_mode) {
  // We accept any length for the parser because it has been approved by the master.
  parser_.reset(new RedisParser(UINT32_MAX, server_mode));
}

uint64_t ProtocolClient::LastIoTime() const {
  return last_io_time_;
}

void ProtocolClient::TouchIoTime() {
  last_io_time_ = Proactor()->GetMonotonicTimeNs();
}

}  // namespace dfly
