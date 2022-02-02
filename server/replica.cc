// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/replica.h"

extern "C" {
#include "redis/rdb.h"
}

#include <absl/strings/escaping.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/strip.h>

#include <boost/asio/ip/tcp.hpp>

#include "base/logging.h"
#include "server/error.h"
#include "server/main_service.h"
#include "server/rdb_load.h"
#include "server/redis_parser.h"
#include "util/proactor_base.h"

namespace dfly {

using namespace std;
using namespace util;
using namespace boost::asio;
namespace this_fiber = ::boost::this_fiber;

namespace {

// TODO: 2. Use time-out on socket-reads so that we would not deadlock on unresponsive master.
//       3. Support ipv6 at some point.
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

  static_assert(INET_ADDRSTRLEN < INET6_ADDRSTRLEN, "");

  res = EAI_FAMILY;
  for (addrinfo* p = servinfo; p != NULL; p = p->ai_next) {
    if (p->ai_family == AF_INET) {
      struct sockaddr_in* ipv4 = (struct sockaddr_in*)p->ai_addr;
      const char* inet_res = inet_ntop(p->ai_family, &ipv4->sin_addr, dest, INET6_ADDRSTRLEN);
      CHECK_NOTNULL(inet_res);
      res = 0;
      break;
    }
    LOG(WARNING) << "Only IPv4 is supported";
  }

  freeaddrinfo(servinfo);

  return res;
}

error_code Recv(FiberSocketBase* input, base::IoBuf* dest) {
  auto buf = dest->AppendBuffer();
  io::Result<size_t> exp_size = input->Recv(buf);
  if (!exp_size)
    return exp_size.error();

  dest->CommitWrite(*exp_size);

  return error_code{};
}


// TODO: to remove usages of this macro and make code crash-less.
#define CHECK_EC(x)                                                                 \
  do {                                                                              \
    auto __ec$ = (x);                                                               \
    CHECK(!__ec$) << "Error: " << __ec$ << " " << __ec$.message() << " for " << #x; \
  } while (false)

}  // namespace

Replica::Replica(string host, uint16_t port, Service* se)
    : service_(*se), host_(std::move(host)), port_(port) {
}

Replica::~Replica() {
  if (sync_fb_.joinable())
    sync_fb_.join();

  if (sock_) {
    auto ec = sock_->Close();
    LOG_IF(ERROR, ec) << "Error closing replica socket " << ec;
  }
}

static const char kConnErr[] = "could not connect to master: ";

bool Replica::Run(ConnectionContext* cntx) {
  CHECK(!sock_ && !sock_thread_);

  sock_thread_ = ProactorBase::me();
  CHECK(sock_thread_);

  error_code ec = ConnectSocket();
  if (ec) {
    (*cntx)->SendError(absl::StrCat(kConnErr, ec.message()));
    return false;
  }

  state_mask_ = R_ENABLED | R_TCP_CONNECTED;
  last_io_time_ = sock_thread_->GetMonotonicTimeNs();
  sync_fb_ = ::boost::fibers::fiber(&Replica::ConnectFb, this);
  (*cntx)->SendOk();

  return true;
}

std::error_code Replica::ConnectSocket() {
  sock_.reset(sock_thread_->CreateSocket());

  char ip_addr[INET6_ADDRSTRLEN];
  int resolve_res = ResolveDns(host_, ip_addr);
  if (resolve_res != 0) {
    LOG(ERROR) << "Dns error " << gai_strerror(resolve_res);
    return make_error_code(errc::host_unreachable);
  }
  auto address = ip::make_address(ip_addr);
  ip::tcp::endpoint ep{address, port_};

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

  return sock_->Connect(ep);
}

void Replica::Stop() {
  if (sock_thread_) {
    sock_thread_->Await([this] {
      state_mask_ = 0;  // Specifically ~R_ENABLED.
      auto ec = sock_->Shutdown(SHUT_RDWR);
      LOG_IF(ERROR, ec) << "Could not shutdown socket " << ec;
    });
  }
  if (sync_fb_.joinable())
    sync_fb_.join();
}

void Replica::ConnectFb() {
  error_code ec;
  constexpr unsigned kOnline = R_SYNC_OK | R_TCP_CONNECTED;

  while (state_mask_ & R_ENABLED) {
    if ((state_mask_ & R_TCP_CONNECTED) == 0) {
      this_fiber::sleep_for(500ms);
      ec = ConnectSocket();
      if (ec) {
        LOG(ERROR) << "Error connecting " << ec;
        continue;
      }
      VLOG(1) << "Replica socket connected";
      state_mask_ |= R_TCP_CONNECTED;
    }

    if ((state_mask_ & kOnline) == R_TCP_CONNECTED) {  // lacks great_ok
      ec = GreatAndSync();
      if (ec) {
        LOG(INFO) << "Error greating " << ec;
        state_mask_ &= ~kOnline;
        continue;
      }
      VLOG(1) << "Replica great ok";
    }

    if ((state_mask_ & kOnline) == kOnline) {
      // There is a data race condition in Redis-master code, where "ACK 0" handler may be
      // triggerred
      // before Redis is ready to transition to the streaming state and it silenty ignores "ACK
      // 0". We reduce the chance it happens with this delay.
      this_fiber::sleep_for(50ms);
      ec = ConsumeRedisStream();

      LOG_IF(ERROR, !FiberSocketBase::IsConnClosed(ec)) << "Replica socket error " << ec;
      state_mask_ &= ~kOnline;
    }
  }

  VLOG(1) << "Replication fiber finished";
}

error_code Replica::GreatAndSync() {
  base::IoBuf io_buf{128};

  ReqSerializer serializer{sock_.get()};

  RedisParser parser{false};  // client mode
  RespVec args;

  serializer.SendCommand("PING");  // optional.
  RETURN_ON_ERR(serializer.ec());
  RETURN_ON_ERR(Recv(sock_.get(), &io_buf));
  last_io_time_ = sock_thread_->GetMonotonicTimeNs();

  uint32_t consumed = 0;
  RedisParser::Result result = parser.Parse(io_buf.InputBuffer(), &consumed, &args);
  CHECK_EQ(result, RedisParser::OK);
  CHECK(!args.empty() && args.front().type == RespExpr::STRING);

  // TODO: to check nauth, permission denied etc responses.
  VLOG(1) << "Master ping reply " << ToSV(args.front().GetBuf());

  io_buf.ConsumeInput(consumed);

  // TODO: we may also send REPLCONF listening-port, ip-address
  serializer.SendCommand("REPLCONF capa eof capa psync2");
  RETURN_ON_ERR(serializer.ec());
  RETURN_ON_ERR(Recv(sock_.get(), &io_buf));

  result = parser.Parse(io_buf.InputBuffer(), &consumed, &args);
  CHECK_EQ(result, RedisParser::OK);
  CHECK(!args.empty() && args.front().type == RespExpr::STRING);

  VLOG(1) << "Master REPLCONF reply " << ToSV(args.front().GetBuf());
  io_buf.ConsumeInput(consumed);

  // Announce that we are the dragonfly client.
  // Note that we currently do not support dragonfly->redis replication.
  //
  serializer.SendCommand("REPLCONF capa dragonfly");
  RETURN_ON_ERR(serializer.ec());
  RETURN_ON_ERR(Recv(sock_.get(), &io_buf));
  result = parser.Parse(io_buf.InputBuffer(), &consumed, &args);
  CHECK_EQ(result, RedisParser::OK);
  CHECK(!args.empty());
  CHECK_EQ(RespExpr::STRING, args[0].type);
  last_io_time_ = sock_thread_->GetMonotonicTimeNs();

  if (args.size() == 1) {
    CHECK_EQ("OK", ToSV(args[0].GetBuf()));
  } else {
    LOG(FATAL) << "Bad response " << args;
  }
  io_buf.ConsumeInput(consumed);

  // Start full sync
  state_mask_ |= R_SYNCING;
  serializer.SendCommand("PSYNC ? -1");
  RETURN_ON_ERR(serializer.ec());

  DCHECK_EQ(0u, io_buf.InputLen());

  return make_error_code(errc::io_error);
}

error_code Replica::ConsumeRedisStream() {
  base::IoBuf io_buf(16_KB);
  parser_.reset(new RedisParser);

  ReqSerializer serializer{sock_.get()};

  // Master waits for this command in order to start sending replication stream.
  serializer.SendCommand("REPLCONF ACK 0");
  CHECK_EC(serializer.ec());

  VLOG(1) << "Before reading repl-log";

  // Redis sends eiher pings every "repl_ping_slave_period" time inside replicationCron().
  // or, alternatively, write commands stream coming from propagate() function.
  // Replica connection must send "REPLCONF ACK xxx" in order to make sure that master replication
  // buffer gets disposed of already processed commands.
  error_code ec;
  time_t last_ack = time(nullptr);
  string ack_cmd;

  while (!ec) {
    io::MutableBytes buf = io_buf.AppendBuffer();
    io::Result<size_t> size_res = sock_->Recv(buf);
    if (!size_res)
      return size_res.error();

    VLOG(1) << "Read replication stream of " << *size_res << " bytes";
    last_io_time_ = sock_thread_->GetMonotonicTimeNs();

    io_buf.CommitWrite(*size_res);
    repl_offs_ += *size_res;

    // Send repl ack back to master.
    if (repl_offs_ > ack_offs_ + 1024 || time(nullptr) > last_ack + 5) {
      ack_cmd.clear();
      absl::StrAppend(&ack_cmd, "REPLCONF ACK ", repl_offs_);
      serializer.SendCommand(ack_cmd);
      CHECK_EC(serializer.ec());
    }

    ec = ParseAndExecute(&io_buf);
  }

  VLOG(1) << "ConsumeRedisStream finished";
  return ec;
}

// Threadsafe, fiber blocking.
auto Replica::GetInfo() const -> Info {
  CHECK(sock_thread_);
  return sock_thread_->AwaitBrief([this] {
    Info res;
    res.host = host_;
    res.port = port_;
    res.master_link_established = (state_mask_ & R_TCP_CONNECTED);
    res.sync_in_progress = (state_mask_ & R_SYNCING);
    res.master_last_io_sec = (ProactorBase::GetMonotonicTimeNs() - last_io_time_) / 1000000000UL;
    return res;
  });
}

error_code Replica::ParseAndExecute(base::IoBuf* io_buf) {
  VLOG(1) << "ParseAndExecute: input len " << io_buf->InputLen();
  if (parser_->stash_size() > 0) {
    DVLOG(1) << "Stash " << *parser_->stash()[0];
  }

  uint32_t consumed = 0;
  RedisParser::Result result = RedisParser::OK;
  RespVec cmd_args;

  do {
    result = parser_->Parse(io_buf->InputBuffer(), &consumed, &cmd_args);

    switch (result) {
      case RedisParser::OK:
      case RedisParser::INPUT_PENDING:
        io_buf->ConsumeInput(consumed);
        break;
      default:
        LOG(ERROR) << "Invalid parser status " << result << " for buffer of size "
                   << io_buf->InputLen();
        return std::make_error_code(std::errc::bad_message);
    }
  } while (io_buf->InputLen() > 0 && result == RedisParser::OK);
  VLOG(1) << "ParseAndExecute: " << io_buf->InputLen() << " " << ToSV(io_buf->InputBuffer());

  return error_code{};
}

}  // namespace dfly
