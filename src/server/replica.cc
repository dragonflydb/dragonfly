// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/replica.h"

extern "C" {
#include "redis/rdb.h"
}

#include <absl/functional/bind_front.h>
#include <absl/strings/escaping.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/strip.h>

#include <boost/asio/ip/tcp.hpp>

#include "base/flags.h"
#include "base/logging.h"
#include "facade/dragonfly_connection.h"
#include "facade/redis_parser.h"
#include "server/error.h"
#include "server/journal/executor.h"
#include "server/journal/serializer.h"
#include "server/main_service.h"
#include "server/rdb_load.h"
#include "util/proactor_base.h"

namespace dfly {

using namespace std;
using namespace util;
using namespace boost::asio;
using namespace facade;
using absl::StrCat;

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

constexpr unsigned kRdbEofMarkSize = 40;

// Distribute flow indices over all available threads (shard_set pool size).
vector<vector<unsigned>> Partition(unsigned num_flows) {
  vector<vector<unsigned>> partition(shard_set->pool()->size());
  for (unsigned i = 0; i < num_flows; ++i) {
    partition[i % partition.size()].push_back(i);
  }
  return partition;
}

}  // namespace

Replica::Replica(string host, uint16_t port, Service* se) : service_(*se) {
  master_context_.host = std::move(host);
  master_context_.port = port;
}

Replica::Replica(const MasterContext& context, uint32_t dfly_flow_id, Service* service)
    : service_(*service), master_context_(context) {
  master_context_.dfly_flow_id = dfly_flow_id;
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

bool Replica::Start(ConnectionContext* cntx) {
  CHECK(!sock_);

  ProactorBase* mythread = ProactorBase::me();
  CHECK(mythread);

  // 1. Connect socket.
  error_code ec = ConnectSocket();
  if (ec) {
    (*cntx)->SendError(StrCat(kConnErr, ec.message()));
    return false;
  }

  // 2. Greet.
  state_mask_ = R_ENABLED | R_TCP_CONNECTED;
  last_io_time_ = mythread->GetMonotonicTimeNs();
  ec = Greet();
  if (ec) {
    (*cntx)->SendError(StrCat("could not greet master ", ec.message()));
    return false;
  }

  // 3. Init basic context.
  cntx_.Reset(absl::bind_front(&Replica::DefaultErrorHandler, this));

  // 4. Spawn main coordination fiber.
  sync_fb_ = ::boost::fibers::fiber(&Replica::MainReplicationFb, this);

  (*cntx)->SendOk();
  return true;
}

void Replica::Stop() {
  // Mark disabled, prevent from retrying.
  if (sock_) {
    sock_->proactor()->Await([this] {
      state_mask_ = 0;  // Specifically ~R_ENABLED.
      cntx_.Cancel();   // Context is fully resposible for cleanup.
    });
  }

  // Make sure the replica fully stopped and did all cleanup,
  // so we can freely release resources (connections).
  sync_fb_.join();
}

void Replica::Pause(bool pause) {
  sock_->proactor()->Await([&] { is_paused_ = pause; });
}

void Replica::MainReplicationFb() {
  error_code ec;
  while (state_mask_ & R_ENABLED) {
    // Discard all previous errors and set default error handler.
    cntx_.Reset(absl::bind_front(&Replica::DefaultErrorHandler, this));

    // 1. Connect socket.
    if ((state_mask_ & R_TCP_CONNECTED) == 0) {
      fibers_ext::SleepFor(500ms);
      if (is_paused_)
        continue;

      ec = ConnectSocket();
      if (ec) {
        LOG(ERROR) << "Error connecting " << ec;
        continue;
      }
      VLOG(1) << "Replica socket connected";
      state_mask_ |= R_TCP_CONNECTED;
    }

    // 2. Greet.
    if ((state_mask_ & R_GREETED) == 0) {
      ec = Greet();
      if (ec) {
        LOG(INFO) << "Error greeting " << ec;
        state_mask_ &= ~R_TCP_CONNECTED;
        continue;
      }
    }

    // 3. Initiate full sync
    if ((state_mask_ & R_SYNC_OK) == 0) {
      // Make sure we're in LOADING state.
      if (service_.SwitchState(GlobalState::ACTIVE, GlobalState::LOADING) != GlobalState::LOADING) {
        state_mask_ = 0;
        continue;
      }

      if (HasDflyMaster())
        ec = InitiateDflySync();
      else
        ec = InitiatePSync();

      service_.SwitchState(GlobalState::LOADING, GlobalState::ACTIVE);

      if (ec) {
        LOG(WARNING) << "Error syncing " << ec << " " << ec.message();
        state_mask_ &= R_ENABLED;  // reset all flags besides R_ENABLED
        JoinAllFlows();
        continue;
      }

      state_mask_ |= R_SYNC_OK;
    }

    // 4. Start stable state sync.
    DCHECK(state_mask_ & R_SYNC_OK);

    if (HasDflyMaster())
      ec = ConsumeDflyStream();
    else
      ec = ConsumeRedisStream();

    JoinAllFlows();
    state_mask_ &= ~R_SYNC_OK;
  }

  VLOG(1) << "Main replication fiber finished";
}

error_code Replica::ConnectSocket() {
  sock_.reset(ProactorBase::me()->CreateSocket());

  char ip_addr[INET6_ADDRSTRLEN];
  int resolve_res = ResolveDns(master_context_.host, ip_addr);
  if (resolve_res != 0) {
    LOG(ERROR) << "Dns error " << gai_strerror(resolve_res) << ", host: " << master_context_.host;
    return make_error_code(errc::host_unreachable);
  }

  master_context_.endpoint = {ip::make_address(ip_addr), master_context_.port};

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

  return sock_->Connect(master_context_.endpoint);
}

error_code Replica::Greet() {
  parser_.reset(new RedisParser{false});
  base::IoBuf io_buf{128};
  ReqSerializer serializer{sock_.get()};
  uint32_t consumed = 0;

  // Corresponds to server.repl_state == REPL_STATE_CONNECTING state in redis
  RETURN_ON_ERR(SendCommand("PING", &serializer));  // optional.
  RETURN_ON_ERR(ReadRespReply(&io_buf, &consumed));
  if (!CheckRespIsSimpleReply("PONG")) {
    LOG(ERROR) << "Bad pong response " << ToSV(io_buf.InputBuffer());
    return make_error_code(errc::bad_message);
  }

  io_buf.ConsumeInput(consumed);

  // TODO: we may also send REPLCONF listening-port, ip-address
  // See server.repl_state == REPL_STATE_SEND_PORT condition in replication.c

  // Corresponds to server.repl_state == REPL_STATE_SEND_CAPA
  RETURN_ON_ERR(SendCommand("REPLCONF capa eof capa psync2", &serializer));
  RETURN_ON_ERR(ReadRespReply(&io_buf, &consumed));
  if (!CheckRespIsSimpleReply("OK")) {
    LOG(ERROR) << "Bad REPLCONF response " << ToSV(io_buf.InputBuffer());
    return make_error_code(errc::bad_message);
  }

  io_buf.ConsumeInput(consumed);

  // Announce that we are the dragonfly client.
  // Note that we currently do not support dragonfly->redis replication.
  RETURN_ON_ERR(SendCommand("REPLCONF capa dragonfly", &serializer));
  RETURN_ON_ERR(ReadRespReply(&io_buf, &consumed));
  if (!CheckRespFirstTypes({RespExpr::STRING})) {
    return make_error_code(errc::bad_message);
  }

  string_view cmd = ToSV(resp_args_[0].GetBuf());
  if (resp_args_.size() == 1) {  // Redis
    if (cmd != "OK") {
      LOG(ERROR) << "Unexpected response " << cmd;
      return make_error_code(errc::bad_message);
    }
  } else if (resp_args_.size() == 3) {  // it's dragonfly master.
    // Reponse is: <master_repl_id, syncid, num_shards>
    if (!CheckRespFirstTypes({RespExpr::STRING, RespExpr::STRING, RespExpr::INT64}) ||
        resp_args_[0].GetBuf().size() != CONFIG_RUN_ID_SIZE) {
      LOG(ERROR) << "Unexpected response " << ToSV(io_buf.InputBuffer());
      return make_error_code(errc::bad_message);
    }

    string_view param0 = ToSV(resp_args_[0].GetBuf());
    string_view param1 = ToSV(resp_args_[1].GetBuf());
    int64 param2 = get<int64_t>(resp_args_[2].u);

    if (param2 <= 0 || param2 > 1024) {
      // sanity check, we support upto 1024 shards.
      // It's not that we can not support more but it's probably highly unlikely that someone
      // will run dragonfly with more than 1024 cores.
      LOG(ERROR) << "Invalid flow count " << param2;
      return make_error_code(errc::bad_message);
    }

    master_context_.master_repl_id = param0;
    master_context_.dfly_session_id = param1;
    num_df_flows_ = param2;

    VLOG(1) << "Master id: " << param0 << ", sync id: " << param1 << ", num journals "
            << num_df_flows_;
  } else {
    LOG(ERROR) << "Bad response " << ToSV(io_buf.InputBuffer());

    return make_error_code(errc::bad_message);
  }

  io_buf.ConsumeInput(consumed);
  state_mask_ |= R_GREETED;

  return error_code{};
}

error_code Replica::InitiatePSync() {
  base::IoBuf io_buf{128};

  ReqSerializer serializer{sock_.get()};

  // Corresponds to server.repl_state == REPL_STATE_SEND_PSYNC
  string id("?");  // corresponds to null master id and null offset
  int64_t offs = -1;
  if (!master_context_.master_repl_id.empty()) {  // in case we synced before
    id = master_context_.master_repl_id;          // provide the replication offset and master id
    offs = repl_offs_;                            // to try incremental sync.
  }

  RETURN_ON_ERR(SendCommand(StrCat("PSYNC ", id, " ", offs), &serializer));

  // Master may delay sync response with "repl_diskless_sync_delay"
  PSyncResponse repl_header;

  RETURN_ON_ERR(ParseReplicationHeader(&io_buf, &repl_header));

  ProactorBase* sock_thread = sock_->proactor();
  string* token = absl::get_if<string>(&repl_header.fullsync);
  size_t snapshot_size = SIZE_MAX;
  if (!token) {
    snapshot_size = absl::get<size_t>(repl_header.fullsync);
  }
  last_io_time_ = sock_thread->GetMonotonicTimeNs();

  // we get token for diskless redis replication. For disk based replication
  // we get the snapshot size.
  if (snapshot_size || token != nullptr) {  // full sync
    // Start full sync
    state_mask_ |= R_SYNCING;

    SocketSource ss{sock_.get()};
    io::PrefixSource ps{io_buf.InputBuffer(), &ss};

    RdbLoader loader(NULL);
    loader.set_source_limit(snapshot_size);
    // TODO: to allow registering callbacks within loader to send '\n' pings back to master.
    // Also to allow updating last_io_time_.
    error_code ec = loader.Load(&ps);
    RETURN_ON_ERR(ec);
    VLOG(1) << "full sync completed";

    if (token) {
      uint8_t buf[kRdbEofMarkSize];
      io::PrefixSource chained(loader.Leftover(), &ps);
      VLOG(1) << "Before reading from chained stream";
      io::Result<size_t> eof_res = chained.Read(io::MutableBytes{buf});
      CHECK(eof_res && *eof_res == kRdbEofMarkSize);

      VLOG(1) << "Comparing token " << ToSV(buf);

      // TODO: handle gracefully...
      CHECK_EQ(0, memcmp(token->data(), buf, kRdbEofMarkSize));
      CHECK(chained.unused_prefix().empty());
    } else {
      CHECK_EQ(0u, loader.Leftover().size());
      CHECK_EQ(snapshot_size, loader.bytes_read());
    }

    CHECK(ps.unused_prefix().empty());
    io_buf.ConsumeInput(io_buf.InputLen());
    last_io_time_ = sock_thread->GetMonotonicTimeNs();
  }

  state_mask_ &= ~R_SYNCING;
  state_mask_ |= R_SYNC_OK;

  // There is a data race condition in Redis-master code, where "ACK 0" handler may be
  // triggered before Redis is ready to transition to the streaming state and it silenty ignores
  // "ACK 0". We reduce the chance it happens with this delay.
  fibers_ext::SleepFor(50ms);

  return error_code{};
}

// Initialize and start sub-replica for each flow.
error_code Replica::InitiateDflySync() {
  DCHECK_GT(num_df_flows_, 0u);

  shard_flows_.resize(num_df_flows_);
  for (unsigned i = 0; i < num_df_flows_; ++i) {
    shard_flows_[i].reset(new Replica(master_context_, i, &service_));
  }

  // Blocked on untill all flows got full sync cut.
  fibers_ext::BlockingCounter sync_block{num_df_flows_};

  auto err_handler = [this, sync_block](const auto& ge) mutable {
    sync_block.Cancel();      // Unblock this function.
    DefaultErrorHandler(ge);  // Close sockets to unblock flows.
  };
  RETURN_ON_ERR(cntx_.Switch(std::move(err_handler)));

  // Start full sync flows.
  auto partition = Partition(num_df_flows_);
  shard_set->pool()->AwaitFiberOnAll([&](unsigned index, auto*) {
    for (auto id : partition[index]) {
      auto ec = shard_flows_[id]->StartFullSyncFlow(sync_block, &cntx_);
      if (ec)
        cntx_.Error(ec);
    }
  });
  RETURN_ON_ERR(cntx_.GetError());

  // Send DFLY SYNC.
  if (auto ec = SendNextPhaseRequest(); ec) {
    return cntx_.Error(ec);
  }
  // Wait for all flows to receive full sync cut.
  // In case of an error, this is unblocked by the error handler.
  LOG(INFO) << "Waiting for all full sync cut confirmations";
  sync_block.Wait();

  LOG(INFO) << "Full sync finished";
  return cntx_.GetError();
}

error_code Replica::ConsumeRedisStream() {
  base::IoBuf io_buf(16_KB);
  parser_.reset(new RedisParser);

  ReqSerializer serializer{sock_.get()};

  // Master waits for this command in order to start sending replication stream.
  RETURN_ON_ERR(SendCommand("REPLCONF ACK 0", &serializer));

  VLOG(1) << "Before reading repl-log";

  // Redis sends eiher pings every "repl_ping_slave_period" time inside replicationCron().
  // or, alternatively, write commands stream coming from propagate() function.
  // Replica connection must send "REPLCONF ACK xxx" in order to make sure that master replication
  // buffer gets disposed of already processed commands.
  error_code ec;
  time_t last_ack = time(nullptr);
  string ack_cmd;

  // basically reflection of dragonfly_connection IoLoop function.
  while (!ec) {
    io::MutableBytes buf = io_buf.AppendBuffer();
    io::Result<size_t> size_res = sock_->Recv(buf);
    if (!size_res)
      return size_res.error();

    VLOG(1) << "Read replication stream of " << *size_res << " bytes";
    last_io_time_ = sock_->proactor()->GetMonotonicTimeNs();

    io_buf.CommitWrite(*size_res);
    repl_offs_ += *size_res;

    // Send repl ack back to master.
    if (repl_offs_ > ack_offs_ + 1024 || time(nullptr) > last_ack + 5) {
      ack_cmd.clear();
      absl::StrAppend(&ack_cmd, "REPLCONF ACK ", repl_offs_);
      RETURN_ON_ERR(SendCommand(ack_cmd, &serializer));
    }

    ec = ParseAndExecute(&io_buf);
  }

  VLOG(1) << "ConsumeRedisStream finished";
  return ec;
}

error_code Replica::ConsumeDflyStream() {
  // Send DFLY STARTSTABLE.
  if (auto ec = SendNextPhaseRequest(); ec) {
    return cntx_.Error(ec);
  }

  // Wait for all flows to finish full sync.
  JoinAllFlows();

  RETURN_ON_ERR(cntx_.Switch(absl::bind_front(&Replica::DefaultErrorHandler, this)));

  vector<vector<unsigned>> partition = Partition(num_df_flows_);
  shard_set->pool()->AwaitFiberOnAll([&](unsigned index, auto*) {
    const auto& local_ids = partition[index];
    for (unsigned id : local_ids) {
      auto ec = shard_flows_[id]->StartStableSyncFlow(&cntx_);
      if (ec)
        cntx_.Error(ec);
    }
  });

  return cntx_.GetError();
}

void Replica::CloseAllSockets() {
  if (sock_) {
    sock_->proactor()->Await([this] {
      auto ec = sock_->Shutdown(SHUT_RDWR);
      LOG_IF(ERROR, ec) << "Could not shutdown socket " << ec;
    });
  }

  for (auto& flow : shard_flows_) {
    flow->CloseAllSockets();
  }
}

void Replica::JoinAllFlows() {
  for (auto& flow : shard_flows_) {
    if (flow->sync_fb_.joinable()) {
      flow->sync_fb_.join();
    }
  }
}

void Replica::DefaultErrorHandler(const GenericError& err) {
  CloseAllSockets();
}

error_code Replica::SendNextPhaseRequest() {
  ReqSerializer serializer{sock_.get()};

  // Ask master to start sending replication stream
  string request = (state_mask_ & R_SYNC_OK) ? "STARTSTABLE" : "SYNC";
  RETURN_ON_ERR(
      SendCommand(StrCat("DFLY ", request, " ", master_context_.dfly_session_id), &serializer));

  base::IoBuf io_buf{128};
  unsigned consumed = 0;
  RETURN_ON_ERR(ReadRespReply(&io_buf, &consumed));
  if (!CheckRespIsSimpleReply("OK")) {
    LOG(ERROR) << "Phase transition failed " << ToSV(io_buf.InputBuffer());
    return make_error_code(errc::bad_message);
  }

  return std::error_code{};
}

error_code Replica::StartFullSyncFlow(fibers_ext::BlockingCounter sb, Context* cntx) {
  CHECK(!sock_);
  DCHECK(!master_context_.master_repl_id.empty() && !master_context_.dfly_session_id.empty());

  ProactorBase* mythread = ProactorBase::me();
  CHECK(mythread);

  sock_.reset(mythread->CreateSocket());
  RETURN_ON_ERR(sock_->Connect(master_context_.endpoint));

  VLOG(1) << "Sending on flow " << master_context_.master_repl_id << " "
          << master_context_.dfly_session_id << " " << master_context_.dfly_flow_id;

  ReqSerializer serializer{sock_.get()};
  auto cmd = StrCat("DFLY FLOW ", master_context_.master_repl_id, " ",
                    master_context_.dfly_session_id, " ", master_context_.dfly_flow_id);
  RETURN_ON_ERR(SendCommand(cmd, &serializer));

  parser_.reset(new RedisParser{false});  // client mode

  leftover_buf_.reset(new base::IoBuf(128));
  unsigned consumed = 0;
  RETURN_ON_ERR(ReadRespReply(leftover_buf_.get(), &consumed));  // uses parser_

  if (!CheckRespFirstTypes({RespExpr::STRING, RespExpr::STRING})) {
    LOG(ERROR) << "Bad FLOW response " << ToSV(leftover_buf_->InputBuffer());
    return make_error_code(errc::bad_message);
  }

  string_view flow_directive = ToSV(resp_args_[0].GetBuf());
  string eof_token;
  if (flow_directive == "FULL") {
    eof_token = ToSV(resp_args_[1].GetBuf());
  } else {
    LOG(ERROR) << "Bad FLOW response " << ToSV(leftover_buf_->InputBuffer());
  }
  leftover_buf_->ConsumeInput(consumed);

  state_mask_ = R_ENABLED | R_TCP_CONNECTED;

  // We can not discard io_buf because it may contain data
  // besides the response we parsed. Therefore we pass it further to ReplicateDFFb.
  sync_fb_ = ::boost::fibers::fiber(&Replica::FullSyncDflyFb, this, move(eof_token), sb, cntx);

  return error_code{};
}

error_code Replica::StartStableSyncFlow(Context* cntx) {
  DCHECK(!master_context_.master_repl_id.empty() && !master_context_.dfly_session_id.empty());
  ProactorBase* mythread = ProactorBase::me();
  CHECK(mythread);

  CHECK(sock_->IsOpen());
  // sock_.reset(mythread->CreateSocket());
  // RETURN_ON_ERR(sock_->Connect(master_context_.master_ep));
  sync_fb_ = ::boost::fibers::fiber(&Replica::StableSyncDflyFb, this, cntx);

  return std::error_code{};
}

void Replica::FullSyncDflyFb(string eof_token, fibers_ext::BlockingCounter bc, Context* cntx) {
  DCHECK(leftover_buf_);
  SocketSource ss{sock_.get()};
  io::PrefixSource ps{leftover_buf_->InputBuffer(), &ss};

  RdbLoader loader(&service_);
  loader.SetFullSyncCutCb([bc, ran = false]() mutable {
    if (!ran) {
      bc.Dec();
      ran = true;
    }
  });

  // Load incoming rdb stream.
  if (std::error_code ec = loader.Load(&ps); ec) {
    cntx->Error(ec, "Error loading rdb format");
    return;
  }

  // Try finding eof token.
  io::PrefixSource chained_tail{loader.Leftover(), &ps};
  if (!eof_token.empty()) {
    unique_ptr<uint8_t[]> buf{new uint8_t[eof_token.size()]};

    io::Result<size_t> res =
        chained_tail.ReadAtLeast(io::MutableBytes{buf.get(), eof_token.size()}, eof_token.size());

    if (!res || *res != eof_token.size()) {
      cntx->Error(std::make_error_code(errc::protocol_error), "Error finding eof token in stream");
      return;
    }
  }

  // Keep loader leftover.
  io::Bytes unused = chained_tail.unused_prefix();
  if (unused.size() > 0) {
    leftover_buf_.reset(new base::IoBuf{unused.size()});
    auto mut_bytes = leftover_buf_->AppendBuffer();
    memcpy(mut_bytes.data(), unused.data(), unused.size());
    leftover_buf_->CommitWrite(unused.size());
  } else {
    leftover_buf_.reset();
  }

  VLOG(1) << "FullSyncDflyFb finished after reading " << loader.bytes_read() << " bytes";
}

void Replica::StableSyncDflyFb(Context* cntx) {
  // Check leftover from full sync.
  io::Bytes prefix{};
  if (leftover_buf_ && leftover_buf_->InputLen() > 0) {
    prefix = leftover_buf_->InputBuffer();
  }

  SocketSource ss{sock_.get()};
  io::PrefixSource ps{prefix, &ss};

  JournalReader reader{&ps, 0};
  JournalExecutor executor{&service_};
  while (!cntx->IsCancelled()) {
    auto res = reader.ReadEntry();
    if (!res) {
      cntx->Error(res.error(), "Journal format error");
      return;
    }

    executor.Execute(std::move(res.value()));

    last_io_time_ = sock_->proactor()->GetMonotonicTimeNs();
  }
  return;
}

error_code Replica::ReadRespReply(base::IoBuf* io_buf, uint32_t* consumed) {
  DCHECK(parser_);

  error_code ec;

  // basically reflection of dragonfly_connection IoLoop function.
  while (!ec) {
    io::MutableBytes buf = io_buf->AppendBuffer();
    io::Result<size_t> size_res = sock_->Recv(buf);
    if (!size_res)
      return size_res.error();

    VLOG(2) << "Read master response of " << *size_res << " bytes";

    last_io_time_ = sock_->proactor()->GetMonotonicTimeNs();

    io_buf->CommitWrite(*size_res);

    RedisParser::Result result = parser_->Parse(io_buf->InputBuffer(), consumed, &resp_args_);

    if (result == RedisParser::OK && !resp_args_.empty()) {
      return error_code{};  // success path
    }

    if (result != RedisParser::INPUT_PENDING) {
      LOG(ERROR) << "Invalid parser status " << result << " for buffer of size "
                 << io_buf->InputLen();
      return std::make_error_code(std::errc::bad_message);
    }
    io_buf->ConsumeInput(*consumed);
  }

  return ec;
}

error_code Replica::ParseReplicationHeader(base::IoBuf* io_buf, PSyncResponse* dest) {
  std::string_view str;

  RETURN_ON_ERR(ReadLine(io_buf, &str));

  DCHECK(!str.empty());

  std::string_view header;
  bool valid = false;

  // non-empty lines
  if (str[0] != '+') {
    goto bad_header;
  }

  header = str.substr(1);
  VLOG(1) << "header: " << header;
  if (absl::ConsumePrefix(&header, "FULLRESYNC ")) {
    // +FULLRESYNC db7bd45bf68ae9b1acac33acb 123\r\n
    //             master_id  repl_offset
    size_t pos = header.find(' ');
    if (pos != std::string_view::npos) {
      if (absl::SimpleAtoi(header.substr(pos + 1), &repl_offs_)) {
        master_context_.master_repl_id = string(header.substr(0, pos));
        valid = true;
        VLOG(1) << "master repl_id " << master_context_.master_repl_id << " / " << repl_offs_;
      }
    }

    if (!valid)
      goto bad_header;

    io_buf->ConsumeInput(str.size() + 2);
    RETURN_ON_ERR(ReadLine(io_buf, &str));  // Read the next line parsed below.

    // Readline checks for non ws character first before searching for eol
    // so str must be non empty.
    DCHECK(!str.empty());

    if (str[0] != '$') {
      goto bad_header;
    }

    std::string_view token = str.substr(1);
    VLOG(1) << "token: " << token;
    if (absl::ConsumePrefix(&token, "EOF:")) {
      CHECK_EQ(kRdbEofMarkSize, token.size()) << token;
      dest->fullsync.emplace<string>(token);
      VLOG(1) << "Token: " << token;
    } else {
      size_t rdb_size = 0;
      if (!absl::SimpleAtoi(token, &rdb_size))
        return std::make_error_code(std::errc::illegal_byte_sequence);

      VLOG(1) << "rdb size " << rdb_size;
      dest->fullsync.emplace<size_t>(rdb_size);
    }
    io_buf->ConsumeInput(str.size() + 2);
  } else if (absl::ConsumePrefix(&header, "CONTINUE")) {
    // we send psync2 so we should get master replid.
    // That could change due to redis failovers.
    // TODO: part sync
    dest->fullsync.emplace<size_t>(0);
  }

  return error_code{};

bad_header:
  LOG(ERROR) << "Bad replication header: " << str;
  return std::make_error_code(std::errc::illegal_byte_sequence);
}

error_code Replica::ReadLine(base::IoBuf* io_buf, string_view* line) {
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

error_code Replica::ParseAndExecute(base::IoBuf* io_buf) {
  VLOG(1) << "ParseAndExecute: input len " << io_buf->InputLen();
  if (parser_->stash_size() > 0) {
    DVLOG(1) << "Stash " << *parser_->stash()[0];
  }

  uint32_t consumed = 0;
  RedisParser::Result result = RedisParser::OK;

  io::NullSink null_sink;  // we never reply back on the commands.
  ConnectionContext conn_context{&null_sink, nullptr};
  conn_context.is_replicating = true;

  do {
    result = parser_->Parse(io_buf->InputBuffer(), &consumed, &resp_args_);

    switch (result) {
      case RedisParser::OK:
        if (!resp_args_.empty()) {
          VLOG(2) << "Got command " << ToSV(resp_args_[0].GetBuf()) << ToSV(resp_args_[1].GetBuf())
                  << "\n consumed: " << consumed;
          facade::RespToArgList(resp_args_, &cmd_str_args_);
          CmdArgList arg_list{cmd_str_args_.data(), cmd_str_args_.size()};
          service_.DispatchCommand(arg_list, &conn_context);
        }
        io_buf->ConsumeInput(consumed);
        break;
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

Replica::Info Replica::GetInfo() const {
  CHECK(sock_);

  return sock_->proactor()->AwaitBrief([this] {
    auto last_io_time = last_io_time_;
    for (const auto& flow : shard_flows_) {  // Get last io time from all sub flows.
      last_io_time = std::max(last_io_time, flow->last_io_time_);
    }

    Info res;
    res.host = master_context_.host;
    res.port = master_context_.port;
    res.master_link_established = (state_mask_ & R_TCP_CONNECTED);
    res.sync_in_progress = (state_mask_ & R_SYNCING);
    res.master_last_io_sec = (ProactorBase::GetMonotonicTimeNs() - last_io_time) / 1000000000UL;
    return res;
  });
}

bool Replica::CheckRespIsSimpleReply(string_view reply) const {
  return resp_args_.size() == 1 || resp_args_.front().type == RespExpr::STRING ||
         ToSV(resp_args_.front().GetBuf()) == reply;
}

bool Replica::CheckRespFirstTypes(initializer_list<RespExpr::Type> types) const {
  unsigned i = 0;
  for (RespExpr::Type type : types) {
    if (i >= resp_args_.size() || resp_args_[i].type != type)
      return false;
    ++i;
  }
  return true;
}

error_code Replica::SendCommand(string_view command, ReqSerializer* serializer) {
  serializer->SendCommand(command);
  error_code ec = serializer->ec();
  if (!ec) {
    last_io_time_ = sock_->proactor()->GetMonotonicTimeNs();
  }
  return ec;
}

}  // namespace dfly
