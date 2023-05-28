// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/replica.h"

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

ABSL_FLAG(bool, enable_multi_shard_sync, false,
          "Execute multi shards commands on replica syncrhonized");
ABSL_FLAG(std::string, masterauth, "", "password for authentication with master");
ABSL_FLAG(int, master_connect_timeout_ms, 20000,
          "Timeout for establishing connection to a replication master");
ABSL_FLAG(int, master_reconnect_timeout_ms, 1000,
          "Timeout for re-establishing connection to a replication master");
ABSL_DECLARE_FLAG(uint32_t, port);

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

  static_assert(INET_ADDRSTRLEN < INET6_ADDRSTRLEN, "");

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

std::string Replica::MasterContext::Description() const {
  return absl::StrCat(host, ":", port);
}

Replica::Replica(string host, uint16_t port, Service* se, std::string_view id)
    : service_(*se), id_{id} {
  master_context_.host = std::move(host);
  master_context_.port = port;
}

Replica::Replica(const MasterContext& context, uint32_t dfly_flow_id, Service* service,
                 std::shared_ptr<Replica::MultiShardExecution> shared_exe_data)
    : service_(*service), master_context_(context) {
  master_context_.dfly_flow_id = dfly_flow_id;
  multi_shard_exe_ = shared_exe_data;
  use_multi_shard_exe_sync_ = GetFlag(FLAGS_enable_multi_shard_sync);
  executor_.reset(new JournalExecutor(service));
}

Replica::~Replica() {
  if (sync_fb_.IsJoinable()) {
    sync_fb_.Join();
  }
  if (acks_fb_.IsJoinable()) {
    acks_fb_.Join();
  }
  if (execution_fb_.IsJoinable()) {
    execution_fb_.Join();
  }

  if (sock_) {
    auto ec = sock_->Close();
    LOG_IF(ERROR, ec) << "Error closing replica socket " << ec;
  }
}

static const char kConnErr[] = "could not connect to master: ";

error_code Replica::Start(ConnectionContext* cntx) {
  VLOG(1) << "Starting replication";
  ProactorBase* mythread = ProactorBase::me();
  CHECK(mythread);

  RETURN_ON_ERR(cntx_.SwitchErrorHandler(absl::bind_front(&Replica::DefaultErrorHandler, this)));

  auto check_connection_error = [this, &cntx](const error_code& ec, const char* msg) -> error_code {
    if (cntx_.IsCancelled()) {
      (*cntx)->SendError("replication cancelled");
      return std::make_error_code(errc::operation_canceled);
    }
    if (ec) {
      (*cntx)->SendError(absl::StrCat(msg, ec.message()));
      cntx_.Cancel();
      return ec;
    }
    return {};
  };

  // 1. Resolve dns.
  VLOG(1) << "Resolving master DNS";
  error_code ec = ResolveMasterDns();
  RETURN_ON_ERR(check_connection_error(ec, "could not resolve master dns"));

  // 2. Connect socket.
  VLOG(1) << "Connecting to master";
  ec = ConnectAndAuth(absl::GetFlag(FLAGS_master_connect_timeout_ms) * 1ms);
  RETURN_ON_ERR(check_connection_error(ec, kConnErr));

  // 3. Greet.
  VLOG(1) << "Greeting";
  state_mask_.store(R_ENABLED | R_TCP_CONNECTED);
  last_io_time_ = mythread->GetMonotonicTimeNs();
  ec = Greet();
  RETURN_ON_ERR(check_connection_error(ec, "could not greet master "));

  // 4. Spawn main coordination fiber.
  sync_fb_ = MakeFiber(&Replica::MainReplicationFb, this);

  (*cntx)->SendOk();
  return {};
}  // namespace dfly

void Replica::Stop() {
  VLOG(1) << "Stopping replication";
  // Stops the loop in MainReplicationFb.
  state_mask_.store(0);  // Specifically ~R_ENABLED.
  cntx_.Cancel();        // Context is fully resposible for cleanup.

  // Make sure the replica fully stopped and did all cleanup,
  // so we can freely release resources (connections).
  waker_.notifyAll();
  if (sync_fb_.IsJoinable())
    sync_fb_.Join();
  if (acks_fb_.IsJoinable())
    acks_fb_.Join();
}

void Replica::Pause(bool pause) {
  VLOG(1) << "Pausing replication";
  sock_->proactor()->Await([&] { is_paused_ = pause; });
}

void Replica::MainReplicationFb() {
  VLOG(1) << "Main replication fiber started";
  // Switch shard states to replication.
  SetShardStates(true);

  error_code ec;
  while (state_mask_.load() & R_ENABLED) {
    // Discard all previous errors and set default error handler.
    cntx_.Reset(absl::bind_front(&Replica::DefaultErrorHandler, this));
    // 1. Connect socket.
    if ((state_mask_.load() & R_TCP_CONNECTED) == 0) {
      ThisFiber::SleepFor(500ms);
      if (is_paused_)
        continue;

      ec = ResolveMasterDns();
      if (ec) {
        LOG(ERROR) << "Error resolving dns to " << master_context_.host << " " << ec;
        continue;
      }

      // Give a lower timeout for connect, because we're
      ec = ConnectAndAuth(absl::GetFlag(FLAGS_master_reconnect_timeout_ms) * 1ms);
      if (ec) {
        LOG(ERROR) << "Error connecting to " << master_context_.Description() << " " << ec;
        continue;
      }
      VLOG(1) << "Replica socket connected";
      state_mask_.fetch_or(R_TCP_CONNECTED);
      continue;
    }

    // 2. Greet.
    if ((state_mask_.load() & R_GREETED) == 0) {
      ec = Greet();
      if (ec) {
        LOG(INFO) << "Error greeting " << master_context_.Description() << " " << ec << " "
                  << ec.message();
        state_mask_.fetch_and(R_ENABLED);
        continue;
      }
      state_mask_.fetch_or(R_GREETED);
      continue;
    }

    // 3. Initiate full sync
    if ((state_mask_.load() & R_SYNC_OK) == 0) {
      if (HasDflyMaster())
        ec = InitiateDflySync();
      else
        ec = InitiatePSync();

      if (ec) {
        LOG(WARNING) << "Error syncing with " << master_context_.Description() << " " << ec << " "
                     << ec.message();
        state_mask_.fetch_and(R_ENABLED);  // reset all flags besides R_ENABLED
        continue;
      }
      state_mask_.fetch_or(R_SYNC_OK);
      continue;
    }

    // 4. Start stable state sync.
    DCHECK(state_mask_.load() & R_SYNC_OK);

    if (HasDflyMaster())
      ec = ConsumeDflyStream();
    else
      ec = ConsumeRedisStream();

    LOG(WARNING) << "Error full sync with " << master_context_.Description() << " " << ec << " "
                 << ec.message();
    state_mask_.fetch_and(R_ENABLED);
  }

  // Wait for unblocking cleanup to finish.
  cntx_.JoinErrorHandler();

  // Revert shard states to normal state.
  SetShardStates(false);

  VLOG(1) << "Main replication fiber finished";
}

error_code Replica::ResolveMasterDns() {
  char ip_addr[INET6_ADDRSTRLEN];
  int resolve_res = ResolveDns(master_context_.host, ip_addr);
  if (resolve_res != 0) {
    LOG(ERROR) << "Dns error " << gai_strerror(resolve_res) << ", host: " << master_context_.host;
    return make_error_code(errc::host_unreachable);
  }

  master_context_.endpoint = {ip::make_address(ip_addr), master_context_.port};

  return error_code{};
}

error_code Replica::ConnectAndAuth(std::chrono::milliseconds connect_timeout_ms) {
  ProactorBase* mythread = ProactorBase::me();
  CHECK(mythread);
  {
    unique_lock lk(sock_mu_);
    // The context closes sock_. So if the context error handler has already
    // run we must not create a new socket. sock_mu_ syncs between the two
    // functions.
    if (!cntx_.IsCancelled())
      sock_.reset(mythread->CreateSocket());
    else
      return cntx_.GetError();
  }

  // We set this timeout because this call blocks other REPLICAOF commands. We don't need it for the
  // rest of the sync.
  {
    uint32_t timeout = sock_->timeout();
    sock_->set_timeout(connect_timeout_ms.count());
    RETURN_ON_ERR(sock_->Connect(master_context_.endpoint));
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
    ReqSerializer serializer{sock_.get()};
    uint32_t consumed = 0;
    base::IoBuf io_buf{128};
    parser_.reset(new RedisParser{false});
    RETURN_ON_ERR(SendCommand(StrCat("AUTH ", masterauth), &serializer));
    RETURN_ON_ERR(ReadRespReply(&io_buf, &consumed));
    if (!CheckRespIsSimpleReply("OK")) {
      LOG(ERROR) << "Failed authentication with masters " << ToSV(io_buf.InputBuffer());
      return make_error_code(errc::bad_message);
    }
  }
  return error_code{};
}

error_code Replica::Greet() {
  parser_.reset(new RedisParser{false});
  base::IoBuf io_buf{128};
  ReqSerializer serializer{sock_.get()};
  uint32_t consumed = 0;
  VLOG(1) << "greeting message handling";
  // Corresponds to server.repl_state == REPL_STATE_CONNECTING state in redis
  RETURN_ON_ERR(SendCommand("PING", &serializer));  // optional.
  RETURN_ON_ERR(ReadRespReply(&io_buf, &consumed));
  if (!CheckRespIsSimpleReply("PONG")) {
    LOG(ERROR) << "Bad pong response " << ToSV(io_buf.InputBuffer());
    return make_error_code(errc::bad_message);
  }

  io_buf.ConsumeInput(consumed);

  // Corresponds to server.repl_state == REPL_STATE_SEND_HANDSHAKE condition in replication.c
  auto port = absl::GetFlag(FLAGS_port);
  RETURN_ON_ERR(SendCommand(StrCat("REPLCONF listening-port ", port), &serializer));
  RETURN_ON_ERR(ReadRespReply(&io_buf, &consumed));
  if (!CheckRespIsSimpleReply("OK")) {
    LOG(ERROR) << "Bad REPLCONF response " << ToSV(io_buf.InputBuffer());
    return make_error_code(errc::bad_message);
  }

  io_buf.ConsumeInput(consumed);

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
  auto err_handler = [&io_buf]() {
    LOG(ERROR) << "Unexpected response " << ToSV(io_buf.InputBuffer());
    return make_error_code(errc::bad_message);
  };

  if (!CheckRespFirstTypes({RespExpr::STRING}))
    return err_handler();

  string_view cmd = ToSV(resp_args_[0].GetBuf());

  if (resp_args_.size() == 1) {  // Redis
    if (cmd != "OK")
      return err_handler();
  } else if (resp_args_.size() >= 3) {  // it's dragonfly master.
    if (auto ec = HandleCapaDflyResp(); ec)
      return err_handler();
    if (auto ec = ConfigureDflyMaster(); ec)
      return ec;
  } else {
    return err_handler();
  }

  state_mask_.fetch_or(R_GREETED);
  return error_code{};
}

std::error_code Replica::HandleCapaDflyResp() {
  // Response is: <master_repl_id, syncid, num_shards [, version]>
  if (!CheckRespFirstTypes({RespExpr::STRING, RespExpr::STRING, RespExpr::INT64}) ||
      resp_args_[0].GetBuf().size() != CONFIG_RUN_ID_SIZE)
    return make_error_code(errc::bad_message);

  int64 param_num_flows = get<int64_t>(resp_args_[2].u);
  if (param_num_flows <= 0 || param_num_flows > 1024) {
    // sanity check, we support upto 1024 shards.
    // It's not that we can not support more but it's probably highly unlikely that someone
    // will run dragonfly with more than 1024 cores.
    LOG(ERROR) << "Invalid flow count " << param_num_flows;
    return make_error_code(errc::bad_message);
  }

  master_context_.master_repl_id = ToSV(resp_args_[0].GetBuf());
  master_context_.dfly_session_id = ToSV(resp_args_[1].GetBuf());
  num_df_flows_ = param_num_flows;

  if (resp_args_.size() >= 4) {
    if (resp_args_[3].type != RespExpr::INT64)
      return make_error_code(errc::bad_message);
    master_context_.version = DflyVersion(get<int64_t>(resp_args_[3].u));
  }
  VLOG(1) << "Master id: " << master_context_.master_repl_id
          << ", sync id: " << master_context_.dfly_session_id << ", num journals: " << num_df_flows_
          << ", version: " << unsigned(master_context_.version);

  return error_code{};
}

std::error_code Replica::ConfigureDflyMaster() {
  base::IoBuf io_buf{128};
  ReqSerializer serializer{sock_.get()};
  uint32_t consumed = 0;

  // We need to send this because we may require to use this for cluster commands.
  // this reason to send this here is that in other context we can get an error reply
  // since we are budy with the replication
  RETURN_ON_ERR(SendCommand(StrCat("REPLCONF CLIENT-ID ", id_), &serializer));
  RETURN_ON_ERR(ReadRespReply(&io_buf, &consumed));
  if (!CheckRespIsSimpleReply("OK")) {
    LOG(WARNING) << "master did not return OK on id message";
  }

  io_buf.ConsumeInput(consumed);

  // Tell the master our version if it supports REPLCONF CLIENT-VERSION
  if (master_context_.version > DflyVersion::VER0) {
    RETURN_ON_ERR(
        SendCommand(StrCat("REPLCONF CLIENT-VERSION ", DflyVersion::CURRENT_VER), &serializer));
    RETURN_ON_ERR(ReadRespReply(&io_buf, &consumed));
    if (!CheckRespIsSimpleReply("OK")) {
      LOG(ERROR) << "Unexpected response " << ToSV(io_buf.InputBuffer());
      return make_error_code(errc::bad_message);
    }
  }

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

  LOG(INFO) << "Starting full sync";

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
    state_mask_.fetch_or(R_SYNCING);

    io::PrefixSource ps{io_buf.InputBuffer(), sock_.get()};

    // Set LOADING state.
    CHECK(service_.SwitchState(GlobalState::ACTIVE, GlobalState::LOADING) == GlobalState::LOADING);
    absl::Cleanup cleanup = [this]() {
      service_.SwitchState(GlobalState::LOADING, GlobalState::ACTIVE);
    };

    JournalExecutor{&service_}.FlushAll();
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
      CHECK(chained.UnusedPrefix().empty());
    } else {
      CHECK_EQ(0u, loader.Leftover().size());
      CHECK_EQ(snapshot_size, loader.bytes_read());
    }

    CHECK(ps.UnusedPrefix().empty());
    io_buf.ConsumeInput(io_buf.InputLen());
    last_io_time_ = sock_thread->GetMonotonicTimeNs();
  }

  state_mask_.fetch_and(~R_SYNCING);
  state_mask_.fetch_or(R_SYNC_OK);

  // There is a data race condition in Redis-master code, where "ACK 0" handler may be
  // triggered before Redis is ready to transition to the streaming state and it silenty ignores
  // "ACK 0". We reduce the chance it happens with this delay.
  ThisFiber::SleepFor(50ms);

  return error_code{};
}

// Initialize and start sub-replica for each flow.
error_code Replica::InitiateDflySync() {
  auto start_time = absl::Now();

  absl::Cleanup cleanup = [this]() {
    // We do the following operations regardless of outcome.
    JoinAllFlows();
    service_.SwitchState(GlobalState::LOADING, GlobalState::ACTIVE);
    state_mask_.fetch_and(~R_SYNCING);
  };

  // Initialize MultiShardExecution.
  multi_shard_exe_.reset(new MultiShardExecution());

  // Initialize shard flows.
  shard_flows_.resize(num_df_flows_);
  for (unsigned i = 0; i < num_df_flows_; ++i) {
    shard_flows_[i].reset(new Replica(master_context_, i, &service_, multi_shard_exe_));
  }

  // Blocked on until all flows got full sync cut.
  BlockingCounter sync_block{num_df_flows_};

  // Switch to new error handler that closes flow sockets.
  auto err_handler = [this, sync_block](const auto& ge) mutable {
    // Unblock this function.
    sync_block.Cancel();

    // Make sure the flows are not in a state transition
    lock_guard lk{flows_op_mu_};

    // Unblock all sockets.
    DefaultErrorHandler(ge);
    for (auto& flow : shard_flows_)
      flow->CloseSocket();
  };
  RETURN_ON_ERR(cntx_.SwitchErrorHandler(std::move(err_handler)));

  // Make sure we're in LOADING state.
  CHECK(service_.SwitchState(GlobalState::ACTIVE, GlobalState::LOADING) == GlobalState::LOADING);

  // Flush dbs.
  JournalExecutor{&service_}.FlushAll();

  // Start full sync flows.
  state_mask_.fetch_or(R_SYNCING);
  {
    auto partition = Partition(num_df_flows_);
    auto shard_cb = [&](unsigned index, auto*) {
      for (auto id : partition[index]) {
        auto ec = shard_flows_[id]->StartFullSyncFlow(sync_block, &cntx_);
        if (ec)
          cntx_.ReportError(ec);
      }
    };

    // Lock to prevent the error handler from running instantly
    // while the flows are in a mixed state.
    lock_guard lk{flows_op_mu_};
    shard_set->pool()->AwaitFiberOnAll(std::move(shard_cb));
  }

  RETURN_ON_ERR(cntx_.GetError());

  // Send DFLY SYNC.
  if (auto ec = SendNextPhaseRequest(false); ec) {
    return cntx_.ReportError(ec);
  }

  LOG(INFO) << absl::StrCat("Started full sync with ", master_context_.Description());

  // Wait for all flows to receive full sync cut.
  // In case of an error, this is unblocked by the error handler.
  VLOG(1) << "Waiting for all full sync cut confirmations";
  sync_block.Wait();

  // Check if we woke up due to cancellation.
  if (cntx_.IsCancelled())
    return cntx_.GetError();

  // Send DFLY STARTSTABLE.
  if (auto ec = SendNextPhaseRequest(true); ec) {
    return cntx_.ReportError(ec);
  }

  // Joining flows and resetting state is done by cleanup.

  double seconds = double(absl::ToInt64Milliseconds(absl::Now() - start_time)) / 1000;
  LOG(INFO) << "Full sync finished in " << strings::HumanReadableElapsedTime(seconds);
  return cntx_.GetError();
}

error_code Replica::ConsumeRedisStream() {
  base::IoBuf io_buf(16_KB);
  io::NullSink null_sink;  // we never reply back on the commands.
  ConnectionContext conn_context{&null_sink, nullptr};
  conn_context.is_replicating = true;
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

  LOG(INFO) << "Transitioned into stable sync";

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

    ec = ParseAndExecute(&io_buf, &conn_context);
  }

  VLOG(1) << "ConsumeRedisStream finished";
  return ec;
}

error_code Replica::ConsumeDflyStream() {
  // Set new error handler that closes flow sockets.
  auto err_handler = [this](const auto& ge) {
    // Make sure the flows are not in a state transition
    lock_guard lk{flows_op_mu_};
    DefaultErrorHandler(ge);
    for (auto& flow : shard_flows_) {
      flow->CloseSocket();
      flow->waker_.notifyAll();
    }

    // Iterate over map and cancel all blocking entities
    {
      lock_guard lk{multi_shard_exe_->map_mu};
      for (auto& tx_data : multi_shard_exe_->tx_sync_execution) {
        tx_data.second.barrier.Cancel();
        tx_data.second.block.Cancel();
      }
    }
  };
  RETURN_ON_ERR(cntx_.SwitchErrorHandler(std::move(err_handler)));

  LOG(INFO) << "Transitioned into stable sync";
  // Transition flows into stable sync.
  {
    auto partition = Partition(num_df_flows_);
    auto shard_cb = [&](unsigned index, auto*) {
      const auto& local_ids = partition[index];
      for (unsigned id : local_ids) {
        auto ec = shard_flows_[id]->StartStableSyncFlow(&cntx_);
        if (ec)
          cntx_.ReportError(ec);
      }
    };

    // Lock to prevent error handler from running on mixed state.
    lock_guard lk{flows_op_mu_};
    shard_set->pool()->AwaitFiberOnAll(std::move(shard_cb));
  }
  JoinAllFlows();

  LOG(INFO) << "Exit stable sync";
  // The only option to unblock is to cancel the context.
  CHECK(cntx_.GetError());

  return cntx_.GetError();
}

void Replica::CloseSocket() {
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

void Replica::JoinAllFlows() {
  for (auto& flow : shard_flows_) {
    if (flow->sync_fb_.IsJoinable()) {
      flow->sync_fb_.Join();
    }
    if (flow->acks_fb_.IsJoinable()) {
      flow->acks_fb_.Join();
    }
    if (flow->execution_fb_.IsJoinable()) {
      flow->execution_fb_.Join();
    }
  }
}

void Replica::SetShardStates(bool replica) {
  shard_set->RunBriefInParallel([replica](EngineShard* shard) { shard->SetReplica(replica); });
}

void Replica::DefaultErrorHandler(const GenericError& err) {
  CloseSocket();
}

error_code Replica::SendNextPhaseRequest(bool stable) {
  ReqSerializer serializer{sock_.get()};

  // Ask master to start sending replication stream
  string_view kind = (stable) ? "STARTSTABLE"sv : "SYNC"sv;
  string request = StrCat("DFLY ", kind, " ", master_context_.dfly_session_id);

  VLOG(1) << "Sending: " << request;
  RETURN_ON_ERR(SendCommand(request, &serializer));

  base::IoBuf io_buf{128};
  unsigned consumed = 0;
  RETURN_ON_ERR(ReadRespReply(&io_buf, &consumed));
  if (!CheckRespIsSimpleReply("OK")) {
    LOG(ERROR) << "Phase transition failed " << ToSV(io_buf.InputBuffer());
    return make_error_code(errc::bad_message);
  }

  return std::error_code{};
}

error_code Replica::StartFullSyncFlow(BlockingCounter sb, Context* cntx) {
  DCHECK(!master_context_.master_repl_id.empty() && !master_context_.dfly_session_id.empty());

  RETURN_ON_ERR(ConnectAndAuth(absl::GetFlag(FLAGS_master_connect_timeout_ms) * 1ms));

  VLOG(1) << "Sending on flow " << master_context_.master_repl_id << " "
          << master_context_.dfly_session_id << " " << master_context_.dfly_flow_id;

  ReqSerializer serializer{sock_.get()};
  auto cmd = StrCat("DFLY FLOW ", master_context_.master_repl_id, " ",
                    master_context_.dfly_session_id, " ", master_context_.dfly_flow_id);
  RETURN_ON_ERR(SendCommand(cmd, &serializer));

  parser_.reset(new RedisParser{false});  // client mode

  leftover_buf_.emplace(128);
  unsigned consumed = 0;
  RETURN_ON_ERR(ReadRespReply(&*leftover_buf_, &consumed));  // uses parser_

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
    return make_error_code(errc::bad_message);
  }
  leftover_buf_->ConsumeInput(consumed);

  state_mask_.fetch_or(R_TCP_CONNECTED);

  // We can not discard io_buf because it may contain data
  // besides the response we parsed. Therefore we pass it further to ReplicateDFFb.
  sync_fb_ = MakeFiber(&Replica::FullSyncDflyFb, this, move(eof_token), sb, cntx);

  return error_code{};
}

error_code Replica::StartStableSyncFlow(Context* cntx) {
  DCHECK(!master_context_.master_repl_id.empty() && !master_context_.dfly_session_id.empty());
  ProactorBase* mythread = ProactorBase::me();
  CHECK(mythread);

  CHECK(sock_->IsOpen());
  // sock_.reset(mythread->CreateSocket());
  // RETURN_ON_ERR(sock_->Connect(master_context_.master_ep));
  sync_fb_ = MakeFiber(&Replica::StableSyncDflyReadFb, this, cntx);
  if (use_multi_shard_exe_sync_) {
    execution_fb_ = MakeFiber(&Replica::StableSyncDflyExecFb, this, cntx);
  }

  return std::error_code{};
}

void Replica::FullSyncDflyFb(string eof_token, BlockingCounter bc, Context* cntx) {
  DCHECK(leftover_buf_);
  io::PrefixSource ps{leftover_buf_->InputBuffer(), sock_.get()};

  RdbLoader loader(&service_);
  loader.SetFullSyncCutCb([bc, ran = false]() mutable {
    if (!ran) {
      bc.Dec();
      ran = true;
    }
  });

  // Load incoming rdb stream.
  if (std::error_code ec = loader.Load(&ps); ec) {
    cntx->ReportError(ec, "Error loading rdb format");
    return;
  }

  // Try finding eof token.
  io::PrefixSource chained_tail{loader.Leftover(), &ps};
  if (!eof_token.empty()) {
    unique_ptr<uint8_t[]> buf{new uint8_t[eof_token.size()]};

    io::Result<size_t> res =
        chained_tail.ReadAtLeast(io::MutableBytes{buf.get(), eof_token.size()}, eof_token.size());

    if (!res || *res != eof_token.size()) {
      cntx->ReportError(std::make_error_code(errc::protocol_error),
                        "Error finding eof token in stream");
      return;
    }
  }

  // Keep loader leftover.
  io::Bytes unused = chained_tail.UnusedPrefix();
  if (unused.size() > 0) {
    leftover_buf_.emplace(unused.size());
    leftover_buf_->WriteAndCommit(unused.data(), unused.size());
  } else {
    leftover_buf_.reset();
  }

  this->journal_rec_executed_.store(loader.journal_offset());
  VLOG(1) << "FullSyncDflyFb finished after reading " << loader.bytes_read() << " bytes";
}

void Replica::StableSyncDflyReadFb(Context* cntx) {
  // Check leftover from full sync.
  io::Bytes prefix{};
  if (leftover_buf_ && leftover_buf_->InputLen() > 0) {
    prefix = leftover_buf_->InputBuffer();
  }

  io::PrefixSource ps{prefix, sock_.get()};

  JournalReader reader{&ps, 0};
  TransactionReader tx_reader{};

  acks_fb_ = MakeFiber(&Replica::StableSyncDflyAcksFb, this, cntx);

  while (!cntx->IsCancelled()) {
    waker_.await([&]() {
      return ((trans_data_queue_.size() < kYieldAfterItemsInQueue) || cntx->IsCancelled());
    });
    if (cntx->IsCancelled())
      break;

    auto tx_data = tx_reader.NextTxData(&reader, cntx);
    if (!tx_data)
      break;

    last_io_time_ = sock_->proactor()->GetMonotonicTimeNs();

    if (!tx_data->is_ping) {
      if (use_multi_shard_exe_sync_) {
        InsertTxDataToShardResource(std::move(*tx_data));
      } else {
        ExecuteTxWithNoShardSync(std::move(*tx_data), cntx);
      }
    } else {
      force_ping_ = true;
      journal_rec_executed_.fetch_add(1, std::memory_order_relaxed);
    }

    waker_.notify();
  }
}

void Replica::StableSyncDflyAcksFb(Context* cntx) {
  constexpr std::chrono::duration kAckTimeMaxInterval = 3s;
  constexpr size_t kAckRecordMaxInterval = 1024;
  std::string ack_cmd;
  ReqSerializer serializer{sock_.get()};

  auto next_ack_tp = std::chrono::steady_clock::now();

  while (!cntx->IsCancelled()) {
    // Handle ACKs with the master. PING opcodes from the master mean we should immediately
    // answer.
    uint64_t current_offset = journal_rec_executed_.load(std::memory_order_relaxed);
    VLOG(1) << "Sending an ACK with offset=" << current_offset << " forced=" << force_ping_;
    ack_cmd = absl::StrCat("REPLCONF ACK ", current_offset);
    force_ping_ = false;
    next_ack_tp = std::chrono::steady_clock::now() + kAckTimeMaxInterval;
    if (auto ec = SendCommand(ack_cmd, &serializer); ec) {
      cntx->ReportError(ec);
      break;
    }
    ack_offs_ = current_offset;

    waker_.await_until(
        [&]() {
          return journal_rec_executed_.load(std::memory_order_relaxed) >
                     ack_offs_ + kAckRecordMaxInterval ||
                 force_ping_ || cntx->IsCancelled();
        },
        next_ack_tp);
  }
}

void Replica::ExecuteTxWithNoShardSync(TransactionData&& tx_data, Context* cntx) {
  if (cntx->IsCancelled()) {
    return;
  }

  bool was_insert = false;
  if (tx_data.IsGlobalCmd()) {
    was_insert = InsertTxToSharedMap(tx_data);
  }

  ExecuteTx(std::move(tx_data), was_insert, cntx);
}

bool Replica::InsertTxToSharedMap(const TransactionData& tx_data) {
  std::lock_guard lk{multi_shard_exe_->map_mu};

  auto [it, was_insert] =
      multi_shard_exe_->tx_sync_execution.emplace(tx_data.txid, tx_data.shard_cnt);
  VLOG(2) << "txid: " << tx_data.txid << " unique_shard_cnt_: " << tx_data.shard_cnt
          << " was_insert: " << was_insert;
  it->second.block.Dec();

  return was_insert;
}

void Replica::InsertTxDataToShardResource(TransactionData&& tx_data) {
  bool was_insert = false;
  if (tx_data.shard_cnt > 1) {
    was_insert = InsertTxToSharedMap(tx_data);
  }

  VLOG(2) << "txid: " << tx_data.txid << " pushed to queue";
  trans_data_queue_.push(std::make_pair(std::move(tx_data), was_insert));
}

void Replica::StableSyncDflyExecFb(Context* cntx) {
  while (!cntx->IsCancelled()) {
    waker_.await([&]() { return (!trans_data_queue_.empty() || cntx->IsCancelled()); });
    if (cntx->IsCancelled()) {
      return;
    }
    DCHECK(!trans_data_queue_.empty());
    auto& data = trans_data_queue_.front();
    ExecuteTx(std::move(data.first), data.second, cntx);
    trans_data_queue_.pop();
    waker_.notify();
  }
}

void Replica::ExecuteTx(TransactionData&& tx_data, bool inserted_by_me, Context* cntx) {
  if (cntx->IsCancelled()) {
    return;
  }
  if (tx_data.shard_cnt <= 1 || (!use_multi_shard_exe_sync_ && !tx_data.IsGlobalCmd())) {
    VLOG(2) << "Execute cmd without sync between shards. txid: " << tx_data.txid;
    executor_->Execute(tx_data.dbid, absl::MakeSpan(tx_data.commands));
    journal_rec_executed_.fetch_add(tx_data.journal_rec_count, std::memory_order_relaxed);
    return;
  }

  VLOG(2) << "Execute txid: " << tx_data.txid;
  multi_shard_exe_->map_mu.lock();
  auto it = multi_shard_exe_->tx_sync_execution.find(tx_data.txid);
  DCHECK(it != multi_shard_exe_->tx_sync_execution.end());
  auto& multi_shard_data = it->second;
  multi_shard_exe_->map_mu.unlock();

  VLOG(2) << "Execute txid: " << tx_data.txid << " waiting for data in all shards";
  // Wait until shards flows got transaction data and inserted to map.
  // This step enforces that replica will execute multi shard commands that finished on master
  // and replica recieved all the commands from all shards.
  multi_shard_data.block.Wait();
  // Check if we woke up due to cancellation.
  if (cntx_.IsCancelled())
    return;
  VLOG(2) << "Execute txid: " << tx_data.txid << " block wait finished";

  if (tx_data.IsGlobalCmd()) {
    VLOG(2) << "Execute txid: " << tx_data.txid << " global command execution";
    // Wait until all shards flows get to execution step of this transaction.
    multi_shard_data.barrier.Wait();
    // Check if we woke up due to cancellation.
    if (cntx_.IsCancelled())
      return;
    // Global command will be executed only from one flow fiber. This ensure corectness of data in
    // replica.
    if (inserted_by_me) {
      executor_->Execute(tx_data.dbid, absl::MakeSpan(tx_data.commands));
    }
    // Wait until exection is done, to make sure we done execute next commands while the global is
    // executed.
    multi_shard_data.barrier.Wait();
    // Check if we woke up due to cancellation.
    if (cntx_.IsCancelled())
      return;
  } else {  // Non global command will be executed by each flow fiber
    VLOG(2) << "Execute txid: " << tx_data.txid << " executing shard transaction commands";
    executor_->Execute(tx_data.dbid, absl::MakeSpan(tx_data.commands));
  }
  journal_rec_executed_.fetch_add(tx_data.journal_rec_count, std::memory_order_relaxed);

  // Erase from map can be done only after all flow fibers executed the transaction commands.
  // The last fiber which will decrease the counter to 0 will be the one to erase the data from
  // map
  auto val = multi_shard_data.counter.fetch_sub(1, std::memory_order_relaxed);
  VLOG(2) << "txid: " << tx_data.txid << " counter: " << val;
  if (val == 1) {
    std::lock_guard lg{multi_shard_exe_->map_mu};
    multi_shard_exe_->tx_sync_execution.erase(tx_data.txid);
  }
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

error_code Replica::ParseAndExecute(base::IoBuf* io_buf, ConnectionContext* cntx) {
  VLOG(1) << "ParseAndExecute: input len " << io_buf->InputLen();
  if (parser_->stash_size() > 0) {
    DVLOG(1) << "Stash " << *parser_->stash()[0];
  }

  uint32_t consumed = 0;
  RedisParser::Result result = RedisParser::OK;

  do {
    result = parser_->Parse(io_buf->InputBuffer(), &consumed, &resp_args_);

    switch (result) {
      case RedisParser::OK:
        if (!resp_args_.empty()) {
          VLOG(2) << "Got command " << ToSV(resp_args_[0].GetBuf()) << "\n consumed: " << consumed;

          facade::RespToArgList(resp_args_, &cmd_str_args_);
          CmdArgList arg_list{cmd_str_args_.data(), cmd_str_args_.size()};
          service_.DispatchCommand(arg_list, cntx);
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
    res.master_link_established = (state_mask_.load() & R_TCP_CONNECTED);
    res.full_sync_in_progress = (state_mask_.load() & R_SYNCING);
    res.full_sync_done = (state_mask_.load() & R_SYNC_OK);
    res.master_last_io_sec = (ProactorBase::GetMonotonicTimeNs() - last_io_time) / 1000000000UL;
    return res;
  });
}

std::vector<uint64_t> Replica::GetReplicaOffset() const {
  std::vector<uint64_t> flow_rec_count;
  flow_rec_count.resize(shard_flows_.size());
  for (const auto& flow : shard_flows_) {
    uint32_t flow_id = flow->master_context_.dfly_flow_id;
    uint64_t rec_count = flow->journal_rec_executed_.load(std::memory_order_relaxed);
    DCHECK_LT(flow_id, shard_flows_.size());
    flow_rec_count[flow_id] = rec_count;
  }
  return flow_rec_count;
}

std::string Replica::GetSyncId() const {
  return master_context_.dfly_session_id;
}

bool Replica::CheckRespIsSimpleReply(string_view reply) const {
  return resp_args_.size() == 1 && resp_args_.front().type == RespExpr::STRING &&
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

bool Replica::TransactionData::AddEntry(journal::ParsedEntry&& entry) {
  ++journal_rec_count;

  switch (entry.opcode) {
    case journal::Op::PING:
      is_ping = true;
      return true;
    case journal::Op::EXPIRED:
    case journal::Op::COMMAND:
      commands.push_back(std::move(entry.cmd));
      [[fallthrough]];
    case journal::Op::EXEC:
      shard_cnt = entry.shard_cnt;
      dbid = entry.dbid;
      txid = entry.txid;
      return true;
    case journal::Op::MULTI_COMMAND:
      commands.push_back(std::move(entry.cmd));
      dbid = entry.dbid;
      return false;
    default:
      DCHECK(false) << "Unsupported opcode";
  }
  return false;
}

bool Replica::TransactionData::IsGlobalCmd() const {
  return commands.size() == 1 && commands.front().cmd_args.size() == 1;
}

Replica::TransactionData Replica::TransactionData::FromSingle(journal::ParsedEntry&& entry) {
  TransactionData data;
  bool res = data.AddEntry(std::move(entry));
  DCHECK(res);
  return data;
}

auto Replica::TransactionReader::NextTxData(JournalReader* reader, Context* cntx)
    -> optional<TransactionData> {
  io::Result<journal::ParsedEntry> res;
  while (true) {
    if (res = reader->ReadEntry(); !res) {
      cntx->ReportError(res.error());
      return std::nullopt;
    }

    // Check if journal command can be executed right away.
    // Expiration checks lock on master, so it never conflicts with running multi transactions.
    if (res->opcode == journal::Op::EXPIRED || res->opcode == journal::Op::COMMAND ||
        res->opcode == journal::Op::PING)
      return TransactionData::FromSingle(std::move(res.value()));

    // Otherwise, continue building multi command.
    DCHECK(res->opcode == journal::Op::MULTI_COMMAND || res->opcode == journal::Op::EXEC);
    DCHECK(res->txid > 0);

    auto txid = res->txid;
    auto& txdata = current_[txid];
    if (txdata.AddEntry(std::move(res.value()))) {
      auto out = std::move(txdata);
      current_.erase(txid);
      return out;
    }
  }

  return std::nullopt;
}

}  // namespace dfly
