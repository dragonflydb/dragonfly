// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/replica.h"

#include "absl/strings/match.h"

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
#include <memory>
#include <utility>

#include "base/logging.h"
#include "facade/dragonfly_connection.h"
#include "facade/redis_parser.h"
#include "server/error.h"
#include "server/journal/executor.h"
#include "server/journal/serializer.h"
#include "server/main_service.h"
#include "server/rdb_load.h"
#include "strings/human_readable.h"

ABSL_FLAG(int, replication_acks_interval, 3000, "Interval between acks in milliseconds.");
ABSL_FLAG(bool, enable_multi_shard_sync, false,
          "Execute multi shards commands on replica syncrhonized");
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

Replica::Replica(string host, uint16_t port, Service* se, std::string_view id)
    : ProtocolClient(std::move(host), port), service_(*se), id_{id} {
}

Replica::~Replica() {
  sync_fb_.JoinIfNeeded();
  acks_fb_.JoinIfNeeded();
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
  ec = ConnectAndAuth(absl::GetFlag(FLAGS_master_connect_timeout_ms) * 1ms, &cntx_);
  RETURN_ON_ERR(check_connection_error(ec, kConnErr));

  // 3. Greet.
  VLOG(1) << "Greeting";
  state_mask_.store(R_ENABLED | R_TCP_CONNECTED);
  ec = Greet();
  RETURN_ON_ERR(check_connection_error(ec, "could not greet master "));

  // 4. Spawn main coordination fiber.
  sync_fb_ = fb2::Fiber("main_replication", &Replica::MainReplicationFb, this);

  (*cntx)->SendOk();
  return {};
}  // namespace dfly

void Replica::EnableReplication(ConnectionContext* cntx) {
  VLOG(1) << "Enabling replication";

  state_mask_.store(R_ENABLED);                             // set replica state to enabled
  sync_fb_ = MakeFiber(&Replica::MainReplicationFb, this);  // call replication fiber

  (*cntx)->SendOk();
}

void Replica::Stop() {
  VLOG(1) << "Stopping replication";
  // Stops the loop in MainReplicationFb.
  state_mask_.store(0);  // Specifically ~R_ENABLED.
  cntx_.Cancel();        // Context is fully resposible for cleanup.

  waker_.notifyAll();

  // Make sure the replica fully stopped and did all cleanup,
  // so we can freely release resources (connections).
  sync_fb_.JoinIfNeeded();
  acks_fb_.JoinIfNeeded();
}

void Replica::Pause(bool pause) {
  VLOG(1) << "Pausing replication";
  Proactor()->Await([&] { is_paused_ = pause; });
}

std::error_code Replica::TakeOver(std::string_view timeout) {
  VLOG(1) << "Taking over";

  std::error_code ec;
  Proactor()->Await(
      [this, &ec, timeout] { ec = SendNextPhaseRequest(absl::StrCat("TAKEOVER ", timeout)); });

  // If we successfully taken over, return and let server_family stop the replication.
  return ec;
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
        LOG(ERROR) << "Error resolving dns to " << server().host << " " << ec;
        continue;
      }

      // Give a lower timeout for connect, because we're
      ec = ConnectAndAuth(absl::GetFlag(FLAGS_master_reconnect_timeout_ms) * 1ms, &cntx_);
      if (ec) {
        LOG(ERROR) << "Error connecting to " << server().Description() << " " << ec;
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
        LOG(INFO) << "Error greeting " << server().Description() << " " << ec << " "
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
        LOG(WARNING) << "Error syncing with " << server().Description() << " " << ec << " "
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

    LOG(WARNING) << "Error stable sync with " << server().Description() << " " << ec << " "
                 << ec.message();
    state_mask_.fetch_and(R_ENABLED);
  }

  // Wait for unblocking cleanup to finish.
  cntx_.JoinErrorHandler();

  // Revert shard states to normal state.
  SetShardStates(false);

  VLOG(1) << "Main replication fiber finished";
}

error_code Replica::Greet() {
  ResetParser(false);
  VLOG(1) << "greeting message handling";
  // Corresponds to server.repl_state == REPL_STATE_CONNECTING state in redis
  RETURN_ON_ERR(SendCommandAndReadResponse("PING"));  // optional.
  PC_RETURN_ON_BAD_RESPONSE(CheckRespIsSimpleReply("PONG"));

  // Corresponds to server.repl_state == REPL_STATE_SEND_HANDSHAKE condition in replication.c
  auto port = absl::GetFlag(FLAGS_port);
  RETURN_ON_ERR(SendCommandAndReadResponse(StrCat("REPLCONF listening-port ", port)));
  PC_RETURN_ON_BAD_RESPONSE(CheckRespIsSimpleReply("OK"));

  // Corresponds to server.repl_state == REPL_STATE_SEND_CAPA
  RETURN_ON_ERR(SendCommandAndReadResponse("REPLCONF capa eof capa psync2"));
  PC_RETURN_ON_BAD_RESPONSE(CheckRespIsSimpleReply("OK"));

  // Announce that we are the dragonfly client.
  // Note that we currently do not support dragonfly->redis replication.
  RETURN_ON_ERR(SendCommandAndReadResponse("REPLCONF capa dragonfly"));
  PC_RETURN_ON_BAD_RESPONSE(CheckRespFirstTypes({RespExpr::STRING}));

  if (LastResponseArgs().size() == 1) {  // Redis
    PC_RETURN_ON_BAD_RESPONSE(CheckRespIsSimpleReply("OK"));
  } else if (LastResponseArgs().size() >= 3) {  // it's dragonfly master.
    PC_RETURN_ON_BAD_RESPONSE(!HandleCapaDflyResp());
    if (auto ec = ConfigureDflyMaster(); ec)
      return ec;
  } else {
    PC_RETURN_ON_BAD_RESPONSE(false);
  }

  state_mask_.fetch_or(R_GREETED);
  return error_code{};
}

std::error_code Replica::HandleCapaDflyResp() {
  // Response is: <master_repl_id, syncid, num_shards [, version]>
  if (!CheckRespFirstTypes({RespExpr::STRING, RespExpr::STRING, RespExpr::INT64}) ||
      LastResponseArgs()[0].GetBuf().size() != CONFIG_RUN_ID_SIZE)
    return make_error_code(errc::bad_message);

  int64 param_num_flows = get<int64_t>(LastResponseArgs()[2].u);
  if (param_num_flows <= 0 || param_num_flows > 1024) {
    // sanity check, we support upto 1024 shards.
    // It's not that we can not support more but it's probably highly unlikely that someone
    // will run dragonfly with more than 1024 cores.
    LOG(ERROR) << "Invalid flow count " << param_num_flows;
    return make_error_code(errc::bad_message);
  }

  master_context_.master_repl_id = ToSV(LastResponseArgs()[0].GetBuf());
  master_context_.dfly_session_id = ToSV(LastResponseArgs()[1].GetBuf());
  num_df_flows_ = param_num_flows;

  if (LastResponseArgs().size() >= 4) {
    PC_RETURN_ON_BAD_RESPONSE(LastResponseArgs()[3].type == RespExpr::INT64);
    master_context_.version = DflyVersion(get<int64_t>(LastResponseArgs()[3].u));
  }
  VLOG(1) << "Master id: " << master_context_.master_repl_id
          << ", sync id: " << master_context_.dfly_session_id << ", num journals: " << num_df_flows_
          << ", version: " << unsigned(master_context_.version);

  return error_code{};
}

std::error_code Replica::ConfigureDflyMaster() {
  // We need to send this because we may require to use this for cluster commands.
  // this reason to send this here is that in other context we can get an error reply
  // since we are budy with the replication
  RETURN_ON_ERR(SendCommandAndReadResponse(StrCat("REPLCONF CLIENT-ID ", id_)));
  if (!CheckRespIsSimpleReply("OK")) {
    LOG(WARNING) << "Bad REPLCONF CLIENT-ID response";
  }

  // Tell the master our version if it supports REPLCONF CLIENT-VERSION
  if (master_context_.version > DflyVersion::VER0) {
    RETURN_ON_ERR(
        SendCommandAndReadResponse(StrCat("REPLCONF CLIENT-VERSION ", DflyVersion::CURRENT_VER)));
    PC_RETURN_ON_BAD_RESPONSE(CheckRespIsSimpleReply("OK"));
  }

  return error_code{};
}

error_code Replica::InitiatePSync() {
  base::IoBuf io_buf{128};

  // Corresponds to server.repl_state == REPL_STATE_SEND_PSYNC
  string id("?");  // corresponds to null master id and null offset
  int64_t offs = -1;
  if (!master_context_.master_repl_id.empty()) {  // in case we synced before
    id = master_context_.master_repl_id;          // provide the replication offset and master id
    offs = repl_offs_;                            // to try incremental sync.
  }

  RETURN_ON_ERR(SendCommand(StrCat("PSYNC ", id, " ", offs)));

  LOG(INFO) << "Starting full sync";

  // Master may delay sync response with "repl_diskless_sync_delay"
  PSyncResponse repl_header;

  RETURN_ON_ERR(ParseReplicationHeader(&io_buf, &repl_header));

  string* token = absl::get_if<string>(&repl_header.fullsync);
  size_t snapshot_size = SIZE_MAX;
  if (!token) {
    snapshot_size = absl::get<size_t>(repl_header.fullsync);
  }
  TouchIoTime();

  // we get token for diskless redis replication. For disk based replication
  // we get the snapshot size.
  if (snapshot_size || token != nullptr) {  // full sync
    // Start full sync
    state_mask_.fetch_or(R_SYNCING);

    io::PrefixSource ps{io_buf.InputBuffer(), Sock()};

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
    TouchIoTime();
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
    shard_flows_[i].reset(
        new DflyShardReplica(server(), master_context_, i, &service_, multi_shard_exe_));
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
      flow->Cancel();
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
  if (auto ec = SendNextPhaseRequest("SYNC"); ec) {
    return cntx_.ReportError(ec);
  }

  LOG(INFO) << absl::StrCat("Started full sync with ", server().Description());

  // Wait for all flows to receive full sync cut.
  // In case of an error, this is unblocked by the error handler.
  VLOG(1) << "Waiting for all full sync cut confirmations";
  sync_block.Wait();

  // Check if we woke up due to cancellation.
  if (cntx_.IsCancelled())
    return cntx_.GetError();

  // Send DFLY STARTSTABLE.
  if (auto ec = SendNextPhaseRequest("STARTSTABLE"); ec) {
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
  ResetParser(true);

  // Master waits for this command in order to start sending replication stream.
  RETURN_ON_ERR(SendCommand("REPLCONF ACK 0"));

  VLOG(1) << "Before reading repl-log";

  // Redis sends either pings every "repl_ping_slave_period" time inside replicationCron().
  // or, alternatively, write commands stream coming from propagate() function.
  // Replica connection must send "REPLCONF ACK xxx" in order to make sure that master replication
  // buffer gets disposed of already processed commands, this is done in a separate fiber.
  error_code ec;
  LOG(INFO) << "Transitioned into stable sync";
  facade::CmdArgVec args_vector;

  acks_fb_ = fb2::Fiber("redis_acks", &Replica::RedisStreamAcksFb, this);

  while (true) {
    auto response = ReadRespReply(&io_buf, /*copy_msg=*/false);
    if (!response.has_value()) {
      VLOG(1) << "ConsumeRedisStream finished";
      acks_fb_.JoinIfNeeded();
      return response.error();
    }

    if (!LastResponseArgs().empty()) {
      VLOG(2) << "Got command " << absl::CHexEscape(ToSV(LastResponseArgs()[0].GetBuf()))
              << "\n consumed: " << response->total_read;

      if (LastResponseArgs()[0].GetBuf()[0] == '\r') {
        for (const auto& arg : LastResponseArgs()) {
          LOG(INFO) << absl::CHexEscape(ToSV(arg.GetBuf()));
        }
      }

      facade::RespToArgList(LastResponseArgs(), &args_vector);
      CmdArgList arg_list{args_vector.data(), args_vector.size()};
      service_.DispatchCommand(arg_list, &conn_context);
    }

    io_buf.ConsumeInput(response->left_in_buffer);
    repl_offs_ += response->total_read;
    waker_.notify();  // Notify to trigger ACKs.
  }
}

error_code Replica::ConsumeDflyStream() {
  // Set new error handler that closes flow sockets.
  auto err_handler = [this](const auto& ge) {
    // Make sure the flows are not in a state transition
    lock_guard lk{flows_op_mu_};
    DefaultErrorHandler(ge);
    for (auto& flow : shard_flows_) {
      flow->Cancel();
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

void Replica::JoinAllFlows() {
  for (auto& flow : shard_flows_) {
    flow->JoinFlow();
  }
}

void Replica::SetShardStates(bool replica) {
  shard_set->RunBriefInParallel([replica](EngineShard* shard) { shard->SetReplica(replica); });
}

void Replica::DefaultErrorHandler(const GenericError& err) {
  CloseSocket();
}

error_code Replica::SendNextPhaseRequest(string_view kind) {
  // Ask master to start sending replication stream
  string request = StrCat("DFLY ", kind, " ", master_context_.dfly_session_id);

  VLOG(1) << "Sending: " << request;
  RETURN_ON_ERR(SendCommandAndReadResponse(request));

  PC_RETURN_ON_BAD_RESPONSE(CheckRespIsSimpleReply("OK"));

  return std::error_code{};
}

error_code DflyShardReplica::StartFullSyncFlow(BlockingCounter sb, Context* cntx) {
  DCHECK(!master_context_.master_repl_id.empty() && !master_context_.dfly_session_id.empty());

  RETURN_ON_ERR(ConnectAndAuth(absl::GetFlag(FLAGS_master_connect_timeout_ms) * 1ms, &cntx_));

  VLOG(1) << "Sending on flow " << master_context_.master_repl_id << " "
          << master_context_.dfly_session_id << " " << flow_id_;

  auto cmd = StrCat("DFLY FLOW ", master_context_.master_repl_id, " ",
                    master_context_.dfly_session_id, " ", flow_id_);

  ResetParser(/*server_mode=*/false);
  leftover_buf_.emplace(128);
  RETURN_ON_ERR(SendCommand(cmd));
  auto read_resp = ReadRespReply(&*leftover_buf_);
  if (!read_resp.has_value()) {
    return read_resp.error();
  }

  PC_RETURN_ON_BAD_RESPONSE(CheckRespFirstTypes({RespExpr::STRING, RespExpr::STRING}));

  string_view flow_directive = ToSV(LastResponseArgs()[0].GetBuf());
  string eof_token;
  PC_RETURN_ON_BAD_RESPONSE(flow_directive == "FULL");
  eof_token = ToSV(LastResponseArgs()[1].GetBuf());

  leftover_buf_->ConsumeInput(read_resp->left_in_buffer);

  // We can not discard io_buf because it may contain data
  // besides the response we parsed. Therefore we pass it further to ReplicateDFFb.
  sync_fb_ = fb2::Fiber("shard_full_sync", &DflyShardReplica::FullSyncDflyFb, this,
                        std::move(eof_token), sb, cntx);

  return error_code{};
}

error_code DflyShardReplica::StartStableSyncFlow(Context* cntx) {
  DCHECK(!master_context_.master_repl_id.empty() && !master_context_.dfly_session_id.empty());
  ProactorBase* mythread = ProactorBase::me();
  CHECK(mythread);

  CHECK(Sock()->IsOpen());
  sync_fb_ =
      fb2::Fiber("shard_stable_sync_read", &DflyShardReplica::StableSyncDflyReadFb, this, cntx);
  if (use_multi_shard_exe_sync_) {
    execution_fb_ =
        fb2::Fiber("shard_stable_sync_exec", &DflyShardReplica::StableSyncDflyExecFb, this, cntx);
  }

  return std::error_code{};
}

void DflyShardReplica::FullSyncDflyFb(const string& eof_token, BlockingCounter bc, Context* cntx) {
  DCHECK(leftover_buf_);
  io::PrefixSource ps{leftover_buf_->InputBuffer(), Sock()};

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

void DflyShardReplica::StableSyncDflyReadFb(Context* cntx) {
  // Check leftover from full sync.
  io::Bytes prefix{};
  if (leftover_buf_ && leftover_buf_->InputLen() > 0) {
    prefix = leftover_buf_->InputBuffer();
  }

  io::PrefixSource ps{prefix, Sock()};

  JournalReader reader{&ps, 0};
  TransactionReader tx_reader{};

  if (master_context_.version > DflyVersion::VER0) {
    acks_fb_ = fb2::Fiber("shard_acks", &DflyShardReplica::StableSyncDflyAcksFb, this, cntx);
  }

  while (!cntx->IsCancelled()) {
    waker_.await([&]() {
      return ((trans_data_queue_.size() < kYieldAfterItemsInQueue) || cntx->IsCancelled());
    });
    if (cntx->IsCancelled())
      break;

    auto tx_data = tx_reader.NextTxData(&reader, cntx);
    if (!tx_data)
      break;

    last_io_time_ = Proactor()->GetMonotonicTimeNs();

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

void Replica::RedisStreamAcksFb() {
  constexpr size_t kAckRecordMaxInterval = 1024;
  std::chrono::duration ack_time_max_interval =
      1ms * absl::GetFlag(FLAGS_replication_acks_interval);
  std::string ack_cmd;
  auto next_ack_tp = std::chrono::steady_clock::now();

  while (!cntx_.IsCancelled()) {
    VLOG(1) << "Sending an ACK with offset=" << repl_offs_;
    ack_cmd = absl::StrCat("REPLCONF ACK ", repl_offs_);
    next_ack_tp = std::chrono::steady_clock::now() + ack_time_max_interval;
    if (auto ec = SendCommand(ack_cmd); ec) {
      cntx_.ReportError(ec);
      break;
    }
    ack_offs_ = repl_offs_;

    waker_.await_until(
        [&]() { return repl_offs_ > ack_offs_ + kAckRecordMaxInterval || cntx_.IsCancelled(); },
        next_ack_tp);
  }
}

void DflyShardReplica::StableSyncDflyAcksFb(Context* cntx) {
  constexpr size_t kAckRecordMaxInterval = 1024;
  std::chrono::duration ack_time_max_interval =
      1ms * absl::GetFlag(FLAGS_replication_acks_interval);
  std::string ack_cmd;
  auto next_ack_tp = std::chrono::steady_clock::now();

  uint64_t current_offset;
  while (!cntx->IsCancelled()) {
    // Handle ACKs with the master. PING opcodes from the master mean we should immediately
    // answer.
    current_offset = journal_rec_executed_.load(std::memory_order_relaxed);
    VLOG(1) << "Sending an ACK with offset=" << current_offset << " forced=" << force_ping_;
    ack_cmd = absl::StrCat("REPLCONF ACK ", current_offset);
    force_ping_ = false;
    next_ack_tp = std::chrono::steady_clock::now() + ack_time_max_interval;
    if (auto ec = SendCommand(ack_cmd); ec) {
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

DflyShardReplica::DflyShardReplica(ServerContext server_context, MasterContext master_context,
                                   uint32_t flow_id, Service* service,
                                   std::shared_ptr<MultiShardExecution> multi_shard_exe)
    : ProtocolClient(server_context), service_(*service), master_context_(master_context),
      multi_shard_exe_(multi_shard_exe), flow_id_(flow_id) {
  use_multi_shard_exe_sync_ = GetFlag(FLAGS_enable_multi_shard_sync);
  executor_ = std::make_unique<JournalExecutor>(service);
}

DflyShardReplica::~DflyShardReplica() {
  JoinFlow();
}

void DflyShardReplica::ExecuteTxWithNoShardSync(TransactionData&& tx_data, Context* cntx) {
  if (cntx->IsCancelled()) {
    return;
  }

  bool was_insert = false;
  if (tx_data.IsGlobalCmd()) {
    was_insert = InsertTxToSharedMap(tx_data);
  }

  ExecuteTx(std::move(tx_data), was_insert, cntx);
}

bool DflyShardReplica::InsertTxToSharedMap(const TransactionData& tx_data) {
  std::lock_guard lk{multi_shard_exe_->map_mu};

  auto [it, was_insert] =
      multi_shard_exe_->tx_sync_execution.emplace(tx_data.txid, tx_data.shard_cnt);
  VLOG(2) << "txid: " << tx_data.txid << " unique_shard_cnt_: " << tx_data.shard_cnt
          << " was_insert: " << was_insert;
  it->second.block.Dec();

  return was_insert;
}

void DflyShardReplica::InsertTxDataToShardResource(TransactionData&& tx_data) {
  bool was_insert = false;
  if (tx_data.shard_cnt > 1) {
    was_insert = InsertTxToSharedMap(tx_data);
  }

  VLOG(2) << "txid: " << tx_data.txid << " pushed to queue";
  trans_data_queue_.emplace(std::move(tx_data), was_insert);
}

void DflyShardReplica::StableSyncDflyExecFb(Context* cntx) {
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

void DflyShardReplica::ExecuteTx(TransactionData&& tx_data, bool inserted_by_me, Context* cntx) {
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

Replica::Info Replica::GetInfo() const {
  auto f = [this]() {
    auto last_io_time = LastIoTime();
    for (const auto& flow : shard_flows_) {  // Get last io time from all sub flows.
      last_io_time = std::max(last_io_time, flow->LastIoTime());
    }

    Info res;
    res.host = server().host;
    res.port = server().port;
    res.master_link_established = (state_mask_.load() & R_TCP_CONNECTED);
    res.full_sync_in_progress = (state_mask_.load() & R_SYNCING);
    res.full_sync_done = (state_mask_.load() & R_SYNC_OK);
    res.master_last_io_sec = (ProactorBase::GetMonotonicTimeNs() - last_io_time) / 1000000000UL;
    return res;
  };

  if (Sock())
    return Proactor()->AwaitBrief(f);
  else {
    /**
     * when this branch happens: there is a very short grace period
     * where Sock() is not initialized, yet the server can
     * receive ROLE/INFO commands. That period happens when launching
     * an instance with '--replicaof' and then immediately
     * sending a command.
     *
     * In that instance, we have to run f() on the current fiber.
     */
    return f();
  }
}

std::vector<uint64_t> Replica::GetReplicaOffset() const {
  std::vector<uint64_t> flow_rec_count;
  flow_rec_count.resize(shard_flows_.size());
  for (const auto& flow : shard_flows_) {
    uint32_t flow_id = flow->FlowId();
    uint64_t rec_count = flow->JournalExecutedCount();
    DCHECK_LT(flow_id, shard_flows_.size());
    flow_rec_count[flow_id] = rec_count;
  }
  return flow_rec_count;
}

std::string Replica::GetSyncId() const {
  return master_context_.dfly_session_id;
}

bool DflyShardReplica::TransactionData::AddEntry(journal::ParsedEntry&& entry) {
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

bool DflyShardReplica::TransactionData::IsGlobalCmd() const {
  if (commands.size() > 1) {
    return false;
  }

  auto& command = commands.front();
  if (command.cmd_args.empty()) {
    return false;
  }

  auto& args = command.cmd_args;
  if (absl::EqualsIgnoreCase(ToSV(args[0]), "FLUSHDB"sv) ||
      absl::EqualsIgnoreCase(ToSV(args[0]), "FLUSHALL"sv) ||
      (absl::EqualsIgnoreCase(ToSV(args[0]), "DFLYCLUSTER"sv) &&
       absl::EqualsIgnoreCase(ToSV(args[1]), "FLUSHSLOTS"sv))) {
    return true;
  }

  return false;
}

DflyShardReplica::TransactionData DflyShardReplica::TransactionData::FromSingle(
    journal::ParsedEntry&& entry) {
  TransactionData data;
  bool res = data.AddEntry(std::move(entry));
  DCHECK(res);
  return data;
}

auto DflyShardReplica::TransactionReader::NextTxData(JournalReader* reader, Context* cntx)
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

uint32_t DflyShardReplica::FlowId() const {
  return flow_id_;
}

uint64_t DflyShardReplica::JournalExecutedCount() const {
  return journal_rec_executed_.load(std::memory_order_relaxed);
}

void DflyShardReplica::JoinFlow() {
  sync_fb_.JoinIfNeeded();
  acks_fb_.JoinIfNeeded();
  execution_fb_.JoinIfNeeded();
}

void DflyShardReplica::Cancel() {
  CloseSocket();
  waker_.notifyAll();
}

}  // namespace dfly
