// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/replica.h"

#include <chrono>

#include "absl/strings/match.h"
#include "facade/service_interface.h"

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
#include "facade/redis_parser.h"
#include "facade/socket_utils.h"
#include "server/error.h"
#include "server/journal/executor.h"
#include "server/journal/serializer.h"
#include "server/main_service.h"
#include "server/rdb_load.h"
#include "strings/human_readable.h"

#define LOG_REPL_ERROR(msg)                                         \
  do {                                                              \
    if (state_mask_ & R_ENABLED) {                                  \
      if ((state_mask_ & R_SYNCING) || (state_mask_ & R_SYNC_OK)) { \
        LOG(WARNING) << msg;                                        \
      } else {                                                      \
        LOG(ERROR) << msg;                                          \
      }                                                             \
    } else {                                                        \
      VLOG(1) << msg;                                               \
    }                                                               \
  } while (0)

ABSL_FLAG(int, replication_acks_interval, 1000, "Interval between acks in milliseconds.");
ABSL_FLAG(int, master_connect_timeout_ms, 20000,
          "Timeout for establishing connection to a replication master");
ABSL_FLAG(int, master_reconnect_timeout_ms, 1000,
          "Timeout for re-establishing connection to a replication master");
ABSL_FLAG(bool, replica_partial_sync, true,
          "Use partial sync to reconnect when a replica connection is interrupted.");
ABSL_FLAG(bool, break_replication_on_master_restart, false,
          "When in replica mode, and master restarts, break replication from master to avoid "
          "flushing the replica's data.");
ABSL_FLAG(std::string, replica_announce_ip, "",
          "IP address that Dragonfly announces to replication master");
ABSL_DECLARE_FLAG(int32_t, port);
ABSL_DECLARE_FLAG(uint16_t, announce_port);
ABSL_FLAG(
    int, replica_priority, 100,
    "Published by info command for sentinel to pick replica based on score during a failover");

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

Replica::Replica(string host, uint16_t port, Service* se, std::string_view id,
                 std::optional<cluster::SlotRange> slot_range)
    : ProtocolClient(std::move(host), port), service_(*se), id_{id}, slot_range_(slot_range) {
  proactor_ = ProactorBase::me();
}

Replica::~Replica() {
  sync_fb_.JoinIfNeeded();
  acks_fb_.JoinIfNeeded();
}

static const char kConnErr[] = "could not connect to master: ";

GenericError Replica::Start() {
  VLOG(1) << "Starting replication " << this;
  ProactorBase* mythread = ProactorBase::me();
  CHECK(mythread);

  auto check_connection_error = [this](error_code ec, const char* msg) -> GenericError {
    if (!exec_st_.IsRunning()) {
      return {"replication cancelled"};
    }
    if (ec) {
      exec_st_.ReportCancelError();
      return {absl::StrCat(msg, ec.message())};
    }
    return ec;
  };

  // 0. Set basic error handler that is reponsible for cleaning up on errors.
  // Can return an error only if replication was cancelled immediately.
  auto err = exec_st_.SwitchErrorHandler([this](const auto& ge) { this->DefaultErrorHandler(ge); });
  RETURN_ON_GENERIC_ERR(check_connection_error(err, "replication cancelled"));

  // 1. Resolve dns.
  VLOG(1) << "Resolving master DNS";
  error_code ec = ResolveHostDns();
  RETURN_ON_GENERIC_ERR(check_connection_error(ec, "could not resolve master dns"));

  // 2. Connect socket.
  VLOG(1) << "Connecting to master";
  ec = ConnectAndAuth(absl::GetFlag(FLAGS_master_connect_timeout_ms) * 1ms, &exec_st_);
  RETURN_ON_GENERIC_ERR(check_connection_error(ec, kConnErr));

  // 3. Greet.
  VLOG(1) << "Greeting";
  state_mask_ = R_ENABLED | R_TCP_CONNECTED;
  ec = Greet();
  RETURN_ON_ERR(check_connection_error(ec, "could not greet master "));

  return {};
}

void Replica::StartMainReplicationFiber(std::optional<LastMasterSyncData> last_master_sync_data) {
  sync_fb_ = fb2::Fiber("main_replication", &Replica::MainReplicationFb, this,
                        std::move(last_master_sync_data));
}

void Replica::EnableReplication() {
  VLOG(1) << "Enabling replication";

  state_mask_ = R_ENABLED;                                           // set replica state to enabled
  sync_fb_ = MakeFiber(&Replica::MainReplicationFb, this, nullopt);  // call replication fiber
}

std::optional<Replica::LastMasterSyncData> Replica::Stop() {
  VLOG(1) << "Stopping replication " << this;
  // Stops the loop in MainReplicationFb.

  proactor_->Await([this] {
    state_mask_ = 0;               // Specifically ~R_ENABLED.
    exec_st_.ReportCancelError();  // Context is fully resposible for cleanup.
  });

  // Make sure the replica fully stopped and did all cleanup,
  // so we can freely release resources (connections).
  sync_fb_.JoinIfNeeded();
  DVLOG(1) << "MainReplicationFb stopped " << this;
  acks_fb_.JoinIfNeeded();
  for (auto& flow : shard_flows_) {
    flow.reset();
  }

  if (last_journal_LSNs_.has_value()) {
    return LastMasterSyncData{master_context_.master_repl_id, last_journal_LSNs_.value()};
  }
  return nullopt;
}

void Replica::Pause(bool pause) {
  VLOG(1) << "Pausing replication";
  Proactor()->Await([&] {
    is_paused_ = pause;
    if (shard_flows_.empty())
      return;

    auto cb = [&](unsigned index, auto*) {
      for (auto id : thread_flow_map_[index]) {
        shard_flows_[id]->Pause(pause);
      }
    };
    shard_set->pool()->AwaitBrief(cb);
  });
}

std::error_code Replica::TakeOver(std::string_view timeout, bool save_flag) {
  VLOG(1) << "Taking over";

  std::error_code ec;
  auto takeOverCmd = absl::StrCat("TAKEOVER ", timeout, (save_flag ? " SAVE" : ""));
  Proactor()->Await([this, &ec, cmd = std::move(takeOverCmd)] { ec = SendNextPhaseRequest(cmd); });

  // If we successfully taken over, return and let server_family stop the replication.
  return ec;
}

void Replica::MainReplicationFb(std::optional<LastMasterSyncData> last_master_sync_data) {
  VLOG(1) << "Main replication fiber started " << this;
  // Switch shard states to replication.
  SetShardStates(true);

  error_code ec;
  while (state_mask_ & R_ENABLED) {
    // Discard all previous errors and set default error handler.
    exec_st_.Reset([this](const GenericError& ge) { this->DefaultErrorHandler(ge); });
    // 1. Connect socket.
    if ((state_mask_ & R_TCP_CONNECTED) == 0) {
      ThisFiber::SleepFor(500ms);
      if (is_paused_)
        continue;

      ec = ResolveHostDns();
      if (ec) {
        LOG(ERROR) << "Error resolving dns to " << server().host << " (phase: " << GetCurrentPhase()
                   << "): " << ec;
        continue;
      }

      // Give a lower timeout for connect, because we're
      reconnect_count_++;
      ec = ConnectAndAuth(absl::GetFlag(FLAGS_master_reconnect_timeout_ms) * 1ms, &exec_st_);
      if (ec) {
        LOG(WARNING) << "Error connecting to " << server().Description()
                     << " (phase: " << GetCurrentPhase() << "): " << ec
                     << ", reason: " << ec.message();
        continue;
      }
      VLOG(1) << "Replica socket connected";
      state_mask_ |= R_TCP_CONNECTED;
      continue;
    }

    DCHECK(Proactor() == proactor_);

    // 2. Greet.
    if ((state_mask_ & R_GREETED) == 0) {
      ec = Greet();
      if (ec) {
        LOG(WARNING) << "Error greeting " << server().Description()
                     << " (phase: " << GetCurrentPhase() << "): " << ec << " " << ec.message()
                     << ", socket state: " + GetSocketInfo(Sock()->native_handle());
        state_mask_ &= R_ENABLED;
        continue;
      }
      state_mask_ |= R_GREETED;
      continue;
    }

    // 3. Initiate full sync
    if ((state_mask_ & R_SYNC_OK) == 0) {
      if (HasDflyMaster()) {
        ec = InitiateDflySync(std::exchange(last_master_sync_data, nullopt));
      } else
        ec = InitiatePSync();

      if (ec) {
        LOG(WARNING) << "Error syncing with " << server().Description()
                     << " (phase: " << GetCurrentPhase() << "): " << ec << " " << ec.message()
                     << ", socket state: " + GetSocketInfo(Sock()->native_handle());
        state_mask_ &= R_ENABLED;  // reset all flags besides R_ENABLED
        continue;
      }
      state_mask_ |= R_SYNC_OK;
      continue;
    }

    // 4. Start stable state sync.
    DCHECK(state_mask_ & R_SYNC_OK);

    if (HasDflyMaster())
      ec = ConsumeDflyStream();
    else
      ec = ConsumeRedisStream();

    auto state = state_mask_ &= R_ENABLED;
    if (state & R_ENABLED) {  // replication was not stopped.
      LOG(WARNING) << "Error stable sync with " << server().Description()
                   << " (phase: " << GetCurrentPhase() << "): " << ec << " " << ec.message()
                   << ", socket state: " + GetSocketInfo(Sock()->native_handle());
    }
  }

  // Wait for unblocking cleanup to finish.
  exec_st_.JoinErrorHandler();

  // Revert shard states to normal state.
  SetShardStates(false);

  VLOG(1) << "Main replication fiber finished";
}

error_code Replica::Greet() {
  ResetParser(RedisParser::Mode::CLIENT);
  VLOG(1) << "greeting message handling";
  // Corresponds to server.repl_state == REPL_STATE_CONNECTING state in redis
  RETURN_ON_ERR(SendCommandAndReadResponse("PING"));  // optional.
  PC_RETURN_ON_BAD_RESPONSE(CheckRespIsSimpleReply("PONG"));

  // Corresponds to server.repl_state == REPL_STATE_SEND_HANDSHAKE condition in replication.c
  uint16_t port = absl::GetFlag(FLAGS_announce_port);
  if (port == 0) {
    port = static_cast<uint16_t>(absl::GetFlag(FLAGS_port));
  }
  RETURN_ON_ERR(SendCommandAndReadResponse(StrCat("REPLCONF listening-port ", port)));
  PC_RETURN_ON_BAD_RESPONSE(CheckRespIsSimpleReply("OK"));

  auto announce_ip = absl::GetFlag(FLAGS_replica_announce_ip);
  if (!announce_ip.empty()) {
    RETURN_ON_ERR(SendCommandAndReadResponse(StrCat("REPLCONF ip-address ", announce_ip)));
    LOG_IF(WARNING, !CheckRespIsSimpleReply("OK"))
        << "Master did not OK announced IP address, perhaps it is using an old version";
  }

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

  state_mask_ |= R_GREETED;
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

  DCHECK(proactor_ == Proactor());

  // If we're syncing a different replication ID, drop the saved LSNs.
  string_view master_repl_id = ToSV(LastResponseArgs()[0].GetBuf());
  if (master_context_.master_repl_id != master_repl_id) {
    if (absl::GetFlag(FLAGS_break_replication_on_master_restart) &&
        !master_context_.master_repl_id.empty()) {
      LOG(ERROR) << "Encountered different master repl id (" << master_repl_id << " vs "
                 << master_context_.master_repl_id << ")";
      state_mask_ = 0;
      return make_error_code(errc::connection_aborted);
    }
    last_journal_LSNs_.reset();
  }
  master_context_.master_repl_id = master_repl_id;
  master_context_.dfly_session_id = ToSV(LastResponseArgs()[1].GetBuf());
  master_context_.num_flows = param_num_flows;

  if (LastResponseArgs().size() >= 4) {
    PC_RETURN_ON_BAD_RESPONSE(LastResponseArgs()[3].type == RespExpr::INT64);
    master_context_.version = DflyVersion(get<int64_t>(LastResponseArgs()[3].u));
  }
  VLOG(1) << "Master id: " << master_context_.master_repl_id
          << ", sync id: " << master_context_.dfly_session_id
          << ", num journals: " << param_num_flows
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

  RETURN_ON_ERR(
      SendCommandAndReadResponse(StrCat("REPLCONF CLIENT-VERSION ", DflyVersion::CURRENT_VER)));
  PC_RETURN_ON_BAD_RESPONSE(CheckRespIsSimpleReply("OK"));

  return error_code{};
}

error_code Replica::InitiatePSync() {
  base::IoBuf io_buf{128};

  // Corresponds to server.repl_state == REPL_STATE_SEND_PSYNC
  string id("?");  // corresponds to null master id and null offset
  int64_t offs = -1;
  if (!master_context_.master_repl_id.empty()) {  // in case we synced before
    id = master_context_.master_repl_id;          // provide the replication offset and master id
    // TBD: for incremental sync send repl_offs_, not supported yet.
    // offs = repl_offs_;
  }

  RETURN_ON_ERR(SendCommand(StrCat("PSYNC ", id, " ", offs)));

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
  if (snapshot_size || token != nullptr) {
    LOG(INFO) << "Starting full sync with Redis master";

    state_mask_ |= R_SYNCING;

    io::PrefixSource ps{io_buf.InputBuffer(), Sock()};

    // Set LOADING state.
    if (!service_.RequestLoadingState()) {
      return exec_st_.ReportError(std::make_error_code(errc::state_not_recoverable),
                                  "Failed to enter LOADING state");
    }

    absl::Cleanup cleanup = [this]() { service_.RemoveLoadingState(); };

    if (slot_range_.has_value()) {
      JournalExecutor{&service_}.FlushSlots(slot_range_.value());
    } else {
      JournalExecutor{&service_}.FlushAll();
    }

    RdbLoader loader(NULL);
    loader.SetLoadUnownedSlots(true);
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
  } else {
    LOG(INFO) << "Re-established sync with Redis master with ID=" << id;
  }

  state_mask_ &= ~R_SYNCING;
  state_mask_ |= R_SYNC_OK;

  // There is a data race condition in Redis-master code, where "ACK 0" handler may be
  // triggered before Redis is ready to transition to the streaming state and it silenty ignores
  // "ACK 0". We reduce the chance it happens with this delay.
  ThisFiber::SleepFor(50ms);

  return error_code{};
}

// Initialize and start sub-replica for each flow.
error_code Replica::InitiateDflySync(std::optional<LastMasterSyncData> last_master_sync_data) {
  auto start_time = absl::Now();

  // Initialize MultiShardExecution.
  multi_shard_exe_.reset(new MultiShardExecution());

  // Initialize shard flows.
  shard_flows_.resize(master_context_.num_flows);
  DCHECK(!shard_flows_.empty());
  for (unsigned i = 0; i < shard_flows_.size(); ++i) {
    shard_flows_[i].reset(
        new DflyShardReplica(server(), master_context_, i, &service_, multi_shard_exe_));
  }
  thread_flow_map_ = Partition(shard_flows_.size());

  // Blocked on until all flows got full sync cut.
  BlockingCounter sync_block{unsigned(shard_flows_.size())};

  // Switch to new error handler that closes flow sockets.
  auto err_handler = [this, sync_block](const auto& ge) mutable {
    // Unblock this function.
    sync_block->Cancel();

    // Make sure the flows are not in a state transition
    lock_guard lk{flows_op_mu_};

    // Unblock all sockets.
    DefaultErrorHandler(ge);
    for (auto& flow : shard_flows_)
      flow->Cancel();
  };

  RETURN_ON_ERR(exec_st_.SwitchErrorHandler(std::move(err_handler)));

  // Start full sync flows.
  state_mask_ |= R_SYNCING;

  std::string_view sync_type;
  absl::Cleanup cleanup = [this, &sync_type]() {
    // We do the following operations regardless of outcome.
    JoinDflyFlows();
    if (sync_type == "full") {
      service_.RemoveLoadingState();
    } else if (service_.IsLoadingState()) {
      // We need this check. We originally set the state unconditionally to LOADING
      // when we call ReplicaOf command. If for some reason we fail to start full sync below
      // or cancel the context, we still need to switch to ACTIVE state.
      // TODO(kostasrim) we can remove this once my proposed changes for replication move forward
      // as the state transitions for ReplicaOf command will be much clearer.
      service_.SwitchState(GlobalState::LOADING, GlobalState::ACTIVE);
    }
    state_mask_ &= ~R_SYNCING;
    last_journal_LSNs_.reset();
  };

  {
    unsigned num_df_flows = shard_flows_.size();
    // Going out of the way to avoid using std::vector<bool>...
    auto is_full_sync = std::make_unique<bool[]>(num_df_flows);
    // The elements of this bool array are not always initialized but we call std::accumulate below
    // unconditionally. For some cases this will accumulate whatever junk that uninitialized memory
    // cell contain. Do not remove the memset below.
    std::memset(is_full_sync.get(), 0, num_df_flows);
    DCHECK(!last_journal_LSNs_ || last_journal_LSNs_->size() == num_df_flows);
    auto shard_cb = [&](unsigned index, auto*) {
      for (auto id : thread_flow_map_[index]) {
        auto ec = shard_flows_[id]->StartSyncFlow(sync_block, &exec_st_,
                                                  last_journal_LSNs_.has_value()
                                                      ? std::optional((*last_journal_LSNs_)[id])
                                                      : std::nullopt,
                                                  last_master_sync_data);
        if (ec.has_value())
          is_full_sync[id] = ec.value();
        else
          exec_st_.ReportError(ec.error());
      }
    };
    // Lock to prevent the error handler from running instantly
    // while the flows are in a mixed state.
    lock_guard lk{flows_op_mu_};

    shard_set->pool()->AwaitFiberOnAll(std::move(shard_cb));
    if (last_journal_LSNs_) {
      ++psync_attempts_;
    }

    last_journal_LSNs_.reset();
    size_t num_full_flows =
        std::accumulate(is_full_sync.get(), is_full_sync.get() + num_df_flows, 0);

    if (num_full_flows == num_df_flows) {
      // Make sure we're in LOADING state.
      if (!service_.RequestLoadingState()) {
        return exec_st_.ReportError(std::make_error_code(errc::state_not_recoverable),
                                    "Failed to enter LOADING state");
      }
      sync_type = "full";

      DVLOG(1) << "Calling Flush on all slots " << this;

      if (slot_range_.has_value()) {
        JournalExecutor{&service_}.FlushSlots(slot_range_.value());
      } else {
        JournalExecutor{&service_}.FlushAll();
      }
      DVLOG(1) << "Flush on all slots ended " << this;
    } else if (num_full_flows == 0) {
      sync_type = "partial";
    } else {
      exec_st_.ReportError(std::make_error_code(errc::state_not_recoverable),
                           "Won't do a partial sync: some flows must fully resync");
    }
  }

  RETURN_ON_ERR(exec_st_.GetError());

  // Send DFLY SYNC.
  if (auto ec = SendNextPhaseRequest("SYNC"); ec) {
    return exec_st_.ReportError(ec);
  }

  LOG(INFO) << "Started " << sync_type << " sync with " << server().Description();

  // Wait for all flows to receive full sync cut.
  // In case of an error, this is unblocked by the error handler.
  VLOG(1) << "Waiting for all full sync cut confirmations";
  sync_block->Wait();

  // Check if we woke up due to cancellation.
  if (!exec_st_.IsRunning())
    return exec_st_.GetError();

  RdbLoader::PerformPostLoad(&service_);

  // Send DFLY STARTSTABLE.
  if (auto ec = SendNextPhaseRequest("STARTSTABLE"); ec) {
    return exec_st_.ReportError(ec);
  }

  // Joining flows and resetting state is done by cleanup.
  double seconds = double(absl::ToInt64Milliseconds(absl::Now() - start_time)) / 1000;
  LOG(INFO) << sync_type << " sync finished in " << strings::HumanReadableElapsedTime(seconds);

  if (sync_type == "partial") {
    ++psync_successes_;
  }

  return exec_st_.GetError();
}

error_code Replica::ConsumeRedisStream() {
  base::IoBuf io_buf(16_KB);
  ConnectionContext conn_context{nullptr, {}};
  conn_context.is_replicating = true;
  conn_context.journal_emulated = true;
  conn_context.skip_acl_validation = true;
  conn_context.ns = &namespaces->GetDefaultNamespace();

  // we never reply back on the commands.
  facade::CapturingReplyBuilder null_builder{facade::ReplyMode::NONE};
  ResetParser(RedisParser::Mode::SERVER);

  // Master waits for this command in order to start sending replication stream.
  RETURN_ON_ERR(SendCommand("REPLCONF ACK 0"));

  VLOG(1) << "Before reading repl-log";

  // Redis sends either pings every "repl_ping_slave_period" time inside replicationCron().
  // or, alternatively, write commands stream coming from propagate() function.
  // Replica connection must send "REPLCONF ACK xxx" in order to make sure that master replication
  // buffer gets disposed of already processed commands, this is done in a separate fiber.
  error_code ec;
  LOG(INFO) << "Transitioned into stable sync";

  // Set new error handler.
  auto err_handler = [this](const auto& ge) {
    // Trigger ack-fiber
    replica_waker_.notifyAll();
    DefaultErrorHandler(ge);
  };
  RETURN_ON_ERR(exec_st_.SwitchErrorHandler(std::move(err_handler)));

  facade::CmdArgVec args_vector;

  acks_fb_ = fb2::Fiber("redis_acks", &Replica::RedisStreamAcksFb, this);

  while (true) {
    auto response = ReadRespReply(&io_buf, /*copy_msg=*/false);
    if (!response.has_value()) {
      LOG_REPL_ERROR("Error in Redis Stream at phase "
                     << GetCurrentPhase() << " with " << server().Description()
                     << ", error: " << response.error()
                     << ", socket state: " + GetSocketInfo(Sock()->native_handle()));
      exec_st_.ReportError(response.error());
      acks_fb_.JoinIfNeeded();
      return response.error();
    }

    if (!LastResponseArgs().empty()) {
      string cmd = absl::CHexEscape(ToSV(LastResponseArgs()[0].GetBuf()));

      // Valkey and Redis may send MULTI and EXEC as part of their replication commands.
      // Dragonfly disallows some commands, such as SELECT, inside of MULTI/EXEC, so here we simply
      // ignore MULTI/EXEC and execute their inner commands individually.
      if (!absl::EqualsIgnoreCase(cmd, "MULTI") && !absl::EqualsIgnoreCase(cmd, "EXEC")) {
        VLOG(2) << "Got command " << cmd << "\n consumed: " << response->total_read;

        if (LastResponseArgs()[0].GetBuf()[0] == '\r') {
          for (const auto& arg : LastResponseArgs()) {
            LOG(INFO) << absl::CHexEscape(ToSV(arg.GetBuf()));
          }
        }

        facade::RespExpr::VecToArgList(LastResponseArgs(), &args_vector);
        CmdArgList arg_list{args_vector.data(), args_vector.size()};
        service_.DispatchCommand(arg_list, &null_builder, &conn_context);
      }
    }

    io_buf.ConsumeInput(response->left_in_buffer);
    repl_offs_ += response->total_read;
    replica_waker_.notify();  // Notify to trigger ACKs.
  }
}

error_code Replica::ConsumeDflyStream() {
  // Set new error handler that closes flow sockets.
  auto err_handler = [this](const auto& ge) {
    // Make sure the flows are not in a state transition
    lock_guard lk{flows_op_mu_};

    LOG_REPL_ERROR("Replication error in phase "
                   << GetCurrentPhase() << " with " << server().Description() << ", error: "
                   << ge.Format() << ", socket state: " + GetSocketInfo(Sock()->native_handle()));

    DefaultErrorHandler(ge);
    for (auto& flow : shard_flows_) {
      flow->Cancel();
    }
    multi_shard_exe_->CancelAllBlockingEntities();
  };
  RETURN_ON_ERR(exec_st_.SwitchErrorHandler(std::move(err_handler)));

  LOG(INFO) << "Transitioned into stable sync";
  // Transition flows into stable sync.
  {
    auto shard_cb = [&](unsigned index, auto*) {
      const auto& local_ids = thread_flow_map_[index];
      for (unsigned id : local_ids) {
        auto ec = shard_flows_[id]->StartStableSyncFlow(&exec_st_);
        if (ec)
          exec_st_.ReportError(ec);
      }
    };

    // Lock to prevent error handler from running on mixed state.
    lock_guard lk{flows_op_mu_};
    shard_set->pool()->AwaitFiberOnAll(std::move(shard_cb));
  }

  JoinDflyFlows();

  last_journal_LSNs_.emplace();
  for (auto& flow : shard_flows_) {
    last_journal_LSNs_->push_back(flow->JournalExecutedCount());
  }

  LOG(INFO) << "Exit stable sync";
  // The only option to unblock is to cancel the context.
  CHECK(exec_st_.GetError());

  return exec_st_.GetError();
}

void Replica::JoinDflyFlows() {
  for (auto& flow : shard_flows_) {
    flow->JoinFlow();
  }
}

void Replica::SetShardStates(bool replica) {
  shard_set->RunBriefInParallel([replica](EngineShard* shard) { shard->SetReplica(replica); });
}

error_code Replica::SendNextPhaseRequest(string_view kind) {
  // Ask master to start sending replication stream
  string request = StrCat("DFLY ", kind, " ", master_context_.dfly_session_id);

  VLOG(1) << "Sending: " << request;
  RETURN_ON_ERR(SendCommandAndReadResponse(request));

  PC_RETURN_ON_BAD_RESPONSE(CheckRespIsSimpleReply("OK"));

  return std::error_code{};
}

io::Result<bool> DflyShardReplica::StartSyncFlow(
    BlockingCounter sb, ExecutionState* cntx, std::optional<LSN> lsn,
    std::optional<Replica::LastMasterSyncData> last_master_data) {
  using nonstd::make_unexpected;
  DCHECK(!master_context_.master_repl_id.empty() && !master_context_.dfly_session_id.empty());
  proactor_index_ = ProactorBase::me()->GetPoolIndex();

  RETURN_ON_ERR_T(make_unexpected,
                  ConnectAndAuth(absl::GetFlag(FLAGS_master_connect_timeout_ms) * 1ms, &exec_st_));

  VLOG(1) << "Sending on flow " << master_context_.master_repl_id << " "
          << master_context_.dfly_session_id << " " << flow_id_;

  // DFLY FLOW <master_id> <session_id> <flow_id> [lsn] [last_master_id lsn-vec]
  std::string cmd = StrCat("DFLY FLOW ", master_context_.master_repl_id, " ",
                           master_context_.dfly_session_id, " ", flow_id_);
  // Try to negotiate a partial sync if possible.
  if (lsn.has_value() && master_context_.version > DflyVersion::VER1 &&
      absl::GetFlag(FLAGS_replica_partial_sync)) {
    absl::StrAppend(&cmd, " ", *lsn);
  }
  if (last_master_data && master_context_.version >= DflyVersion::VER5 &&
      absl::GetFlag(FLAGS_replica_partial_sync)) {
    string lsn_str = absl::StrJoin(last_master_data.value().last_journal_LSNs, "-");
    absl::StrAppend(&cmd, " ", last_master_data.value().id, " ", lsn_str);
    VLOG(1) << "Sending last master sync flow " << last_master_data.value().id << " " << lsn_str;
  }

  ResetParser(RedisParser::Mode::CLIENT);
  leftover_buf_.emplace(128);
  RETURN_ON_ERR_T(make_unexpected, SendCommand(cmd));
  auto read_resp = ReadRespReply(&*leftover_buf_);
  if (!read_resp.has_value()) {
    return make_unexpected(read_resp.error());
  }

  PC_RETURN_ON_BAD_RESPONSE_T(make_unexpected,
                              CheckRespFirstTypes({RespExpr::STRING, RespExpr::STRING}));

  string_view flow_directive = ToSV(LastResponseArgs()[0].GetBuf());
  string eof_token;
  PC_RETURN_ON_BAD_RESPONSE_T(make_unexpected,
                              flow_directive == "FULL" || flow_directive == "PARTIAL");
  bool is_full_sync = flow_directive == "FULL";

  eof_token = ToSV(LastResponseArgs()[1].GetBuf());

  leftover_buf_->ConsumeInput(read_resp->left_in_buffer);

  // We can not discard io_buf because it may contain data
  // besides the response we parsed. Therefore we pass it further to ReplicateDFFb.
  sync_fb_ = fb2::Fiber("shard_full_sync", &DflyShardReplica::FullSyncDflyFb, this,
                        std::move(eof_token), sb, cntx);

  return is_full_sync;
}

error_code DflyShardReplica::StartStableSyncFlow(ExecutionState* cntx) {
  DCHECK(!master_context_.master_repl_id.empty() && !master_context_.dfly_session_id.empty());
  ProactorBase* mythread = ProactorBase::me();
  CHECK(mythread);

  if (!Sock()->IsOpen()) {
    return std::make_error_code(errc::io_error);
  }
  rdb_loader_.reset();  // we do not need it anymore.
  sync_fb_ =
      fb2::Fiber("shard_stable_sync_read", &DflyShardReplica::StableSyncDflyReadFb, this, cntx);

  return std::error_code{};
}

void DflyShardReplica::FullSyncDflyFb(std::string eof_token, BlockingCounter bc,
                                      ExecutionState* cntx) {
  DCHECK(leftover_buf_);
  io::PrefixSource ps{leftover_buf_->InputBuffer(), Sock()};

  rdb_loader_->SetFullSyncCutCb([bc, ran = false]() mutable {
    if (!ran) {
      bc->Dec();
      ran = true;
    }
  });

  // In the no point-in-time replication flow, it's possible to serialize a journal change
  // before serializing the bucket that the key was updated in on the master side. As a result,
  // when loading the serialized bucket data on the replica, it may overwrite the earlier entry
  // added by the journal change. This is an expected and valid scenario, so to avoid unnecessary
  // warnings, we enable SetOverrideExistingKeys(true).
  rdb_loader_->SetOverrideExistingKeys(true);

  // Load incoming rdb stream.
  if (std::error_code ec = rdb_loader_->Load(&ps); ec) {
    cntx->ReportError(ec, "Error loading rdb format");
    return;
  }

  // Try finding eof token.
  io::PrefixSource chained_tail{rdb_loader_->Leftover(), &ps};
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
  if (!unused.empty()) {
    leftover_buf_.emplace(unused.size());
    leftover_buf_->WriteAndCommit(unused.data(), unused.size());
  } else {
    leftover_buf_.reset();
  }

  if (auto jo = rdb_loader_->journal_offset(); jo.has_value()) {
    this->journal_rec_executed_.store(*jo);
  } else {
    cntx->ReportError(std::make_error_code(errc::protocol_error),
                      "Error finding journal offset in stream");
  }
  VLOG(1) << "FullSyncDflyFb finished after reading " << rdb_loader_->bytes_read() << " bytes";
}

void DflyShardReplica::StableSyncDflyReadFb(ExecutionState* cntx) {
  DCHECK_EQ(proactor_index_, ProactorBase::me()->GetPoolIndex());

  // Check leftover from full sync.
  io::Bytes prefix{};
  if (leftover_buf_ && leftover_buf_->InputLen() > 0) {
    prefix = leftover_buf_->InputBuffer();
  }

  io::PrefixSource ps{prefix, Sock()};

  JournalReader reader{&ps, 0};
  DCHECK_GE(journal_rec_executed_, 1u);
  TransactionReader tx_reader{journal_rec_executed_.load(std::memory_order_relaxed) - 1};

  acks_fb_ = fb2::Fiber("shard_acks", &DflyShardReplica::StableSyncDflyAcksFb, this, cntx);

  std::optional<TransactionData> tx_data;
  while ((tx_data = tx_reader.NextTxData(&reader, cntx))) {
    DVLOG(3) << "Lsn: " << tx_data->lsn;

    last_io_time_ = Proactor()->GetMonotonicTimeNs();
    if (tx_data->opcode == journal::Op::LSN) {
      //  Do nothing
    } else if (tx_data->opcode == journal::Op::PING) {
      force_ping_ = true;
      journal_rec_executed_.fetch_add(1, std::memory_order_relaxed);
    } else {
      const bool is_successful = ExecuteTx(std::move(*tx_data), cntx);
      if (is_successful) {
        // We only increment upon successful execution of the transaction.
        // The reason for this is that during partial sync we sent this
        // number as the lsn number to resume from. However, if for example
        // we increment this when a command fails (because the context
        // got cancelled, e.g, replication connection broke), we will get
        // inconsistent data because the replica will resume from the next
        // lsn of the master and this lsn entry will be lost.
        journal_rec_executed_.fetch_add(1, std::memory_order_relaxed);
      }
    }
    shard_replica_waker_.notifyAll();
  }
}

void Replica::RedisStreamAcksFb() {
  constexpr size_t kAckRecordMaxInterval = 1024;
  std::chrono::duration ack_time_max_interval =
      1ms * absl::GetFlag(FLAGS_replication_acks_interval);
  std::string ack_cmd;
  auto next_ack_tp = std::chrono::steady_clock::now();

  while (exec_st_.IsRunning()) {
    VLOG(2) << "Sending an ACK with offset=" << repl_offs_;
    ack_cmd = absl::StrCat("REPLCONF ACK ", repl_offs_);
    next_ack_tp = std::chrono::steady_clock::now() + ack_time_max_interval;
    if (auto ec = SendCommand(ack_cmd); ec) {
      exec_st_.ReportError(ec);
      break;
    }
    ack_offs_ = repl_offs_;

    replica_waker_.await_until(
        [&]() { return repl_offs_ > ack_offs_ + kAckRecordMaxInterval || (!exec_st_.IsRunning()); },
        next_ack_tp);
  }
}

void DflyShardReplica::StableSyncDflyAcksFb(ExecutionState* cntx) {
  DCHECK_EQ(proactor_index_, ProactorBase::me()->GetPoolIndex());

  constexpr size_t kAckRecordMaxInterval = 1024;
  std::chrono::duration ack_time_max_interval =
      1ms * absl::GetFlag(FLAGS_replication_acks_interval);
  std::string ack_cmd;
  auto next_ack_tp = std::chrono::steady_clock::now();

  uint64_t current_offset;
  while (cntx->IsRunning()) {
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

    shard_replica_waker_.await_until(
        [&]() {
          return journal_rec_executed_.load(std::memory_order_relaxed) >
                     ack_offs_ + kAckRecordMaxInterval ||
                 force_ping_ || (!cntx->IsRunning());
        },
        next_ack_tp);
  }
}

DflyShardReplica::DflyShardReplica(ServerContext server_context, MasterContext master_context,
                                   uint32_t flow_id, Service* service,
                                   std::shared_ptr<MultiShardExecution> multi_shard_exe)
    : ProtocolClient(server_context),
      service_(*service),
      master_context_(master_context),
      multi_shard_exe_(multi_shard_exe),
      flow_id_(flow_id) {
  executor_ = std::make_unique<JournalExecutor>(service);
  rdb_loader_ = std::make_unique<RdbLoader>(&service_);
  rdb_loader_->SetLoadUnownedSlots(true);
}

DflyShardReplica::~DflyShardReplica() {
  JoinFlow();
}

bool DflyShardReplica::ExecuteTx(TransactionData&& tx_data, ExecutionState* cntx) {
  if (!cntx->IsRunning()) {
    return false;
  }

  if (!tx_data.IsGlobalCmd()) {
    VLOG(3) << "Execute cmd without sync between shards. txid: " << tx_data.txid;
    return executor_->Execute(tx_data.dbid, tx_data.command) == facade::DispatchResult::OK;
  }

  bool inserted_by_me =
      multi_shard_exe_->InsertTxToSharedMap(tx_data.txid, master_context_.num_flows);

  auto& multi_shard_data = multi_shard_exe_->Find(tx_data.txid);

  VLOG(2) << "Execute txid: " << tx_data.txid << " waiting for data in all shards";
  // Wait until shards flows got transaction data and inserted to map.
  // This step enforces that replica will execute multi shard commands that finished on master
  // and replica recieved all the commands from all shards.
  multi_shard_data.block->Wait();
  // Check if we woke up due to cancellation.
  if (!exec_st_.IsRunning())
    return false;
  VLOG(2) << "Execute txid: " << tx_data.txid << " block wait finished";

  VLOG(2) << "Execute txid: " << tx_data.txid << " global command execution";
  // Wait until all shards flows get to execution step of this transaction.
  multi_shard_data.barrier.Wait();
  // Check if we woke up due to cancellation.
  if (!exec_st_.IsRunning())
    return false;
  // Global command will be executed only from one flow fiber. This ensure corectness of data in
  // replica.
  bool execution_res = true;
  if (inserted_by_me) {
    execution_res = executor_->Execute(tx_data.dbid, tx_data.command) == facade::DispatchResult::OK;
  }
  // Wait until exection is done, to make sure we done execute next commands while the global is
  // executed.
  multi_shard_data.barrier.Wait();
  // Check if we woke up due to cancellation.
  if (!exec_st_.IsRunning())
    return false;

  // Erase from map can be done only after all flow fibers executed the transaction commands.
  // The last fiber which will decrease the counter to 0 will be the one to erase the data from
  // map
  auto val = multi_shard_data.counter.fetch_sub(1, std::memory_order_relaxed);
  VLOG(2) << "txid: " << tx_data.txid << " counter: " << val;
  if (val == 1) {
    multi_shard_exe_->Erase(tx_data.txid);
  }
  return execution_res;
}

error_code Replica::ParseReplicationHeader(base::IoBuf* io_buf, PSyncResponse* dest) {
  std::string_view str;

  RETURN_ON_ERR(ReadLine(io_buf, &str));

  DCHECK(!str.empty());

  std::string_view header;
  bool valid = false;

  auto bad_header = [str]() {
    LOG(ERROR) << "Bad replication header: " << str;
    return std::make_error_code(std::errc::illegal_byte_sequence);
  };

  // non-empty lines
  if (str[0] != '+') {
    return bad_header();
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
      return bad_header();

    io_buf->ConsumeInput(str.size() + 2);
    RETURN_ON_ERR(ReadLine(io_buf, &str));  // Read the next line parsed below.

    // Readline checks for non ws character first before searching for eol
    // so str must be non empty.
    DCHECK(!str.empty());

    if (str[0] != '$') {
      return bad_header();
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
    LOG(ERROR) << "Partial replication not supported yet";
    return std::make_error_code(std::errc::not_supported);
  } else {
    LOG(ERROR) << "Unknown replication header";
    return bad_header();
  }

  return error_code{};
}

auto Replica::GetSummary() const -> Summary {
  auto f = [this]() {
    auto last_io_time = LastIoTime();

    // Note: we access LastIoTime from foreigh thread in unsafe manner. However, specifically here
    // it's unlikely to cause a real bug.
    for (const auto& flow : shard_flows_) {  // Get last io time from all sub flows.
      last_io_time = std::max(last_io_time, flow->LastIoTime());
    }

    Summary res;
    res.host = server().host;
    res.port = server().port;
    res.master_link_established = (state_mask_ & R_TCP_CONNECTED);
    res.full_sync_in_progress = (state_mask_ & R_SYNCING);
    res.full_sync_done = (state_mask_ & R_SYNC_OK);

    uint64_t current_time = ProactorBase::GetMonotonicTimeNs();
    // last_io_time is derived above by reading last_io_time_ from all the flows,
    // by accessing them from a foreign thread, see the loop above. As a result some
    // threads may have last_io_time_ bigger than our current time, so we fix it here.
    if (last_io_time > current_time) {
      res.master_last_io_sec = 0;
    } else {
      res.master_last_io_sec = (current_time - last_io_time) / 1000000000UL;
    }

    res.master_id = master_context_.master_repl_id;
    res.reconnect_count = reconnect_count_;
    res.repl_offset_sum = 0;
    for (uint64_t offs : GetReplicaOffset()) {
      res.repl_offset_sum += offs;
    }
    res.psync_successes = psync_successes_;
    res.psync_attempts = psync_attempts_;
    return res;
  };

  if (Sock())
    return Proactor()->AwaitBrief(f);

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

std::string Replica::GetCurrentPhase() const {
  if (!(state_mask_ & R_ENABLED))
    return "DISABLED";
  if (!(state_mask_ & R_TCP_CONNECTED))
    return "TCP_CONNECTING";
  if (!(state_mask_ & R_GREETED))
    return "GREETING";
  if (!(state_mask_ & R_SYNC_OK))
    return "INITIAL_SYNC";
  if (state_mask_ & R_SYNCING)
    return "FULL_SYNC_IN_PROGRESS";

  return "STABLE_SYNC";
}

uint32_t DflyShardReplica::FlowId() const {
  return flow_id_;
}

void DflyShardReplica::Pause(bool pause) {
  if (rdb_loader_) {
    rdb_loader_->Pause(pause);
  }
}

void DflyShardReplica::JoinFlow() {
  sync_fb_.JoinIfNeeded();
  acks_fb_.JoinIfNeeded();
}

void DflyShardReplica::Cancel() {
  if (rdb_loader_)
    rdb_loader_->stop();
  CloseSocket();
  shard_replica_waker_.notifyAll();
}

}  // namespace dfly
