// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/incoming_slot_migration.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>

#include "base/flags.h"
#include "base/logging.h"
#include "cluster_utility.h"
#include "server/error.h"
#include "server/journal/executor.h"
#include "server/journal/tx_executor.h"
#include "server/main_service.h"
#include "util/fibers/synchronization.h"

ABSL_DECLARE_FLAG(int, migration_finalization_timeout_ms);

namespace dfly::cluster {

using namespace std;
using namespace util;
using namespace facade;

// ClusterShardMigration manage data receiving in slots migration process.
// It is created per shard on the target node to initiate FLOW step.
class ClusterShardMigration {
 public:
  ClusterShardMigration(uint32_t shard_id, Service* service, IncomingSlotMigration* in_migration,
                        util::fb2::BlockingCounter bc)
      : source_shard_id_(shard_id),
        is_finished_(false),
        socket_(nullptr),
        executor_(service),
        in_migration_(in_migration),
        bc_(bc) {
  }

  void Pause(bool pause) {
    pause_ = pause;
  }

  void Start(Context* cntx, util::FiberSocketBase* source) ABSL_LOCKS_EXCLUDED(mu_) {
    {
      util::fb2::LockGuard lk(mu_);
      if (is_finished_) {
        return;
      }
      is_finished_ = true;
      socket_ = source;
    }

    absl::Cleanup cleanup([this]() ABSL_LOCKS_EXCLUDED(mu_) {
      util::fb2::LockGuard lk(mu_);
      socket_ = nullptr;
    });
    JournalReader reader{source, 0};
    TransactionReader tx_reader;

    while (!cntx->IsCancelled()) {
      if (pause_) {
        ThisFiber::SleepFor(100ms);
        continue;
      }

      auto tx_data = tx_reader.NextTxData(&reader, cntx);
      if (!tx_data) {
        in_migration_->ReportError(GenericError("No tx data"));
        VLOG(1) << "No tx data";
        break;
      }

      while (tx_data->opcode == journal::Op::LSN) {
        VLOG(2) << "Attempt to finalize flow " << source_shard_id_ << " attempt " << tx_data->lsn;
        last_attempt_.store(tx_data->lsn);
        bc_->Dec();  // we can Join the flow now
        // if we get new data, attempt is failed
        if (tx_data = tx_reader.NextTxData(&reader, cntx); !tx_data) {
          VLOG(1) << "Finalized flow " << source_shard_id_;
          return;
        }
        if (!tx_data->command.cmd_args.empty()) {
          VLOG(1) << "Flow finalization failed " << source_shard_id_ << " by "
                  << tx_data->command.cmd_args[0];
        } else {
          VLOG(1) << "Flow finalization failed " << source_shard_id_ << " by opcode "
                  << (int)tx_data->opcode;
        }

        bc_->Add();  // the flow isn't finished so we lock it again
      }
      if (tx_data->opcode == journal::Op::PING) {
        // TODO check about ping logic
      } else {
        ExecuteTx(std::move(*tx_data), cntx);
      }
    }

    VLOG(2) << "Flow " << source_shard_id_ << " canceled";
    bc_->Dec();  // we should provide ability to join the flow
  }

  std::error_code Cancel() {
    util::fb2::LockGuard lk(mu_);
    if (socket_ != nullptr) {
      return socket_->proactor()->Await([s = socket_]() {
        if (s->IsOpen()) {
          return s->Shutdown(SHUT_RDWR);  // Does not Close(), only forbids further I/O.
        }
        return std::error_code();
      });
    }
    if (!is_finished_) {
      is_finished_ = true;
      bc_->Dec();  // we should provide ability to join the flow if the Start() wasn't called
    }

    return {};
  }

  long GetLastAttempt() const {
    return last_attempt_.load();
  }

 private:
  void ExecuteTx(TransactionData&& tx_data, Context* cntx) {
    if (cntx->IsCancelled()) {
      return;
    }
    if (!tx_data.IsGlobalCmd()) {
      executor_.Execute(tx_data.dbid, tx_data.command);
    } else {
      // TODO check which global commands should be supported
      std::string error =
          absl::StrCat("We don't support command: ", ToSV(tx_data.command.cmd_args[0]),
                       " in cluster migration process.");
      LOG(ERROR) << error;
      cntx->ReportError(error);
      in_migration_->ReportError(error);
    }
  }

  uint32_t source_shard_id_;
  util::fb2::Mutex mu_;
  bool is_finished_ ABSL_GUARDED_BY(mu_);
  util::FiberSocketBase* socket_ ABSL_GUARDED_BY(mu_);
  JournalExecutor executor_;
  IncomingSlotMigration* in_migration_;
  util::fb2::BlockingCounter bc_;
  atomic_long last_attempt_{-1};
  atomic_bool pause_ = false;
};

IncomingSlotMigration::IncomingSlotMigration(string source_id, Service* se, SlotRanges slots,
                                             uint32_t shards_num)
    : source_id_(std::move(source_id)),
      service_(*se),
      slots_(std::move(slots)),
      state_(MigrationState::C_CONNECTING),
      bc_(shards_num) {
  shard_flows_.resize(shards_num);
  for (unsigned i = 0; i < shards_num; ++i) {
    shard_flows_[i].reset(new ClusterShardMigration(i, &service_, this, bc_));
  }
}

IncomingSlotMigration::~IncomingSlotMigration() {
}

void IncomingSlotMigration::Pause(bool pause) {
  VLOG(1) << "Pausing migration " << pause;
  for (auto& flow : shard_flows_) {
    flow->Pause(pause);
  }
}

bool IncomingSlotMigration::Join(long attempt) {
  const absl::Time start = absl::Now();
  const absl::Duration timeout =
      absl::Milliseconds(absl::GetFlag(FLAGS_migration_finalization_timeout_ms));

  while (true) {
    const absl::Time now = absl::Now();
    const absl::Duration passed = now - start;
    VLOG_EVERY_N(1, 10000) << "Checking whether to continue with join " << passed << " vs "
                           << timeout;
    if (passed >= timeout) {
      LOG(WARNING) << "Can't join migration in time for " << source_id_;
      ReportError(GenericError("Can't join migration in time"));
      return false;
    }

    if ((bc_->WaitFor(absl::ToInt64Milliseconds(timeout - passed) * 1ms)) &&
        (std::all_of(shard_flows_.begin(), shard_flows_.end(),
                     [&](const auto& flow) { return flow->GetLastAttempt() == attempt; }))) {
      state_.store(MigrationState::C_FINISHED);
      keys_number_ = cluster::GetKeyCount(slots_);
      return true;
    }
  }
}

void IncomingSlotMigration::Stop() {
  string_view log_state = state_.load() == MigrationState::C_FINISHED ? "Finishing" : "Cancelling";
  LOG(INFO) << log_state << " incoming migration of slots " << slots_.ToString();
  cntx_.Cancel();

  for (auto& flow : shard_flows_) {
    if (auto err = flow->Cancel(); err) {
      VLOG(1) << "Error during flow Stop: " << err;
    }
  }

  // we need to Join the migration process to prevent data corruption
  const absl::Time start = absl::Now();
  const absl::Duration timeout =
      absl::Milliseconds(absl::GetFlag(FLAGS_migration_finalization_timeout_ms));

  while (true) {
    const absl::Time now = absl::Now();
    const absl::Duration passed = now - start;
    VLOG(1) << "Checking whether to continue with stop " << passed << " vs " << timeout;

    if (bc_->WaitFor(absl::ToInt64Milliseconds(timeout - passed) * 1ms)) {
      return;
    } else if (passed >= timeout) {
      LOG(ERROR) << "Can't stop migration in time";
      return;
    }
  }
}

void IncomingSlotMigration::StartFlow(uint32_t shard, util::FiberSocketBase* source) {
  state_.store(MigrationState::C_SYNC);

  shard_flows_[shard]->Start(&cntx_, source);
  VLOG(1) << "Incoming flow " << shard << " finished for " << source_id_;
}

size_t IncomingSlotMigration::GetKeyCount() const {
  if (state_.load() == MigrationState::C_FINISHED) {
    return keys_number_;
  }
  return cluster::GetKeyCount(slots_);
}

}  // namespace dfly::cluster
