// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/incoming_slot_migration.h"

#include "absl/cleanup/cleanup.h"
#include "base/logging.h"
#include "cluster_utility.h"
#include "server/error.h"
#include "server/journal/executor.h"
#include "server/journal/tx_executor.h"
#include "server/main_service.h"

namespace dfly::cluster {

using namespace std;
using namespace util;
using namespace facade;
using absl::GetFlag;

// ClusterShardMigration manage data receiving in slots migration process.
// It is created per shard on the target node to initiate FLOW step.
class ClusterShardMigration {
 public:
  ClusterShardMigration(uint32_t shard_id, Service* service)
      : source_shard_id_(shard_id), socket_(nullptr), executor_(service) {
  }

  void Start(Context* cntx, util::FiberSocketBase* source, util::fb2::BlockingCounter bc) {
    {
      std::lock_guard lk(mu_);
      socket_ = source;
    }

    absl::Cleanup cleanup([this]() {
      std::lock_guard lk(mu_);
      socket_ = nullptr;
    });
    JournalReader reader{source, 0};
    TransactionReader tx_reader;

    while (!cntx->IsCancelled()) {
      auto tx_data = tx_reader.NextTxData(&reader, cntx);
      if (!tx_data) {
        // TODO add error processing
        VLOG(1) << "No tx data";
        break;
      }

      while (tx_data->opcode == journal::Op::FIN) {
        VLOG(2) << "Attempt to finalize flow " << source_shard_id_;
        bc->Dec();  // we can Join the flow now
        // if we get new data, attempt is failed
        if (tx_data = tx_reader.NextTxData(&reader, cntx); !tx_data) {
          VLOG(1) << "Finalized flow " << source_shard_id_;
          return;
        }
        bc->Add();  // the flow isn't finished so we lock it again
      }
      if (tx_data->opcode == journal::Op::PING) {
        // TODO check about ping logic
      } else {
        ExecuteTxWithNoShardSync(std::move(*tx_data), cntx);
      }
    }

    bc->Dec();  // we should provide ability to join the flow
  }

  std::error_code Cancel() {
    std::lock_guard lk(mu_);
    if (socket_ != nullptr) {
      return socket_->proactor()->Await([s = socket_, sid = source_shard_id_]() {
        if (s->IsOpen()) {
          return s->Shutdown(SHUT_RDWR);  // Does not Close(), only forbids further I/O.
        }
        return std::error_code();
      });
    }
    return {};
  }

 private:
  void ExecuteTxWithNoShardSync(TransactionData&& tx_data, Context* cntx) {
    if (cntx->IsCancelled()) {
      return;
    }
    CHECK(tx_data.shard_cnt <= 1);  // we don't support sync for multishard execution
    if (!tx_data.IsGlobalCmd()) {
      VLOG(3) << "Execute cmd without sync between shards. txid: " << tx_data.txid;
      executor_.Execute(tx_data.dbid, tx_data.command);
    } else {
      // TODO check which global commands should be supported
      CHECK(false) << "We don't support command: " << ToSV(tx_data.command.cmd_args[0])
                   << "in cluster migration process.";
    }
  }

 private:
  uint32_t source_shard_id_;
  util::fb2::Mutex mu_;
  util::FiberSocketBase* socket_ ABSL_GUARDED_BY(mu_);
  JournalExecutor executor_;
};

IncomingSlotMigration::IncomingSlotMigration(string source_id, Service* se, SlotRanges slots,
                                             uint32_t shards_num)
    : source_id_(std::move(source_id)),
      service_(*se),
      slots_(std::move(slots)),
      state_(MigrationState::C_CONNECTING),
      bc_(0) {
  shard_flows_.resize(shards_num);
  for (unsigned i = 0; i < shards_num; ++i) {
    shard_flows_[i].reset(new ClusterShardMigration(i, &service_));
  }
}

IncomingSlotMigration::~IncomingSlotMigration() {
}

void IncomingSlotMigration::Join() {
  // TODO add timeout
  bc_->Wait();
  state_.store(MigrationState::C_FINISHED);
  keys_number_ = cluster::GetKeyCount(slots_);
}

void IncomingSlotMigration::Cancel() {
  LOG(INFO) << "Cancelling incoming migration of slots " << SlotRange::ToString(slots_);
  cntx_.Cancel();

  for (auto& flow : shard_flows_) {
    flow->Cancel();
  }
}

void IncomingSlotMigration::StartFlow(uint32_t shard, util::FiberSocketBase* source) {
  VLOG(1) << "Start flow for shard: " << shard;
  state_.store(MigrationState::C_SYNC);

  bc_->Add();
  shard_flows_[shard]->Start(&cntx_, source, bc_);
}

size_t IncomingSlotMigration::GetKeyCount() const {
  if (state_.load() == MigrationState::C_FINISHED) {
    return keys_number_;
  }
  return cluster::GetKeyCount(slots_);
}

}  // namespace dfly::cluster
