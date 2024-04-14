// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/incoming_slot_migration.h"

#include "base/logging.h"
#include "server/error.h"
#include "server/journal/executor.h"
#include "server/journal/tx_executor.h"
#include "server/main_service.h"

namespace dfly {

using namespace std;
using namespace util;
using namespace facade;
using absl::GetFlag;

// ClusterShardMigration manage data receiving in slots migration process.
// It is created per shard on the target node to initiate FLOW step.
class ClusterShardMigration {
 public:
  ClusterShardMigration(uint32_t shard_id, Service* service) : source_shard_id_(shard_id) {
    executor_ = std::make_unique<JournalExecutor>(service);
  }

  void Start(Context* cntx, util::FiberSocketBase* source) {
    socket_ = source;
    JournalReader reader{source, 0};
    TransactionReader tx_reader{false};

    while (!cntx->IsCancelled()) {
      if (cntx->IsCancelled())
        break;

      // LOG(ERROR) << "XXX Reading tx for " << source_shard_id_;
      auto tx_data = tx_reader.NextTxData(&reader, cntx);
      // LOG(ERROR) << "XXX Finished reading a tx for " << source_shard_id_;
      if (!tx_data) {
        VLOG(1) << "No tx data";
        break;
      }

      if (tx_data->opcode == journal::Op::FIN) {
        VLOG(2) << "Flow " << source_shard_id_ << " is finalized";
        break;
      } else if (tx_data->opcode == journal::Op::PING) {
        // TODO check about ping logic
      } else {
        ExecuteTxWithNoShardSync(std::move(*tx_data), cntx);
      }
    }
  }

  void Cancel() {
    if (socket_ != nullptr) {
      LOG(ERROR) << "XXX Cancelling incoming migration " << source_shard_id_;
      socket_->proactor()->Dispatch([s = socket_, sid = source_shard_id_]() {
        LOG(ERROR) << "XXX Closing socket for " << sid;
        s->Shutdown(SHUT_RDWR);
        s->Close();
        LOG(ERROR) << "XXX socket closed for " << sid;
      });
    }
  }

 private:
  void ExecuteTxWithNoShardSync(TransactionData&& tx_data, Context* cntx) {
    if (cntx->IsCancelled()) {
      return;
    }
    CHECK(tx_data.shard_cnt <= 1);  // we don't support sync for multishard execution
    if (!tx_data.IsGlobalCmd()) {
      VLOG(3) << "Execute cmd without sync between shards. txid: " << tx_data.txid;
      executor_->Execute(tx_data.dbid, absl::MakeSpan(tx_data.commands));
    } else {
      // TODO check which global commands should be supported
      CHECK(false) << "We don't support command: " << ToSV(tx_data.commands.front().cmd_args[0])
                   << "in cluster migration process.";
    }
  }

 private:
  uint32_t source_shard_id_;
  util::FiberSocketBase* socket_ = nullptr;
  std::unique_ptr<JournalExecutor> executor_;
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
    shard_flows_[i].reset(new ClusterShardMigration(i, &service_));
  }
}

IncomingSlotMigration::~IncomingSlotMigration() {
}

void IncomingSlotMigration::Join() {
  bc_->Wait();
  state_ = MigrationState::C_FINISHED;
}

void IncomingSlotMigration::Cancel() {
  cntx_.Cancel();

  auto cb = [this](util::ProactorBase* pb) {
    if (const auto* shard = EngineShard::tlocal(); shard) {
      if (auto& flow = shard_flows_[shard->shard_id()]; flow) {
        flow->Cancel();
      }
    }
  };
  shard_set->pool()->AwaitFiberOnAll(std::move(cb));
}

void IncomingSlotMigration::StartFlow(uint32_t shard, util::FiberSocketBase* source) {
  VLOG(1) << "Start flow for shard: " << shard;

  shard_flows_[shard]->Start(&cntx_, source);
  bc_->Dec();
}

}  // namespace dfly
