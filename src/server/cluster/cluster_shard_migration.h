// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include "base/io_buf.h"
#include "server/journal/executor.h"
#include "server/protocol_client.h"

namespace dfly {

class Service;

struct MultiShardExecution;

// ClusterShardMigration manage data receiving in slots migration process.
// It is created per shard on the target node to initiate FLOW step.
class ClusterShardMigration : public ProtocolClient {
 public:
  ClusterShardMigration(ServerContext server_context, uint32_t shard_id, uint32_t sync_id,
                        Service* service, std::shared_ptr<MultiShardExecution> cmse);
  ~ClusterShardMigration();

  std::error_code StartSyncFlow(Context* cntx);
  void Cancel();

  struct TransactionData {  // TODO use the same as replica.h
    bool AddEntry(journal::ParsedEntry&& entry);

    bool IsGlobalCmd() const;

    static TransactionData FromSingle(journal::ParsedEntry&& entry);

    TxId txid{0};
    DbIndex dbid{0};
    uint32_t shard_cnt{0};
    absl::InlinedVector<journal::ParsedEntry::CmdData, 1> commands{0};
    uint32_t journal_rec_count{0};  // Count number of source entries to check offset.
    bool is_ping = false;           // For Op::PING entries.
  };

  struct TransactionReader {
    std::optional<TransactionData> NextTxData(JournalReader* reader, Context* cntx);

   private:
    // Stores ongoing multi transaction data.
    absl::flat_hash_map<TxId, TransactionData> current_;
  };

 private:
  void FullSyncShardFb(Context* cntx);
  void JoinFlow();

  void ExecuteTx(TransactionData&& tx_data, bool inserted_by_me, Context* cntx);
  void ExecuteTxWithNoShardSync(TransactionData&& tx_data, Context* cntx);
  bool InsertTxToSharedMap(const TransactionData& tx_data);

 private:
  uint32_t source_shard_id_;
  uint32_t sync_id_;
  Service& service_;
  std::optional<base::IoBuf> leftover_buf_;

  std::unique_ptr<JournalExecutor> executor_;

  EventCount waker_;
  std::queue<std::pair<TransactionData, bool>> trans_data_queue_;

  Fiber sync_fb_;

  std::shared_ptr<MultiShardExecution> multi_shard_exe_;
};

}  // namespace dfly
