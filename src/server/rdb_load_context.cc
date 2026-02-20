// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/rdb_load_context.h"

#include <absl/strings/match.h>

#include "base/logging.h"
#include "facade/redis_parser.h"
#include "facade/reply_capture.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"
#include "server/main_service.h"
#include "server/namespaces.h"
#include "server/search/doc_index.h"

namespace dfly {

void LoadSearchCommandFromAux(Service* service, std::string&& def, std::string_view command_name,
                              std::string_view error_context, bool add_NX) {
  facade::CapturingReplyBuilder crb;

  ConnectionContext cntx{nullptr, acl::UserCredentials{}};
  cntx.is_replicating = true;
  cntx.journal_emulated = true;
  cntx.skip_acl_validation = true;
  cntx.ns = &namespaces->GetDefaultNamespace();

  uint32_t consumed = 0;
  facade::RespVec resp_vec;
  facade::RedisParser parser;

  // Prepend a whitespace so names starting with ':' are treated as names, not RESP tokens.
  def.insert(def.begin(), ' ');

  // Add resp terminator
  constexpr std::string_view kRespTerminator = "\r\n";
  def += kRespTerminator;

  std::string_view printable_def{def.data(), def.size() - kRespTerminator.size()};

  io::MutableBytes buffer{reinterpret_cast<uint8_t*>(def.data()), def.size()};
  auto res = parser.Parse(buffer, &consumed, &resp_vec);

  if (res != facade::RedisParser::Result::OK) {
    LOG(ERROR) << "Bad " << error_context << ": " << printable_def;
    return;
  }

  // Temporary migration fix for backwards compatibility with old snapshots where TAG fields were
  // serialized as "TAG SORTABLE SEPARATOR x" but parser expects "TAG SEPARATOR x SORTABLE".
  // Reorder arguments if needed.
  // TODO: Remove this workaround after Apr 2026.
  for (size_t i = 0; i + 2 < resp_vec.size(); ++i) {
    std::string_view cur = resp_vec[i].GetView();
    std::string_view next = resp_vec[i + 1].GetView();
    if (absl::EqualsIgnoreCase(cur, "SORTABLE") && absl::EqualsIgnoreCase(next, "SEPARATOR")) {
      // SORTABLE SEPARATOR x -> SEPARATOR x SORTABLE
      std::swap(resp_vec[i], resp_vec[i + 1]);      // SEPARATOR SORTABLE x
      std::swap(resp_vec[i + 1], resp_vec[i + 2]);  // SEPARATOR x SORTABLE
    }
  }

  // Prepend command name (FT.CREATE or FT.SYNUPDATE)
  CommandContext cntx_cmd;
  cntx_cmd.Init(&crb, &cntx);

  cntx_cmd.PushArg(command_name);
  cntx_cmd.PushArg(resp_vec[0].GetView());  // index name
  if (add_NX) {
    cntx_cmd.PushArg("NX");
  }
  for (unsigned i = 1; i < resp_vec.size(); i++) {
    cntx_cmd.PushArg(resp_vec[i].GetView());
  }
  service->DispatchCommand(facade::ParsedArgs{cntx_cmd}, &cntx_cmd,
                           facade::AsyncPreference::ONLY_SYNC);

  auto response = crb.Take();
  if (auto err = facade::CapturingReplyBuilder::TryExtractError(response); err) {
    LOG(ERROR) << "Bad " << error_context << ": " << def << " " << err->first;
  }
}

void RdbLoadContext::AddPendingSynonymCommand(std::string cmd) {
  util::fb2::LockGuard<util::fb2::Mutex> lk(mu_);
  pending_synonym_cmds_.push_back(std::move(cmd));
}

void RdbLoadContext::AddPendingIndexMapping(uint32_t shard_id, PendingIndexMapping mapping) {
  util::fb2::LockGuard<util::fb2::Mutex> lk(mu_);
  pending_index_mappings_[shard_id].emplace_back(std::move(mapping));
}

void RdbLoadContext::AddPendingHnswMetadata(PendingHnswMetadata metadata) {
  util::fb2::LockGuard<util::fb2::Mutex> lk(mu_);
  pending_hnsw_metadata_.emplace_back(std::move(metadata));
}

std::optional<search::HnswIndexMetadata> RdbLoadContext::FindHnswMetadata(
    std::string_view index_name, std::string_view field_name) const {
  util::fb2::LockGuard<util::fb2::Mutex> lk(mu_);
  for (const auto& phm : pending_hnsw_metadata_) {
    if (phm.index_name == index_name && phm.field_name == field_name) {
      return phm.metadata;
    }
  }
  return std::nullopt;
}

std::vector<std::string> RdbLoadContext::TakePendingSynonymCommands() {
  util::fb2::LockGuard<util::fb2::Mutex> lk(mu_);
  std::vector<std::string> result;
  result.swap(pending_synonym_cmds_);
  return result;
}

absl::flat_hash_map<uint32_t, std::vector<PendingIndexMapping>>
RdbLoadContext::TakePendingIndexMappings() {
  util::fb2::LockGuard<util::fb2::Mutex> lk(mu_);
  decltype(pending_index_mappings_) result;
  std::swap(result, pending_index_mappings_);
  return result;
}

void RdbLoadContext::PerformPostLoad(Service* service, bool is_error) {
  const CommandId* cmd = service->FindCmd("FT.CREATE");
  if (cmd == nullptr)  // In case search module is disabled
    return;

  // Capture before clearing — indicates HNSW graphs were loaded and need restore path
  bool has_hnsw_restore;
  {
    util::fb2::LockGuard<util::fb2::Mutex> lk(mu_);
    has_hnsw_restore = !pending_hnsw_metadata_.empty();
    pending_hnsw_metadata_.clear();
  }

  std::vector<std::string> synonym_cmds = TakePendingSynonymCommands();
  auto index_mappings = TakePendingIndexMappings();

  if (is_error)
    return;

  if (!index_mappings.empty()) {
    // Apply mappings on each shard (assuming same shard count as when snapshot was taken)
    shard_set->AwaitRunningOnShardQueue([&index_mappings](EngineShard* es) {
      auto it = index_mappings.find(es->shard_id());
      if (it == index_mappings.end())
        return;
      for (const auto& pim : it->second) {
        if (auto* index = es->search_indices()->GetIndex(pim.index_name); index) {
          index->RestoreKeyIndex(pim.mappings);
          VLOG(1) << "Restored " << pim.mappings.size() << " key mappings for index "
                  << pim.index_name << " on shard " << es->shard_id();
        }
      }
    });
  }
  shard_set->AwaitRunningOnShardQueue([has_hnsw_restore](EngineShard* es) {
    OpArgs op_args{es, nullptr,
                   DbContext{&namespaces->GetDefaultNamespace(), 0, GetCurrentTimeMs()}};
    es->search_indices()->RebuildAllIndices(op_args, has_hnsw_restore);
  });

  // Now execute all pending synonym commands after indices are rebuilt
  for (auto& syn_cmd : synonym_cmds) {
    LoadSearchCommandFromAux(service, std::move(syn_cmd), "FT.SYNUPDATE", "synonym definition");
  }

  // Wait until index building ends
  shard_set->RunBlockingInParallel(
      [](EngineShard* es) { es->search_indices()->BlockUntilConstructionEnd(); });
}

}  // namespace dfly
