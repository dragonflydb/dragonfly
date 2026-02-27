// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/rdb_load_context.h"

#include <absl/container/flat_hash_set.h>
#include <absl/strings/match.h>

#include <algorithm>
#include <limits>

#include "base/logging.h"
#include "facade/redis_parser.h"
#include "facade/reply_capture.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"
#include "server/main_service.h"
#include "server/namespaces.h"
#include "server/search/doc_index.h"
#include "server/search/global_hnsw_index.h"
#include "server/sharding.h"

namespace dfly {

namespace {
constexpr search::GlobalDocId kInvalidRemapGid = std::numeric_limits<search::GlobalDocId>::max();
}  // namespace

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

void RdbLoadContext::AddPendingHnswNodes(PendingHnswNodes nodes) {
  util::fb2::LockGuard<util::fb2::Mutex> lk(mu_);
  pending_hnsw_nodes_.emplace_back(std::move(nodes));
}

void RdbLoadContext::SetMasterShardCount(uint32_t count) {
  master_shard_count_ = count;
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

std::vector<PendingHnswNodes> RdbLoadContext::TakePendingHnswNodes() {
  util::fb2::LockGuard<util::fb2::Mutex> lk(mu_);
  return std::move(pending_hnsw_nodes_);
}

RdbLoadContext::HnswRemapTable RdbLoadContext::RemapHnswForDifferentShardCount(
    const absl::flat_hash_map<uint32_t, std::vector<PendingIndexMapping>>& index_mappings,
    std::vector<PendingHnswNodes>& pending_nodes,
    const std::vector<PendingHnswMetadata>& hnsw_metadata) {
  const ShardId new_shard_count = shard_set->size();

  // Build compact remap table: for each (index, master_shard, old_doc_id) store new_global_id.
  HnswRemapTable remap_table;
  absl::flat_hash_map<std::string, absl::flat_hash_map<uint32_t, search::DocId>> doc_id_counters;

  for (const auto& [master_shard_id, pim_vec] : index_mappings) {
    for (const auto& pim : pim_vec) {
      auto& shard_remap = remap_table[pim.index_name];
      auto& counters = doc_id_counters[pim.index_name];

      for (const auto& [key, old_doc_id] : pim.mappings) {
        ShardId new_shard_id = Shard(key, new_shard_count);
        search::DocId new_doc_id = counters[new_shard_id]++;
        search::GlobalDocId new_gid = search::CreateGlobalDocId(new_shard_id, new_doc_id);

        auto& vec = shard_remap[master_shard_id];
        if (old_doc_id >= vec.size()) {
          vec.resize(old_doc_id + 1, kInvalidRemapGid);
        }
        vec[old_doc_id] = new_gid;
      }
    }
  }

  // Remap global_ids in deferred HNSW nodes and restore the graphs.
  absl::flat_hash_set<std::string> failed_indices;

  for (auto& pn : pending_nodes) {
    auto remap_it = remap_table.find(pn.index_name);

    auto hnsw_index = GlobalHnswIndexRegistry::Instance().Get(pn.index_name, pn.field_name);
    if (!hnsw_index) {
      LOG(ERROR) << "HNSW index not found for deferred restoration: " << pn.index_name << ":"
                 << pn.field_name << ". Will rebuild from scratch.";
      failed_indices.insert(pn.index_name);
      continue;
    }

    if (remap_it == remap_table.end()) {
      LOG(WARNING) << "No remap table for index " << pn.index_name << ":" << pn.field_name
                   << " (no key mappings). Will rebuild from scratch.";
      failed_indices.insert(pn.index_name);
      continue;
    }

    size_t remapped = 0;
    for (auto& node : pn.nodes) {
      auto [shard_id, doc_id] = search::DecomposeGlobalDocId(node.global_id);
      auto shard_it = remap_it->second.find(shard_id);
      if (shard_it != remap_it->second.end() && doc_id < shard_it->second.size()) {
        search::GlobalDocId new_gid = shard_it->second[doc_id];
        if (new_gid != kInvalidRemapGid) {
          node.global_id = new_gid;
          ++remapped;
        }
      }
    }

    if (remapped != pn.nodes.size()) {
      LOG(WARNING) << "Incomplete remap for HNSW index " << pn.index_name << ":" << pn.field_name
                   << " (" << remapped << "/" << pn.nodes.size()
                   << " nodes). Will rebuild from scratch.";
      failed_indices.insert(pn.index_name);
      continue;
    }

    const PendingHnswMetadata* phm_ptr = nullptr;
    for (const auto& phm : hnsw_metadata) {
      if (phm.index_name == pn.index_name && phm.field_name == pn.field_name) {
        phm_ptr = &phm;
        break;
      }
    }
    DCHECK(phm_ptr) << "HNSW metadata missing for " << pn.index_name << ":" << pn.field_name;

    hnsw_index->RestoreFromNodes(pn.nodes, phm_ptr->metadata);
    LOG(INFO) << "Restored HNSW index " << pn.index_name << ":" << pn.field_name << " with "
              << pn.nodes.size() << " nodes (" << remapped << " global_ids remapped)";
  }

  // Remove failed indices — their key mappings won't be redistributed, causing full rebuild.
  for (const auto& name : failed_indices) {
    remap_table.erase(name);
  }

  return remap_table;
}

void RdbLoadContext::PerformPostLoad(Service* service, bool is_error) {
  const CommandId* cmd = service->FindCmd("FT.CREATE");
  if (cmd == nullptr)  // In case search module is disabled
    return;

  std::vector<std::string> synonym_cmds = TakePendingSynonymCommands();
  auto index_mappings = TakePendingIndexMappings();
  auto pending_nodes = TakePendingHnswNodes();

  // Extract remaining shared state under lock. After this, no member access is needed.
  std::vector<PendingHnswMetadata> hnsw_metadata;
  {
    util::fb2::LockGuard<util::fb2::Mutex> lk(mu_);
    hnsw_metadata.swap(pending_hnsw_metadata_);
  }
  uint32_t master_shards = master_shard_count_;

  bool has_hnsw_restore = !hnsw_metadata.empty();

  if (is_error)
    return;

  // When shard counts differ, remap HNSW global_ids and redistribute key mappings on-the-fly.
  bool shard_count_differs = master_shards != 0 && master_shards != shard_set->size();

  if (shard_count_differs && !index_mappings.empty()) {
    auto remap_table =
        RemapHnswForDifferentShardCount(index_mappings, pending_nodes, hnsw_metadata);

    // Each shard filters its own keys from all original mappings using the remap table.
    // This avoids building a pre-redistributed copy of all key strings.
    shard_set->AwaitRunningOnShardQueue([&](EngineShard* es) {
      const ShardId my_shard = es->shard_id();
      absl::flat_hash_map<std::string, std::vector<std::pair<std::string, search::DocId>>>
          per_index;

      for (const auto& [master_shard_id, pim_vec] : index_mappings) {
        for (const auto& pim : pim_vec) {
          auto idx_it = remap_table.find(pim.index_name);
          if (idx_it == remap_table.end())
            continue;
          auto shard_it = idx_it->second.find(master_shard_id);
          if (shard_it == idx_it->second.end())
            continue;
          const auto& remap_vec = shard_it->second;

          for (const auto& [key, old_doc_id] : pim.mappings) {
            if (old_doc_id >= remap_vec.size())
              continue;
            search::GlobalDocId new_gid = remap_vec[old_doc_id];
            if (new_gid == kInvalidRemapGid)
              continue;
            auto [new_shard_id, new_doc_id] = search::DecomposeGlobalDocId(new_gid);
            if (new_shard_id != my_shard)
              continue;
            per_index[pim.index_name].emplace_back(key, new_doc_id);
          }
        }
      }

      for (auto& [name, mappings] : per_index) {
        if (auto* index = es->search_indices()->GetIndex(name); index) {
          index->RestoreKeyIndex(mappings);
          VLOG(1) << "Restored " << mappings.size() << " key mappings for index " << name
                  << " on shard " << my_shard;
        }
      }
    });
  } else {
    if (shard_count_differs && !pending_nodes.empty()) {
      LOG(WARNING) << "Have " << pending_nodes.size()
                   << " deferred HNSW node sets but no key mappings for remapping. "
                      "Affected indices will be rebuilt from scratch.";
    }

    if (!index_mappings.empty()) {
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
