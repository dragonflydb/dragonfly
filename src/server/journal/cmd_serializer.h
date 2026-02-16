// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/types/span.h>

#include <string>
#include <string_view>

#include "server/table.h"
#include "server/tiered_storage.h"
#include "server/tx_base.h"

namespace dfly {

class RdbSerializer;

// CmdSerializer serializes DB entries (key+value) into command(s) in RESP format string.
// Small entries are serialized as RESTORE commands, while bigger ones (see
// serialization_max_chunk_size) are split into multiple commands (like rpush, hset, etc).
// Expiration and stickiness are also serialized into commands.
class CmdSerializer {
 public:
  using FlushSerialized = std::function<void(std::string)>;

  explicit CmdSerializer(DbSlice* db_slice, FlushSerialized cb,
                         size_t max_serialization_buffer_size);

  // Returns how many commands we broke this entry into (like multiple HSETs etc)
  size_t SerializeEntry(std::string_view key, const PrimeKey& pk, const PrimeValue& pv,
                        uint64_t expire_ms);

  // Serialize delayed entries. If force is true, blocks until all are resolved.
  // If force is false, only serializes entries whose futures are already resolved.
  // If tiered_keys is provided, only serializes entries whose keys are in the set.
  size_t SerializeDelayedEntries(bool force, absl::flat_hash_set<std::string>* tiered_keys);

 private:
  void SerializeCommand(std::string_view cmd, absl::Span<const std::string_view> args);
  void SerializeStickIfNeeded(std::string_view key, const PrimeKey& pk);
  void SerializeExpireIfNeeded(std::string_view key, uint64_t expire_ms);

  size_t SerializeSet(std::string_view key, const PrimeValue& pv);
  size_t SerializeZSet(std::string_view key, const PrimeValue& pv);
  size_t SerializeHash(std::string_view key, const PrimeValue& pv);
  size_t SerializeList(std::string_view key, const PrimeValue& pv);
  size_t SerializeString(std::string_view key, const PrimeValue& pv, uint64_t expire_ms);
  void SerializeRestore(std::string_view key, const PrimeKey& pk, const PrimeValue& pv,
                        uint64_t expire_ms);
  void SerializeExternal(std::string_view key, const PrimeValue& pv, time_t expire_time);

  DbSlice* db_slice_;
  FlushSerialized cb_;
  size_t max_serialization_buffer_size_;
  std::unique_ptr<RdbSerializer> serializer_;
  absl::flat_hash_map<std::string, std::unique_ptr<TieredDelayedEntry>> delayed_entries_;
};

}  // namespace dfly
