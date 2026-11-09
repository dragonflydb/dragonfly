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

  explicit CmdSerializer(FlushSerialized cb, size_t max_serialization_buffer_size);

  // Returns how many commands we broke this entry into (like multiple HSETs etc)
  size_t SerializeEntry(DbIndex db_index, std::string_view key, const PrimeKey& pk,
                        const PrimeValue& pv, uint64_t expire_ms);

 private:
  void SerializeCommand(DbIndex db_index, std::string_view cmd,
                        absl::Span<const std::string_view> args);
  void SerializeStickIfNeeded(DbIndex db_index, std::string_view key, const PrimeKey& pk);
  void SerializeExpireIfNeeded(DbIndex db_index, std::string_view key, uint64_t expire_ms);

  size_t SerializeSet(DbIndex db_index, std::string_view key, const PrimeValue& pv);
  size_t SerializeZSet(DbIndex db_index, std::string_view key, const PrimeValue& pv);
  size_t SerializeHash(DbIndex db_index, std::string_view key, const PrimeValue& pv);
  size_t SerializeList(DbIndex db_index, std::string_view key, const PrimeValue& pv);
  size_t SerializeString(DbIndex db_index, std::string_view key, const PrimeValue& pv,
                         uint64_t expire_ms);
  void SerializeRestore(DbIndex db_index, std::string_view key, const PrimeKey& pk,
                        const PrimeValue& pv, uint64_t expire_ms);

  FlushSerialized cb_;
  size_t max_serialization_buffer_size_;
  std::unique_ptr<RdbSerializer> serializer_;
};

}  // namespace dfly
