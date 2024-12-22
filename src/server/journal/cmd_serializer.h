// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/types/span.h>

#include <string>
#include <string_view>

#include "server/table.h"

namespace dfly {

// CmdSerializer serializes DB entries (key+value) into command(s) in RESP format string.
// Small entries are serialized as RESTORE commands, while bigger ones (see
// serialization_max_chunk_size) are split into multiple commands (like rpush, hset, etc).
// Expiration and stickiness are also serialized into commands.
class CmdSerializer {
 public:
  using FlushSerialized = std::function<void(std::string)>;

  explicit CmdSerializer(FlushSerialized cb, size_t max_serialization_buffer_size);

  void SerializeEntry(std::string_view key, const PrimeValue& pk, const PrimeValue& pv,
                      uint64_t expire_ms);

 private:
  void SerializeCommand(std::string_view cmd, absl::Span<const std::string_view> args);
  void SerializeStickIfNeeded(std::string_view key, const PrimeValue& pk);
  void SerializeExpireIfNeeded(std::string_view key, uint64_t expire_ms);

  void SerializeSet(std::string_view key, const PrimeValue& pv);
  void SerializeZSet(std::string_view key, const PrimeValue& pv);
  void SerializeHash(std::string_view key, const PrimeValue& pv);
  void SerializeList(std::string_view key, const PrimeValue& pv);
  void SerializeRestore(std::string_view key, const PrimeValue& pk, const PrimeValue& pv,
                        uint64_t expire_ms);

  FlushSerialized cb_;
  size_t max_serialization_buffer_size_;
};

}  // namespace dfly
