// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <string_view>

namespace dfly {

using ShardId = uint16_t;  // Forward declare typedef from tx_base.h

ShardId Shard(std::string_view v, ShardId shard_num);

namespace sharding {
void InitThreadLocals(uint32_t shard_set_size);
}

}  // namespace dfly
