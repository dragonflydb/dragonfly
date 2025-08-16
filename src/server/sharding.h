// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string_view>

#include "tx_base.h"

namespace dfly {

ShardId Shard(std::string_view v, ShardId shard_num);

namespace sharding {
void InitThreadLocals(uint32_t shard_set_size);
}

}  // namespace dfly
