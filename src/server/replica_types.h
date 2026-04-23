// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <string>
#include <vector>

#include "server/common_types.h"

namespace dfly {

struct ReplicaSummary {
  std::string host;
  uint16_t port;
  bool master_link_established;
  bool full_sync_in_progress;
  bool full_sync_done;
  time_t master_last_io_sec;  // monotonic clock.
  std::string master_id;
  uint32_t reconnect_count;

  // sum of the offsets on all the flows.
  uint64_t repl_offset_sum;
  size_t psync_attempts;
  size_t psync_successes;
  // We can't rely on full_sync_done or full_sync_in_progress because
  // on disconnects the replica state mask is cleared. We use this variable
  // to track if the replica reached full sync. When master disconnects,
  // we use this variable to print the journal offsets in info command even
  // when the link is down. It's reset whenever a full sync is initiated again.
  bool passed_full_sync;
};

struct LastMasterSyncData {
  std::string id;
  std::vector<LSN> last_journal_LSNs;  // lsn for each master shard.
};

}  // namespace dfly
