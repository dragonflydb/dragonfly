// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <string_view>
#include <system_error>
#include <vector>

namespace dfly {

class Service;

// Where a shard's log resumes after replay.
struct AofResume {
  uint64_t seq = 0;
  uint64_t next_lsn = 1;
};

#ifdef __linux__

// Replays every shard's AOF chain in parallel, global commands meeting at the barrier, and fills
// resume with where each shard's log continues. Runs while the server is LOADING.
std::error_code ReplayAof(std::string_view dir, Service* service, std::vector<AofResume>* resume);

#else

// The chain reader needs io_uring files, so --aof is rejected before replay.
inline std::error_code ReplayAof(std::string_view dir, Service* service,
                                 std::vector<AofResume>* resume) {
  return std::make_error_code(std::errc::not_supported);
}

#endif  // __linux__

}  // namespace dfly
