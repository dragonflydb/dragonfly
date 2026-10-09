// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#include "io/io.h"

namespace dfly {

inline constexpr uint32_t kAofManifestVersion = 1;
inline constexpr std::string_view kAofManifestName = "appendonly.manifest";

// Names the base and where each shard's log starts after it.
struct AofManifest {
  enum class State : uint8_t {
    // First start in progress: on startup, delete the other AOF files and bootstrap again.
    kBootstrapping,
    kActive,
  };

  struct Cut {
    // First segment to read.
    uint64_t seq = 0;
    // First record not in the base.
    uint64_t lsn = 0;

    bool operator==(const Cut&) const = default;
  };

  State state = State::kBootstrapping;
  uint64_t checkpoint_id = 0;
  // AOF-owned bases are deleted once superseded; user dumps never are.
  bool base_owned = false;
  // TODO: only DFS for now. Relative to --dir; empty without a base.
  std::string base_path;
  // Logical-time option only.
  uint64_t cut_time_ms = 0;
  // Indexed by shard id; its size is the shard count.
  std::vector<Cut> cuts;

  bool operator==(const AofManifest&) const = default;
};

std::string EncodeAofManifest(const AofManifest& manifest);
io::Result<AofManifest> DecodeAofManifest(std::string_view text);

// Replaces the manifest atomically: tmp file -> fdatasync -> rename -> fsync dir.
std::error_code WriteAofManifest(std::string_view dir, const AofManifest& manifest);

// ENOENT if there is none.
io::Result<AofManifest> ReadAofManifest(std::string_view dir);

// Paths of the AOF files in dir: the manifest, the segments and their leftovers.
io::Result<std::vector<std::string>> ListAofFiles(std::string_view dir);

// Deletes every AOF file but the manifest, to redo an interrupted bootstrap.
std::error_code RemoveAofFilesExceptManifest(std::string_view dir);

// Deletes the segments below each shard's cut and leftover tmp files. *.discarded are kept until
// the next checkpoint, which passes remove_discarded.
std::error_code CollectAofGarbage(std::string_view dir, const AofManifest& manifest,
                                  bool remove_discarded = false);

}  // namespace dfly
