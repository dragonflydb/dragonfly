// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <memory>

#include "server/tiering/common.h"
#include "util/fibers/future.h"
#ifdef __linux__

#include <absl/container/flat_hash_map.h>

#include "server/common.h"
#include "server/table.h"

namespace dfly {

class DbSlice;

namespace tiering {
class SmallBins;
};

// Manages offloaded values
class TieredStorageV2 {
  class ShardOpManager;

  const static size_t kMinValueSize = 64;

  // Min sizes of values taking up full page on their own
  const static size_t kMinOccupancySize = tiering::kPageSize / 2;

 public:
  explicit TieredStorageV2(DbSlice* db_slice);
  ~TieredStorageV2();  // drop forward declared unique_ptrs

  TieredStorageV2(TieredStorageV2&& other) = delete;
  TieredStorageV2(const TieredStorageV2& other) = delete;

  std::error_code Open(std::string_view path);
  void Close();

  // Read offloaded value. It must be of external type
  util::fb2::Future<std::string> Read(std::string_view key, const PrimeValue& value);

  // Apply modification to offloaded value, return generic result from callback
  template <typename T>
  util::fb2::Future<T> Modify(std::string_view key, const PrimeValue& value,
                              std::function<T(std::string*)> modf);

  // Stash value. Sets IO_PENDING flag and unsets it on error or when finished
  void Stash(std::string_view key, PrimeValue* value);

  // Delete value. Must either have pending IO or be offloaded (of external type)
  void Delete(std::string_view key, PrimeValue* value);

  // Returns if a value should be stashed
  bool ShouldStash(const PrimeValue& pv);

  TieredStats GetStats() const;

 private:
  std::unique_ptr<ShardOpManager> op_manager_;
  std::unique_ptr<tiering::SmallBins> bins_;
};

}  // namespace dfly

#else

#include "server/common.h"

class DbSlice;

// This is a stub implementation for non-linux platforms.
namespace dfly {

// Manages offloaded values
class TieredStorageV2 {
  class ShardOpManager;

  const static size_t kMinValueSize = tiering::kPageSize / 2;

 public:
  explicit TieredStorageV2(DbSlice* db_slice) {
  }

  TieredStorageV2(TieredStorageV2&& other) = delete;
  TieredStorageV2(const TieredStorageV2& other) = delete;

  std::error_code Open(std::string_view path) {
  }

  void Close() {
  }

  // Read offloaded value. It must be of external type
  util::fb2::Future<std::string> Read(std::string_view key, const PrimeValue& value) {
    return {};
  }

  // Stash value. Sets IO_PENDING flag and unsets it on error or when finished
  void Stash(std::string_view key, PrimeValue* value) {
  }

  // Delete value. Must either have pending IO or be offloaded (of external type)
  void Delete(std::string_view key, PrimeValue* value) {
  }

  TieredStats GetStats() {
    return TieredStats{};
  }
};

}  // namespace dfly

#endif  // __linux__
