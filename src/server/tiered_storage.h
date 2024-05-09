// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <memory>
#include <utility>

#include "server/tiering/common.h"
#include "server/tx_base.h"
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
class TieredStorage {
  class ShardOpManager;

 public:
  const static size_t kMinValueSize = 64;

  // Min sizes of values taking up full page on their own
  const static size_t kMinOccupancySize = tiering::kPageSize / 2;

  explicit TieredStorage(DbSlice* db_slice, size_t max_size);
  ~TieredStorage();  // drop forward declared unique_ptrs

  TieredStorage(TieredStorage&& other) = delete;
  TieredStorage(const TieredStorage& other) = delete;

  std::error_code Open(std::string_view path);
  void Close();

  // Read offloaded value. It must be of external type
  util::fb2::Future<std::string> Read(DbIndex dbid, std::string_view key, const PrimeValue& value);

  // Read offloaded value. It must be of external type
  void Read(DbIndex dbid, std::string_view key, const PrimeValue& value,
            std::function<void(const std::string&)> readf);

  // Apply modification to offloaded value, return generic result from callback
  template <typename T>
  util::fb2::Future<T> Modify(DbIndex dbid, std::string_view key, const PrimeValue& value,
                              std::function<T(std::string*)> modf);

  // Stash value. Sets IO_PENDING flag and unsets it on error or when finished
  void Stash(DbIndex dbid, std::string_view key, PrimeValue* value);

  // Delete value, must be offloaded (external type)
  void Delete(PrimeValue* value);

  // Cancel pending stash for value, must have IO_PENDING flag set
  void CancelStash(DbIndex dbid, std::string_view key, PrimeValue* value);

  // Returns if a value should be stashed
  bool ShouldStash(const PrimeValue& pv) const;

  TieredStats GetStats() const;

  // Run offloading loop until i/o device is loaded or all entries were traversed
  void RunOffloading(DbIndex dbid);

 private:
  PrimeTable::Cursor offloading_cursor_{};  // where RunOffloading left off

  std::unique_ptr<ShardOpManager> op_manager_;
  std::unique_ptr<tiering::SmallBins> bins_;
};

}  // namespace dfly

#else

#include "server/common.h"

class DbSlice;

// This is a stub implementation for non-linux platforms.
namespace dfly {
class TieredStorage {
  class ShardOpManager;

 public:
  const static size_t kMinValueSize = 64;

  // Min sizes of values taking up full page on their own
  const static size_t kMinOccupancySize = tiering::kPageSize / 2;

  explicit TieredStorage(DbSlice* db_slice, size_t max_size) {
  }

  TieredStorage(TieredStorage&& other) = delete;
  TieredStorage(const TieredStorage& other) = delete;

  std::error_code Open(std::string_view path) {
    return {};
  }

  void Close() {
  }

  util::fb2::Future<std::string> Read(DbIndex dbid, std::string_view key, const PrimeValue& value) {
    return {};
  }

  template <typename T>
  util::fb2::Future<T> Modify(DbIndex dbid, std::string_view key, const PrimeValue& value,
                              std::function<T(std::string*)> modf) {
    return {};
  }

  void Stash(DbIndex dbid, std::string_view key, PrimeValue* value) {
  }

  void Delete(PrimeValue* value) {
  }

  void CancelStash(DbIndex dbid, std::string_view key, PrimeValue* value) {
  }

  bool ShouldStash(const PrimeValue& pv) {
    return false;
  }

  TieredStats GetStats() const {
    return {};
  }

  void RunOffloading(DbIndex dbid) {
  }
};

}  // namespace dfly

#endif  // __linux__
