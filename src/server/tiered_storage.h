// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <boost/intrusive/list.hpp>
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

  explicit TieredStorage(size_t max_file_size, DbSlice* db_slice);
  ~TieredStorage();  // drop forward declared unique_ptrs

  TieredStorage(TieredStorage&& other) = delete;
  TieredStorage(const TieredStorage& other) = delete;

  std::error_code Open(std::string_view path);
  void Close();

  void SetMemoryLowWatermark(size_t mem_limit);

  // Read offloaded value. It must be of external type
  util::fb2::Future<std::string> Read(DbIndex dbid, std::string_view key, const PrimeValue& value);

  // Read offloaded value. It must be of external type
  void Read(DbIndex dbid, std::string_view key, const PrimeValue& value,
            std::function<void(const std::string&)> readf);

  // Apply modification to offloaded value, return generic result from callback.
  // Unlike immutable Reads - the modified value must be uploaded back to memory.
  // This is handled by OpManager when modf completes.
  template <typename T>
  util::fb2::Future<T> Modify(DbIndex dbid, std::string_view key, const PrimeValue& value,
                              std::function<T(std::string*)> modf);

  // Stash value. Sets IO_PENDING flag and unsets it on error or when finished
  // Returns true if item was scheduled for stashing.
  bool TryStash(DbIndex dbid, std::string_view key, PrimeValue* value);

  // Delete value, must be offloaded (external type)
  void Delete(DbIndex dbid, PrimeValue* value);

  // Cancel pending stash for value, must have IO_PENDING flag set
  void CancelStash(DbIndex dbid, std::string_view key, PrimeValue* value);

  // Percentage (0-1) of currently used storage_write_depth for ongoing stashes
  float WriteDepthUsage() const;

  TieredStats GetStats() const;

  // Run offloading loop until i/o device is loaded or all entries were traversed
  void RunOffloading(DbIndex dbid);

  // Prune cool entries to reach the set memory goal with freed memory
  size_t ReclaimMemory(size_t goal);

  // Returns the primary value, and deletes the cool item as well as its offloaded storage.
  PrimeValue Warmup(DbIndex dbid, PrimeValue::CoolItem item);

  size_t CoolMemoryUsage() const {
    return stats_.cool_memory_used;
  }

 private:
  // Returns if a value should be stashed
  bool ShouldStash(const PrimeValue& pv) const;

  // Moves pv contents to the cool storage and updates pv to point to it.
  void CoolDown(DbIndex db_ind, std::string_view str, const tiering::DiskSegment& segment,
                PrimeValue* pv);

  PrimeValue DeleteCool(detail::TieredColdRecord* record);
  detail::TieredColdRecord* PopCool();

  PrimeTable::Cursor offloading_cursor_{};  // where RunOffloading left off

  std::unique_ptr<ShardOpManager> op_manager_;
  std::unique_ptr<tiering::SmallBins> bins_;
  typedef ::boost::intrusive::list<detail::TieredColdRecord> CoolQueue;

  CoolQueue cool_queue_;

  unsigned write_depth_limit_ = 10;
  struct {
    uint64_t stash_overflow_cnt = 0;
    uint64_t total_deletes = 0;
    uint64_t offloading_steps = 0;
    uint64_t offloading_stashes = 0;
    size_t cool_memory_used = 0;
  } stats_;
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

  explicit TieredStorage(size_t max_size, DbSlice* db_slice) {
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

  void Read(DbIndex dbid, std::string_view key, const PrimeValue& value,
            std::function<void(const std::string&)> readf) {
  }

  template <typename T>
  util::fb2::Future<T> Modify(DbIndex dbid, std::string_view key, const PrimeValue& value,
                              std::function<T(std::string*)> modf) {
    return {};
  }

  void TryStash(DbIndex dbid, std::string_view key, PrimeValue* value) {
  }

  void Delete(DbIndex dbid, PrimeValue* value) {
  }

  size_t ReclaimMemory(size_t goal) {
    return 0;
  }

  float WriteDepthUsage() const {
    return 0;
  }

  size_t CoolMemoryUsage() const {
    return 0;
  }

  void CancelStash(DbIndex dbid, std::string_view key, PrimeValue* value) {
  }

  bool ShouldStash(const PrimeValue& pv) const {
    return false;
  }

  TieredStats GetStats() const {
    return {};
  }

  void RunOffloading(DbIndex dbid) {
  }

  PrimeValue Warmup(DbIndex dbid, PrimeValue::CoolItem item) {
    return PrimeValue{};
  }
};

}  // namespace dfly

#endif  // __linux__
