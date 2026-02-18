// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/container/flat_hash_map.h>

#include <boost/intrusive/list.hpp>
#include <memory>
#include <utility>
#include <vector>

#include "io/io.h"  // for io::Result (TODO: replace with nonstd/expected)
#include "server/stats.h"
#include "server/table.h"
#include "server/tiering/common.h"
#include "server/tiering/entry_map.h"
#include "util/fibers/future.h"

namespace dfly {

class DbSlice;

namespace tiering {
class SmallBins;
struct Decoder;
};  // namespace tiering

struct TieredStorageBase {
  // Min sizes of values taking up full page on their own
  const static size_t kMinOccupancySize = tiering::kPageSize / 2;
  struct StashDescriptor {
    std::variant<std::array<std::string_view, 2>, uint8_t*> blob;
    CompactObj::ExternalRep rep;

    size_t EstimatedSerializedSize() const;
    size_t Serialize(io::MutableBytes buffer) const;
  };

  template <typename T> using TResult = util::fb2::Future<io::Result<T>>;
};

#ifdef WITH_TIERING

// Manages offloaded values
class TieredStorage : public TieredStorageBase {
  class ShardOpManager;

 public:
  explicit TieredStorage(size_t max_file_size, DbSlice* db_slice);
  ~TieredStorage();  // drop forward declared unique_ptrs

  TieredStorage(TieredStorage&& other) = delete;
  TieredStorage(const TieredStorage& other) = delete;

  std::error_code Open(std::string_view path);
  void Close();

  // Enqueue read external value with generic decoder.
  template <typename D, typename F>
  void Read(DbIndex dbid, std::string_view key, const tiering::DiskSegment& segment,
            const D& decoder, F&& f) {
    // TODO(vlad): untangle endless callback wrapping!
    // Templates don't consider implicit conversions, so explicitly convert to std::function
    auto wrapped_cb = [f = std::forward<F>(f)](io::Result<tiering::Decoder*> res) mutable {
      f(res.transform([](auto* d) { return static_cast<D*>(d); }));
    };
    ReadInternal(dbid, key, segment, decoder, wrapped_cb);
  }

  // Returns StashDescriptor if a value should be stashed.
  std::optional<StashDescriptor> ShouldStash(const PrimeValue& pv) const;

  // Stash value, returns optional future for backpressure
  // if `provide_bp` is set and conditions are met.
  std::optional<util::fb2::Future<bool>> Stash(DbIndex dbid, std::string_view key,
                                               const StashDescriptor& blobs, bool provide_bp);

  // Delete value, must be offloaded (external type)
  void Delete(DbIndex dbid, PrimeValue* value);

  // Cancel pending stash for value, must have IO_PENDING flag set
  void CancelStash(DbIndex dbid, std::string_view key, PrimeValue* value);

  // Run offloading loop until i/o device is loaded or all entries were traversed
  void RunOffloading(DbIndex dbid);

  // Prune cool entries to reach the set memory goal with freed memory
  size_t ReclaimMemory(size_t goal);

  // Returns the primary value, and deletes the cool item as well as its offloaded storage.
  PrimeValue Warmup(DbIndex dbid, PrimeValue::CoolItem item);

  TieredStats GetStats() const;

  void UpdateFromFlags();  // Update internal values based on current flag values
  static std::vector<std::string> GetMutableFlagNames();  // Triggers UpdateFromFlags

  bool ShouldOffload() const;     // True if below tiered_offload_threshold
  float WriteDepthUsage() const;  // Ratio (0-1) of used storage_write_depth for stashes

  // How much we are above tiered_upload_threshold. Can be negative!
  int64_t UploadBudget() const;
  size_t CoolMemoryUsage() const {
    return stats_.cool_memory_used;
  }

 private:
  void ReadInternal(DbIndex dbid, std::string_view key, const tiering::DiskSegment& segment,
                    const tiering::Decoder& decoder,
                    std::function<void(io::Result<tiering::Decoder*>)> cb);

  // Moves pv contents to the cool storage and updates pv to point to it.
  void CoolDown(DbIndex db_ind, std::string_view str, const tiering::DiskSegment& segment,
                CompactObj::ExternalRep rep, PrimeValue* pv);

  PrimeValue DeleteCool(detail::TieredColdRecord* record);
  detail::TieredColdRecord* PopCool();

  PrimeTable::Cursor offloading_cursor_{};  // where RunOffloading left off

  // Stash operations waiting for completion to throttle
  tiering::EntryMap<::util::fb2::Future<bool>> stash_backpressure_;

  std::unique_ptr<ShardOpManager> op_manager_;
  std::unique_ptr<tiering::SmallBins> bins_;

  using CoolQueue = ::boost::intrusive::list<detail::TieredColdRecord>;
  CoolQueue cool_queue_;

  struct {
    size_t min_value_size;
    bool experimental_cooling;
    unsigned write_depth_limit;
    float offload_threshold;
    float upload_threshold;
    bool experimental_hash_offload;
  } config_;

  mutable struct {
    uint64_t stash_overflow_cnt = 0;
    uint64_t total_deletes = 0;
    uint64_t offloading_steps = 0;
    uint64_t offloading_stashes = 0;
    uint64_t total_clients_throttled = 0;
    size_t cool_memory_used = 0;
  } stats_;
};

// Read offloaded value. It must be of external string type
void ReadTiered(DbIndex dbid, std::string_view key, const PrimeValue& value,
                std::function<void(io::Result<std::string_view>)> readf, TieredStorage* ts);

// Read offloaded value and apply transformation cb on the read result. Returns future of the
// transformed result.
template <typename T>
TieredStorage::TResult<T> ReadTiered(DbIndex dbid, std::string_view key, const PrimeValue& value,
                                     std::function<T(std::string_view)> cb, TieredStorage* ts) {
  TieredStorage::TResult<T> fut;
  auto read_cb = [fut, cb = std::move(cb)](io::Result<std::string_view> res) mutable {
    fut.Resolve(res.transform([&](std::string_view sv) { return cb(sv); }));
  };
  ReadTiered(dbid, key, value, std::move(read_cb), ts);
  return fut;
}

inline TieredStorage::TResult<std::string> ReadTieredString(DbIndex dbid, std::string_view key,
                                                            const PrimeValue& value,
                                                            TieredStorage* ts) {
  return ReadTiered<std::string>(
      dbid, key, value, [](std::string_view val) { return std::string(val); }, ts);
}

// Reads offloaded value, and applies modifications on it and return generic result from callback.
// Unlike with immutable Reads - the modified value will be uploaded back to memory.
// This is handled by OpManager when modf completes.
template <typename T>
TieredStorage::TResult<T> ModifyTiered(DbIndex dbid, std::string_view key, const PrimeValue& value,
                                       std::function<T(std::string*)> modf, TieredStorage* ts);

std::optional<util::fb2::Future<bool>> StashPrimeValue(DbIndex dbid, std::string_view key,
                                                       bool provide_bp, PrimeValue* pv,
                                                       TieredStorage* ts);
#else

class TieredStorage : public TieredStorageBase {
  class ShardOpManager;

 public:
  explicit TieredStorage(size_t max_size, DbSlice* db_slice) {
  }

  TieredStorage(TieredStorage&& other) = delete;
  TieredStorage(const TieredStorage& other) = delete;

  std::error_code Open(std::string_view path) {
    return {};
  }

  void Close() {
  }

  // Read offloaded value. It must be of external type
  void Read(DbIndex dbid, std::string_view key, const PrimeValue& value,
            std::function<void(io::Result<std::string_view>)> readf) {
  }

  template <typename D, typename F>
  void Read(DbIndex dbid, std::string_view key, const tiering::DiskSegment& value, const D& decoder,
            F&& f) {
  }

  template <typename T>
  TResult<T> Modify(DbIndex dbid, std::string_view key, const PrimeValue& value,
                    std::function<T(std::string*)> modf) {
    return {};
  }

  std::optional<StashDescriptor> ShouldStash(const PrimeValue& pv) const {
    return {};
  }

  std::optional<util::fb2::Future<bool>> Stash(DbIndex dbid, std::string_view key,
                                               const StashDescriptor& blobs, bool provide_bp) {
    return {};
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

  TieredStats GetStats() const {
    return {};
  }

  void RunOffloading(DbIndex dbid) {
  }

  void UpdateFromFlags() {
  }

  static std::vector<std::string> GetMutableFlagNames() {
    return {};
  }

  bool ShouldOffload() const {
    return false;
  }

  int64_t UploadBudget() const {
    return 0;
  }

  PrimeValue Warmup(DbIndex dbid, PrimeValue::CoolItem item) {
    return PrimeValue{};
  }
};

template <typename T>
TieredStorage::TResult<T> ReadTiered(DbIndex dbid, std::string_view key, const PrimeValue& value,
                                     std::function<T(std::string_view)> cb, TieredStorage* ts) {
  return {};
}

inline void ReadTiered(DbIndex dbid, std::string_view key, const PrimeValue& value,
                       std::function<void(io::Result<std::string_view>)> readf, TieredStorage* ts) {
}

inline TieredStorage::TResult<std::string> ReadTieredString(DbIndex dbid, std::string_view key,
                                                            const PrimeValue& value,
                                                            TieredStorage* ts) {
  return {};
}

template <typename T>
TieredStorage::TResult<T> ModifyTiered(DbIndex dbid, std::string_view key, const PrimeValue& value,
                                       std::function<T(std::string*)> modf, TieredStorage* ts) {
  return {};
}

inline std::optional<util::fb2::Future<bool>> StashPrimeValue(DbIndex dbid, std::string_view key,
                                                              bool provide_bp, PrimeValue* pv,
                                                              TieredStorage* ts) {
  return {};
}

#endif  // WITH_TIERING

}  // namespace dfly
