// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/container/node_hash_map.h>

#include <boost/intrusive/list.hpp>
#include <memory>
#include <utility>
#include <vector>

#include "core/tiering_types.h"
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
  struct StashDescriptor : public tiering::Fragment::SerializationDescr {
    StashDescriptor() = default;

    StashDescriptor(const tiering::Fragment::SerializationDescr& params)  // NOLINT
        : tiering::Fragment::SerializationDescr(params) {
    }

    size_t EstimatedSerializedSize() const;
    size_t Serialize(io::MutableBytes buffer) const;
  };

  template <typename T> using TResult = util::fb2::Future<io::Result<T>>;
};

struct TieredDelayedEntry {
  DbIndex dbid;
  PrimeKey key;
  util::fb2::Future<io::Result<std::string>> value;
  time_t expire;
  uint32_t mc_flags;
};

using BackPressureFuture = std::optional<util::fb2::Future<bool>>;

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
            const D& decoder, F&& f, bool read_only = true) {
    // TODO(vlad): untangle endless callback wrapping!
    // Templates don't consider implicit conversions, so explicitly convert to std::function
    auto wrapped_cb = [f = std::forward<F>(f)](io::Result<tiering::Decoder*> res) mutable {
      f(res.transform([](auto* d) { return static_cast<D*>(d); }));
    };
    ReadInternal(dbid, key, segment, decoder, wrapped_cb, read_only);
  }

  // Returns StashDescriptor if a value should be stashed.
  std::optional<StashDescriptor> ShouldStash(const tiering::Fragment& fragment) const;

  // Stash value, returns optional future for backpressure is not null.
  // if `provide_bp` is set and conditions are met.
  void Stash(DbIndex dbid, std::string_view key, const StashDescriptor& blobs,
             BackPressureFuture* backpressure);

  // Delete value, must be offloaded (external type)
  void Delete(DbIndex dbid, tiering::Fragment* fragment);

  // Returns true if there is a pending modification for the given segment.
  bool HasModificationPending(tiering::DiskSegment segment) const;

  // Cancel pending stash for the value. Pending fragment will be matched by (dbid, key)
  // and removed from pending fragment container. Must have HasStashPending() true.
  void CancelStash(tiering::PendingId id);

  // Run offloading loop until i/o device is loaded or all entries were traversed
  void RunOffloading(DbIndex dbid);

  // Prune cool entries to reach the set memory goal with freed memory
  size_t ReclaimMemory(size_t goal);

  // Returns the primary value, and deletes the cool item as well as its offloaded storage.
  // Also deregisters the fragment from the central container.
  PrimeValue Warmup(DbIndex dbid, PrimeValue& pv);

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

  // Create a Fragment for the given value and register it as pending under (dbid, key).
  // Returns a stable pointer (node_hash_map guarantees pointer stability).
  tiering::Fragment* AddFragment(DbIndex dbid, std::string_view key,
                                 tiering::Fragment::FragmentType pv);
  void RemoveFragment(tiering::Fragment* fragment);

 private:
  void ReadInternal(DbIndex dbid, std::string_view key, const tiering::DiskSegment& segment,
                    const tiering::Decoder& decoder,
                    std::function<void(io::Result<tiering::Decoder*>)> cb, bool read_only);

  // Moves pv contents to the cool storage and updates pv to point to it.
  void CoolDown(DbIndex db_ind, std::string_view str, const tiering::DiskSegment& segment,
                PrimeValue* pv, tiering::Fragment* fragment);

  PrimeValue DeleteCool(tiering::TieredCoolRecord* record);
  tiering::TieredCoolRecord* PopCool();

  PrimeTable::Cursor offloading_cursor_;  // where RunOffloading left off

  // Stash operations waiting for completion to throttle
  tiering::EntryMap<::util::fb2::Future<bool>> stash_backpressure_;

  std::unique_ptr<ShardOpManager> op_manager_;
  std::unique_ptr<tiering::SmallBins> bins_;

  using CoolQueue = ::boost::intrusive::list<tiering::TieredCoolRecord>;
  CoolQueue cool_queue_;

  // Fragment container — keyed by monotonic id, pointer-stable.
  using TieredFragments = absl::node_hash_map<size_t, tiering::Fragment>;
  size_t next_fragment_id_ = 0;
  TieredFragments tiered_fragments_;

  // Maps (dbid, key) -> fragment id for stash-pending entries.
  // Safe against DashTable relocations (keys are copied strings).
  tiering::EntryMap<size_t> pending_fragments_;

  // Look up and remove a pending fragment by (dbid, key). Returns the Fragment pointer.
  tiering::Fragment* TakePendingFragment(const tiering::KeyRef& key);

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

// Stash value if it meets criteria. If the value was stashed and `backpressure` is not nullptr,
// assign/set the backpressure future to `*backpressure`.
void StashPrimeValue(DbIndex dbid, std::string_view key, PrimeValue* pv, TieredStorage* ts,
                     BackPressureFuture* backpressure);
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
            F&& f, bool read_only = true) {
  }

  template <typename T>
  TResult<T> Modify(DbIndex dbid, std::string_view key, const PrimeValue& value,
                    std::function<T(std::string*)> modf) {
    return {};
  }

  std::optional<StashDescriptor> ShouldStash(const tiering::Fragment& fragment) const {
    return {};
  }

  void Stash(DbIndex dbid, std::string_view key, const StashDescriptor& blobs,
             BackPressureFuture* backpressure) {
  }

  void Delete(DbIndex dbid, tiering::Fragment* fragment) {
  }

  bool HasModificationPending(tiering::DiskSegment segment) const {
    return false;
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

  void CancelStash(tiering::PendingId id) {
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

  PrimeValue Warmup(DbIndex dbid, PrimeValue& pv) {
    return PrimeValue{};
  }

  tiering::Fragment* AddFragment(DbIndex dbid, std::string_view key,
                                 tiering::Fragment::FragmentType pv) {
    return nullptr;
  }

  void RemoveFragment(tiering::Fragment* fragment) {
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

inline void StashPrimeValue(DbIndex dbid, std::string_view key, PrimeValue* pv, TieredStorage* ts,
                            BackPressureFuture* backpressure) {
}

#endif  // WITH_TIERING

}  // namespace dfly
