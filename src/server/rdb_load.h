// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/container/btree_set.h>
#include <absl/container/flat_hash_map.h>

#include <system_error>

extern "C" {
#include "redis/rdb.h"
}

#include "base/mpsc_intrusive_queue.h"
#include "base/pod_array.h"
#include "io/io.h"
#include "io/io_buf.h"
#include "server/common.h"
#include "server/journal/serializer.h"
#include "server/tiering/common.h"

namespace dfly {

class EngineShardSet;
class ScriptMgr;
class CompactObj;
class Service;

class DecompressImpl;

using RdbVersion = std::uint16_t;

class RdbLoaderBase {
 protected:
  RdbLoaderBase();
  ~RdbLoaderBase();

  struct LoadTrace;
  using MutableBytes = ::io::MutableBytes;

  struct LzfString {
    base::PODArray<uint8_t> compressed_blob;
    uint64_t uncompressed_len;
  };

  struct RdbSBF {
    double grow_factor, fp_prob;
    size_t prev_size, current_size;
    size_t max_capacity;

    struct Filter {
      unsigned hash_cnt;
      std::string blob;
      Filter(unsigned h, std::string b) : hash_cnt(h), blob(std::move(b)) {
      }
    };
    std::vector<Filter> filters;
  };

  struct RdbTieredSegment {
    size_t offset, length;
    uint8_t enc_mask;
  };

  struct RdbTieredPage {
    size_t offset;
    std::string blob;
  };

  using RdbVariant = std::variant<long long, base::PODArray<char>, LzfString,
                                  std::unique_ptr<LoadTrace>, RdbSBF, RdbTieredSegment>;

  struct OpaqueObj {
    RdbVariant obj;
    int rdb_type;
  };

  struct LoadBlob {
    RdbVariant rdb_var;
    union {
      unsigned encoding;
      double score;
    };
  };

  struct StreamPelTrace {
    std::array<uint8_t, 16> rawid;
    int64_t delivery_time;
    uint64_t delivery_count;
  };

  struct StreamConsumerTrace {
    RdbVariant name;
    int64_t seen_time;
    std::vector<std::array<uint8_t, 16>> nack_arr;
  };

  struct StreamCGTrace {
    RdbVariant name;
    uint64_t ms;
    uint64_t seq;

    std::vector<StreamPelTrace> pel_arr;
    std::vector<StreamConsumerTrace> cons_arr;
  };

  struct StreamTrace {
    size_t lp_len;
    size_t stream_len;
    uint64_t ms, seq;
    std::vector<StreamCGTrace> cgroup;
  };

  struct LoadTrace {
    // Some traces are very long. We divide them into multiple segments.
    std::vector<std::vector<LoadBlob>> arr;
    std::unique_ptr<StreamTrace> stream_trace;

    size_t blob_count() const {
      size_t count = 0;
      for (const auto& seg : arr) {
        count += seg.size();
      }
      return count;
    }
  };

  class OpaqueObjLoader;

  io::Result<uint8_t> FetchType();

  template <typename T> io::Result<T> FetchInt();

  static std::error_code FromOpaque(const OpaqueObj& opaque, CompactObj* pv);

  io::Result<uint64_t> LoadLen(bool* is_encoded);
  std::error_code FetchBuf(size_t size, void* dest);

  io::Result<std::string> FetchGenericString();
  io::Result<std::string> FetchLzfStringObject();
  io::Result<std::string> FetchIntegerObject(int enctype);

  io::Result<double> FetchBinaryDouble();
  io::Result<double> FetchDouble();

  ::io::Result<std::string> ReadKey();

  std::error_code ReadObj(int rdbtype, OpaqueObj* dest);
  std::error_code ReadStringObj(RdbVariant* rdb_variant);
  ::io::Result<long long> ReadIntObj(int encoding);
  ::io::Result<LzfString> ReadLzf();

  ::io::Result<OpaqueObj> ReadSet(int rdbtype);
  ::io::Result<OpaqueObj> ReadIntSet();
  ::io::Result<OpaqueObj> ReadGeneric(int rdbtype);
  ::io::Result<OpaqueObj> ReadHMap(int rdbtype);
  ::io::Result<OpaqueObj> ReadZSet(int rdbtype);
  ::io::Result<OpaqueObj> ReadZSetZL();
  ::io::Result<OpaqueObj> ReadListQuicklist(int rdbtype);
  ::io::Result<OpaqueObj> ReadStreams();
  ::io::Result<OpaqueObj> ReadRedisJson();
  ::io::Result<OpaqueObj> ReadJson();
  ::io::Result<OpaqueObj> ReadSBF();
  ::io::Result<OpaqueObj> ReadTieredSegment();

  std::error_code SkipModuleData();
  std::error_code HandleCompressedBlob(int op_type);
  std::error_code HandleCompressedBlobFinish();
  void AllocateDecompressOnce(int op_type);

  std::error_code HandleJournalBlob(Service* service);

  static size_t StrLen(const RdbVariant& tset);

  std::error_code EnsureRead(size_t min_sz);

  std::error_code EnsureReadInternal(size_t min_to_read);

  base::IoBuf* mem_buf_ = nullptr;
  base::IoBuf origin_mem_buf_;
  ::io::Source* src_ = nullptr;

  size_t bytes_read_ = 0;
  size_t source_limit_ = SIZE_MAX;

  base::PODArray<uint8_t> compr_buf_;
  std::unique_ptr<DecompressImpl> decompress_impl_;

  JournalReader journal_reader_{nullptr, 0};
  std::optional<uint64_t> journal_offset_ = std::nullopt;

  RdbVersion rdb_version_ = RDB_VERSION;
};

class RdbLoader : protected RdbLoaderBase {
 public:
  explicit RdbLoader(Service* service);

  ~RdbLoader();

  std::error_code Load(::io::Source* src);
  void set_source_limit(size_t n) {
    source_limit_ = n;
  }

  ::io::Bytes Leftover() const {
    return mem_buf_->InputBuffer();
  }

  size_t bytes_read() const {
    return bytes_read_;
  }

  size_t keys_loaded() const {
    return keys_loaded_;
  }

  // returns time in seconds.
  double load_time() const {
    return load_time_;
  }

  void stop() {
    stop_early_.store(true);
  }

  // Return the offset that was received with a RDB_OPCODE_JOURNAL_OFFSET command,
  // or 0 if no offset was received.
  std::optional<uint64_t> journal_offset() const {
    return journal_offset_;
  }

  // Set callback for receiving RDB_OPCODE_FULLSYNC_END.
  // This opcode is used by a master instance to notify it finished streaming static data
  // and is ready to switch to stable state sync.
  void SetFullSyncCutCb(std::function<void()> cb) {
    full_sync_cut_cb = std::move(cb);
  }

  // Perform pre load procedures after transitioning into the global LOADING state.
  static void PerformPreLoad(Service* service);

  // Performs post load procedures while still remaining in global LOADING state.
  // Called once immediately after loading the snapshot / full sync succeeded from the coordinator.
  static void PerformPostLoad(Service* service);

 private:
  struct Item {
    std::string key;
    OpaqueObj val;
    uint64_t expire_ms;
    std::atomic<Item*> next;
    bool is_sticky = false;

    friend void MPSC_intrusive_store_next(Item* dest, Item* nxt) {
      dest->next.store(nxt, std::memory_order_release);
    }

    friend Item* MPSC_intrusive_load_next(const Item& src) {
      return src.next.load(std::memory_order_acquire);
    }
  };

  using ItemsBuf = std::vector<Item*>;

  struct ObjSettings;

  std::error_code LoadKeyValPair(int type, ObjSettings* settings);
  void ResizeDb(size_t key_num, size_t expire_num);
  std::error_code HandleAux();

  std::error_code VerifyChecksum();

  void FinishLoad(absl::Time start_time, size_t* keys_loaded);

  void FlushShardAsync(ShardId sid);
  void FlushAllShards();

  void Add(Item* item);
  void LoadItemsBuffer(DbIndex db_ind, const ItemsBuf& ib);

  void LoadScriptFromAux(std::string&& value);

  void HandleSmallItems(bool flush);
  std::error_code LoadTieredPage();

  // Load index definition from RESP string describing it in FT.CREATE format,
  // issues an FT.CREATE call, but does not start indexing
  void LoadSearchIndexDefFromAux(std::string&& value);

 private:
  Service* service_;
  ScriptMgr* script_mgr_;
  std::vector<ItemsBuf> shard_buf_;

  size_t keys_loaded_ = 0;
  double load_time_ = 0;

  DbIndex cur_db_index_ = 0;

  AggregateError ec_;
  std::atomic_bool stop_early_{false};
  std::atomic_uint blocked_shards_{0};

  // Callback when receiving RDB_OPCODE_FULLSYNC_END
  std::function<void()> full_sync_cut_cb;

  base::MPSCIntrusiveQueue<Item> item_queue_;

  absl::flat_hash_map<size_t /* offset */, std::vector<Item*>> small_items_;
  absl::btree_set<std::pair<size_t /* num entries*/, size_t /* offset */>, std::greater<>>
      small_items_sizes_;
  absl::flat_hash_map<size_t /* offset  */, std::variant<std::string, tiering::DiskSegment>>
      small_items_pages_;
};

}  // namespace dfly
