// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <boost/fiber/mutex.hpp>
#include <system_error>

extern "C" {
#include "redis/object.h"
}

#include "base/io_buf.h"
#include "base/pod_array.h"
#include "core/mpsc_intrusive_queue.h"
#include "io/io.h"
#include "server/common.h"

namespace dfly {

class EngineShardSet;
class ScriptMgr;
class CompactObj;

class DecompressImpl;

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

  using RdbVariant =
      std::variant<long long, base::PODArray<char>, LzfString, std::unique_ptr<LoadTrace>>;

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
    std::vector<LoadBlob> arr;
    std::unique_ptr<StreamTrace> stream_trace;
  };

  class OpaqueObjLoader;

  ::io::Result<uint8_t> FetchType() {
    return FetchInt<uint8_t>();
  }

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

  ::io::Result<OpaqueObj> ReadSet();
  ::io::Result<OpaqueObj> ReadIntSet();
  ::io::Result<OpaqueObj> ReadHZiplist();
  ::io::Result<OpaqueObj> ReadHMap();
  ::io::Result<OpaqueObj> ReadZSet(int rdbtype);
  ::io::Result<OpaqueObj> ReadZSetZL();
  ::io::Result<OpaqueObj> ReadListQuicklist(int rdbtype);
  ::io::Result<OpaqueObj> ReadStreams();
  std::error_code HandleCompressedBlob(int op_type);
  std::error_code HandleCompressedBlobFinish();
  void AlocateDecompressOnce(int op_type);

  static size_t StrLen(const RdbVariant& tset);

  std::error_code EnsureRead(size_t min_sz);

  std::error_code EnsureReadInternal(size_t min_sz);

 protected:
  base::IoBuf* mem_buf_ = nullptr;
  base::IoBuf origin_mem_buf_;
  ::io::Source* src_ = nullptr;

  size_t bytes_read_ = 0;
  size_t source_limit_ = SIZE_MAX;
  base::PODArray<uint8_t> compr_buf_;
  std::unique_ptr<DecompressImpl> decompress_impl_;
};

class RdbLoader : protected RdbLoaderBase {
 public:
  explicit RdbLoader(ScriptMgr* script_mgr);

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

  // Set callback for receiving RDB_OPCODE_FULLSYNC_END.
  // This opcode is used by a master instance to notify it finished streaming static data
  // and is ready to switch to stable state sync.
  void SetFullSyncCutCb(std::function<void()> cb) {
    full_sync_cut_cb = std::move(cb);
  }

 private:
  struct Item {
    std::string key;
    OpaqueObj val;
    uint64_t expire_ms;
    std::atomic<Item*> next;

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
  void FlushShardAsync(ShardId sid);
  void FinishLoad(absl::Time start_time, size_t* keys_loaded);

  void LoadItemsBuffer(DbIndex db_ind, const ItemsBuf& ib);

  ScriptMgr* script_mgr_;
  std::unique_ptr<ItemsBuf[]> shard_buf_;

  size_t keys_loaded_ = 0;
  double load_time_ = 0;

  DbIndex cur_db_index_ = 0;

  AggregateError ec_;
  std::atomic_bool stop_early_{false};

  // Callback when receiving RDB_OPCODE_FULLSYNC_END
  std::function<void()> full_sync_cut_cb;
  detail::MPSCIntrusiveQueue<Item> item_queue_;
};

}  // namespace dfly
