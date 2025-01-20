// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/container/flat_hash_map.h>

extern "C" {
#include "redis/lzfP.h"
#include "redis/quicklist.h"
}

#include <optional>

#include "base/pod_array.h"
#include "io/io.h"
#include "io/io_buf.h"
#include "server/common.h"
#include "server/detail/compressor.h"
#include "server/journal/serializer.h"
#include "server/journal/types.h"
#include "server/table.h"

typedef struct rax rax;
typedef struct streamCG streamCG;
typedef struct quicklistNode quicklistNode;

namespace dfly {

uint8_t RdbObjectType(const PrimeValue& pv);

class EngineShard;
class Service;

class AlignedBuffer : public ::io::Sink {
 public:
  using io::Sink::Write;

  AlignedBuffer(size_t cap, ::io::Sink* upstream);
  ~AlignedBuffer();

  std::error_code Write(std::string_view buf) {
    return Write(io::Buffer(buf));
  }

  io::Result<size_t> WriteSome(const iovec* v, uint32_t len) final;

  std::error_code Flush();

  ::io::Sink* upstream() {
    return upstream_;
  }

 private:
  size_t capacity_;
  ::io::Sink* upstream_;
  char* aligned_buf_ = nullptr;

  off_t buf_offs_ = 0;
};

// SaveMode for snapshot. Used by RdbSaver to adjust internals.
enum class SaveMode {
  SUMMARY,                    // Save only header values (summary.dfs). Expected to read no shards.
  SINGLE_SHARD,               // Save single shard values (XXXX.dfs). Expected to read one shard.
  SINGLE_SHARD_WITH_SUMMARY,  // Save single shard value with the global summary. Used in the
                              // replication's fully sync stage.
  RDB,                        // Save .rdb file. Expected to read all shards.
};

enum class CompressionMode : uint8_t { NONE, SINGLE_ENTRY, MULTI_ENTRY_ZSTD, MULTI_ENTRY_LZ4 };

CompressionMode GetDefaultCompressionMode();

class RdbSaver {
 public:
  // Global data which doesn't belong to shards and is serialized in header
  struct GlobalData {
    const StringVec lua_scripts;     // bodies of lua scripts
    const StringVec search_indices;  // ft.create commands to re-create search indices
  };

  // single_shard - true means that we run RdbSaver on a single shard and we do not use
  // to snapshot all the datastore shards.
  // single_shard - false, means we capture all the data using a single RdbSaver instance
  // (corresponds to legacy, redis compatible mode)
  // if align_writes is true - writes data in aligned chunks of 4KB to fit direct I/O requirements.
  explicit RdbSaver(::io::Sink* sink, SaveMode save_mode, bool align_writes);

  ~RdbSaver();

  // Initiates the serialization in the shard's thread.
  // cll allows breaking in the middle.
  void StartSnapshotInShard(bool stream_journal, Context* cntx, EngineShard* shard);

  // Send only the incremental snapshot since start_lsn.
  void StartIncrementalSnapshotInShard(LSN start_lsn, Context* cntx, EngineShard* shard);

  // Stops full-sync serialization for replication in the shard's thread.
  std::error_code StopFullSyncInShard(EngineShard* shard);

  // Wait for snapshotting finish in shard thread. Called from save flows in shard thread.
  std::error_code WaitSnapshotInShard(EngineShard* shard);

  // Stores auxiliary (meta) values and header_info
  std::error_code SaveHeader(const GlobalData& header_info);

  // Writes the RDB file into sink. Waits for the serialization to finish.
  // Called only for save rdb flow and save df on summary file.
  std::error_code SaveBody(const Context& cntx);

  // Fills freq_map with the histogram of rdb types.
  void FillFreqMap(RdbTypeFreqMap* freq_map);

  void CancelInShard(EngineShard* shard);

  SaveMode Mode() const {
    return save_mode_;
  }

  // Get total size of all rdb serializer buffers and items currently placed in channel
  size_t GetTotalBuffersSize() const;

  struct SnapshotStats {
    size_t current_keys = 0;
    size_t total_keys = 0;
    size_t big_value_preemptions = 0;
  };

  SnapshotStats GetCurrentSnapshotProgress() const;

  // Fetch global data to be serialized in summary part of a snapshot / full sync.
  static GlobalData GetGlobalData(const Service* service);

  // Returns time in nanos of start of the last pending write interaction.
  // Returns -1 if no write operations are currently pending.
  int64_t GetLastWriteTime() const;

 private:
  class Impl;

  std::error_code SaveEpilog();

  std::error_code SaveAux(const GlobalData&);
  std::error_code SaveAuxFieldStrInt(std::string_view key, int64_t val);

  std::unique_ptr<Impl> impl_;
  SaveMode save_mode_;
  CompressionMode compression_mode_;
};

class SerializerBase {
 public:
  enum class FlushState { kFlushMidEntry, kFlushEndEntry };

  explicit SerializerBase(CompressionMode compression_mode);
  virtual ~SerializerBase() = default;

  // Dumps `obj` in DUMP command format into `out`. Uses default compression mode.
  static void DumpObject(const CompactObj& obj, io::StringSink* out);

  // Internal buffer size. Might shrink after flush due to compression.
  size_t SerializedLen() const;

  // Flush internal buffer to sink.
  virtual std::error_code FlushToSink(io::Sink* s, FlushState flush_state);

  size_t GetBufferCapacity() const;
  virtual size_t GetTempBufferSize() const;

  std::error_code WriteRaw(const ::io::Bytes& buf);

  // Write journal entry as an embedded journal blob.
  std::error_code WriteJournalEntry(std::string_view entry);

  // Send FULL_SYNC_CUT opcode to notify that all static data was sent.
  std::error_code SendFullSyncCut();

  std::error_code WriteOpcode(uint8_t opcode);

  std::error_code SaveLen(size_t len);
  std::error_code SaveString(std::string_view val);
  std::error_code SaveString(const uint8_t* buf, size_t len) {
    return SaveString(io::View(io::Bytes{buf, len}));
  }

 protected:
  // Prepare internal buffer for flush. Compress it.
  io::Bytes PrepareFlush(FlushState flush_state);

  // If membuf data is compressable use compression impl to compress the data and write it to membuf
  void CompressBlob();
  void AllocateCompressorOnce();

  std::error_code SaveLzfBlob(const ::io::Bytes& src, size_t uncompressed_len);

  CompressionMode compression_mode_;
  io::IoBuf mem_buf_;
  std::unique_ptr<detail::CompressorImpl> compressor_impl_;

  static constexpr size_t kMinStrSizeToCompress = 256;
  static constexpr double kMinCompressionReductionPrecentage = 0.95;
  struct CompressionStats {
    uint32_t compression_no_effective = 0;
    uint32_t small_str_count = 0;
    uint32_t compression_failed = 0;
    uint32_t compressed_blobs = 0;
  };
  std::optional<CompressionStats> compression_stats_;
  base::PODArray<uint8_t> tmp_buf_;
  std::unique_ptr<LZF_HSLOT[]> lzf_;
  size_t number_of_chunks_ = 0;
};

class RdbSerializer : public SerializerBase {
 public:
  explicit RdbSerializer(CompressionMode compression_mode,
                         std::function<void(size_t, FlushState)> flush_fun = {});

  ~RdbSerializer();

  std::error_code FlushToSink(io::Sink* s, FlushState flush_state) override;
  std::error_code SelectDb(uint32_t dbid);

  // Must be called in the thread to which `it` belongs.
  // Returns the serialized rdb_type or the error.
  // expire_ms = 0 means no expiry.
  // This function might preempt if flush_fun_ is used.
  io::Result<uint8_t> SaveEntry(const PrimeKey& pk, const PrimeValue& pv, uint64_t expire_ms,
                                uint32_t mc_flags, DbIndex dbid);

  // This would work for either string or an object.
  // The arg pv is taken from it->second if accessing
  // this by finding the key. This function is used
  // for the dump command - thus it is public function.
  // This function might preempt if flush_fun_ is used.
  std::error_code SaveValue(const PrimeValue& pv);

  std::error_code SendJournalOffset(uint64_t journal_offset);

  size_t GetTempBufferSize() const override;
  std::error_code SendEofAndChecksum();

 private:
  // Might preempt if flush_fun_ is used
  std::error_code SaveObject(const PrimeValue& pv);
  std::error_code SaveListObject(const PrimeValue& pv);
  std::error_code SaveSetObject(const PrimeValue& pv);
  std::error_code SaveHSetObject(const PrimeValue& pv);
  std::error_code SaveZSetObject(const PrimeValue& pv);
  std::error_code SaveStreamObject(const PrimeValue& obj);
  std::error_code SaveJsonObject(const PrimeValue& pv);
  std::error_code SaveSBFObject(const PrimeValue& pv);

  std::error_code SaveLongLongAsString(int64_t value);
  std::error_code SaveBinaryDouble(double val);
  std::error_code SaveListPackAsZiplist(uint8_t* lp);
  std::error_code SaveStreamPEL(rax* pel, bool nacks);
  std::error_code SaveStreamConsumers(bool save_active, streamCG* cg);
  std::error_code SavePlainNodeAsZiplist(const quicklistNode* node);

  // Might preempt
  void FlushIfNeeded(FlushState flush_state);

  std::string tmp_str_;
  DbIndex last_entry_db_index_ = kInvalidDbId;
  std::function<void(size_t, FlushState)> flush_fun_;
};

}  // namespace dfly
