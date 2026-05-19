// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/container/flat_hash_set.h>
#include <absl/types/span.h>

extern "C" {
#include "redis/lzfP.h"
}

#include <optional>

#include "base/pod_array.h"
#include "io/io.h"
#include "io/io_buf.h"
#include "server/detail/compressor.h"
#include "server/execution_state.h"
#include "server/table.h"
#include "server/version.h"

typedef struct rax rax;
typedef struct streamCG streamCG;

namespace dfly::search {
struct HnswNodeData;
}  // namespace dfly::search

namespace dfly {

// keys are RDB_TYPE_xxx constants.
using RdbTypeFreqMap = absl::flat_hash_map<unsigned, size_t>;

uint8_t RdbObjectType(const CompactObj& pv);

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

using StringVec = std::vector<std::string>;

// Manages per-entry IO buffers for the RDB serializer, enabling tagged chunk framing for
// interleaved serialization of multiple keys. When tagging is enabled, entries that were split
// across multiple flushes are prefixed with a [opcode:1][stream_id:4][payload_length:4] header so
// the loader can reassemble them.
class MemBufController {
  friend class MemBufControllerTest;

 public:
  using EntryId = uint32_t;
  // Tagged chunk envelope: [RDB_OPCODE_TAGGED_CHUNK:1][stream_id:4][payload_length:4]
  static constexpr auto kHeaderSize = 9;

  // Makes entry_buffer_ the current write target and assigns a new entry id.
  // Must be paired with FinishEntry().
  void StartEntry();

  // Finalizes the active entry. Drains any remaining data from entry_buffer_ into the
  // default buffer (tagging it if the entry was split), then resets to default state.
  void FinishEntry();

  // Moves data from entry_buffer_ into the default buffer. If the entry was
  // split and tagging is enabled, a tag header is prepended.
  void TagAndDrainToDefaultBuffer();

  io::IoBuf* CurrentBuffer() const {
    return current_buffer_;
  }

  // Marks the active entry as having been split across multiple flushes. Once marked,
  // later flushes of this entry's data will be tagged with a chunk header.
  void MarkEntrySplit() {
    split_entries_.insert(active_id_);
  }

  // Total bytes available for flushing: current entry buffer + any previously drained
  // data sitting in the default buffer.
  size_t FlushableSize() const;

  // Captures the active entry id and points the current buffer to the default buffer. Called before
  // the serializer's consume callback, which may preempt and allow other entries to interleave.
  [[nodiscard]] EntryId SaveStateBeforeConsume();

  // Restores a previously saved entry id after the consume callback returns. Points the current
  // buffer to entry_buffer_.
  void RestoreStateAfterConsume(EntryId id);

  // Assembles a flush blob in the following steps:
  // 1. Prepends any data in the default buffer (from previously finished entries) as a prefix.
  // 2. If the active entry was split, a tag header is inserted before current_bytes.
  // 3. current_bytes is added.
  // 3. Consumes all buffers used.
  // current_bytes is typically CurrentBuffer()->InputBuffer(), passed explicitly because it works
  // on data returned by PrepareFlush.
  [[nodiscard]] std::string BuildBlob(io::Bytes current_bytes);

  void SetTagEntries(bool tag_entries) {
    send_tagged_entries_ = tag_entries;
  }

 private:
  // Builds a 9-byte tagged chunk header for the active entry with the given payload size.
  std::array<uint8_t, 9> MakeTagHeader(size_t size) const;

  bool send_tagged_entries_ = false;

  EntryId next_id_ = 1;
  EntryId active_id_ = 0;

  io::IoBuf default_buffer_{4096};
  io::IoBuf entry_buffer_{4096};

  // intent lock to check that some entry id does not own the entry buffer before writing to it
  EntryId entry_buffer_owner_ = 0;
  io::IoBuf* current_buffer_ = &default_buffer_;
  absl::flat_hash_set<EntryId> split_entries_;
};

class RdbSaver {
 public:
  // Global data which doesn't belong to shards and is serialized in header
  struct GlobalData {
    const StringVec lua_scripts;      // bodies of lua scripts
    const StringVec search_indices;   // ft.create commands to re-create search indices
    const StringVec search_synonyms;  // ft.synupdate commands to restore synonyms
    size_t table_used_memory = 0;     // total memory used by all tables in all shards
  };

  // single_shard - true means that we run RdbSaver on a single shard and we do not use
  // to snapshot all the datastore shards.
  // single_shard - false, means we capture all the data using a single RdbSaver instance
  // (corresponds to legacy, redis compatible mode)
  // if align_writes is true - writes data in aligned chunks of 4KB to fit direct I/O requirements.
  // snapshot_id - allows to identify that group of files belongs to the same snapshot
  // replica_dfly_version - upper bound for conditional serialization of new features.
  explicit RdbSaver(::io::Sink* sink, SaveMode save_mode, bool align_writes,
                    std::string snapshot_id, DflyVersion replica_dfly_version);

  ~RdbSaver();

  // Initiates the serialization in the shard's thread.
  // cll allows breaking in the middle.
  void StartSnapshotInShard(bool stream_journal, ExecutionState* cntx, EngineShard* shard);

  // Stops full-sync serialization for replication in the shard's thread.
  std::error_code StopFullSyncInShard(EngineShard* shard);

  // Wait for snapshotting finish in shard thread. Called from save flows in shard thread.
  std::error_code WaitSnapshotInShard(EngineShard* shard);

  // Stores auxiliary (meta) values and header_info
  std::error_code SaveHeader(const GlobalData& header_info);

  // Writes the RDB file into sink. Waits for the serialization to finish.
  // Called only for save rdb flow and save df on summary file.
  std::error_code SaveBody(const ExecutionState& cntx);

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

  // Fetch global data to be serialized in snapshot.
  // is_summary: true for summary file (full data with JSON search indices),
  //             false for per-shard files (only simple search index restore commands)
  static GlobalData GetGlobalData(const Service* service, bool is_summary);

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
  DflyVersion replica_dfly_version_ = DflyVersion::CURRENT_VER;
  std::string snapshot_id_;
};

class RdbSerializer {
 public:
  enum class FlushState : uint8_t { kFlushMidEntry, kFlushEndEntry };

  // ConsumeFun is called when internal buffer exceeds flush_threshold.
  // The callback receives the extracted data.
  using ConsumeFun = std::function<void(std::string)>;

  explicit RdbSerializer(CompressionMode compression_mode, ConsumeFun consume_fun = {},
                         size_t flush_threshold = 0);

  ~RdbSerializer();

  // Dumps `obj` in DUMP command format into `out`. Uses default compression mode.
  static std::string DumpValue(const PrimeValue& obj, bool ignore_crc = false);
  static std::string DumpValue(RdbSerializer* serializer, const PrimeValue& obj,
                               bool ignore_crc = false);

  // Internal buffer size. Might shrink after flush due to compression.
  size_t SerializedLen() const;

  // Flush internal buffer and return serialized blob.
  std::string Flush(FlushState flush_state);

  size_t GetBufferCapacity() const;
  size_t GetTempBufferSize() const;

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

  uint64_t GetSerializationPeakBytes() const {
    return serialization_peak_bytes_;
  }

  void SetCompressionMode(CompressionMode mode) {
    compression_mode_ = mode;
  }

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

  // Save HNSW index entry using provided tmp_buf for serialization to avoid repeated allocations.
  std::error_code SaveHNSWEntry(const search::HnswNodeData& node, absl::Span<uint8_t> tmp_buf);

  std::error_code SendEofAndChecksum();

 private:
  // Prepare internal buffer for flush. Compress it.
  io::Bytes PrepareFlush(FlushState flush_state);

  // If membuf data is compressable use compression impl to compress the data and write it to membuf
  void CompressBlob();
  void AllocateCompressorOnce();

  std::error_code SaveLzfBlob(const ::io::Bytes& src, size_t uncompressed_len);

  // Might preempt if flush_fun_ is used
  std::error_code SaveObject(const PrimeValue& pv);
  std::error_code SaveListObject(const PrimeValue& pv);
  std::error_code SaveSetObject(const PrimeValue& pv);
  std::error_code SaveHSetObject(const PrimeValue& pv);
  std::error_code SaveZSetObject(const PrimeValue& pv);
  std::error_code SaveStreamObject(const PrimeValue& obj);
  std::error_code SaveJsonObject(const PrimeValue& pv);
  std::error_code SaveSBFObject(const PrimeValue& pv);
  std::error_code SaveTOPKObject(const PrimeValue& pv);
  std::error_code SaveCMSObject(const PrimeValue& pv);

  std::error_code SaveLongLongAsString(int64_t value);
  std::error_code SaveBinaryDouble(double val);
  std::error_code SaveStreamPEL(rax* pel, bool nacks);
  std::error_code SaveStreamConsumers(bool save_active, streamCG* cg);

  // Might preempt
  void PushToConsumerIfNeeded(FlushState flush_state);

  static constexpr size_t kFilterChunkSize = 1ULL << 26;
  static constexpr size_t kMinStrSizeToCompress = 256;
  static constexpr size_t kMaxStrSizeToCompress = 1 * 1024 * 1024;
  static constexpr double kMinCompressionReductionPrecentage = 0.95;
  struct CompressionStats {
    uint32_t compression_no_effective = 0;
    uint32_t size_skip_count = 0;
    uint32_t compression_failed = 0;
    uint32_t compressed_blobs = 0;
  };

  CompressionMode compression_mode_;
  io::IoBuf mem_buf_;
  std::unique_ptr<detail::CompressorImpl> compressor_impl_;
  std::optional<CompressionStats> compression_stats_;
  base::PODArray<uint8_t> tmp_buf_;
  std::unique_ptr<LZF_HSLOT[]> lzf_;
  size_t number_of_chunks_ = 0;
  uint64_t serialization_peak_bytes_ = 0;

  std::string tmp_str_;
  DbIndex last_entry_db_index_ = kInvalidDbId;
  ConsumeFun consume_fun_;
  size_t flush_threshold_ = 0;
};

}  // namespace dfly
