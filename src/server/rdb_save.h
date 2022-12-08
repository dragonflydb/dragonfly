// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/container/flat_hash_map.h>

extern "C" {
#include "redis/lzfP.h"
#include "redis/object.h"
}

#include <optional>

#include "base/io_buf.h"
#include "base/pod_array.h"
#include "io/io.h"
#include "server/common.h"
#include "server/table.h"

typedef struct rax rax;
typedef struct streamCG streamCG;

namespace dfly {

uint8_t RdbObjectType(unsigned type, unsigned encoding);

class EngineShard;

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
  SUMMARY,       // Save only header values (summary.dfs). Expected to read no shards.
  SINGLE_SHARD,  // Save single shard values (XXXX.dfs). Expected to read one shard.
  RDB,           // Save .rdb file. Expected to read all shards.
};

enum class CompressionMode { NONE, SINGLE_ENTRY, MULTY_ENTRY_ZSTD, MULTY_ENTRY_LZ4 };

class RdbSaver {
 public:
  // single_shard - true means that we run RdbSaver on a single shard and we do not use
  // to snapshot all the datastore shards.
  // single_shard - false, means we capture all the data using a single RdbSaver instance
  // (corresponds to legacy, redis compatible mode)
  // if align_writes is true - writes data in aligned chunks of 4KB to fit direct I/O requirements.
  explicit RdbSaver(::io::Sink* sink, SaveMode save_mode, bool align_writes);

  ~RdbSaver();

  // Initiates the serialization in the shard's thread.
  // TODO: to implement break functionality to allow stopping early.
  void StartSnapshotInShard(bool stream_journal, const Cancellation* cll, EngineShard* shard);

  // Stops serialization in journal streaming mode in the shard's thread.
  void StopSnapshotInShard(EngineShard* shard);

  // Stores auxiliary (meta) values and lua scripts.
  std::error_code SaveHeader(const StringVec& lua_scripts);

  // Writes the RDB file into sink. Waits for the serialization to finish.
  // Fills freq_map with the histogram of rdb types.
  // freq_map can optionally be null.
  std::error_code SaveBody(const Cancellation* cll, RdbTypeFreqMap* freq_map);

  void Cancel();

  SaveMode Mode() const {
    return save_mode_;
  }

 private:
  class Impl;

  std::error_code SaveEpilog();

  std::error_code SaveAux(const StringVec& lua_scripts);
  std::error_code SaveAuxFieldStrInt(std::string_view key, int64_t val);

  std::unique_ptr<Impl> impl_;
  SaveMode save_mode_;
  CompressionMode compression_mode_;
};

class CompressorImpl;

class RdbSerializer {
 public:
  RdbSerializer(CompressionMode compression_mode);

  ~RdbSerializer();

  std::error_code WriteOpcode(uint8_t opcode) {
    return WriteRaw(::io::Bytes{&opcode, 1});
  }

  std::error_code SelectDb(uint32_t dbid);

  // Must be called in the thread to which `it` belongs.
  // Returns the serialized rdb_type or the error.
  // expire_ms = 0 means no expiry.
  io::Result<uint8_t> SaveEntry(const PrimeKey& pk, const PrimeValue& pv, uint64_t expire_ms);
  std::error_code WriteRaw(const ::io::Bytes& buf);
  std::error_code SaveString(std::string_view val);

  std::error_code SaveString(const uint8_t* buf, size_t len) {
    return SaveString(std::string_view{reinterpret_cast<const char*>(buf), len});
  }

  std::error_code FlushToSink(io::Sink* s);
  std::error_code SaveLen(size_t len);

  // This would work for either string or an object.
  // The arg pv is taken from it->second if accessing
  // this by finding the key. This function is used
  // for the dump command - thus it is public function
  std::error_code SaveValue(const PrimeValue& pv);

  std::error_code SendFullSyncCut(io::Sink* s);
  size_t SerializedLen() const;

 private:
  std::error_code SaveLzfBlob(const ::io::Bytes& src, size_t uncompressed_len);
  std::error_code SaveObject(const PrimeValue& pv);
  std::error_code SaveListObject(const robj* obj);
  std::error_code SaveSetObject(const PrimeValue& pv);
  std::error_code SaveHSetObject(const robj* obj);
  std::error_code SaveZSetObject(const robj* obj);
  std::error_code SaveStreamObject(const robj* obj);
  std::error_code SaveLongLongAsString(int64_t value);
  std::error_code SaveBinaryDouble(double val);
  std::error_code SaveListPackAsZiplist(uint8_t* lp);
  std::error_code SaveStreamPEL(rax* pel, bool nacks);
  std::error_code SaveStreamConsumers(streamCG* cg);
  // If membuf data is compressable use compression impl to compress the data and write it to membuf
  void CompressBlob();

  std::unique_ptr<LZF_HSLOT[]> lzf_;
  base::IoBuf mem_buf_;
  base::PODArray<uint8_t> tmp_buf_;
  std::string tmp_str_;
  CompressionMode compression_mode_;
  // TODO : This compressor impl should support different compression algorithms zstd/lz4 etc.
  std::unique_ptr<CompressorImpl> compressor_impl_;

  static constexpr size_t kMinStrSizeToCompress = 256;
  static constexpr double kMinCompressionReductionPrecentage = 0.95;
  struct CompressionStats {
    uint32_t compression_no_effective = 0;
    uint32_t small_str_count = 0;
    uint32_t compression_failed = 0;
  };
  std::optional<CompressionStats> compression_stats_;
};

}  // namespace dfly
