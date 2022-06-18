// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/container/flat_hash_map.h>

extern "C" {
#include "redis/lzfP.h"
#include "redis/object.h"
}

#include "base/io_buf.h"
#include "base/pod_array.h"
#include "io/io.h"
#include "server/common.h"
#include "server/table.h"


typedef struct rax rax;
typedef struct streamCG streamCG;

namespace dfly {

class EngineShard;

class AlignedBuffer {
 public:
  AlignedBuffer(size_t cap, ::io::Sink* upstream);
  ~AlignedBuffer();

  // TODO: maybe to derive AlignedBuffer from Sink?
  std::error_code Write(std::string_view buf) {
    return Write(io::Buffer(buf));
  }

  std::error_code Write(io::Bytes buf);

  std::error_code Flush();

  ::io::Sink* upstream() { return upstream_;}

 private:
  size_t capacity_;
  ::io::Sink* upstream_;
  char* aligned_buf_ = nullptr;

  off_t buf_offs_ = 0;
};

class RdbSaver {
 public:
  explicit RdbSaver(::io::Sink* sink);
  ~RdbSaver();

  std::error_code SaveHeader(const StringVec& lua_scripts);

  // Writes the RDB file into sink. Waits for the serialization to finish.
  // Fills freq_map with the histogram of rdb types.
  // freq_map can optionally be null.
  std::error_code SaveBody(RdbTypeFreqMap* freq_map);

  // Initiates the serialization in the shard's thread.
  void StartSnapshotInShard(EngineShard* shard);

 private:
  struct Impl;

  std::error_code SaveEpilog();

  std::error_code SaveAux(const StringVec& lua_scripts);
  std::error_code SaveAuxFieldStrStr(std::string_view key, std::string_view val);
  std::error_code SaveAuxFieldStrInt(std::string_view key, int64_t val);

  AlignedBuffer aligned_buf_;
  std::unique_ptr<Impl> impl_;
};

// TODO: it does not make sense that RdbSerializer will buffer into unaligned
// mem_buf_ and then flush it into the next level. We should probably use AlignedBuffer
// directly.
class RdbSerializer {
 public:
  RdbSerializer(::io::Sink* s);
  RdbSerializer(AlignedBuffer* aligned_buf);

  ~RdbSerializer();

  // The ownership stays with the caller.
  void set_sink(::io::Sink* s) {
    sink_ = s;
  }

  std::error_code WriteOpcode(uint8_t opcode) {
    return WriteRaw(::io::Bytes{&opcode, 1});
  }

  std::error_code SelectDb(uint32_t dbid);

  // Must be called in the thread to which `it` belongs.
  // Returns the serialized rdb_type or the error.
  io::Result<uint8_t> SaveEntry(const PrimeKey& pk, const PrimeValue& pv, uint64_t expire_ms);
  std::error_code WriteRaw(const ::io::Bytes& buf);
  std::error_code SaveString(std::string_view val);

  std::error_code SaveString(const uint8_t* buf, size_t len) {
    return SaveString(std::string_view{reinterpret_cast<const char*>(buf), len});
  }

  std::error_code SaveLen(size_t len);

  std::error_code FlushMem();

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

  ::io::Sink* sink_ = nullptr;
  AlignedBuffer* aligned_buf_ = nullptr;

  std::unique_ptr<LZF_HSLOT[]> lzf_;
  base::IoBuf mem_buf_;
  base::PODArray<uint8_t> tmp_buf_;
  std::string tmp_str_;
};

}  // namespace dfly
