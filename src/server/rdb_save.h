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
#include "io/io.h"
#include "server/table.h"
#include "server/common.h"

namespace dfly {
class EngineShard;

// keys are RDB_TYPE_xxx constants.
using RdbTypeFreqMap = absl::flat_hash_map<unsigned, size_t>;
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

  ::io::Sink* sink_;
  std::unique_ptr<Impl> impl_;
};

class RdbSerializer {
 public:
  RdbSerializer(::io::Sink* s = nullptr);

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
  std::error_code SaveEntry(const PrimeKey& pk, const PrimeValue& pv, uint64_t expire_ms);
  std::error_code WriteRaw(const ::io::Bytes& buf);
  std::error_code SaveString(std::string_view val);

  std::error_code SaveString(const uint8_t* buf, size_t len) {
    return SaveString(std::string_view{reinterpret_cast<const char*>(buf), len});
  }

  std::error_code SaveLen(size_t len);

  std::error_code FlushMem();

  const RdbTypeFreqMap& type_freq_map() const {
    return type_freq_map_;
  }

 private:
  std::error_code SaveLzfBlob(const ::io::Bytes& src, size_t uncompressed_len);
  std::error_code SaveObject(const PrimeValue& pv);
  std::error_code SaveListObject(const robj* obj);
  std::error_code SaveSetObject(const PrimeValue& pv);
  std::error_code SaveHSetObject(const robj* obj);
  std::error_code SaveZSetObject(const robj* obj);
  std::error_code SaveLongLongAsString(int64_t value);
  std::error_code SaveBinaryDouble(double val);
  std::error_code SaveListPackAsZiplist(uint8_t* lp);

  ::io::Sink* sink_ = nullptr;
  std::unique_ptr<LZF_HSLOT[]> lzf_;
  base::IoBuf mem_buf_;
  base::PODArray<uint8_t> tmp_buf_;
  std::string tmp_str_;
  RdbTypeFreqMap type_freq_map_;
};

}  // namespace dfly
