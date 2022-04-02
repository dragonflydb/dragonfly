// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

extern "C" {
#include "redis/lzfP.h"
#include "redis/object.h"
}

#include "base/io_buf.h"
#include "io/io.h"

namespace dfly {
class EngineShardSet;
class EngineShard;

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

  std::error_code SaveKeyVal(std::string_view key, const robj* val, uint64_t expire_ms);
  std::error_code SaveKeyVal(std::string_view key, std::string_view value, uint64_t expire_ms);
  std::error_code WriteRaw(const ::io::Bytes& buf);
  std::error_code SaveString(std::string_view val);

  std::error_code SaveString(const uint8_t* buf, size_t len) {
    return SaveString(std::string_view{reinterpret_cast<const char*>(buf), len});
  }

  std::error_code SaveLen(size_t len);

  std::error_code FlushMem();

 private:
  std::error_code SaveLzfBlob(const ::io::Bytes& src, size_t uncompressed_len);
  std::error_code SaveObject(const robj* o);
  std::error_code SaveStringObject(const robj* obj);
  std::error_code SaveListObject(const robj* obj);
  std::error_code SaveSetObject(const robj* obj);
  std::error_code SaveLongLongAsString(int64_t value);

  ::io::Sink* sink_ = nullptr;
  std::unique_ptr<LZF_HSLOT[]> lzf_;
  base::IoBuf mem_buf_;
  base::PODArray<uint8_t> tmp_buf_;
};

class RdbSaver {
 public:
  RdbSaver(EngineShardSet* ess, ::io::Sink* sink);
  ~RdbSaver();

  std::error_code SaveHeader();
  std::error_code SaveBody();

  void StartSnapshotInShard(EngineShard* shard);

 private:
  struct Impl;

  std::error_code SaveEpilog();

  std::error_code SaveAux();
  std::error_code SaveAuxFieldStrStr(std::string_view key, std::string_view val);
  std::error_code SaveAuxFieldStrInt(std::string_view key, int64_t val);

  EngineShardSet* ess_;
  ::io::Sink* sink_;
  std::unique_ptr<Impl> impl_;
};

}  // namespace dfly
