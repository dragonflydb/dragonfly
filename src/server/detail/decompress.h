// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <algorithm>
#include <memory>

#include "io/io.h"
#include "io/io_buf.h"
#include "server/rdb_extensions.h"

namespace dfly {

namespace detail {

class DecompressImpl {
 public:
  static std::unique_ptr<DecompressImpl> CreateLZ4();
  static std::unique_ptr<DecompressImpl> CreateZstd();

  DecompressImpl() : uncompressed_mem_buf_{1U << 14} {
  }
  virtual ~DecompressImpl() {
  }

  virtual io::Result<io::IoBuf*> Decompress(std::string_view str, size_t max_uncomp_size) = 0;

 protected:
  io::IoBuf uncompressed_mem_buf_;

  bool GrowBuffer(size_t declared_size) {
    if (uncompressed_mem_buf_.InputLen() >= declared_size)
      return false;
    uncompressed_mem_buf_.Reserve(uncompressed_mem_buf_.InputLen() + 1);
    return !uncompressed_mem_buf_.AppendBuffer().empty();
  }

  io::IoBuf* AppendEndOpcode() {
    uint8_t op = RDB_OPCODE_COMPRESSED_BLOB_END;
    uncompressed_mem_buf_.WriteAndCommit(&op, 1);
    return &uncompressed_mem_buf_;
  }
};

}  // namespace detail
}  // namespace dfly
