// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <memory>

#include "io/io.h"
#include "io/io_buf.h"

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

  virtual io::Result<io::IoBuf*> Decompress(std::string_view str) = 0;

 protected:
  io::IoBuf uncompressed_mem_buf_;
};

}  // namespace detail
}  // namespace dfly
