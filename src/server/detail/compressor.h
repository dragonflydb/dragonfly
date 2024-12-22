// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <memory>

#include "base/pod_array.h"
#include "io/io.h"

namespace dfly::detail {

class CompressorImpl {
 public:
  static std::unique_ptr<CompressorImpl> CreateZstd();
  static std::unique_ptr<CompressorImpl> CreateLZ4();

  CompressorImpl();
  virtual ~CompressorImpl();
  virtual io::Result<io::Bytes> Compress(io::Bytes data) = 0;

 protected:
  int compression_level_ = 1;
  size_t compressed_size_total_ = 0;
  size_t uncompressed_size_total_ = 0;
  base::PODArray<uint8_t> compr_buf_;
};

}  // namespace dfly::detail
