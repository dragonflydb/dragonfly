// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/detail/compressor.h"

#include <absl/flags/flag.h>
#include <lz4frame.h>
#include <zstd.h>

#include "base/logging.h"

ABSL_FLAG(int, compression_level, 2, "The compression level to use on zstd/lz4 compression");

namespace dfly::detail {

using namespace std;

class ZstdCompressor : public CompressorImpl {
 public:
  ZstdCompressor() {
    cctx_ = ZSTD_createCCtx();
  }
  ~ZstdCompressor() {
    ZSTD_freeCCtx(cctx_);
  }

  io::Result<io::Bytes> Compress(io::Bytes data);

 private:
  ZSTD_CCtx* cctx_;
  base::PODArray<uint8_t> compr_buf_;
};

io::Result<io::Bytes> ZstdCompressor::Compress(io::Bytes data) {
  size_t buf_size = ZSTD_compressBound(data.size());
  if (compr_buf_.capacity() < buf_size) {
    compr_buf_.reserve(buf_size);
  }
  size_t compressed_size = ZSTD_compressCCtx(cctx_, compr_buf_.data(), compr_buf_.capacity(),
                                             data.data(), data.size(), compression_level_);

  if (ZSTD_isError(compressed_size)) {
    LOG(ERROR) << "ZSTD_compressCCtx failed with error " << ZSTD_getErrorName(compressed_size);
    return nonstd::make_unexpected(make_error_code(errc::operation_not_supported));
  }
  compressed_size_total_ += compressed_size;
  uncompressed_size_total_ += data.size();
  return io::Bytes(compr_buf_.data(), compressed_size);
}

class Lz4Compressor : public CompressorImpl {
 public:
  Lz4Compressor() {
    LZ4F_errorCode_t code = LZ4F_createCompressionContext(&cctx_, LZ4F_VERSION);
    CHECK(!LZ4F_isError(code));
  }

  ~Lz4Compressor() {
    LZ4F_errorCode_t code = LZ4F_freeCompressionContext(cctx_);
    CHECK(!LZ4F_isError(code));
  }

  // compress a string of data
  io::Result<io::Bytes> Compress(io::Bytes data);

 private:
  LZ4F_cctx* cctx_;
};

io::Result<io::Bytes> Lz4Compressor::Compress(io::Bytes data) {
  LZ4F_preferences_t lz4_pref = LZ4F_INIT_PREFERENCES;
  lz4_pref.compressionLevel = compression_level_;
  lz4_pref.frameInfo.contentSize = data.size();

  size_t buf_size = LZ4F_compressFrameBound(data.size(), &lz4_pref);
  if (compr_buf_.capacity() < buf_size) {
    compr_buf_.reserve(buf_size);
  }

  size_t frame_size =
      LZ4F_compressFrame_usingCDict(cctx_, compr_buf_.data(), compr_buf_.capacity(), data.data(),
                                    data.size(), nullptr /* dict */, &lz4_pref);
  if (LZ4F_isError(frame_size)) {
    LOG(ERROR) << "LZ4F_compressFrame failed with error " << LZ4F_getErrorName(frame_size);
    return nonstd::make_unexpected(make_error_code(errc::operation_not_supported));
  }

  compressed_size_total_ += frame_size;
  uncompressed_size_total_ += data.size();
  return io::Bytes(compr_buf_.data(), frame_size);
}

CompressorImpl::CompressorImpl() {
  compression_level_ = absl::GetFlag(FLAGS_compression_level);
}

CompressorImpl::~CompressorImpl() {
  VLOG(1) << "compressed size: " << compressed_size_total_;
  VLOG(1) << "uncompressed size: " << uncompressed_size_total_;
}

unique_ptr<CompressorImpl> CompressorImpl::CreateZstd() {
  return make_unique<ZstdCompressor>();
}

unique_ptr<CompressorImpl> CompressorImpl::CreateLZ4() {
  return make_unique<Lz4Compressor>();
}

}  // namespace dfly::detail
