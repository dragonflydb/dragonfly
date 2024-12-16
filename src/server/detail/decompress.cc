// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/detail/decompress.h"

#include <lz4frame.h>
#include <zstd.h>

#include "base/logging.h"
#include "server/error.h"
#include "server/rdb_extensions.h"

namespace dfly {

namespace detail {

using io::IoBuf;
using rdb::errc;
using namespace std;

inline auto Unexpected(errc ev) {
  return nonstd::make_unexpected(RdbError(ev));
}

class ZstdDecompress : public DecompressImpl {
 public:
  ZstdDecompress() {
    dctx_ = ZSTD_createDCtx();
  }
  ~ZstdDecompress() {
    ZSTD_freeDCtx(dctx_);
  }

  io::Result<io::IoBuf*> Decompress(std::string_view str);

 private:
  ZSTD_DCtx* dctx_;
};

io::Result<io::IoBuf*> ZstdDecompress::Decompress(std::string_view str) {
  // Prepare membuf memory to uncompressed string.
  auto uncomp_size = ZSTD_getFrameContentSize(str.data(), str.size());
  if (uncomp_size == ZSTD_CONTENTSIZE_UNKNOWN) {
    LOG(ERROR) << "Zstd compression missing frame content size";
    return Unexpected(errc::invalid_encoding);
  }
  if (uncomp_size == ZSTD_CONTENTSIZE_ERROR) {
    LOG(ERROR) << "Invalid ZSTD compressed string";
    return Unexpected(errc::invalid_encoding);
  }

  uncompressed_mem_buf_.Reserve(uncomp_size + 1);

  // Uncompress string to membuf
  IoBuf::Bytes dest = uncompressed_mem_buf_.AppendBuffer();
  if (dest.size() < uncomp_size) {
    return Unexpected(errc::out_of_memory);
  }
  size_t const d_size =
      ZSTD_decompressDCtx(dctx_, dest.data(), dest.size(), str.data(), str.size());
  if (d_size == 0 || d_size != uncomp_size) {
    LOG(ERROR) << "Invalid ZSTD compressed string";
    return Unexpected(errc::rdb_file_corrupted);
  }
  uncompressed_mem_buf_.CommitWrite(d_size);

  // Add opcode of compressed blob end to membuf.
  dest = uncompressed_mem_buf_.AppendBuffer();
  if (dest.size() < 1) {
    return Unexpected(errc::out_of_memory);
  }
  dest[0] = RDB_OPCODE_COMPRESSED_BLOB_END;
  uncompressed_mem_buf_.CommitWrite(1);

  return &uncompressed_mem_buf_;
}

class Lz4Decompress : public DecompressImpl {
 public:
  Lz4Decompress() {
    auto result = LZ4F_createDecompressionContext(&dctx_, LZ4F_VERSION);
    CHECK(!LZ4F_isError(result));
  }
  ~Lz4Decompress() {
    auto result = LZ4F_freeDecompressionContext(dctx_);
    CHECK(!LZ4F_isError(result));
  }

  io::Result<base::IoBuf*> Decompress(std::string_view str);

 private:
  LZ4F_dctx* dctx_;
};

io::Result<base::IoBuf*> Lz4Decompress::Decompress(std::string_view data) {
  LZ4F_frameInfo_t frame_info;
  size_t frame_size = data.size();

  // Get content size from frame data
  size_t consumed = frame_size;  // The nb of bytes consumed from data will be written into consumed
  size_t res = LZ4F_getFrameInfo(dctx_, &frame_info, data.data(), &consumed);
  if (LZ4F_isError(res)) {
    LOG(ERROR) << "LZ4F_getFrameInfo failed with error " << LZ4F_getErrorName(res);
    return Unexpected(errc::rdb_file_corrupted);
    ;
  }

  if (frame_info.contentSize == 0) {
    LOG(ERROR) << "Missing frame content size";
    return Unexpected(errc::rdb_file_corrupted);
  }

  // reserve place for uncompressed data and end opcode
  size_t reserve = frame_info.contentSize + 1;
  uncompressed_mem_buf_.Reserve(reserve);
  IoBuf::Bytes dest = uncompressed_mem_buf_.AppendBuffer();
  if (dest.size() < reserve) {
    return Unexpected(errc::out_of_memory);
  }

  // Uncompress data to membuf
  string_view src = data.substr(consumed);
  size_t src_size = src.size();

  size_t ret = 1;
  while (ret != 0) {
    IoBuf::Bytes dest = uncompressed_mem_buf_.AppendBuffer();
    size_t dest_capacity = dest.size();

    // It will read up to src_size bytes from src,
    // and decompress data into dest, of capacity dest_capacity
    // The nb of bytes consumed from src will be written into src_size
    // The nb of bytes decompressed into dest will be written into dest_capacity
    ret = LZ4F_decompress(dctx_, dest.data(), &dest_capacity, src.data(), &src_size, nullptr);
    if (LZ4F_isError(ret)) {
      LOG(ERROR) << "LZ4F_decompress failed with error " << LZ4F_getErrorName(ret);
      return Unexpected(errc::rdb_file_corrupted);
    }
    consumed += src_size;

    uncompressed_mem_buf_.CommitWrite(dest_capacity);
    src = src.substr(src_size);
    src_size = src.size();
  }
  if (consumed != frame_size) {
    return Unexpected(errc::rdb_file_corrupted);
  }
  if (uncompressed_mem_buf_.InputLen() != frame_info.contentSize) {
    return Unexpected(errc::rdb_file_corrupted);
  }

  // Add opcode of compressed blob end to membuf.
  dest = uncompressed_mem_buf_.AppendBuffer();
  if (dest.size() < 1) {
    return Unexpected(errc::out_of_memory);
  }
  dest[0] = RDB_OPCODE_COMPRESSED_BLOB_END;
  uncompressed_mem_buf_.CommitWrite(1);

  return &uncompressed_mem_buf_;
}

unique_ptr<DecompressImpl> DecompressImpl::CreateLZ4() {
  return make_unique<Lz4Decompress>();
}

unique_ptr<DecompressImpl> DecompressImpl::CreateZstd() {
  return make_unique<ZstdDecompress>();
}

}  // namespace detail
}  // namespace dfly
