// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "core/stream_node.h"

#include <zstd.h>

#include <limits>
#include <memory>
#include <vector>

#include "absl/flags/flag.h"
#include "base/logging.h"
#include "core/dict_builder.h"

ABSL_DECLARE_FLAG(uint32_t, stream_node_zstd_dict_threshold);

extern "C" {
#include "redis/listpack.h"
#include "redis/zmalloc.h"
}

namespace dfly {

namespace {

constexpr size_t kMinCompressBytesThreshold = 512;

// Per-thread ZSTD compression state.
struct ZstdCompressionCtx {
  ZSTD_CDict* cdict = nullptr;
  ZSTD_DDict* ddict = nullptr;
  ZSTD_CCtx* cctx = nullptr;
  ZSTD_DCtx* dctx = nullptr;

  // Accumulated samples and sizes used for dictionary training.
  std::vector<uint8_t> training_data_bytes;
  std::vector<uint32_t> training_sample_sizes;
  size_t training_data_size = 0;

  // Temporary buffer used for compression/decompression.
  uint8_t* scratch_buffer = nullptr;
  size_t scratch_buffer_capacity = 0;

  // Compressed node whose decompressed form currently lives in scratch_buffer.
  const uint8_t* last_compressed_node = nullptr;

  explicit ZstdCompressionCtx(uint32_t dict_threshold) {
    training_data_bytes.reserve(dict_threshold);
    training_sample_sizes.reserve(32);
  }

  bool IsDictReady() const {
    return cdict != nullptr;
  }

  void ResetDict() {
    if (cdict) {
      ZSTD_freeCDict(cdict);
      cdict = nullptr;
    }
    if (ddict) {
      ZSTD_freeDDict(ddict);
      ddict = nullptr;
    }
    if (cctx) {
      ZSTD_freeCCtx(cctx);
      cctx = nullptr;
    }
    if (dctx) {
      ZSTD_freeDCtx(dctx);
      dctx = nullptr;
    }
  }

  ~ZstdCompressionCtx() {
    ResetDict();
    zfree(scratch_buffer);
  }
};

thread_local std::unique_ptr<ZstdCompressionCtx> tl_zstd_ctx;

bool TrainZstdDict(ZstdCompressionCtx& ctx) {
  if (ctx.IsDictReady()) {
    return true;
  }

  std::vector<std::pair<const uint8_t*, size_t>> pieces;
  pieces.reserve(ctx.training_sample_sizes.size());
  const uint8_t* cursor = ctx.training_data_bytes.data();
  for (uint32_t sz : ctx.training_sample_sizes) {
    pieces.emplace_back(cursor, sz);
    cursor += sz;
  }

  // Ratio > 0.6 means the data is too random to compress well; skip training.
  double ratio = EstimateCompressibility(absl::MakeSpan(pieces), 2);
  if (ratio > 0.6) {
    VLOG(2) << "StreamNodeObj data not compressible (ratio=" << ratio << ")";
    return false;
  }

  std::string dict_raw = TrainDictionary(absl::MakeSpan(pieces), 4096, 64);
  if (dict_raw.empty()) {
    return false;
  }

  ctx.cdict = ZSTD_createCDict(dict_raw.data(), dict_raw.size(), 1);
  ctx.ddict = ZSTD_createDDict(dict_raw.data(), dict_raw.size());
  ctx.cctx = ZSTD_createCCtx();
  ctx.dctx = ZSTD_createDCtx();

  if (!ctx.cdict || !ctx.ddict || !ctx.cctx || !ctx.dctx) {
    ctx.ResetDict();
    return false;
  }

  return true;
}

}  // namespace

uint8_t* StreamNodeObj::GetListpack() const {
  if (IsRaw()) {
    return Ptr();
  }

  DCHECK(IsCompressed());
  DCHECK(absl::GetFlag(FLAGS_stream_node_zstd_dict_threshold) > 0);
  DCHECK(tl_zstd_ctx && tl_zstd_ctx->IsDictReady());

  const uint8_t* buf = Ptr();

  if (tl_zstd_ctx->last_compressed_node == buf) {
    return tl_zstd_ctx->scratch_buffer;
  }

  uint32_t uncompressed_size, csz;
  memcpy(&uncompressed_size, buf, sizeof(uncompressed_size));
  memcpy(&csz, buf + sizeof(uncompressed_size), sizeof(csz));
  const uint8_t* compressed_data = buf + sizeof(uncompressed_size) + sizeof(csz);

  if (tl_zstd_ctx->scratch_buffer_capacity < uncompressed_size) {
    tl_zstd_ctx->scratch_buffer =
        static_cast<uint8_t*>(zrealloc(tl_zstd_ctx->scratch_buffer, uncompressed_size));
    tl_zstd_ctx->scratch_buffer_capacity = zmalloc_size(tl_zstd_ctx->scratch_buffer);
  }

  size_t dsz =
      ZSTD_decompress_usingDDict(tl_zstd_ctx->dctx, tl_zstd_ctx->scratch_buffer, uncompressed_size,
                                 compressed_data, csz, tl_zstd_ctx->ddict);
  if (ZSTD_isError(dsz)) {
    LOG(DFATAL) << "ZSTD decompression error: " << ZSTD_getErrorName(dsz);
    return nullptr;
  }

  tl_zstd_ctx->last_compressed_node = buf;

  return tl_zstd_ctx->scratch_buffer;
}

uint32_t StreamNodeObj::UncompressedSize() const {
  if (IsRaw()) {
    return static_cast<uint32_t>(lpBytes(Ptr()));
  }
  DCHECK(IsCompressed());
  uint32_t sz;
  memcpy(&sz, Ptr(), sizeof(sz));
  return sz;
}

StreamNodeObj StreamNodeObj::TryCompress() const {
  DCHECK(IsRaw());
  static const uint32_t dict_threshold = absl::GetFlag(FLAGS_stream_node_zstd_dict_threshold);
  DCHECK(dict_threshold > 0);

  if (!tl_zstd_ctx) {
    tl_zstd_ctx = std::make_unique<ZstdCompressionCtx>(dict_threshold);
  }

  uint8_t* lp = Ptr();
  const size_t lp_size = lpBytes(lp);

  if (lp_size < kMinCompressBytesThreshold) {
    return *this;
  }

  if (!tl_zstd_ctx->IsDictReady()) {
    tl_zstd_ctx->training_data_bytes.insert(tl_zstd_ctx->training_data_bytes.end(), lp,
                                            lp + lp_size);
    tl_zstd_ctx->training_sample_sizes.push_back(static_cast<uint32_t>(lp_size));
    tl_zstd_ctx->training_data_size += lp_size;
    if (tl_zstd_ctx->training_data_size < dict_threshold) {
      return *this;
    }
    if (!TrainZstdDict(*tl_zstd_ctx)) {
      tl_zstd_ctx->training_data_bytes.clear();
      tl_zstd_ctx->training_sample_sizes.clear();
      tl_zstd_ctx->training_data_size = 0;
      return *this;
    }
    std::vector<uint8_t>().swap(tl_zstd_ctx->training_data_bytes);
    std::vector<uint32_t>().swap(tl_zstd_ctx->training_sample_sizes);
    tl_zstd_ctx->training_data_size = 0;
  }

  const size_t bound = ZSTD_compressBound(lp_size);

  if (tl_zstd_ctx->scratch_buffer_capacity < bound) {
    tl_zstd_ctx->scratch_buffer =
        static_cast<uint8_t*>(zrealloc(tl_zstd_ctx->scratch_buffer, bound));
    tl_zstd_ctx->scratch_buffer_capacity = zmalloc_size(tl_zstd_ctx->scratch_buffer);
  }

  tl_zstd_ctx->last_compressed_node = nullptr;
  const size_t csz = ZSTD_compress_usingCDict(tl_zstd_ctx->cctx, tl_zstd_ctx->scratch_buffer, bound,
                                              lp, lp_size, tl_zstd_ctx->cdict);

  // Reject if compression failed or saved less than 30%.
  if (ZSTD_isError(csz) || csz >= lp_size * 7 / 10) {
    return *this;
  }

  // Allocate the exact final size and copy header + compressed payload in one shot.
  DCHECK_LE(csz, std::numeric_limits<uint32_t>::max());
  const uint32_t uncompressed_sz = static_cast<uint32_t>(lp_size);
  const uint32_t compressed_sz = static_cast<uint32_t>(csz);

  // Compressed buffer layout: [4B uncompressed_sz][4B csz][compressed data]
  uint8_t* buf = static_cast<uint8_t*>(zmalloc(sizeof(uint32_t) * 2 + csz));
  memcpy(buf, &uncompressed_sz, sizeof(uncompressed_sz));
  memcpy(buf + sizeof(uncompressed_sz), &compressed_sz, sizeof(compressed_sz));
  memcpy(buf + sizeof(uint32_t) * 2, tl_zstd_ctx->scratch_buffer, csz);
  zfree(lp);

  // Create new node object and tag it as compressed
  StreamNodeObj compressed_node_obj;
  compressed_node_obj.ptr_ = reinterpret_cast<uintptr_t>(buf) | kCompressedBit;
  return compressed_node_obj;
}

uint8_t* StreamNodeObj::MaterializeListpack(uint8_t* lp) {
  DCHECK(lp != nullptr);
  DCHECK(tl_zstd_ctx && tl_zstd_ctx->IsDictReady());
  if (lp == tl_zstd_ctx->scratch_buffer) {
    const uint32_t sz = static_cast<uint32_t>(lpBytes(lp));
    uint8_t* copy = static_cast<uint8_t*>(zmalloc(sz));
    memcpy(copy, lp, sz);
    tl_zstd_ctx->scratch_buffer_capacity = zmalloc_size(lp);
    return copy;
  }
  tl_zstd_ctx->scratch_buffer = nullptr;
  tl_zstd_ctx->scratch_buffer_capacity = 0;
  tl_zstd_ctx->last_compressed_node = nullptr;
  return lp;
}

void StreamNodeObj::Free() const {
  zfree(Ptr());
}

void StreamNodeObj::InvalidateDecompressionState() {
  DCHECK(tl_zstd_ctx && tl_zstd_ctx->IsDictReady());
  if (tl_zstd_ctx && tl_zstd_ctx->IsDictReady()) {
    tl_zstd_ctx->scratch_buffer = nullptr;
    tl_zstd_ctx->scratch_buffer_capacity = 0;
    tl_zstd_ctx->last_compressed_node = nullptr;
  }
}

size_t StreamNodeObj::MallocSize() const {
  return zmalloc_size(Ptr());
}

}  // namespace dfly
