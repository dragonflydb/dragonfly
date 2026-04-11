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

enum Encoding : uint8_t { kRaw = 0, kZstd = 1 };

constexpr size_t kStreamNodeSize = sizeof(StreamNode);
static_assert(kStreamNodeSize == 12);

// Per-thread ZSTD compression state.
struct ZstdCompressionCtx {
  ZSTD_CDict* cdict = nullptr;
  ZSTD_DDict* ddict = nullptr;
  ZSTD_CCtx* cctx = nullptr;
  ZSTD_DCtx* dctx = nullptr;

  // Accumulated samples and size used for dictionary training.
  std::vector<uint8_t> training_data_bytes;
  std::vector<uint32_t> training_sample_sizes;
  size_t training_data_size = 0;

  // Decompression output buffer and its allocated capacity.
  uint8_t* decompress_buf = nullptr;
  size_t decompress_buf_capacity = 0;

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
    zfree(decompress_buf);
  }
};

thread_local std::unique_ptr<ZstdCompressionCtx> tl_zstd_ctx;

constexpr size_t kMinCompressBytesThreshold = 512;

bool TrainZstdDict(ZstdCompressionCtx& ctx) {
  if (ctx.IsDictReady()) {
    return true;
  }

  std::vector<std::pair<const uint8_t*, size_t>> pieces;
  pieces.reserve(ctx.training_data_bytes.size());
  const uint8_t* cursor = ctx.training_data_bytes.data();
  for (uint32_t sz : ctx.training_sample_sizes) {
    pieces.emplace_back(cursor, sz);
    cursor += sz;
  }

  // Ratio > 0.6 means the data is too random to compress well; skip training.
  double ratio = EstimateCompressibility(absl::MakeSpan(pieces), 2);
  if (ratio > 0.6) {
    VLOG(2) << "StreamNode data not compressible (ratio=" << ratio << ")";
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

StreamNode* StreamNode::New(uint8_t* lp) {
  DCHECK(lp != nullptr);
  StreamNode* node = static_cast<StreamNode*>(zcalloc(sizeof(StreamNode)));
  node->encoding_ = kRaw;
  node->SetListpack(lp);
  return node;
}

void StreamNode::Free(void* node) {
  DCHECK(node != nullptr);
  StreamNode* stream_node = static_cast<StreamNode*>(node);
  zfree(stream_node->data_);
  zfree(stream_node);
}

void StreamNode::Reset(uint8_t* lp) {
  DCHECK(lp != nullptr);
  if (encoding_ != kRaw) {
    zfree(data_);
    encoding_ = kRaw;
  }
  SetListpack(lp);
}

void StreamNode::SetListpack(uint8_t* lp) {
  DCHECK(lp != nullptr);
  DCHECK(encoding_ == kRaw);
  DCHECK(lpBytes(lp) < (1u << 30));
  uncompressed_size_ = static_cast<uint32_t>(lpBytes(lp));
  // When compressed data is decompressed into a temporary buffer and the underlying
  // pointer remains unchanged after modification (StreamIteratorRemoveEntry). We
  // must allocate new memory and perform a deep copy to preserve ownership.
  if (tl_zstd_ctx && tl_zstd_ctx->IsDictReady() && lp == tl_zstd_ctx->decompress_buf) {
    uint8_t* owned = static_cast<uint8_t*>(zmalloc(uncompressed_size_));
    memcpy(owned, lp, uncompressed_size_);
    data_ = owned;
  } else if (data_ != lp) {
    data_ = lp;
  }
}

uint8_t* StreamNode::GetListpack() const {
  if (encoding_ == kRaw) {
    return data_;
  }

  DCHECK(absl::GetFlag(FLAGS_stream_node_zstd_dict_threshold) > 0);
  DCHECK(tl_zstd_ctx && tl_zstd_ctx->IsDictReady());

  uint32_t csz;
  memcpy(&csz, data_, sizeof(csz));
  const unsigned char* compressed_data = data_ + sizeof(csz);

  if (tl_zstd_ctx->decompress_buf_capacity < uncompressed_size_) {
    zfree(tl_zstd_ctx->decompress_buf);
    tl_zstd_ctx->decompress_buf = static_cast<unsigned char*>(zmalloc(uncompressed_size_));
    tl_zstd_ctx->decompress_buf_capacity = uncompressed_size_;
  }

  size_t dsz =
      ZSTD_decompress_usingDDict(tl_zstd_ctx->dctx, tl_zstd_ctx->decompress_buf, uncompressed_size_,
                                 compressed_data, csz, tl_zstd_ctx->ddict);
  if (ZSTD_isError(dsz)) {
    LOG(DFATAL) << "ZSTD decompression error: " << ZSTD_getErrorName(dsz);
    return nullptr;
  }
  return tl_zstd_ctx->decompress_buf;
}

bool StreamNode::TryCompress() {
  DCHECK(encoding_ == kRaw);
  const uint32_t dict_threshold = absl::GetFlag(FLAGS_stream_node_zstd_dict_threshold);
  DCHECK(dict_threshold > 0);

  // Lazily initialize compression context.
  if (!tl_zstd_ctx) {
    tl_zstd_ctx = std::make_unique<ZstdCompressionCtx>(dict_threshold);
  }

  unsigned char* lp = data_;
  size_t lp_size = uncompressed_size_;

  if (lp_size < kMinCompressBytesThreshold) {
    return false;
  }

  if (!tl_zstd_ctx->IsDictReady()) {
    tl_zstd_ctx->training_data_bytes.insert(tl_zstd_ctx->training_data_bytes.end(), lp,
                                            lp + lp_size);
    tl_zstd_ctx->training_sample_sizes.push_back(static_cast<uint32_t>(lp_size));
    tl_zstd_ctx->training_data_size += uncompressed_size_;
    if (tl_zstd_ctx->training_data_size < dict_threshold) {
      return false;
    }
    if (!TrainZstdDict(*tl_zstd_ctx)) {
      // Training failed for the current samples. Reset and start with fresh samples.
      tl_zstd_ctx->training_data_bytes.clear();
      tl_zstd_ctx->training_sample_sizes.clear();
      tl_zstd_ctx->training_data_size = 0;
      return false;
    }
    // Clear training data and free memory
    std::vector<uint8_t>().swap(tl_zstd_ctx->training_data_bytes);
    std::vector<uint32_t>().swap(tl_zstd_ctx->training_sample_sizes);
    tl_zstd_ctx->training_data_size = 0;
  }

  size_t bound = ZSTD_compressBound(lp_size);
  unsigned char* buf = static_cast<unsigned char*>(zmalloc(sizeof(uint32_t) + bound));

  size_t csz = ZSTD_compress_usingCDict(tl_zstd_ctx->cctx, buf + sizeof(uint32_t), bound, lp,
                                        lp_size, tl_zstd_ctx->cdict);

  // Reject compression if it failed or saved less than 30%.
  if (ZSTD_isError(csz) || csz >= lp_size * 7 / 10) {
    zfree(buf);
    return false;
  }

  DCHECK_LE(csz, std::numeric_limits<uint32_t>::max());
  memcpy(buf, &csz, sizeof(uint32_t));
  data_ = static_cast<unsigned char*>(zrealloc(buf, sizeof(uint32_t) + csz));
  zfree(lp);
  encoding_ = kZstd;
  return true;
}

size_t StreamNode::MallocSize() const {
  return kStreamNodeSize + zmalloc_size(data_);
}

}  // namespace dfly
