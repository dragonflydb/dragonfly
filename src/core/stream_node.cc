// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "core/stream_node.h"

#include <zstd.h>

#include "base/logging.h"

extern "C" {
#include "redis/listpack.h"
#include "redis/zmalloc.h"
}

namespace dfly {

namespace {

enum Encoding : uint8_t { kRaw = 0, kZstd = 1 };

constexpr size_t kStreamNodeSize = sizeof(StreamNode);
static_assert(kStreamNodeSize == 12);

/* Thread-local decompression buffer. Cleanup on exit. */
struct TlDecompressBuffer {
  uint8_t* p_ = nullptr;
  ~TlDecompressBuffer() {
    zfree(p_);
  }
};
thread_local TlDecompressBuffer tl_decompress_buffer;

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

void StreamNode::Reset() {
  if (encoding_ != kRaw) {
    zfree(data_);
    data_ = nullptr;
    encoding_ = kRaw;
    tl_decompress_buffer.p_ = nullptr;
  }
}

void StreamNode::SetListpack(uint8_t* lp) {
  DCHECK(lp != nullptr);
  DCHECK(lpBytes(lp) < (1u << 30));
  DCHECK(encoding_ == kRaw);
  uncompressed_size_ = static_cast<uint32_t>(lpBytes(lp));
  if (data_ != lp) {
    data_ = lp;
  }
}

uint8_t* StreamNode::GetListpack() const {
  if (encoding_ == kRaw) {
    return data_;
  }
  /* Read the compressed size stored as a prefix before the ZSTD frame. */
  uint32_t csz;
  memcpy(&csz, data_, sizeof(csz));
  const unsigned char* compressed_data = data_ + sizeof(csz);
  zfree(tl_decompress_buffer.p_);
  tl_decompress_buffer.p_ = static_cast<unsigned char*>(zmalloc(uncompressed_size_));
  size_t dsz = ZSTD_decompress(tl_decompress_buffer.p_, uncompressed_size_, compressed_data, csz);
  if (ZSTD_isError(dsz)) {
    LOG(DFATAL) << "ZSTD decompression error: " << ZSTD_getErrorName(dsz);
    return nullptr;
  }
  return tl_decompress_buffer.p_;
}

bool StreamNode::TryCompress() {
  DCHECK(encoding_ == kRaw);
  unsigned char* lp = data_;
  size_t lp_size = uncompressed_size_;
  size_t bound = ZSTD_compressBound(lp_size);
  unsigned char* buf = static_cast<unsigned char*>(zmalloc(sizeof(uint32_t) + bound));
  uint32_t csz = ZSTD_compress(buf + sizeof(uint32_t), bound, lp, lp_size, 1);
  // Reject compression if it failed or if the result is not at least 30% smaller.
  if (ZSTD_isError(csz) || csz >= lp_size * 7 / 10) {
    zfree(buf);
    return false;
  }
  memcpy(buf, &csz, sizeof(csz));
  data_ = static_cast<unsigned char*>(zrealloc(buf, sizeof(uint32_t) + csz));
  zfree(lp);
  encoding_ = kZstd;
  return true;
}

size_t StreamNode::MallocSize() const {
  return kStreamNodeSize + zmalloc_size(data_);
}

}  // namespace dfly
