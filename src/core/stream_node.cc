// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.

#include "core/stream_node.h"

#include "base/logging.h"

extern "C" {
#include "redis/listpack.h"
#include "redis/zmalloc.h"
}

namespace dfly {

namespace {

enum Encoding : uint8_t {
  kRaw = 0,
};

constexpr size_t kStreamNodeSize = sizeof(StreamNode);
static_assert(kStreamNodeSize == 12);

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
  DCHECK(encoding_ == kRaw);
  return data_;
}

size_t StreamNode::MallocSize() const {
  return kStreamNodeSize + zmalloc_size(data_);
}

}  // namespace dfly
