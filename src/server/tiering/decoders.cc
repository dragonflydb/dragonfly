// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tiering/decoders.h"

#include <cstring>

#include "base/logging.h"
#include "core/compact_object.h"
#include "core/detail/listpack_wrap.h"
#include "core/qlist.h"

extern "C" {
#include "redis/redis_aux.h"  // for OBJ_HASH
#include "redis/zmalloc.h"
}

namespace dfly::tiering {

std::unique_ptr<Decoder> BareDecoder::Clone() const {
  return std::make_unique<BareDecoder>();
}

void BareDecoder::Initialize(std::string_view slice) {
  this->slice = slice;
}

void BareDecoder::Upload(void* obj) {
  ABSL_UNREACHABLE();
}

Decoder::UploadMetrics BareDecoder::GetMetrics() const {
  ABSL_UNREACHABLE();
  return UploadMetrics{};
}

StringDecoder::StringDecoder(const CompactObj& obj) : StringDecoder{obj.GetStrEncoding()} {
}

StringDecoder::StringDecoder(CompactObj::StrEncoding encoding) : encoding_{encoding} {
}

std::unique_ptr<Decoder> StringDecoder::Clone() const {
  return std::unique_ptr<StringDecoder>{new StringDecoder(encoding_)};
}

void StringDecoder::Initialize(std::string_view slice) {
  slice_ = slice;
  value_ = encoding_.Decode(slice);
}

void StringDecoder::Upload(void* obj) {
  CompactObj* compact_obj = reinterpret_cast<CompactObj*>(obj);
  if (modified_)
    compact_obj->Materialize(value_.view(), false);
  else
    compact_obj->Materialize(slice_, true);
}

Decoder::UploadMetrics StringDecoder::GetMetrics() const {
  return UploadMetrics{
      .modified = modified_,
      .estimated_mem_usage = value_.view().size(),
  };
}

std::string* StringDecoder::Write() {
  modified_ = true;
  return value_.GetMutable();
}

ListpackMapDecoder::~ListpackMapDecoder() {
  if (owned_lw_) {
    zfree(owned_lw_->GetPointer());
  }
}

std::unique_ptr<Decoder> ListpackMapDecoder::Clone() const {
  return std::make_unique<ListpackMapDecoder>();
}

void ListpackMapDecoder::Initialize(std::string_view slice) {
  slice_ = slice;
}

Decoder::UploadMetrics ListpackMapDecoder::GetMetrics() const {
  size_t bytes = owned_lw_ ? owned_lw_->UsedBytes() : slice_.size();
  return UploadMetrics{
      .modified = owned_lw_ != nullptr,
      .estimated_mem_usage = bytes,
  };
}

void ListpackMapDecoder::Upload(void* robj) {
  auto* obj = static_cast<CompactObj*>(robj);

  if (!owned_lw_)
    GetMutable();  // Need an owned copy to hand off

  obj->InitRobj(OBJ_HASH, kEncodingListPack, owned_lw_->GetPointer());
  owned_lw_.reset();
}

detail::ListpackWrap ListpackMapDecoder::Get() const {
  if (owned_lw_)
    return *owned_lw_;
  return detail::ListpackWrap::Readonly(
      const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(slice_.data())));
}

detail::ListpackWrap* ListpackMapDecoder::GetMutable() {
  if (!owned_lw_) {
    uint8_t* lp = (uint8_t*)zmalloc(slice_.size());
    memcpy(lp, slice_.data(), slice_.size());
    owned_lw_ = std::make_unique<detail::ListpackWrap>(lp);
  }
  return owned_lw_.get();
}

ListNodeDecoder::ListNodeDecoder(QList* ql) : ql_(ql) {
}

std::unique_ptr<Decoder> ListNodeDecoder::Clone() const {
  return std::make_unique<ListNodeDecoder>(ql_);
}

void ListNodeDecoder::Initialize(std::string_view slice) {
  slice_ = slice;
}

void ListNodeDecoder::Upload(void* obj) {
  QList::Node* node_obj = reinterpret_cast<QList::Node*>(obj);
  node_obj->Upload(ql_, slice_);
}

Decoder::UploadMetrics ListNodeDecoder::GetMetrics() const {
  return UploadMetrics{.modified = false, .estimated_mem_usage = slice_.size()};
}

}  // namespace dfly::tiering
