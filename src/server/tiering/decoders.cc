// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tiering/decoders.h"

#include "base/logging.h"
#include "core/compact_object.h"
#include "core/detail/listpack_wrap.h"
#include "core/qlist.h"
#include "server/tiering/serialized_map.h"

extern "C" {
#include "redis/redis_aux.h"  // for OBJ_HASH
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

std::unique_ptr<Decoder> SerializedMapDecoder::Clone() const {
  return std::make_unique<SerializedMapDecoder>();
}

void SerializedMapDecoder::Initialize(std::string_view slice) {
  map_ = std::make_unique<SerializedMap>(slice);
}

Decoder::UploadMetrics SerializedMapDecoder::GetMetrics() const {
  return UploadMetrics{.modified = false,
                       .estimated_mem_usage = map_->DataBytes() + map_->size() * 2 * 8};
}

void SerializedMapDecoder::Upload(void* obj) {
  CompactObj* compact_obj = reinterpret_cast<CompactObj*>(obj);
  auto lw = detail::ListpackWrap::WithCapacity(GetMetrics().estimated_mem_usage);
  for (const auto& [key, value] : *map_)
    lw.Insert(key, value, true);
  compact_obj->InitRobj(OBJ_HASH, kEncodingListPack, lw.GetPointer());
}

SerializedMap* SerializedMapDecoder::Get() const {
  return map_.get();
}

ListNodeDecoder::ListNodeDecoder(QList* ql) : ql_(ql) {
}

std::unique_ptr<Decoder> ListNodeDecoder::Clone() const {
  return std::make_unique<ListNodeDecoder>(ql_);
}

void ListNodeDecoder::Initialize(std::string_view slice) {
  this->slice = slice;
}

void ListNodeDecoder::Upload(void* obj) {
  ABSL_UNREACHABLE();
}

Decoder::UploadMetrics ListNodeDecoder::GetMetrics() const {
  ABSL_UNREACHABLE();
  return UploadMetrics{};
}

}  // namespace dfly::tiering
