// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tiering/decoders.h"

#include "server/tiering/serialized_map.h"

namespace dfly::tiering {

std::unique_ptr<Decoder> BareDecoder::Clone() const {
  return std::make_unique<BareDecoder>();
}

void BareDecoder::Initialize(std::string_view slice) {
  this->slice = slice;
}

void BareDecoder::Upload(CompactObj* obj) {
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

void StringDecoder::Upload(CompactObj* obj) {
  if (modified_)
    obj->Materialize(value_.view(), false);
  else
    obj->Materialize(slice_, true);
}

Decoder::UploadMetrics StringDecoder::GetMetrics() const {
  return UploadMetrics{
      .modified = modified_,
      .estimated_mem_usage = value_.view().size(),
  };
}

std::string_view StringDecoder::Read() const {
  return value_.view();
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
  estimated_mem_usage = slice.size() + map_->size() * 8 /* bytes per ptr */;
}

void SerializedMapDecoder::Upload(CompactObj* obj) {
  ABSL_UNREACHABLE();
}

SerializedMap* SerializedMapDecoder::Get() const {
  return map_.get();
}

}  // namespace dfly::tiering
