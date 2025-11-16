// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tiering/decoders.h"

<<<<<<< HEAD
#include "base/logging.h"
=======
#include "core/compact_object.h"
>>>>>>> fbcc8d59 (more than POc)
#include "core/detail/listpack_wrap.h"
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
}

Decoder::UploadMetrics SerializedMapDecoder::GetMetrics() const {
  return UploadMetrics{.modified = modified_,
                       .estimated_mem_usage = map_->DataBytes() + map_->size() * 2 * 8};
}

void SerializedMapDecoder::Upload(CompactObj* obj) {
  if (std::holds_alternative<std::unique_ptr<SerializedMap>>(map_))
    MakeOwned();

  obj->InitRobj(OBJ_HASH, kEncodingListPack, Write()->GetPointer());
}

std::variant<SerializedMap*, detail::ListpackWrap*> SerializedMapDecoder::Read() const {
  using RT = std::variant<SerializedMap*, detail::ListpackWrap*>;
  return std::visit([](auto& ptr) -> RT { return ptr.get(); }, map_);
}

detail::ListpackWrap* SerializedMapDecoder::Write() {
  if (std::holds_alternative<std::unique_ptr<detail::ListpackWrap>>(map_))
    return std::get<std::unique_ptr<detail::ListpackWrap>>(map_).get();

  // Convert SerializedMap to listpack
  MakeOwned();
  modified_ = true;
  return Write();
}

void SerializedMapDecoder::MakeOwned() {
  auto& map = std::get<std::unique_ptr<SerializedMap>>(map_);

  auto lw = detail::ListpackWrap::WithCapacity(GetMetrics().estimated_mem_usage);
  for (const auto& [key, value] : *map)
    lw.Insert(key, value, true);

  map_ = std::make_unique<detail::ListpackWrap>(lw);
}

}  // namespace dfly::tiering
