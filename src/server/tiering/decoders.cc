// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/tiering/decoders.h"

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
  estimated_mem_usage = slice.length();  // will be encoded back
}

void StringDecoder::Upload(CompactObj* obj) {
  if (modified)
    obj->Materialize(value_.view(), false);
  else
    obj->Materialize(slice_, true);
}

std::string_view StringDecoder::Read() const {
  return value_.view();
}

std::string* StringDecoder::Write() {
  modified = true;
  return value_.BorrowMut();
}

}  // namespace dfly::tiering
