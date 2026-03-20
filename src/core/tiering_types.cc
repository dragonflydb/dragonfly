// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/tiering_types.h"

#include "core/compact_object.h"
#include "redis/redis_aux.h"

namespace dfly::tiering {

bool Fragment::IsExternal() const {
  return std::visit([](CompactValue* pv) { return pv->IsExternal(); }, val_);
}

void Fragment::RemoveExternal() {
  std::visit([](CompactValue* pv) { pv->RemoveExternal(); }, val_);
}

void Fragment::SetExternal() {
  std::visit([this](CompactValue* pv) { pv->SetExternal(this); }, val_);
}

bool Fragment::HasStashPending() const {
  return std::visit([](CompactValue* pv) { return pv->HasStashPending(); }, val_);
}

void Fragment::SetStashPending(bool b) {
  std::visit([b](CompactValue* pv) { pv->SetStashPending(b); }, val_);
}

CompactObjType Fragment::ObjType() const {
  return std::visit([](CompactValue* pv) { return pv->ObjType(); }, val_);
}

auto Fragment::GetDescr(const CompactValue* pv) -> SerializationDescr {
  switch (pv->ObjType()) {
    case OBJ_STRING: {
      if (!pv->HasAllocated())
        return {};
      auto strs = pv->GetRawString();
      return {strs, CompactObj::ExternalRep::STRING};
    }
    case OBJ_HASH: {
      if (pv->Encoding() == kEncodingListPack) {
        return {static_cast<uint8_t*>(pv->RObjPtr()), CompactObj::ExternalRep::SERIALIZED_MAP};
      }
      return {};
    }
    default:
      return {};
  };
}

auto Fragment::GetSerializationDescr() const -> SerializationDescr {
  return std::visit([](CompactValue* pv) { return GetDescr(pv); }, val_);
}

std::pair<size_t, size_t> Fragment::GetExternalSlice() const {
  return {offset_, serialized_size_};
}

void Fragment::SetSegmentInfo(size_t offset, size_t length, CompactObj::ExternalRep rep) {
  offset_ = offset;
  serialized_size_ = length;
  representation_ = static_cast<uint8_t>(rep);
}

CompactObj::ExternalRep Fragment::GetExternalRep() const {
  return static_cast<CompactObj::ExternalRep>(representation_);
}

}  // namespace dfly::tiering
