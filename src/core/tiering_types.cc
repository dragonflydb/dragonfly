// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/tiering_types.h"

#include <utility>

#include "core/compact_object.h"
#include "core/overloaded.h"
#include "redis/redis_aux.h"

namespace dfly::tiering {

bool FragmentRef::IsOffloaded() const {
  return std::visit(Overloaded{[](CompactValue* pv) { return pv->IsExternal(); },
                               [](QList::Node* node) { return node->offloaded != 0; }},
                    val_);
}

void FragmentRef::ClearOffloaded() {
  std::visit(Overloaded{[](CompactValue* pv) { pv->RemoveExternal(); },
                        [](QList::Node* node) { node->offloaded = 0; }},
             val_);
}

bool FragmentRef::HasStashPending() const {
  return std::visit(Overloaded{[](CompactValue* pv) { return pv->HasStashPending(); },
                               [](QList::Node* node) { return node->io_pending != 0; }},
                    val_);
}

void FragmentRef::SetStashPending(bool b) {
  std::visit(Overloaded{[b](CompactValue* pv) { pv->SetStashPending(b); },
                        [b](QList::Node* node) { node->io_pending = b ? 1 : 0; }},
             val_);
}

CompactObjType FragmentRef::ObjType() const {
  return std::visit(Overloaded{[](CompactValue* pv) { return pv->ObjType(); },
                               [](QList::Node*) -> CompactObjType { return OBJ_LIST; }},
                    val_);
}

auto FragmentRef::GetDescr(const CompactValue* pv) -> SerializationDescr {
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

auto FragmentRef::GetDescr(const QList::Node* node) -> SerializationDescr {
  if (!node->u_.entry || node->sz == 0)
    return {};

  std::string_view entry_view{reinterpret_cast<const char*>(node->u_.entry), node->GetEntrySize()};
  return {std::array<std::string_view, 2>{entry_view, {}}, CompactObj::ExternalRep::LIST_NODE};
}

auto FragmentRef::GetSerializationDescr() const -> SerializationDescr {
  return std::visit(Overloaded{[](CompactValue* pv) { return GetDescr(pv); },
                               [](QList::Node* node) { return GetDescr(node); }},
                    val_);
}

TieredCoolRecord* FragmentRef::GetCoolRecord() const {
  return std::visit(Overloaded{[](CompactValue* pv) -> TieredCoolRecord* {
                                 return pv->IsExternal() && pv->IsCool() ? pv->GetCool().record
                                                                         : nullptr;
                               },
                               [](QList::Node* node) -> TieredCoolRecord* { return nullptr; }},
                    val_);
}

std::pair<size_t, size_t> FragmentRef::GetExternalSlice() const {
  return std::visit([](auto* val) { return val->GetExternalSlice(); }, val_);
}

}  // namespace dfly::tiering
