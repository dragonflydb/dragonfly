// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/tiering_types.h"

#include "redis/redis_aux.h"

namespace dfly::tiering {

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

}  // namespace dfly::tiering
