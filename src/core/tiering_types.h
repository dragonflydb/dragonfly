// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <boost/intrusive/list_hook.hpp>

#include "core/compact_object.h"

namespace dfly::tiering {

struct TieredColdRecord : public ::boost::intrusive::list_base_hook<
                              boost::intrusive::link_mode<boost::intrusive::normal_link>> {
  uint64_t key_hash;  // Allows searching the entry in the dbslice.
  CompactValue value;
  uint16_t db_index;
  uint32_t page_index;
};
static_assert(sizeof(TieredColdRecord) == 48);

class FragmentRef {
 public:
  // Describes how this fragment should be serialized for offloading.
  // Used by stashing flow.
  struct SerializationDescr {
    std::variant<std::array<std::string_view, 2>, uint8_t*> blob;
    CompactObj::ExternalRep rep = CompactObj::ExternalRep::STRING;
  };

  FragmentRef(CompactValue& pv) : val_(&pv) {  // NOLINT
  }

  FragmentRef(CompactValue* pv) : val_(pv) {  // NOLINT
  }

  bool IsOffloaded() const {
    return std::visit([](auto* pv) { return pv->IsExternal(); }, val_);
  }

  bool HasStashPending() const {
    return std::visit([](auto* pv) { return pv->HasStashPending(); }, val_);
  }

  void ClearStashPending() {
    std::visit([](auto* pv) { pv->SetStashPending(false); }, val_);
  }

  CompactObjType ObjType() const {
    return std::visit([](auto* pv) { return pv->ObjType(); }, val_);
  }

  // Determine required byte size and encoding type based on value.
  SerializationDescr GetSerializationDescr() const {
    return std::visit([](auto* pv) { return GetDescr(pv); }, val_);
  }

 private:
  static SerializationDescr GetDescr(const CompactValue* pv);

  std::variant<CompactValue*> val_;
};

}  // namespace dfly::tiering
