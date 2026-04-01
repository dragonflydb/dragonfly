// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <boost/intrusive/list_hook.hpp>

#include "core/compact_object.h"
#include "core/qlist.h"

namespace dfly::tiering {

// TieredCoolRecord is part of the cooling cache. It allows offloading values to disk
// while still keeping some of them in-memory to avoid disk reads in case they are requested again
// soon after offloading. When a value is moved to the cold storage, TieredCoolRecord and only
// the external reference is kept. When the value is warmed up, the record is removed from the cool
// storage and the value is read back to memory.
struct TieredCoolRecord : public ::boost::intrusive::list_base_hook<
                              boost::intrusive::link_mode<boost::intrusive::normal_link>> {
  uint64_t key_hash;  // Allows searching the entry in the dbslice.
  CompactValue value;
  uint16_t db_index;
  uint32_t page_index;
};
static_assert(sizeof(TieredCoolRecord) == 48);

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

  FragmentRef(QList::Node& node) : val_(&node) {  // NOLINT
  }

  FragmentRef(QList::Node* node) : val_(node) {  // NOLINT
  }

  bool IsOffloaded() const;

  // Resets offloaded state for this fragment.
  void ClearOffloaded();

  bool HasStashPending() const;
  void SetStashPending(bool b);

  CompactObjType ObjType() const;

  // Determine required byte size and encoding type based on value.
  SerializationDescr GetSerializationDescr() const;

  // Returns a pointer to TieredCoolRecord if this fragment is cool, and null otherwise.
  TieredCoolRecord* GetCoolRecord() const;

  // Returns the external slice of the offloaded value. Only valid if IsOffloaded() is true.
  std::pair<size_t, size_t> GetExternalSlice() const;

 private:
  static SerializationDescr GetDescr(const CompactValue* pv);
  static SerializationDescr GetDescr(const QList::Node* node);

  std::variant<CompactValue*, QList::Node*> val_;
};

}  // namespace dfly::tiering
