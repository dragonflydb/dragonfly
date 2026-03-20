// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <boost/intrusive/list_hook.hpp>

#include "core/compact_object.h"

namespace dfly::tiering {

// TieredCoolRecord is part of the cooling cache. It allows offloading values to disk
// while still keeping some of them in-memory to avoid disk reads in case they are requested again
// soon after offloading. When a value is moved to the cold storage, TieredCoolRecord and only
// the external reference is kept. When the value is warmed up, the record is removed from the cool
// storage and the value is read back to memory.
struct TieredCoolRecord : public ::boost::intrusive::list_base_hook<
                              boost::intrusive::link_mode<boost::intrusive::normal_link>> {
  uint64_t key_hash;  // Allows searching the entry in the dbslice.
  uint16_t db_index;
  CompactValue value;
};
static_assert(sizeof(TieredCoolRecord) == 48);

class Fragment {
 public:
  // Describes how this fragment should be serialized for offloading.
  // Used by stashing flow.
  struct SerializationDescr {
    std::variant<std::array<std::string_view, 2>, uint8_t*> blob;
    CompactObj::ExternalRep rep = CompactObj::ExternalRep::STRING;
  };

  using FragmentType = std::variant<CompactValue*>;

  Fragment(CompactValue& pv) : val_(&pv) {  // NOLINT
  }

  Fragment(CompactValue* pv) : val_(pv) {  // NOLINT
  }

  bool IsExternal() const;
  void RemoveExternal();
  void SetExternal();

  bool HasStashPending() const;
  void SetStashPending(bool b);

  CompactObjType ObjType() const;

  // Determine required byte size and encoding type based on value.
  SerializationDescr GetSerializationDescr() const;

  bool IsCool() const {
    return is_cool_;
  }

  void SetCoolRecord(TieredCoolRecord* record) {
    cool_record_ = record;
    is_cool_ = (record != nullptr);
  }

  TieredCoolRecord* GetCoolRecord() const {
    return cool_record_;
  }

  std::pair<size_t, size_t> GetExternalSlice() const;

  void SetSegmentInfo(size_t offset, size_t length, CompactObj::ExternalRep rep);

  size_t Offset() const {
    return offset_;
  }

  size_t Size() const {
    return serialized_size_;
  }

  CompactObj::ExternalRep GetExternalRep() const;

  void SetFirstByte(uint8_t byte) {
    first_byte_ = byte;
  }

  uint8_t GetFirstByte() const {
    return first_byte_;
  }

  void UpdateValue(CompactValue* pv) {
    val_ = pv;
  }

  void SetId(size_t id) {
    id_ = id;
  }

  size_t Id() const {
    return id_;
  }

 private:
  static SerializationDescr GetDescr(const CompactValue* pv);

  size_t id_ = 0;

  uint8_t is_cool_ : 1 = 0;         // Whether the values is in the cooling storage.
  uint8_t representation_ : 2 = 0;  // See ExternalRep
  uint8_t reserved_ : 5 = 0;

  TieredCoolRecord* cool_record_ = nullptr;

  uint32_t serialized_size_ = 0;
  size_t offset_ = 0;

  // First byte of the value if Huffman encoded
  uint8_t first_byte_ = 0;

  FragmentType val_;
};

}  // namespace dfly::tiering
