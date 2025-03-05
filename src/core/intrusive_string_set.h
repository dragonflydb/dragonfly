// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstring>
#include <memory>
#include <string_view>
#include <vector>

namespace dfly {

class ISSEntry {
 public:
  ISSEntry(std::string_view key) {
    ISSEntry* next = nullptr;
    uint32_t key_size = key.size();

    auto size = sizeof(next) + sizeof(key_size) + key_size;

    data_ = (char*)malloc(size);

    std::memcpy(data_, &next, sizeof(next));

    auto* key_size_pos = data_ + sizeof(next);
    std::memcpy(key_size_pos, &key_size, sizeof(key_size));

    auto* key_pos = key_size_pos + sizeof(key_size);
    std::memcpy(key_pos, key.data(), key_size);
  }

  std::string_view Key() const {
    return {GetKeyData(), GetKeySize()};
  }

  ISSEntry* Next() const {
    ISSEntry* next = nullptr;
    std::memcpy(&next, data_, sizeof(next));
    return next;
  }

  void SetNext(ISSEntry* next) {
    std::memcpy(data_, &next, sizeof(next));
  }

 private:
  const char* GetKeyData() const {
    return data_ + sizeof(ISSEntry*) + sizeof(uint32_t);
  }

  uint32_t GetKeySize() const {
    uint32_t size = 0;
    std::memcpy(&size, data_ + sizeof(ISSEntry*), sizeof(size));
    return size;
  }

  // TODO consider use SDS strings or other approach
  // TODO add optimization for big keys
  // memory daya layout [ISSEntry*, key_size, key]
  char* data_;
};

class ISMEntry {
 public:
  ISMEntry(std::string_view key, std::string_view val) {
    ISMEntry* next = nullptr;
    uint32_t key_size = key.size();
    uint32_t val_size = val.size();

    auto size = sizeof(next) + sizeof(key_size) + sizeof(val_size) + key_size + val_size;

    data_ = (char*)malloc(size);

    std::memcpy(data_, &next, sizeof(next));

    auto* key_size_pos = data_ + sizeof(next);
    std::memcpy(key_size_pos, &key_size, sizeof(key_size));

    auto* val_size_pos = key_size_pos + sizeof(key_size);
    std::memcpy(val_size_pos, &val_size, sizeof(val_size));

    auto* key_pos = val_size_pos + sizeof(val_size);
    std::memcpy(key_pos, key.data(), key_size);

    auto* val_pos = key_pos + key_size;
    std::memcpy(val_pos, val.data(), val_size);
  }

  std::string_view Key() const {
    return {GetKeyData(), GetKeySize()};
  }

  std::string_view Val() const {
    return {GetValData(), GetValSize()};
  }

  ISMEntry* Next() const {
    ISMEntry* next = nullptr;
    std::memcpy(&next, data_, sizeof(next));
    return next;
  }

  void SetVal(std::string_view val) {
    // TODO add optimization for the same size key
    uint32_t val_size = val.size();
    auto new_size =
        sizeof(ISMEntry*) + sizeof(uint32_t) + sizeof(uint32_t) + GetKeySize() + val_size;

    data_ = (char*)realloc(data_, new_size);

    auto* val_size_pos = data_ + sizeof(ISMEntry*) + sizeof(uint32_t);
    std::memcpy(val_size_pos, &val_size, sizeof(val_size));

    auto* val_pos = val_size_pos + sizeof(val_size) + GetKeySize();
    std::memcpy(val_pos, val.data(), val_size);
  }

  void SetNext(ISMEntry* next) {
    std::memcpy(data_, &next, sizeof(next));
  }

 private:
  const char* GetKeyData() const {
    return data_ + sizeof(ISMEntry*) + sizeof(uint32_t) + sizeof(uint32_t);
  }

  uint32_t GetKeySize() const {
    uint32_t size = 0;
    std::memcpy(&size, data_ + sizeof(ISMEntry*), sizeof(size));
    return size;
  }

  const char* GetValData() const {
    return GetKeyData() + GetKeySize();
  }

  uint32_t GetValSize() const {
    uint32_t size = 0;
    std::memcpy(&size, data_ + sizeof(ISMEntry*) + sizeof(uint32_t), sizeof(size));
    return size;
  }

  // TODO consider use SDS strings or other approach
  // TODO add optimization for big keys
  // memory daya layout [ISMEntry*, key_size, val_size, key, val]
  char* data_;
};

template <class EntryT> class IntrusiveStringSet {
 public:
 private:
  std::vector<EntryT*> entries_;
};

}  // namespace dfly
