// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/container/inlined_vector.h>

#include <string_view>

namespace cmn {

#ifdef ABSL_HAVE_ADDRESS_SANITIZER
constexpr size_t kReqStorageSize = 88;
#else
constexpr size_t kReqStorageSize = 88;
#endif

struct BackedArguments {
  BackedArguments() {
  }

  size_t StorageCapacity() const {
    return lens.capacity() + storage.capacity();
  }

  // The capacity is chosen so that we allocate a fully utilized (256 bytes) block.
  using StorageType = absl::InlinedVector<char, kReqStorageSize>;

  std::string_view key() const {
    return lens.empty() ? std::string_view{} : std::string_view{storage.data(), lens[0]};
  }

  auto MakeArgs() const {
    absl::InlinedVector<std::string_view, 5> args(lens.size());
    const char* ptr = storage.data();
    for (size_t i = 0; i < lens.size(); ++i) {
      args[i] = std::string_view{ptr, lens[i]};
      ptr += lens[i] + 1;  // +1 for '\0'
    }
    return args;
  }

  absl::InlinedVector<uint32_t, 5> lens;
  StorageType storage;
};

static_assert(sizeof(BackedArguments) == 128);

}  // namespace cmn
