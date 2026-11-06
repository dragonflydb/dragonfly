// Copyright 2024, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "cluster_support.h"

#include <array>

#include "base/endian.h"
#include "base/flags.h"
#include "base/logging.h"
#include "common.h"

using namespace std;

ABSL_FLAG(string, cluster_mode, "",
          "Cluster mode supported. Possible values are "
          "'emulated', 'yes' or ''");

ABSL_FLAG(bool, experimental_cluster_shard_by_slot, false,
          "If true, cluster mode is enabled and sharding is done by slot. "
          "Otherwise, sharding is done by hash tag.");

namespace dfly {

namespace {

constexpr unsigned kCrc16Slices = 16;
using Crc16Tables = std::array<std::array<uint16_t, 256>, kCrc16Slices>;

// kCrc16Tables[0] is the classic byte-at-a-time table. kCrc16Tables[k][b] is the CRC of byte b
// followed by k zero bytes, so XOR-ing kCrc16Tables[n - 1 - i][block[i]] over an n-byte block
// advances the CRC over the whole block at once ("slicing-by-n").
alignas(64) constexpr Crc16Tables kCrc16Tables = [] {
  Crc16Tables tables{};
  for (unsigned b = 0; b < 256; ++b) {
    uint16_t crc = b << 8;
    for (unsigned bit = 0; bit < 8; ++bit)
      crc = (crc & 0x8000) ? (crc << 1) ^ 0x1021 : crc << 1;
    tables[0][b] = crc;
  }
  for (unsigned k = 1; k < kCrc16Slices; ++k) {
    for (unsigned b = 0; b < 256; ++b) {
      uint16_t prev = tables[k - 1][b];
      tables[k][b] = (prev << 8) ^ tables[0][prev >> 8];
    }
  }
  return tables;
}();

// Folds 8 block bytes, packed into `v` with the first byte in the lowest bits, that are followed
// by `trailing` more bytes in the block.
inline uint16_t Fold8(uint64_t v, unsigned trailing) {
  uint16_t res = 0;
  for (unsigned i = 0; i < 8; ++i)
    res ^= kCrc16Tables[trailing + 7 - i][(v >> (8 * i)) & 0xff];
  return res;
}

// CRC16-XMODEM, compatible with Redis Cluster, processing up to 16 bytes per iteration.
//
// Graviton2 (Neoverse-N1), CRC-only throughput is 36.1 -> 13.9 ns/call for 16 bytes (2.6x) and
// 192 -> 44.4 ns/call for 64 bytes (4.3x). Including Tag(), BM_KeySlot measured 205 -> 51.5 ns
// for 64-byte keys (4.0x).
uint16_t Crc16(std::string_view data) {
  const uint8_t* p = reinterpret_cast<const uint8_t*>(data.data());
  size_t len = data.size();
  uint16_t crc = 0;

  // The CRC register is XOR-ed into the first two bytes of a block, most significant byte first.
  auto load_with_crc = [&crc](const uint8_t* block) {
    return absl::little_endian::Load64(block) ^ ((crc >> 8) | ((crc & 0xff) << 8));
  };

  for (; len >= 16; len -= 16, p += 16)
    crc = Fold8(load_with_crc(p), 8) ^ Fold8(absl::little_endian::Load64(p + 8), 0);

  if (len >= 8) {
    crc = Fold8(load_with_crc(p), 0);
    len -= 8;
    p += 8;
  }

  // The serial 1-7 byte tail limits gains for lengths not divisible by 8: on the same setup,
  // 7-byte inputs take 14.8 ns/call versus Redis's 13.7 ns/call.
  for (; len > 0; --len, ++p)
    crc = (crc << 8) ^ kCrc16Tables[0][(crc >> 8) ^ *p];

  return crc;
}

}  // namespace

void UniqueSlotChecker::Add(std::string_view key) {
  if (!IsClusterEnabled()) {
    return;
  }

  Add(KeySlot(key));
}

void UniqueSlotChecker::Add(SlotId slot_id) {
  if (!IsClusterEnabled()) {
    return;
  }

  if (slot_id_ == kNoSlotId) {
    slot_id_ = slot_id;
  } else if (slot_id_ != slot_id) {
    slot_id_ = kCrossSlot;
  }
}

optional<SlotId> UniqueSlotChecker::GetUniqueSlotId() const {
  return slot_id_ > kMaxSlotNum ? optional<SlotId>() : slot_id_;
}

using namespace detail;

void InitializeCluster() {
  string cluster_mode_str = absl::GetFlag(FLAGS_cluster_mode);

  if (cluster_mode_str == "emulated") {
    cluster_mode = ClusterMode::kEmulatedCluster;
  } else if (cluster_mode_str == "yes") {
    cluster_mode = ClusterMode::kRealCluster;
  } else if (cluster_mode_str.empty()) {
    cluster_mode = ClusterMode::kNoCluster;
  } else {
    LOG(ERROR) << "Invalid value for flag --cluster_mode. Exiting...";
    exit(1);
  }

  if (cluster_mode == ClusterMode::kNoCluster) {
    // Reset the cached value so it doesn't leak in tests.
    cluster_shard_by_slot = false;
  } else {
    cluster_shard_by_slot = absl::GetFlag(FLAGS_experimental_cluster_shard_by_slot);
  }
}

SlotId KeySlot(std::string_view key) {
  return Crc16(LockTagOptions::instance().Tag(key)) & kMaxSlotNum;
}

bool IsClusterShardedByTag() {
  return IsClusterEnabledOrEmulated() || LockTagOptions::instance().enabled;
}

}  // namespace dfly
