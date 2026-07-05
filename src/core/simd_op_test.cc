// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/simd_op.h"

#include <bit>
#include <cstdint>

#include "base/gtest.h"

namespace dfly {

using U64x2 = SimdOp<std::uint64_t, 2>;
using U64x4 = SimdOp<std::uint64_t, 4>;

TEST(SimdOpTest, FillAndLoadAreEquivalent) {
  std::uint64_t arr[4] = {7, 7, 7, 7};
  auto a = U64x4::Fill(7);
  auto b = U64x4::Load(arr);
  EXPECT_EQ((a == b).GetMSBs(), 0xFu);
}

TEST(SimdOpTest, LoadKeepsLaneOrder) {
  std::uint64_t arr[4] = {1, 2, 3, 4};
  auto v = U64x4::Load(arr);
  EXPECT_EQ((v == uint64_t(1)).GetMSBs(), 0x1u);
  EXPECT_EQ((v == uint64_t(2)).GetMSBs(), 0x2u);
  EXPECT_EQ((v == uint64_t(3)).GetMSBs(), 0x4u);
  EXPECT_EQ((v == uint64_t(4)).GetMSBs(), 0x8u);
  EXPECT_EQ((v == uint64_t(99)).GetMSBs(), 0x0u);
}

TEST(SimdOpTest, ScalarEqDetectsZeroLanes) {
  std::uint64_t arr[4] = {0, 5, 0, 9};
  auto v = U64x4::Load(arr);
  EXPECT_EQ((v == uint64_t(0)).GetMSBs(), 0b0101u);
}

TEST(SimdOpTest, BitwiseAndOrShift) {
  std::uint64_t arr[4] = {0xFF00FF00u, 0x00FF00FFu, 0xFFFF0000u, 0x0000FFFFu};
  auto v = U64x4::Load(arr);
  auto masked = v & U64x4::Fill(0xFFFF0000ULL);
  std::uint64_t expected[4] = {0xFF000000ULL, 0x00FF0000ULL, 0xFFFF0000ULL, 0x00000000ULL};
  EXPECT_EQ((masked == U64x4::Load(expected)).GetMSBs(), 0xFu);

  auto shifted = masked >> 16;
  std::uint64_t shifted_expected[4] = {0xFF00ULL, 0xFFULL, 0xFFFFULL, 0x0ULL};
  EXPECT_EQ((shifted == U64x4::Load(shifted_expected)).GetMSBs(), 0xFu);

  std::uint64_t a_arr[4] = {0xAAAAULL, 0x0ULL, 0xAAAAULL, 0x0ULL};
  std::uint64_t b_arr[4] = {0x0ULL, 0x5555ULL, 0x5555ULL, 0x0ULL};
  std::uint64_t or_expected[4] = {0xAAAAULL, 0x5555ULL, 0xFFFFULL, 0x0ULL};
  auto a = U64x4::Load(a_arr);
  auto b = U64x4::Load(b_arr);
  EXPECT_EQ(((a | b) == U64x4::Load(or_expected)).GetMSBs(), 0xFu);
}

TEST(SimdOpTest, NotInvertsAllBits) {
  std::uint64_t arr[4] = {0, 0, 0, 0};
  auto v = U64x4::Load(arr);
  EXPECT_EQ((~v == ~uint64_t(0)).GetMSBs(), 0xFu);
}

TEST(SimdOpTest, ToBitsLsbIsLaneZero) {
  // Build a result with only lane 0 set, then confirm bit 0 is the one that
  // pops out (regression for any byte-order surprise).
  std::uint64_t arr[4] = {42, 0, 0, 0};
  auto v = U64x4::Load(arr);
  EXPECT_EQ((v == uint64_t(42)).GetMSBs(), 0x1u);
}

TEST(SimdOpTest, GetMSBsReadsLaneSignBits) {
  std::uint64_t arr[4] = {0x8000000000000000ULL, 0x7FFFFFFFFFFFFFFFULL, 0xFFFFFFFFFFFFFFFFULL, 0};
  EXPECT_EQ(U64x4::Load(arr).GetMSBs(), 0b0101u);
  EXPECT_EQ(U64x2::Load(arr).GetMSBs(), 0b0001u);
}

TEST(SimdOpTest, StoreRoundTripsLoad) {
  // Store is used by OAHSet::PrefetchWindow to spill masked addresses before prefetch.
  std::uint64_t src4[4] = {0x1111111111111111ULL, 0x2222222222222222ULL, 0x3333333333333333ULL,
                           0x4444444444444444ULL};
  std::uint64_t dst4[4] = {};
  U64x4::Load(src4).Store(dst4);
  for (int i = 0; i < 4; ++i)
    EXPECT_EQ(dst4[i], src4[i]);

  std::uint64_t src2[2] = {0xAAAAAAAAAAAAAAAAULL, 0xBBBBBBBBBBBBBBBBULL};
  std::uint64_t dst2[2] = {};
  (U64x2::Load(src2) & U64x2::Fill(0xFFFFFFFFULL)).Store(dst2);
  EXPECT_EQ(dst2[0], 0xAAAAAAAAULL);
  EXPECT_EQ(dst2[1], 0xBBBBBBBBULL);
}

TEST(SimdOpTest, U64x2WorksForVectorSearch) {
  // The 2-lane version is used by OAHSet::ProbeExtensionVector.
  std::uint64_t arr[2] = {0xDEAD, 0xBEEF};
  auto v = U64x2::Load(arr);
  EXPECT_EQ((v == uint64_t(0xDEAD)).GetMSBs(), 0x1u);
  EXPECT_EQ((v == uint64_t(0xBEEF)).GetMSBs(), 0x2u);
  EXPECT_EQ((v == uint64_t(0)).GetMSBs(), 0x0u);

  std::uint64_t with_zero[2] = {0, 0xBEEF};
  auto v2 = U64x2::Load(with_zero);
  EXPECT_EQ((v2 == uint64_t(0)).GetMSBs(), 0x1u);
}

// Exercises the exact composition pattern from OAHSet::Add:
//   ((hash == ext_hash) | (hash == 0)) & ~is_empty
TEST(SimdOpTest, MimicsOAHSetProbeComposition) {
  constexpr std::uint64_t kExtHashShift = 52;
  constexpr std::uint64_t kExtHashMask = 0xFFFULL << kExtHashShift;

  std::uint64_t buckets[4] = {
      0ULL,                                // empty
      (42ULL << kExtHashShift) | 0x10ULL,  // matching hash + payload
      (0ULL << kExtHashShift) | 0x20ULL,   // lazy-init hash + payload
      (99ULL << kExtHashShift) | 0x30ULL,  // non-matching hash + payload
  };

  auto data_v = U64x4::Load(buckets);
  auto hash_v = (data_v & U64x4::Fill(kExtHashMask)) >> kExtHashShift;
  auto is_empty = data_v == uint64_t(0);
  auto candidate = ((hash_v == uint64_t(42)) | (hash_v == uint64_t(0))) & ~is_empty;

  EXPECT_EQ(candidate.GetMSBs(), 0b0110u);
  EXPECT_EQ(is_empty.GetMSBs(), 0b0001u);
}

TEST(SimdOpTest, ToBitsIterationViaCountrZero) {
  std::uint64_t buckets[4] = {0, 5, 0, 5};
  auto v = U64x4::Load(buckets);
  auto bits = (v == uint64_t(5)).GetMSBs();
  std::vector<unsigned> found;
  while (bits) {
    found.push_back(std::countr_zero(bits));
    bits &= bits - 1;
  }
  ASSERT_EQ(found.size(), 2u);
  EXPECT_EQ(found[0], 1u);
  EXPECT_EQ(found[1], 3u);
}

}  // namespace dfly
