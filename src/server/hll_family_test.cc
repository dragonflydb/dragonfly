// Copyright 2023, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
extern "C" {
#include "redis/hyperloglog.h"

/* Internal to hyperloglog.c. Declared here so that the benchmarks at the bottom
 * can time the two kernels that AVX2/NEON accelerate on their own, without the
 * validation and cardinality work pfmerge()/pfcountMulti() wrap them in. */
void hllMergeDense(uint8_t* reg_raw, const uint8_t* reg_dense);
void hllDenseCompress(uint8_t* reg_dense, const uint8_t* reg_raw);
}

#include <absl/functional/function_ref.h>

#include "base/gtest.h"
#include "base/logging.h"
#include "facade/error.h"
#include "facade/facade_test.h"
#include "server/test_utils.h"

using namespace testing;
using namespace std;
using namespace util;
using namespace facade;

namespace dfly {

namespace {

constexpr char kCorruptedHllError[] = "INVALIDOBJ Corrupted HLL object detected.";

// Builds the CVE-2025-32023 payload: a sparse HLL whose XZERO run lengths sum
// past INT_MAX, so the `idx` cursor wraps negative and the trailing VAL opcode
// slips past the `(runlen + idx) > HLL_REGISTERS` guard. Before the run-length
// checks were added to every opcode branch, decoding this wrote a register
// through a wild pointer and crashed the server.
std::string MakeOverflowingSparseHll() {
  // 155486 * 16384 wraps `idx` to a value whose *6 (bits per register) is still
  // negative, so the resulting byte offset is a huge unsigned number.
  constexpr int kXZeroOps = 155486;

  std::string hll;
  hll.reserve(16 + kXZeroOps * 2 + 1);
  hll.append("HYLL");   // magic
  hll.push_back(1);     // encoding = HLL_SPARSE
  hll.append(3, '\0');  // notused
  hll.append(8, '\0');  // cached cardinality
  for (int i = 0; i < kXZeroOps; ++i) {
    hll.push_back('\x7f');  // XZERO, 14-bit length-1 == 16383
    hll.push_back('\xff');
  }
  hll.push_back('\x80');  // VAL: value 1, run length 1
  return hll;
}

// Builds a dense HLL whose register i holds value_at(i). Elements added through
// PFADD only ever produce small register values, which leaves the top bits of
// the 6-bit registers untested; the SIMD kernels pack and unpack all six, so
// their tests need registers spanning the whole 0..63 range.
std::string MakeDenseHll(absl::FunctionRef<unsigned(int)> value_at) {
  constexpr int kRegisters = 16384;
  constexpr int kBits = 6;

  // One byte of slack: setting the last register touches the byte just past the
  // register array, which is the implicit terminator hyperloglog.c relies on.
  std::vector<uint8_t> buf(getDenseHllSize() + 1, 0);
  CHECK_EQ(createDenseHll({buf.data(), getDenseHllSize()}), 0);

  uint8_t* regs = buf.data() + (getDenseHllSize() - kRegisters * kBits / 8);
  for (int i = 0; i < kRegisters; ++i) {
    const unsigned v = value_at(i) & 63;
    const unsigned byte = i * kBits / 8;
    const unsigned fb = (i * kBits) & 7;
    regs[byte] &= ~(63u << fb);
    regs[byte] |= v << fb;
    regs[byte + 1] &= ~(63u >> (8 - fb));
    regs[byte + 1] |= v >> (8 - fb);
  }
  return std::string(reinterpret_cast<char*>(buf.data()), getDenseHllSize());
}

}  // namespace

class HllFamilyTest : public BaseFamilyTest {
 protected:
  std::string GenerateUniqueValue(int index) {
    return "Value_{" + std::to_string(index) + "}";
  }
};

TEST_F(HllFamilyTest, Simple) {
  EXPECT_EQ(CheckedInt({"pfadd", "key", "1"}), 1);
  EXPECT_EQ(CheckedInt({"pfadd", "key", "1"}), 0);
  EXPECT_EQ(CheckedInt({"pfcount", "key"}), 1);
}

TEST_F(HllFamilyTest, Promote) {
  int unique_values = 20000;
  // Sparse hll is promoted to dense at the 1660th+- insertion
  // This value varies if any parameter in hyperloglog.c changes.
  int promote_i = 1660;
  // Keep consistent with hyperloglog.c
  int kHllSparseMaxBytes = 3000;
  int kHllDenseSize = 12304;
  for (int i = 0; i < unique_values; ++i) {
    std::string newkey = GenerateUniqueValue(i);
    Run({"pfadd", "key", newkey});
    if (i < promote_i) {
      EXPECT_LT(CheckedInt({"strlen", "key"}), kHllSparseMaxBytes + 1);
    } else {
      EXPECT_EQ(CheckedInt({"strlen", "key"}), kHllDenseSize);
    }
  }
  // HyperLogLog computations come with a
  // margin of error, with a standard error rate of 0.81%.
  // Set it to 5% so this test won't fail unless something went wrong badly.
  EXPECT_LT(std::abs(CheckedInt({"pfcount", "key"}) - unique_values * 1.0) / unique_values, 0.05);
}

TEST_F(HllFamilyTest, MultipleValues) {
  EXPECT_EQ(CheckedInt({"pfadd", "key", "1", "2", "3"}), 1);
  EXPECT_EQ(CheckedInt({"pfcount", "key"}), 3);
  EXPECT_EQ(CheckedInt({"pfadd", "key", "1", "2", "3"}), 0);
  EXPECT_EQ(CheckedInt({"pfcount", "key"}), 3);
  EXPECT_EQ(CheckedInt({"pfadd", "key", "1"}), 0);
  EXPECT_EQ(CheckedInt({"pfcount", "key"}), 3);
  EXPECT_EQ(CheckedInt({"pfadd", "key", "2"}), 0);
  EXPECT_EQ(CheckedInt({"pfcount", "key"}), 3);
  EXPECT_EQ(CheckedInt({"pfadd", "key", "3"}), 0);
  EXPECT_EQ(CheckedInt({"pfcount", "key"}), 3);
  EXPECT_EQ(CheckedInt({"pfadd", "key", "3", "4"}), 1);
  EXPECT_EQ(CheckedInt({"pfcount", "key"}), 4);
  EXPECT_EQ(CheckedInt({"pfadd", "key", "5"}), 1);
  EXPECT_EQ(CheckedInt({"pfcount", "key"}), 5);
  EXPECT_EQ(CheckedInt({"pfadd", "key", "1", "2", "3", "4", "5"}), 0);
  EXPECT_EQ(CheckedInt({"pfcount", "key"}), 5);
}

TEST_F(HllFamilyTest, MultipleValues_random) {
  int insertions = 20000;
  int unique_values = 0;
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(1, 20);
  // cumulated pfadd result
  for (int i = 0; i < insertions; ++i) {
    // Number of values to insert
    int num_values = dis(gen);
    unique_values += num_values;

    // Prepare the command
    std::vector<std::string> values;
    values.reserve(num_values + 2);
    values.push_back("pfadd");
    values.push_back("key");

    // Generate and add unique values to the command
    for (int j = 0; j < num_values; ++j) {
      values.push_back(GenerateUniqueValue(i * 20 + j));
    }

    std::vector<std::string_view> commandViews;
    for (const auto& val : values) {
      commandViews.push_back(val);
    }
    Run(commandViews);
  }
  // HyperLogLog computations come with a
  // margin of error, with a standard error rate of 0.81%.
  // Set it to 5% so this test won't fail unless something went wrong badly.
  EXPECT_LT(std::abs(CheckedInt({"pfcount", "key"}) - unique_values * 1.0) / unique_values, 0.05);
}

TEST_F(HllFamilyTest, AddInvalid) {
  EXPECT_EQ(Run({"set", "key", "..."}), "OK");
  EXPECT_THAT(Run({"pfadd", "key", "1"}), ErrArg(kInvalidHllError));
  EXPECT_THAT(Run({"pfcount", "key"}), ErrArg(kInvalidHllError));
}

TEST_F(HllFamilyTest, OtherType) {
  Run({"zadd", "key", "1", "a"});
  EXPECT_THAT(Run({"pfadd", "key", "1"}),
              ErrArg("Operation against a key holding the wrong kind of value"));
  EXPECT_THAT(Run({"pfcount", "key"}),
              ErrArg("Operation against a key holding the wrong kind of value"));
}

TEST_F(HllFamilyTest, CountEmpty) {
  EXPECT_EQ(CheckedInt({"pfcount", "nonexisting"}), 0);
}

TEST_F(HllFamilyTest, CountInvalid) {
  EXPECT_EQ(Run({"set", "key", "..."}), "OK");
  EXPECT_THAT(Run({"pfcount", "key"}), ErrArg(kInvalidHllError));
}

TEST_F(HllFamilyTest, CountMultiple) {
  EXPECT_EQ(CheckedInt({"pfadd", "key1", "1", "2", "3"}), 1);
  EXPECT_EQ(CheckedInt({"pfcount", "key1"}), 3);

  EXPECT_EQ(CheckedInt({"pfadd", "key2", "1", "2", "3"}), 1);
  EXPECT_EQ(CheckedInt({"pfcount", "key2"}), 3);

  EXPECT_EQ(CheckedInt({"pfadd", "key3", "2", "3"}), 1);
  EXPECT_EQ(CheckedInt({"pfcount", "key3"}), 2);

  EXPECT_EQ(CheckedInt({"pfadd", "key4", "4", "5"}), 1);
  EXPECT_EQ(CheckedInt({"pfcount", "key4"}), 2);

  EXPECT_EQ(CheckedInt({"pfcount", "key1", "key4"}), 5);

  EXPECT_EQ(CheckedInt({"pfcount", "non-existing-key1", "non-existing-key2"}), 0);

  EXPECT_EQ(CheckedInt({"pfcount", "key1", "non-existing-key"}), 3);

  EXPECT_EQ(CheckedInt({"pfcount", "key1", "key2"}), 3);
  EXPECT_EQ(CheckedInt({"pfcount", "key1", "key3"}), 3);
  EXPECT_EQ(CheckedInt({"pfcount", "key1", "key2", "key3"}), 3);
  EXPECT_EQ(CheckedInt({"pfcount", "key1", "key2", "key3", "key4"}), 5);
  EXPECT_EQ(CheckedInt({"pfcount", "key1", "key2", "key3", "key4", "non-existing"}), 5);
  EXPECT_EQ(CheckedInt({"pfcount", "key1", "key4"}), 5);
}

TEST_F(HllFamilyTest, CountMultipleWithWrongType) {
  EXPECT_EQ(Run({"set", "key1", "value1"}), "OK");
  EXPECT_EQ(CheckedInt({"pfadd", "key", "value"}), 1);
  EXPECT_EQ(CheckedInt({"pfadd", "list1 element1", "data"}), 1);

  EXPECT_THAT(Run({"pfcount", "key1", "key", "list1 element1"}),
              ErrArg("INVALIDOBJ Corrupted HLL object detected."));
}

TEST_F(HllFamilyTest, MergeToNew) {
  EXPECT_EQ(CheckedInt({"pfadd", "key1", "1", "2", "3"}), 1);
  EXPECT_EQ(CheckedInt({"pfadd", "key2", "4", "5"}), 1);
  EXPECT_EQ(Run({"pfmerge", "key3", "key1", "key2"}), "OK");
  EXPECT_EQ(CheckedInt({"pfcount", "key3"}), 5);
}

TEST_F(HllFamilyTest, MergeToExisting) {
  EXPECT_EQ(CheckedInt({"pfadd", "key1", "1", "2", "3"}), 1);
  EXPECT_EQ(CheckedInt({"pfadd", "key2", "4", "5"}), 1);
  EXPECT_EQ(Run({"pfmerge", "key3", "key2", "key1"}), "OK");
  EXPECT_EQ(CheckedInt({"pfcount", "key3"}), 5);
  EXPECT_EQ(Run({"pfmerge", "key3", "key3"}), "OK");
  EXPECT_EQ(CheckedInt({"pfcount", "key3"}), 5);
  EXPECT_EQ(Run({"pfmerge", "key3"}), "OK");
  EXPECT_EQ(CheckedInt({"pfcount", "key3"}), 5);
  EXPECT_EQ(CheckedInt({"pfadd", "key4", "4", "5", "6"}), 1);
  EXPECT_EQ(Run({"pfmerge", "key3", "key4"}), "OK");
  EXPECT_EQ(CheckedInt({"pfcount", "key3"}), 6);
}

TEST_F(HllFamilyTest, MergeNonExisting) {
  EXPECT_EQ(CheckedInt({"pfadd", "key1", "1", "2", "3"}), 1);
  EXPECT_EQ(Run({"pfmerge", "key3", "key1", "key2"}), "OK");
  EXPECT_EQ(CheckedInt({"pfcount", "key3"}), 3);
}

TEST_F(HllFamilyTest, MergeOverlapping) {
  EXPECT_EQ(CheckedInt({"pfadd", "key1", "1", "2", "3"}), 1);
  EXPECT_EQ(CheckedInt({"pfadd", "key2", "2", "3"}), 1);
  EXPECT_EQ(CheckedInt({"pfadd", "key3", "1", "3"}), 1);
  EXPECT_EQ(CheckedInt({"pfadd", "key4", "2", "3"}), 1);
  EXPECT_EQ(CheckedInt({"pfadd", "key5", "3"}), 1);
  EXPECT_EQ(Run({"pfmerge", "key6", "key1", "key2", "key3", "key4", "key5"}), "OK");
  EXPECT_EQ(CheckedInt({"pfcount", "key6"}), 3);
}

TEST_F(HllFamilyTest, MergeInvalid) {
  Run({"exists", "key1", "key4"});
  ASSERT_EQ(GetDebugInfo().shards_count, 2);  // ensure 2 shards

  EXPECT_EQ(CheckedInt({"pfadd", "key1", "1", "2", "3"}), 1);
  EXPECT_EQ(Run({"set", "key4", "..."}), "OK");
  EXPECT_THAT(Run({"pfmerge", "key1", "key4"}),
              ErrArg("INVALIDOBJ Corrupted HLL object detected."));
  EXPECT_EQ(CheckedInt({"pfcount", "key1"}), 3);
}

TEST_F(HllFamilyTest, MergeWithInvalidHllFormat) {
  EXPECT_EQ(CheckedInt({"pfadd", "complex@key \"weird!field\" \"value\\nwith\\tescape sequences\"",
                        "some_element"}),
            1);
  EXPECT_EQ(CheckedInt({"append", "complex@key \"weird!field\" \"value\\nwith\\tescape sequences\"",
                        "corrupt_data"}),
            33);
  EXPECT_EQ(CheckedInt({"pfadd", "\"key with \\\"quotes\\\"\" \"value with \\\\backslashes\\\\\"",
                        "element1"}),
            1);
  EXPECT_THAT(Run({"pfmerge", "result_key",
                   "complex@key \"weird!field\" \"value\\nwith\\tescape sequences\"",
                   "\"key with \\\"quotes\\\"\" \"value with \\\\backslashes\\\\\""}),
              ErrArg("INVALIDOBJ Corrupted HLL object detected."));
}

// CVE-2025-32023. Reading this payload used to run the sparse decoder's cursor
// past INT_MAX and write a register through a wild pointer; every opcode branch
// now checks the run length, so all three commands report the HLL as corrupted.
TEST_F(HllFamilyTest, CorruptedSparseRunLengthOverflow) {
  const string payload = MakeOverflowingSparseHll();
  ASSERT_EQ(Run({"set", "overflow", payload}), "OK");

  // PFCOUNT and PFMERGE decode through convertSparseToDenseHll().
  EXPECT_THAT(Run({"pfcount", "overflow"}), ErrArg(kCorruptedHllError));

  EXPECT_EQ(CheckedInt({"pfadd", "src", "hi"}), 1);
  EXPECT_THAT(Run({"pfmerge", "dest", "overflow", "src"}), ErrArg(kCorruptedHllError));

  // PFADD decodes through hllSparseSet()'s promote path: the value is far above
  // HLL_SPARSE_MAX_BYTES, so the very first insert tries to convert to dense.
  EXPECT_THAT(Run({"pfadd", "overflow", "foo"}), ErrArg(kInvalidHllError));
}

// Covers the ZERO/XZERO arm of the same guard on an input small enough not to
// need integer overflow: an over-long run must be rejected, never truncated.
// (This case was already rejected before the fix, by the trailing
// `idx != HLL_REGISTERS` check.)
TEST_F(HllFamilyTest, CorruptedSparseTruncatedRun) {
  // XZERO covering 16384 registers followed by another one: the second overruns
  // the register space, and nothing else in the value can make up for it.
  string hll("HYLL", 4);
  hll.push_back(1);
  hll.append(3, '\0');
  hll.append(8, '\0');
  for (int i = 0; i < 2; ++i) {
    hll.push_back('\x7f');
    hll.push_back('\xff');
  }
  ASSERT_EQ(Run({"set", "truncated", hll}), "OK");
  EXPECT_THAT(Run({"pfcount", "truncated"}), ErrArg(kCorruptedHllError));
}

// PFCOUNT over several keys merges into a raw register array; it used to write
// that array HLL_HDR_SIZE bytes too low, so the estimate silently dropped the
// first 16 registers and counted 16 zeroed ones instead. The union estimate has
// to match the cardinality of the same keys merged with PFMERGE exactly.
TEST_F(HllFamilyTest, CountMultipleAgreesWithMerge) {
  constexpr int kValuesPerKey = 20000;
  for (int i = 0; i < kValuesPerKey; ++i) {
    Run({"pfadd", "k1", GenerateUniqueValue(i)});
    Run({"pfadd", "k2", GenerateUniqueValue(kValuesPerKey + i)});
  }

  ASSERT_EQ(Run({"pfmerge", "merged", "k1", "k2"}), "OK");
  const int64_t merged = CheckedInt({"pfcount", "merged"});
  EXPECT_EQ(CheckedInt({"pfcount", "k1", "k2"}), merged);

  // Sanity check that the shared estimate is in the right ballpark.
  EXPECT_LT(std::abs(merged - 2.0 * kValuesPerKey) / (2.0 * kValuesPerKey), 0.05);
}

// The AVX2 / NEON dense<->raw conversions must agree bit for bit with the scalar
// loops they replace. PFMERGE exercises both directions (hllMergeDense to unpack
// each input, hllDenseCompress to pack the result); PFCOUNT over several keys
// exercises the unpack direction on its own.
TEST_F(HllFamilyTest, SimdMatchesScalar) {
  if (!hllEnableSimd(1)) {
    GTEST_SKIP() << "no SIMD fast path on this CPU";
  }

  // Three patterns that between them put every value in 0..63 into every
  // position within the 4-registers-per-3-bytes packing group, and that differ
  // per input so the max() in the merge is exercised too.
  ASSERT_EQ(Run({"set", "s1", MakeDenseHll([](int i) -> unsigned { return i % 64; })}), "OK");
  ASSERT_EQ(Run({"set", "s2", MakeDenseHll([](int i) -> unsigned { return (i * 7 + 3) % 64; })}),
            "OK");
  ASSERT_EQ(Run({"set", "s3", MakeDenseHll([](int i) -> unsigned { return (i / 64) % 64; })}),
            "OK");

  ASSERT_EQ(Run({"pfmerge", "simd", "s1", "s2", "s3"}), "OK");
  const int64_t simd_count = CheckedInt({"pfcount", "s1", "s2", "s3"});
  const string simd_merged = Run({"get", "simd"}).GetString();

  ASSERT_EQ(hllEnableSimd(0), 0);
  ASSERT_EQ(Run({"pfmerge", "scalar", "s1", "s2", "s3"}), "OK");
  const int64_t scalar_count = CheckedInt({"pfcount", "s1", "s2", "s3"});
  const string scalar_merged = Run({"get", "scalar"}).GetString();
  hllEnableSimd(1);

  EXPECT_EQ(simd_count, scalar_count);
  EXPECT_EQ(simd_merged, scalar_merged);
}

// hllSparseSet() promotes straight to dense when the count cannot be held by a
// VAL opcode. The generated input has 35 zero bits after the 14-bit HLL
// register index, giving hllPatLen() a count of 36. It was generated with:
//   python3 murmur_invert.py --trailing-zeroes 35
TEST_F(HllFamilyTest, SparseSetPromotesOnLargeCount) {
  constexpr string_view kPromotingValue = ".K{bTLLX";

  EXPECT_EQ(CheckedInt({"pfadd", "key", kPromotingValue}), 1);
  EXPECT_EQ(CheckedInt({"strlen", "key"}), getDenseHllSize());
  EXPECT_EQ(CheckedInt({"pfcount", "key"}), 1);
}

// Benchmarks comparing the AVX2/NEON kernels against the scalar loops they
// replace. They call hyperloglog.c directly so that the numbers are not diluted
// by command parsing, the transaction framework or key lookups:
//
//   build-opt/hll_family_test --bench --gtest_filter=-'*' --benchmark_min_time=0.5s
//
// The gtest filter matters: without it the 21 tests run first. Use a release
// build -- at -O0 the scalar C loops are penalized far more than in production.
namespace {

constexpr int kRegisters = 16384;
constexpr size_t kDenseRegBytes = kRegisters * 6 / 8;

// Registers of a dense HLL built by MakeDenseHll(), i.e. its buffer past the
// header. hllMergeDense() takes this rather than the whole HLL.
const uint8_t* DenseRegisters(const string& hll) {
  return reinterpret_cast<const uint8_t*>(hll.data()) + (getDenseHllSize() - kDenseRegBytes);
}

// Register values spanning the full 0..63 range, so that the packing and
// unpacking of all six bits is timed. `seed` makes sources differ from each
// other, which keeps the max() in the merge from degenerating.
unsigned BenchRegister(int seed, int i) {
  return (i * 2654435761u + seed * 40503u) >> 26;
}

// Selects the scalar or the SIMD implementation for the duration of a
// benchmark, restoring the default afterwards. `state.range(0)` is the SIMD
// flag. The SIMD variants are skipped when the host CPU has neither AVX2 nor
// NEON, so that the two rows never both report the scalar timing.
class SimdSelector {
 public:
  explicit SimdSelector(benchmark::State& state) {
    const bool want_simd = state.range(0) != 0;
    if (want_simd && hllEnableSimd(1) == 0) {
      state.SkipWithMessage("no AVX2/NEON implementation on this CPU");
      skipped_ = true;
    } else if (!want_simd) {
      hllEnableSimd(0);
    }
  }

  ~SimdSelector() {
    hllEnableSimd(1);
  }

  bool skipped() const {
    return skipped_;
  }

 private:
  bool skipped_ = false;
};

// Dense HLLs to merge, plus the HllBufferPtr array pointing at them.
struct MergeInputs {
  explicit MergeInputs(int64_t count) {
    hlls.reserve(count);
    ptrs.reserve(count);
    for (int64_t i = 0; i < count; ++i)
      hlls.push_back(MakeDenseHll([i](int reg) { return BenchRegister(i + 1, reg); }));
    for (string& hll : hlls)
      ptrs.push_back({reinterpret_cast<unsigned char*>(hll.data()), hll.size()});
  }

  vector<string> hlls;
  vector<HllBufferPtr> ptrs;
};

}  // namespace

// Dense -> raw: the inner loop of both PFMERGE and a multi-key PFCOUNT.
static void BM_HllMergeDense(benchmark::State& state) {
  SimdSelector simd(state);
  if (simd.skipped())
    return;

  const string dense = MakeDenseHll([](int i) { return BenchRegister(1, i); });
  const uint8_t* regs = DenseRegisters(dense);
  vector<uint8_t> raw(kRegisters, 0);

  while (state.KeepRunning()) {
    hllMergeDense(raw.data(), regs);
  }
  state.SetItemsProcessed(state.iterations() * kRegisters);
}
BENCHMARK(BM_HllMergeDense)->ArgName("simd")->Arg(0)->Arg(1);

// Raw -> dense: PFMERGE's write-back step, run once per command.
//
// Read the simd:0 row as "the fallback on a CPU without AVX2/NEON", not as the
// cost of the work itself. HLL_DENSE_SET_REGISTER does four byte-wide
// read-modify-writes per register, and consecutive registers share bytes, so
// every load waits on the previous store to forward: the loop is one long
// dependency chain at ~20 cycles per register. BM_HllDenseCompressUnchained
// below packs the same bytes without that chain and is ~19x faster while still
// scalar, which is where most of the simd:1 speedup here actually comes from.
static void BM_HllDenseCompress(benchmark::State& state) {
  SimdSelector simd(state);
  if (simd.skipped())
    return;

  vector<uint8_t> raw(kRegisters);
  for (int i = 0; i < kRegisters; ++i)
    raw[i] = BenchRegister(1, i);

  // One byte of slack: writing the last register touches the byte just past the
  // register array, the same terminator MakeDenseHll() accounts for.
  vector<uint8_t> dense(kDenseRegBytes + 1, 0);

  while (state.KeepRunning()) {
    hllDenseCompress(dense.data(), raw.data());
  }
  state.SetItemsProcessed(state.iterations() * kRegisters);
}
BENCHMARK(BM_HllDenseCompress)->ArgName("simd")->Arg(0)->Arg(1);

// Scalar reference baseline, not used in production: emits the same bytes as
// hllDenseCompress() by building each output byte once instead of updating it
// twice, so no byte is read back after being written. Its only job is to keep
// the simd:1 number above honest -- comparing AVX2 against *this* isolates the
// vectorization win from the store-forwarding stall the macro loop pays.
static void BM_HllDenseCompressUnchained(benchmark::State& state) {
  vector<uint8_t> raw(kRegisters);
  for (int i = 0; i < kRegisters; ++i)
    raw[i] = BenchRegister(1, i);

  vector<uint8_t> dense(kDenseRegBytes + 1, 0);
  auto compress = [&] {
    // 4 registers pack into 3 bytes: {bbaaaaaa|ccccbbbb|ddddddcc}.
    for (int i = 0; i < kRegisters; i += 4) {
      const uint32_t a = raw[i], b = raw[i + 1], c = raw[i + 2], d = raw[i + 3];
      uint8_t* out = dense.data() + i * 6 / 8;
      out[0] = a | (b << 6);
      out[1] = (b >> 2) | (c << 4);
      out[2] = (c >> 4) | (d << 2);
    }
  };

  // Guard against the reference drifting from the real thing.
  vector<uint8_t> expected(kDenseRegBytes + 1, 0);
  hllDenseCompress(expected.data(), raw.data());
  compress();
  CHECK(memcmp(dense.data(), expected.data(), kDenseRegBytes) == 0);

  while (state.KeepRunning()) {
    compress();
  }
  state.SetItemsProcessed(state.iterations() * kRegisters);
}
BENCHMARK(BM_HllDenseCompressUnchained);

// PFMERGE end to end: one merge per source, then one compress.
static void BM_PfMerge(benchmark::State& state) {
  SimdSelector simd(state);
  if (simd.skipped())
    return;

  MergeInputs inputs(state.range(1));
  string out = MakeDenseHll([](int) -> unsigned { return 0; });
  const HllBufferPtr out_ptr{reinterpret_cast<unsigned char*>(out.data()), out.size()};

  while (state.KeepRunning()) {
    CHECK_EQ(pfmerge(inputs.ptrs.data(), inputs.ptrs.size(), out_ptr), 0);
  }
  state.SetItemsProcessed(state.iterations() * state.range(1) * kRegisters);
}
BENCHMARK(BM_PfMerge)->ArgNames({"simd", "sources"})->ArgsProduct({{0, 1}, {1, 8}});

// PFCOUNT over several keys: one merge per source, then the cardinality estimate.
static void BM_PfCountMulti(benchmark::State& state) {
  SimdSelector simd(state);
  if (simd.skipped())
    return;

  MergeInputs inputs(state.range(1));

  while (state.KeepRunning()) {
    benchmark::DoNotOptimize(pfcountMulti(inputs.ptrs.data(), inputs.ptrs.size()));
  }
  state.SetItemsProcessed(state.iterations() * state.range(1) * kRegisters);
}
BENCHMARK(BM_PfCountMulti)->ArgNames({"simd", "sources"})->ArgsProduct({{0, 1}, {1, 8}});

}  // namespace dfly
