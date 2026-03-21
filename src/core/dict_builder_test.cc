// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/dict_builder.h"

#include <gmock/gmock.h>
#include <zstd.h>

#include <random>
#include <string>
#include <vector>

#include "base/logging.h"

namespace dfly {

using namespace std;

class DictBuilderTest : public ::testing::Test {
 protected:
  using DataPiece = pair<const uint8_t*, size_t>;

  // Generate Celery-like JSON entries with small variations.
  vector<string> GenerateCeleryEntries(unsigned count) {
    vector<string> entries;
    entries.reserve(count);
    for (unsigned i = 0; i < count; ++i) {
      string id = to_string(100000 + i);
      string entry =
          "{\"body\": \"W10=\", \"content-encoding\": \"utf-8\", "
          "\"content-type\": \"application/json\", "
          "\"headers\": {\"lang\": \"py\", \"task\": \"process_job\", "
          "\"id\": \"b3e4b923-8a77-4053-aff0-" +
          id +
          "\", \"shadow\": null, \"eta\": null, "
          "\"expires\": null, \"group\": null, \"retries\": 0, "
          "\"timelimit\": [null, null], "
          "\"root_id\": \"b3e4b923-8a77-4053-aff0-" +
          id +
          "\", \"parent_id\": null, "
          "\"argsrepr\": \"('job" +
          to_string(i) +
          "',)\", \"kwargsrepr\": \"{}\", "
          "\"origin\": \"gen917779@hut\"}, "
          "\"properties\": {\"correlation_id\": \"b3e4b923\", "
          "\"reply_to\": \"9933040c\", \"delivery_mode\": 2, "
          "\"delivery_info\": {\"exchange\": \"\", \"routing_key\": \"my_queue\"}, "
          "\"priority\": 0}}";
      entries.push_back(std::move(entry));
    }
    return entries;
  }

  vector<DataPiece> ToPieces(const vector<string>& entries) {
    vector<DataPiece> pieces;
    pieces.reserve(entries.size());
    for (const auto& e : entries) {
      pieces.emplace_back(reinterpret_cast<const uint8_t*>(e.data()), e.size());
    }
    return pieces;
  }

  // Generate random binary data.
  vector<string> GenerateRandomEntries(unsigned count, size_t entry_size) {
    vector<string> entries;
    entries.reserve(count);
    mt19937 rng(42);
    for (unsigned i = 0; i < count; ++i) {
      string entry(entry_size, '\0');
      for (auto& c : entry) {
        c = static_cast<char>(rng() & 0xFF);
      }
      entries.push_back(std::move(entry));
    }
    return entries;
  }
};

TEST_F(DictBuilderTest, RepetitiveDataIsCompressible) {
  auto entries = GenerateCeleryEntries(200);
  auto pieces = ToPieces(entries);

  double ratio = EstimateCompressibility(pieces, 1);
  LOG(INFO) << "Celery data uniqueness ratio: " << ratio;
  EXPECT_LT(ratio, 0.5f);
}

TEST_F(DictBuilderTest, RandomDataIsIncompressible) {
  auto entries = GenerateRandomEntries(200, 400);
  auto pieces = ToPieces(entries);

  double ratio = EstimateCompressibility(pieces, 1);
  LOG(INFO) << "Random data uniqueness ratio: " << ratio;
  EXPECT_FALSE(ratio < 0.85);
}

TEST_F(DictBuilderTest, TrainDictionaryProducesOutput) {
  auto entries = GenerateCeleryEntries(200);
  auto pieces = ToPieces(entries);

  string dict = TrainDictionary(pieces, 4096, 256);
  LOG(INFO) << "Trained dictionary size: " << dict.size() << " bytes";
  EXPECT_GT(dict.size(), 0u);
  EXPECT_LE(dict.size(), 4096u);
}

TEST_F(DictBuilderTest, TrainDictionaryEmptyForTinyData) {
  // Single small entry - not enough for segment selection.
  string tiny = "hello";
  vector<DataPiece> pieces = {{reinterpret_cast<const uint8_t*>(tiny.data()), tiny.size()}};

  string dict = TrainDictionary(pieces, 4096, 256);
  EXPECT_TRUE(dict.empty());
}

TEST_F(DictBuilderTest, ZstdCompressionWithTrainedDict) {
  auto entries = GenerateCeleryEntries(200);
  auto pieces = ToPieces(entries);

  string dict = TrainDictionary(pieces, 4096, 256);
  ASSERT_GT(dict.size(), 0u);

  // Create ZSTD CDict/DDict from trained dictionary.
  ZSTD_CDict* cdict = ZSTD_createCDict(dict.data(), dict.size(), 1);
  ASSERT_TRUE(cdict);
  ZSTD_DDict* ddict = ZSTD_createDDict(dict.data(), dict.size());
  ASSERT_TRUE(ddict);

  ZSTD_CCtx* cctx = ZSTD_createCCtx();
  ZSTD_DCtx* dctx = ZSTD_createDCtx();

  size_t total_raw = 0;
  size_t total_compressed_dict = 0;
  size_t total_compressed_nodict = 0;

  for (const auto& entry : entries) {
    total_raw += entry.size();

    // Compress with dictionary.
    size_t bound = ZSTD_compressBound(entry.size());
    string compressed(bound, '\0');
    size_t csz =
        ZSTD_compress_usingCDict(cctx, compressed.data(), bound, entry.data(), entry.size(), cdict);
    ASSERT_FALSE(ZSTD_isError(csz)) << ZSTD_getErrorName(csz);
    compressed.resize(csz);
    total_compressed_dict += csz;

    // Compress without dictionary for comparison.
    string compressed_nodict(bound, '\0');
    size_t csz_nodict =
        ZSTD_compressCCtx(cctx, compressed_nodict.data(), bound, entry.data(), entry.size(), 1);
    ASSERT_FALSE(ZSTD_isError(csz_nodict));
    total_compressed_nodict += csz_nodict;

    // Verify roundtrip.
    string decompressed(entry.size(), '\0');
    size_t dsz = ZSTD_decompress_usingDDict(dctx, decompressed.data(), entry.size(),
                                            compressed.data(), csz, ddict);
    ASSERT_FALSE(ZSTD_isError(dsz)) << ZSTD_getErrorName(dsz);
    ASSERT_EQ(dsz, entry.size());
    EXPECT_EQ(decompressed, entry);
  }

  double ratio_dict = double(total_raw) / double(total_compressed_dict);
  double ratio_nodict = double(total_raw) / double(total_compressed_nodict);
  LOG(INFO) << "Total raw: " << total_raw << " bytes";
  LOG(INFO) << "With dict: " << total_compressed_dict << " bytes (ratio " << ratio_dict << "x)";
  LOG(INFO) << "No dict:   " << total_compressed_nodict << " bytes (ratio " << ratio_nodict << "x)";
  LOG(INFO) << "Dict advantage: " << ratio_dict / ratio_nodict << "x better";

  // Dictionary compression should be significantly better for repetitive data.
  EXPECT_GT(ratio_dict, ratio_nodict);
  EXPECT_GT(ratio_dict, 3.0f);  // Expect at least 3x compression with dict.

  ZSTD_freeCCtx(cctx);
  ZSTD_freeDCtx(dctx);
  ZSTD_freeCDict(cdict);
  ZSTD_freeDDict(ddict);
}

TEST_F(DictBuilderTest, StepParameterWorks) {
  auto entries = GenerateCeleryEntries(200);
  auto pieces = ToPieces(entries);

  double step1_ratio = EstimateCompressibility(pieces, 1);
  double step4_ratio = EstimateCompressibility(pieces, 4);

  // Both should detect compressibility, though with slightly different ratios.
  EXPECT_TRUE(step1_ratio < 0.85);
  EXPECT_TRUE(step4_ratio < 0.85);
  LOG(INFO) << "Step=1 ratio: " << step1_ratio << ", Step=4 ratio: " << step4_ratio;
}

}  // namespace dfly
