// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/base/macros.h>
#include <gmock/gmock.h>
#include <zstd.h>

#include <random>

#include "base/logging.h"

namespace dfly {

using namespace std;

constexpr unsigned kLevel = 1;

class ZStdTest : public ::testing::Test {
 protected:
  string Compress(const string& src, const ZSTD_CDict* cdict) {
    ZSTD_CCtx* cctx = ZSTD_createCCtx();
    size_t c_buffer_size = ZSTD_compressBound(src.size());
    string res(c_buffer_size, '\0');
    size_t compressed_size =
        ZSTD_compress_usingCDict(cctx, res.data(), c_buffer_size, src.c_str(), src.size(), cdict);

    ZSTD_freeCCtx(cctx);
    res.resize(compressed_size);
    return res;
  }

  string Decompress(const string& src, const ZSTD_DDict* ddict, size_t decompressed_size) {
    string res(decompressed_size, '\0');
    ZSTD_DCtx* dctx = ZSTD_createDCtx();
    size_t decompressed_size_actual = ZSTD_decompress_usingDDict(
        dctx, res.data(), decompressed_size, src.c_str(), src.size(), ddict);
    CHECK_EQ(decompressed_size, decompressed_size_actual);
    ZSTD_freeDCtx(dctx);
    return res;
  }

  string CompressNoDict(const string& src) {
    ZSTD_CCtx* cctx = ZSTD_createCCtx();
    size_t c_buffer_size = ZSTD_compressBound(src.size());
    string res(c_buffer_size, '\0');
    size_t compressed_size =
        ZSTD_compressCCtx(cctx, res.data(), c_buffer_size, src.c_str(), src.size(), kLevel);
    ZSTD_freeCCtx(cctx);
    res.resize(compressed_size);
    return res;
  }
};

// Dictionary works well for small messages where we do not have enough data to reference
// previous stream to have significant savings.
// For large messages, it may not be less beneficial.
TEST_F(ZStdTest, Dict) {
  const char* kRandomPieces[] = {"ABCD", "EFGH", "IJKL", "MNOP", "QRST", "UVWX", "YZAB", "CDEF"};
  string dict_source;
  random_device rd;

  for (unsigned i = 0; i < 1000; ++i) {
    dict_source += kRandomPieces[rd() % ABSL_ARRAYSIZE(kRandomPieces)];
  }
  LOG(INFO) << "Creating CDICT from " << dict_source.size() << " bytes of random data";
  ZSTD_CDict* cdict = ZSTD_createCDict(dict_source.data(), dict_source.size(), 7);
  ASSERT_TRUE(cdict);
  size_t actual_dict_size = ZSTD_sizeof_CDict(cdict);
  LOG(INFO) << "ZSTD_CDict created, size: " << actual_dict_size << " bytes";

  ZSTD_DDict* ddict = ZSTD_createDDict(dict_source.data(), dict_source.size());
  ASSERT_TRUE(ddict);
  size_t actual_ddict_size = ZSTD_sizeof_DDict(ddict);
  LOG(INFO) << "ZSTD_DDict created, size: " << actual_ddict_size << " bytes";

  // 3. Data to compress
  std::string data_to_compress;
  for (unsigned j = 0; j < 30; ++j) {
    data_to_compress += kRandomPieces[rd() % ABSL_ARRAYSIZE(kRandomPieces)];
  }
  size_t data_to_compress_size = data_to_compress.size();

  // 4. Compress data
  string compressed = Compress(data_to_compress, cdict);

  LOG(INFO) << "Compressed data size: " << compressed.size() << " bytes vs "
            << data_to_compress_size << " bytes of original data";

  string compress_no_dict = CompressNoDict(data_to_compress);
  LOG(INFO) << "Compressed data size without dict: " << compress_no_dict.size() << " bytes";

  // 5. Decompress data
  string decompressed = Decompress(compressed, ddict, data_to_compress_size);
  ASSERT_EQ(data_to_compress, decompressed);

  // 7. Free memory
  ZSTD_freeCDict(cdict);
  ZSTD_freeDDict(ddict);
}

}  // namespace dfly