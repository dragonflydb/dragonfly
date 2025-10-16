// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/flags/flag.h>
#include <gtest/gtest.h>

#include <chrono>

#include "base/gtest.h"
#include "facade/facade_test.h"
#include "server/search/search_family.h"
#include "server/test_utils.h"

ABSL_DECLARE_FLAG(bool, enable_global_vector_search);

using namespace testing;
using namespace std;
using namespace facade;

namespace dfly {

class PerformanceTest : public BaseFamilyTest {
 protected:
  void SetUp() override {
    BaseFamilyTest::SetUp();
  }

  std::string FloatsToBytes(const std::vector<float>& floats) {
    return std::string(reinterpret_cast<const char*>(floats.data()), floats.size() * sizeof(float));
  }

  auto TimeIt(std::function<void()> func) {
    auto start = std::chrono::high_resolution_clock::now();
    func();
    auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  }
};

TEST_F(PerformanceTest, GlobalVsShardBasedComparison) {
  // Create index with many documents
  Run({"FT.CREATE", "perf_idx", "ON", "HASH", "SCHEMA", "vec", "VECTOR", "FLAT", "6", "TYPE",
       "FLOAT32", "DIM", "128", "DISTANCE_METRIC", "L2"});

  // Add 10K documents to see global index benefits
  const size_t num_docs = 10000;
  for (size_t i = 0; i < num_docs; ++i) {
    std::vector<float> vec(128);
    for (size_t j = 0; j < 128; ++j) {
      vec[j] = static_cast<float>(i + j) / 1000.0f;
    }

    std::string key = "doc:" + std::to_string(i);
    Run({"HSET", key, "vec", FloatsToBytes(vec)});
  }

  std::vector<float> query_vec(128, 0.5f);
  std::string query_bytes = FloatsToBytes(query_vec);

  // Test 1: NOCONTENT query (should be fastest with global index)
  std::cout << "\n=== NOCONTENT Query Performance ===" << std::endl;

  // Shard-based search
  absl::SetFlag(&FLAGS_enable_global_vector_search, false);
  auto shard_time = TimeIt([&]() {
    Run({"FT.SEARCH", "perf_idx", "*=>[KNN 50 @vec $qvec]", "NOCONTENT", "PARAMS", "2", "qvec",
         query_bytes});
  });

  // Global search
  absl::SetFlag(&FLAGS_enable_global_vector_search, true);
  auto global_time = TimeIt([&]() {
    Run({"FT.SEARCH", "perf_idx", "*=>[KNN 50 @vec $qvec]", "NOCONTENT", "PARAMS", "2", "qvec",
         query_bytes});
  });

  std::cout << "Shard-based NOCONTENT: " << shard_time << " μs" << std::endl;
  std::cout << "Global NOCONTENT: " << global_time << " μs" << std::endl;
  std::cout << "Speedup: " << (double)shard_time / global_time << "x" << std::endl;

  // Test 2: Full query with fields
  std::cout << "\n=== Full Query Performance ===" << std::endl;

  absl::SetFlag(&FLAGS_enable_global_vector_search, false);
  auto shard_full_time = TimeIt([&]() {
    Run({"FT.SEARCH", "perf_idx", "*=>[KNN 50 @vec $qvec]", "PARAMS", "2", "qvec", query_bytes});
  });

  absl::SetFlag(&FLAGS_enable_global_vector_search, true);
  auto global_full_time = TimeIt([&]() {
    Run({"FT.SEARCH", "perf_idx", "*=>[KNN 50 @vec $qvec]", "PARAMS", "2", "qvec", query_bytes});
  });

  std::cout << "Shard-based full: " << shard_full_time << " μs" << std::endl;
  std::cout << "Global full: " << global_full_time << " μs" << std::endl;
  std::cout << "Speedup: " << (double)shard_full_time / global_full_time << "x" << std::endl;

  // Test 3: With SORTBY
  std::cout << "\n=== SORTBY Query Performance ===" << std::endl;

  absl::SetFlag(&FLAGS_enable_global_vector_search, false);
  auto shard_sortby_time = TimeIt([&]() {
    Run({"FT.SEARCH", "perf_idx", "*=>[KNN 50 @vec $qvec]", "SORTBY", "__vector_score", "PARAMS",
         "2", "qvec", query_bytes});
  });

  absl::SetFlag(&FLAGS_enable_global_vector_search, true);
  auto global_sortby_time = TimeIt([&]() {
    Run({"FT.SEARCH", "perf_idx", "*=>[KNN 50 @vec $qvec]", "SORTBY", "__vector_score", "PARAMS",
         "2", "qvec", query_bytes});
  });

  std::cout << "Shard-based SORTBY: " << shard_sortby_time << " μs" << std::endl;
  std::cout << "Global SORTBY: " << global_sortby_time << " μs" << std::endl;
  std::cout << "Speedup: " << (double)shard_sortby_time / global_sortby_time << "x" << std::endl;

  // Reset flag
  absl::SetFlag(&FLAGS_enable_global_vector_search, false);
}

}  // namespace dfly
