// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <random>
#include <unordered_set>
#include <chrono>

#include "base/gtest.h"
#include <gperftools/profiler.h> 

#include "core/heavy_keeper.h"

namespace dfly {

class HeavyKeeperTest : public ::testing::Test {
 protected:
  void SetUp() final  {
    hk_ = std::make_unique<HeavyKeeper>(hotkey_lens);
  }

  void TearDown() final {}

  static constexpr int hotkey_lens = 30;
  std::unique_ptr<HeavyKeeper> hk_;
};

std::random_device rd;
std::mt19937 gen(rd());
 
int random(int low, int high) {
    std::uniform_int_distribution<> dist(low, high);
    return dist(gen);
}

TEST_F(HeavyKeeperTest, Basic) {
  size_t m = 100000;
  hk_->Clear();
  auto changeValue  = [](size_t para) -> string {
      return to_string(para) + "df";
  };

  int sum = 0;
  map<string ,int> B,C;
  for (size_t i = 0; i <= m; i++) {
      B[changeValue(i)]++;
      sum++;
  }

  for (size_t i = 0; i < hotkey_lens; i++) {
      size_t s = random(1, m);
      // Simulation of hotkey.
      for (size_t j=0; j<=10000 + s; j++) {
          B[changeValue(s)]++;
          sum++;
      }
  }
  //ProfilerStart("HeavyKeeperTest.prof");
  for (const auto& item : B) {
    uint32_t num = item.second;
    for (size_t i = 0; i < num; i++) {
      hk_->Insert(item.first);
    }
  }
  //ProfilerStop();

  hk_->SortHotKey();

  int cnt=0;
  struct node {string x; int y;} p[B.size()];
  for (auto itera = B.begin(); itera != B.end(); itera++) {   
      p[cnt].x = itera->first;
      p[cnt].y = itera->second;
      cnt++; 
  }

  auto cmp = [](node lhs, node rhs) {return lhs.y > rhs.y;};
  sort(p, p+cnt, cmp);

  // The actual recorded hotkey_lens hotkeys.
  for (int i = 0; i < min(static_cast<int>(B.size()), hotkey_lens); i++) {
      C[p[i].x]=p[i].y;
  }

  // Calculating PRE, ARE, AAE
  int hk_sum=0, hk_AAE=0;
  double hk_ARE=0;
  string hk_string; 
  int hk_num;

  for (int i = 0; i < min(static_cast<int>(B.size()), hotkey_lens); i++) {
      hk_string = (hk_->Query(i))->first; 
      hk_num = static_cast<int>((hk_->Query(i))->second);
      hk_AAE += abs(B[hk_string] - hk_num); 
      hk_ARE += abs(B[hk_string] - hk_num)/(B[hk_string] + 0.0);
      // The actual hotkey identified
      if (C[hk_string]){
        hk_sum++;
      }
  }

  // HeavyKeeper is a probabilistic data structure, and for the reliability
  // of the single test pass, we mitigate the constraints appropriately.
  EXPECT_GE(hk_sum, hotkey_lens-1);

  printf("HeavyKeeper accepted: %d/%d  %.10f\nARE: %.10f\nAAE: %.10d\n", 
    hk_sum, hotkey_lens, (hk_sum/(hotkey_lens+0.0)), hk_ARE, hk_AAE);
}

TEST_F(HeavyKeeperTest, HotkeyEliminationTest) {
  hk_->Clear();
  unordered_set<string> B;
  auto changeValue  = [](size_t para) -> string {
      return to_string(para) + "dragonfly20001030";
  };

  for (size_t i = 0; i < 200; i++) {
    for (size_t j = 0; j < 10000+i; j++) {
      hk_->Insert(changeValue(i));
    }
  }
  hk_->SortHotKey();
  
  for (size_t i = 200-hotkey_lens; i < 200; i++) {
    B.insert(changeValue(i));
  }

  int hk_sum = 0;
  string hk_string; 
  for (int i = 0; i < min(static_cast<int>(B.size()), hotkey_lens); i++) {
      hk_string = (hk_->Query(i))->first; 
      if (B.count(hk_string)){
        hk_sum++;
      }
  }
  EXPECT_EQ(hk_sum, hotkey_lens);
}

static void HK_Insert(benchmark::State& state) {
  unsigned count = state.range(0);
  
  while (state.KeepRunning()) {
    HeavyKeeper dt(100);

    for (unsigned i = 0; i < count; ++i) {
      dt.Insert(std::to_string(i));
    }
  }
}
BENCHMARK(HK_Insert)->Arg(10000)->Arg(100000)->Arg(1000000);

}