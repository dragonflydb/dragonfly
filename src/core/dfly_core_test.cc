// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/strings/charconv.h>
#include <absl/strings/numbers.h>
#include <fast_float/fast_float.h>
#include <reflex/matcher.h>

#include <random>

#include "base/gtest.h"
#include "base/logging.h"
#include "core/glob_matcher.h"
#include "core/intent_lock.h"
#include "core/tx_queue.h"

namespace dfly {

using namespace std;

std::random_device rd;

static string GetRandomHex(size_t len) {
  std::string res(len, '\0');
  size_t indx = 0;

  for (; indx < len; indx += 16) {  // 2 chars per byte
    absl::numbers_internal::FastHexToBufferZeroPad16(rd(), res.data() + indx);
  }

  if (indx < len) {
    char buf[24];
    absl::numbers_internal::FastHexToBufferZeroPad16(rd(), buf);

    for (unsigned j = 0; indx < len; indx++, j++) {
      res[indx] = buf[j];
    }
  }

  return res;
}

class TxQueueTest : public ::testing::Test {
 protected:
  TxQueueTest() {
  }

  uint64_t Pop() {
    if (pq_.Empty())
      return uint64_t(-1);
    TxQueue::ValueType val = pq_.Front();
    pq_.PopFront();

    return std::get<uint64_t>(val);
  }

  TxQueue pq_;
};

TEST_F(TxQueueTest, Basic) {
  pq_.Insert(4);
  pq_.Insert(3);
  pq_.Insert(2);

  unsigned cnt = 0;
  auto head = pq_.Head();
  auto it = head;
  do {
    ++cnt;
    it = pq_.Next(it);
  } while (it != head);
  EXPECT_EQ(3, cnt);

  ASSERT_EQ(2, Pop());
  ASSERT_EQ(3, Pop());
  ASSERT_EQ(4, Pop());
  ASSERT_TRUE(pq_.Empty());

  EXPECT_EQ(TxQueue::kEnd, pq_.Head());

  pq_.Insert(10);
  ASSERT_EQ(10, Pop());
}

class IntentLockTest : public ::testing::Test {
 protected:
  IntentLock lk_;
};

TEST_F(IntentLockTest, Basic) {
  ASSERT_TRUE(lk_.Acquire(IntentLock::SHARED));
  ASSERT_FALSE(lk_.Acquire(IntentLock::EXCLUSIVE));
  lk_.Release(IntentLock::EXCLUSIVE);

  ASSERT_FALSE(lk_.Check(IntentLock::EXCLUSIVE));
  lk_.Release(IntentLock::SHARED);
  ASSERT_TRUE(lk_.Check(IntentLock::EXCLUSIVE));
}

class StringMatchTest : public ::testing::Test {
 protected:
  // wrapper around stringmatchlen with stringview arguments
  int MatchLen(string_view pattern, string_view str, bool nocase) {
    GlobMatcher matcher(pattern, !nocase);
    return matcher.Matches(str);
  }
};

TEST_F(StringMatchTest, Basic) {
  EXPECT_EQ(MatchLen("", "", 0), 1);

  EXPECT_EQ(MatchLen("*", "", 0), 0);
  EXPECT_EQ(MatchLen("*", "", 1), 0);
  EXPECT_EQ(MatchLen("\\\\", "\\", 0), 1);
  EXPECT_EQ(MatchLen("h\\\\llo", "h\\llo", 0), 1);

  // ExactMatch
  EXPECT_EQ(MatchLen("hello", "hello", 0), 1);
  EXPECT_EQ(MatchLen("hello", "world", 0), 0);

  // Wildcards
  EXPECT_EQ(MatchLen("*", "hello", 0), 1);
  EXPECT_EQ(MatchLen("h*", "hello", 0), 1);
  EXPECT_EQ(MatchLen("h*", "abc", 0), 0);
  EXPECT_EQ(MatchLen("h*o", "hello", 0), 1);
  EXPECT_EQ(MatchLen("hel*o*", "hello*", 0), 1);
  EXPECT_EQ(MatchLen("h\\*llo", "h*llo", 0), 1);

  // Single character wildcard
  EXPECT_EQ(MatchLen("h[aeiou]llo", "hello", 0), 1);
  EXPECT_EQ(MatchLen("h[aeiou]llo", "hallo", 0), 1);
  EXPECT_EQ(MatchLen("h[^aeiou]llo", "hallo", 0), 0);
  EXPECT_EQ(MatchLen("h[a-z]llo", "hello", 0), 1);
  EXPECT_EQ(MatchLen("h[A-Z]llo", "HeLLO", 1), 1);
  EXPECT_EQ(MatchLen("[[]", "[", 0), 1);

  // ?
  EXPECT_EQ(MatchLen("h?llo", "hello", 0), 1);
  EXPECT_EQ(MatchLen("h??llo", "ha llo", 0), 1);
  EXPECT_EQ(MatchLen("h??llo", "hallo", 0), 0);
  EXPECT_EQ(MatchLen("h\\?llo", "hallo", 0), 0);
  EXPECT_EQ(MatchLen("h\\?llo", "h?llo", 0), 1);

  // special regex chars
  EXPECT_EQ(MatchLen("h\\[^|", "h[^|", 0), 1);
  EXPECT_EQ(MatchLen("[^", "[^", 0), 0);
  EXPECT_EQ(MatchLen("[$?^]a", "?a", 0), 1);
}

using benchmark::DoNotOptimize;

// Parse Double benchmarks
static void BM_ParseFastFloat(benchmark::State& state) {
  std::vector<std::string> args(100);
  std::random_device rd;

  for (auto& arg : args) {
    arg = std::to_string(std::uniform_real_distribution<double>(0, 1e5)(rd));
  }
  double res;
  while (state.KeepRunning()) {
    for (const auto& arg : args) {
      fast_float::from_chars(arg.data(), arg.data() + arg.size(), res);
    }
  }
}
BENCHMARK(BM_ParseFastFloat);

static void BM_ParseDoubleAbsl(benchmark::State& state) {
  std::vector<std::string> args(100);

  for (auto& arg : args) {
    arg = std::to_string(std::uniform_real_distribution<double>(0, 1e5)(rd));
  }

  double res;
  while (state.KeepRunning()) {
    for (const auto& arg : args) {
      absl::from_chars(arg.data(), arg.data() + arg.size(), res);
    }
  }
}
BENCHMARK(BM_ParseDoubleAbsl);

static void BM_MatchGlob(benchmark::State& state) {
  string random_val = GetRandomHex(state.range(0));
  GlobMatcher matcher("*foobar*", true);
  while (state.KeepRunning()) {
    DoNotOptimize(matcher.Matches(random_val));
  }
}
BENCHMARK(BM_MatchGlob)->Arg(1000)->Arg(10000);

static void BM_MatchFindSubstr(benchmark::State& state) {
  string random_val = GetRandomHex(state.range(0));

  while (state.KeepRunning()) {
    DoNotOptimize(random_val.find("foobar"));
  }
}
BENCHMARK(BM_MatchFindSubstr)->Arg(1000)->Arg(10000);

static void BM_MatchReflexFind(benchmark::State& state) {
  string random_val = GetRandomHex(state.range(0));
  reflex::Matcher matcher("foobar");
  while (state.KeepRunning()) {
    matcher.input(random_val);
    DoNotOptimize(matcher.find());
  }
}
BENCHMARK(BM_MatchReflexFind)->Arg(1000)->Arg(10000);

static void BM_MatchReflexFindStar(benchmark::State& state) {
  string random_val = GetRandomHex(state.range(0));
  reflex::Matcher matcher(".*foobar");

  while (state.KeepRunning()) {
    matcher.input(random_val);
    DoNotOptimize(matcher.find());
  }
}
BENCHMARK(BM_MatchReflexFindStar)->Arg(1000)->Arg(10000);

}  // namespace dfly
