// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/strings/charconv.h>
#include <absl/strings/numbers.h>
#include <fast_float/fast_float.h>

#ifdef USE_PCRE2
#define PCRE2_CODE_UNIT_WIDTH 8
#include <pcre2.h>
#endif

#ifdef USE_RE2
#include <re2/re2.h>
#endif

#include <reflex/matcher.h>

#include <random>
#include <regex>

#include "base/gtest.h"
#include "base/logging.h"
#include "core/glob_matcher.h"
#include "core/huff_coder.h"
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

extern int stringmatchlen(const char* pattern, int patternLen, const char* string, int stringLen,
                          int nocase);

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
  bool MatchLen(string_view pattern, string_view str, bool nocase) {
    GlobMatcher matcher(pattern, !nocase);
    return matcher.Matches(str);
  }
};

TEST_F(StringMatchTest, Glob2Regex) {
  EXPECT_EQ(GlobMatcher::Glob2Regex(""), "");
  EXPECT_EQ(GlobMatcher::Glob2Regex("*"), ".*");
  EXPECT_EQ(GlobMatcher::Glob2Regex("\\*"), "\\*");
  EXPECT_EQ(GlobMatcher::Glob2Regex("\\?"), "\\?");
  EXPECT_EQ(GlobMatcher::Glob2Regex("[abc]"), "[abc]");
  EXPECT_EQ(GlobMatcher::Glob2Regex("[^abc]"), "[^abc]");
  EXPECT_EQ(GlobMatcher::Glob2Regex("h\\[^|"), "h\\[\\^\\|");
  EXPECT_EQ(GlobMatcher::Glob2Regex("[$?^]a"), "[$?^]a");
  EXPECT_EQ(GlobMatcher::Glob2Regex("[^]a"), ".a");
  EXPECT_EQ(GlobMatcher::Glob2Regex("[]a"), "[]a");
  EXPECT_EQ(GlobMatcher::Glob2Regex("\\d"), "d");
  EXPECT_EQ(GlobMatcher::Glob2Regex("[\\d]"), "[\\\\d]");
  EXPECT_EQ(GlobMatcher::Glob2Regex("abc\\"), "abc\\\\");
  EXPECT_EQ(GlobMatcher::Glob2Regex("[\\]]"), "[\\]]");
  reflex::Matcher matcher("abc[\\\\d]e");
  matcher.input("abcde");
  ASSERT_TRUE(matcher.find());
}

TEST_F(StringMatchTest, Basic) {
  EXPECT_EQ(MatchLen("", "", 0), 1);

  EXPECT_EQ(MatchLen("*", "", 0), 0);
  EXPECT_EQ(MatchLen("*", "", 1), 0);
  EXPECT_EQ(MatchLen("\\\\", "\\", 0), 1);
  EXPECT_EQ(MatchLen("h\\\\llo", "h\\llo", 0), 1);
  EXPECT_EQ(MatchLen("a\\bc", "ABC", 1), 1);

  // ExactMatch
  EXPECT_EQ(MatchLen("hello", "hello", 0), 1);
  EXPECT_EQ(MatchLen("hello", "world", 0), 0);

  // Wildcards
  EXPECT_EQ(MatchLen("*", "hello", 0), 1);
  EXPECT_EQ(MatchLen("*", "1234567890123456", 0), 1);
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
  EXPECT_EQ(MatchLen("[^]a", "xa", 0), 1);

  // ?
  EXPECT_EQ(MatchLen("h?llo", "hello", 0), 1);
  EXPECT_EQ(MatchLen("h??llo", "ha llo", 0), 1);
  EXPECT_EQ(MatchLen("h??llo", "hallo", 0), 0);
  EXPECT_EQ(MatchLen("h\\?llo", "hallo", 0), 0);
  EXPECT_EQ(MatchLen("h\\?llo", "h?llo", 0), 1);
  EXPECT_EQ(MatchLen("abc?", "abc\n", 0), 1);
}

#define TEST_STRINGMATCH(pattern, str, case_res, nocase_res) \
  {                                                          \
    EXPECT_EQ(int(MatchLen(pattern, str, 0)), case_res);     \
    EXPECT_EQ(int(MatchLen(pattern, str, 1)), nocase_res);   \
  }

TEST_F(StringMatchTest, Special) {
  EXPECT_TRUE(MatchLen("h\\[^|", "h[^|", 0));
  EXPECT_FALSE(MatchLen("[^", "[^", 0));
  EXPECT_TRUE(MatchLen("[$?^]a", "?a", 0));
  EXPECT_TRUE(MatchLen("abc[\\d]e", "abcde", 0));
  EXPECT_TRUE(MatchLen("foo\\", "foo\\", 0));

  /* Case sensitivity: */
  TEST_STRINGMATCH("a", "a", 1, 1);
  TEST_STRINGMATCH("a", "A", 0, 1);
  TEST_STRINGMATCH("A", "A", 1, 1);
  TEST_STRINGMATCH("A", "a", 0, 1);
  TEST_STRINGMATCH("\\a", "a", 1, 1);
  TEST_STRINGMATCH("\\a", "A", 0, 1);
  TEST_STRINGMATCH("\\A", "A", 1, 1);
  TEST_STRINGMATCH("\\A", "a", 0, 1);
  TEST_STRINGMATCH("[\\a]", "a", 1, 1);

  // TODO: to fix this: TEST_STRINGMATCH("[\\a]", "A", 0, 1);
  TEST_STRINGMATCH("[\\A]", "A", 1, 1);
  // TODO: to fix this: TEST_STRINGMATCH("[\\A]", "a", 0, 1);

  /* Escaped metacharacters: */
  TEST_STRINGMATCH("\\*", "*", 1, 1);
  TEST_STRINGMATCH("\\?", "?", 1, 1);
  TEST_STRINGMATCH("\\\\", "\\", 1, 1);
  TEST_STRINGMATCH("\\[", "[", 1, 1);
  TEST_STRINGMATCH("\\]", "]", 1, 1);
  TEST_STRINGMATCH("\\^", "^", 1, 1);
  TEST_STRINGMATCH("\\-", "-", 1, 1);
  TEST_STRINGMATCH("[\\*]", "*", 1, 1);
  TEST_STRINGMATCH("[\\?]", "?", 1, 1);
  TEST_STRINGMATCH("[\\\\]", "\\", 1, 1);
  TEST_STRINGMATCH("[\\[]", "[", 1, 1);
  TEST_STRINGMATCH("[\\]]", "]", 1, 1);
  TEST_STRINGMATCH("[\\^]", "^", 1, 1);
  TEST_STRINGMATCH("[\\-]", "-", 1, 1);

  /* Not special outside character classes: */
  TEST_STRINGMATCH("]", "]", 1, 1);
  TEST_STRINGMATCH("^", "^", 1, 1);
  TEST_STRINGMATCH("-", "-", 1, 1);
  /* Not special inside character classes: */
  TEST_STRINGMATCH("[*]", "*", 1, 1);
  TEST_STRINGMATCH("[?]", "?", 1, 1);
  TEST_STRINGMATCH("[[]", "[", 1, 1);
  /* Not special as the first character in a character class: */
  TEST_STRINGMATCH("[-]", "-", 1, 1);

  /* Not special as range end (undocumented): */
  TEST_STRINGMATCH("[+-]]", "*", 0, 0); /*   but not * (below) */
  TEST_STRINGMATCH("[+-]]", "^", 0, 0); /*   or ^ (above) */
  TEST_STRINGMATCH("[+--]", ",", 1, 1); /* ASCII range + to - includes , */
  TEST_STRINGMATCH("[+--]", "*", 0, 0); /*   but not * (below) */
  TEST_STRINGMATCH("[+--]", ".", 0, 0); /*   or . (above) */

  /* And the same, but unclosed: */
  TEST_STRINGMATCH("[+-]", "*", 0, 0);
  TEST_STRINGMATCH("[+-]", "^", 0, 0);
  TEST_STRINGMATCH("[+--", ",", 1, 1);
  TEST_STRINGMATCH("[+--", "*", 0, 0);
  TEST_STRINGMATCH("[+--", ".", 0, 0);

  /* Escaped ] alone is literal: */
  TEST_STRINGMATCH("[\\]a]", "]", 1, 1);
  TEST_STRINGMATCH("[\\]a]", "a", 1, 1);

  /* Escapes at range end: */
  TEST_STRINGMATCH("[+-\\\\]", ",", 1, 1); /* ASCII range + to \ includes , */
  TEST_STRINGMATCH("[+-\\\\]", "*", 0, 0); /*   but not * (below) */
  TEST_STRINGMATCH("[+-\\]]", "*", 0, 0);  /*   but not * (below) */
  TEST_STRINGMATCH("[+-\\]]", "^", 0, 0);  /*   or ^ (above) */

  /* Unclosed is the same: */
  TEST_STRINGMATCH("[+-\\\\", ",", 1, 1);
  TEST_STRINGMATCH("[+-\\\\", "*", 0, 0);
  TEST_STRINGMATCH("[+-\\\\", "]", 0, 0);
  TEST_STRINGMATCH("[+-\\]", ",", 1, 1);
  TEST_STRINGMATCH("[+-\\]", "*", 0, 0);
  TEST_STRINGMATCH("[+-\\]", "^", 0, 0);
  /* An incomplete escape is treated as literal backslash: */
  TEST_STRINGMATCH("[+-\\", ",", 1, 1);
  TEST_STRINGMATCH("[+-\\", "*", 0, 0);
  TEST_STRINGMATCH("[+-\\", "]", 0, 0);

  /* Empty character class matches nothing: */
  TEST_STRINGMATCH("[]", "", 0, 0);
  TEST_STRINGMATCH("[]", "a", 0, 0);
  TEST_STRINGMATCH("[", "", 0, 0); /* Unclosed is the same */
  TEST_STRINGMATCH("[", "a", 0, 0);

  /* Empty negated character class is equivalent to pattern "?": */
  TEST_STRINGMATCH("[^]", "", 0, 0);
  TEST_STRINGMATCH("[^]", "a", 1, 1);
  TEST_STRINGMATCH("[^]", "ab", 0, 0);
  TEST_STRINGMATCH("[^", "", 0, 0); /* Unclosed is the same */
  TEST_STRINGMATCH("[^", "a", 1, 1);
  TEST_STRINGMATCH("[^", "ab", 0, 0);

  /* Unclosed character classes are not an error (undocumented): */
  TEST_STRINGMATCH("[A-", "B", 0, 0);
}

class HuffCoderTest : public ::testing::Test {
 protected:
  HuffmanEncoder encoder_;
  HuffmanDecoder decoder_;
  string error_msg_;
  const string_view good_table_{
      "\x1b\x10\xd8\n\n\x19\xc6\x0c\xc3\x30\x0c\x43\x1e\x93\xe4\x11roB\xf6\xde\xbb\x18V\xc2Zk\x03"sv};
};

TEST_F(HuffCoderTest, Load) {
  string data("bad");

  ASSERT_FALSE(encoder_.Load(data, &error_msg_));

  data = good_table_;
  ASSERT_TRUE(encoder_.Load(data, &error_msg_)) << error_msg_;

  data.append("foo");
  encoder_.Reset();
  ASSERT_FALSE(encoder_.Load(data, &error_msg_));
}

TEST_F(HuffCoderTest, Encode) {
  ASSERT_TRUE(encoder_.Load(good_table_, &error_msg_)) << error_msg_;

  EXPECT_EQ(1, encoder_.GetNBits('x'));
  EXPECT_EQ(3, encoder_.GetNBits(':'));
  EXPECT_EQ(5, encoder_.GetNBits('2'));
  EXPECT_EQ(5, encoder_.GetNBits('3'));

  string data("x:23xx");

  array<uint8_t, 100> dest;
  uint32_t dest_size = dest.size();
  ASSERT_TRUE(encoder_.Encode(data, dest.data(), &dest_size, &error_msg_));
  ASSERT_EQ(3, dest_size);

  // testing small destination buffer.
  data = "3333333333333333333";
  dest_size = 16;
  EXPECT_TRUE(encoder_.Encode(data, dest.data(), &dest_size, &error_msg_));

  // destination too small
  ASSERT_EQ(0, dest_size);
  ASSERT_EQ("", error_msg_);
}

TEST_F(HuffCoderTest, Decode) {
  array<unsigned, 256> hist;
  hist.fill(1);
  hist['a'] = 100;
  hist['b'] = 50;

  ASSERT_TRUE(encoder_.Build(hist.data(), hist.size() - 1, &error_msg_));
  string data("aab");

  array<uint8_t, 100> encoded{0};
  uint32_t encoded_size = encoded.size();
  ASSERT_TRUE(encoder_.Encode(data, encoded.data(), &encoded_size, &error_msg_));
  ASSERT_EQ(1, encoded_size);

  EXPECT_EQ(2, encoder_.GetNBits('a'));
  EXPECT_EQ(3, encoder_.GetNBits('b'));

  string bindata = encoder_.Export();
  ASSERT_TRUE(decoder_.Load(bindata, &error_msg_)) << error_msg_;

  const char* src_ptr = reinterpret_cast<const char*>(encoded.data());
  array<char, 100> decode_dest{0};
  size_t decoded_size = data.size();
  ASSERT_TRUE(decoder_.Decode({src_ptr, encoded_size}, decoded_size, decode_dest.data()));
  ASSERT_EQ("aab", string_view(decode_dest.data(), decoded_size));
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

template <clockid_t cid> void BM_ClockType(benchmark::State& state) {
  timespec ts;
  while (state.KeepRunning()) {
    DoNotOptimize(clock_gettime(cid, &ts));
  }
}

BENCHMARK_TEMPLATE(BM_ClockType, CLOCK_REALTIME);
BENCHMARK_TEMPLATE(BM_ClockType, CLOCK_MONOTONIC);
BENCHMARK_TEMPLATE(BM_ClockType, CLOCK_PROCESS_CPUTIME_ID);
BENCHMARK_TEMPLATE(BM_ClockType, CLOCK_THREAD_CPUTIME_ID);

// These clocks are not available on apple platform
#if !defined(__APPLE__)
BENCHMARK_TEMPLATE(BM_ClockType, CLOCK_REALTIME_COARSE);
BENCHMARK_TEMPLATE(BM_ClockType, CLOCK_MONOTONIC_COARSE);
BENCHMARK_TEMPLATE(BM_ClockType, CLOCK_BOOTTIME);
BENCHMARK_TEMPLATE(BM_ClockType, CLOCK_BOOTTIME_ALARM);
#endif

static void BM_MatchGlob(benchmark::State& state) {
  string random_val = GetRandomHex(state.range(0));
  GlobMatcher matcher("*foobar*", true);
  while (state.KeepRunning()) {
    DoNotOptimize(matcher.Matches(random_val));
  }
}
BENCHMARK(BM_MatchGlob)->Arg(32)->Arg(1000)->Arg(10000);

static void BM_MatchGlob2(benchmark::State& state) {
  string random_val = GetRandomHex(state.range(0));
  GlobMatcher matcher("bull:*:meta", true);
  while (state.KeepRunning()) {
    DoNotOptimize(matcher.Matches(random_val));
  }
}
BENCHMARK(BM_MatchGlob2)->Arg(32)->Arg(1000)->Arg(10000);

// See https://nvd.nist.gov/vuln/detail/cve-2022-36021
static void BM_MatchGlobExp(benchmark::State& state) {
  GlobMatcher matcher("a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*b", true);
  while (state.KeepRunning()) {
    DoNotOptimize(matcher.Matches("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
  }
}
BENCHMARK(BM_MatchGlobExp);

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

static void BM_MatchStd(benchmark::State& state) {
  string random_val = GetRandomHex(state.range(0));
  std::regex regex(".*foobar");
  std::match_results<std::string::const_iterator> results;
  while (state.KeepRunning()) {
    std::regex_match(random_val, results, regex);
  }
}
BENCHMARK(BM_MatchStd)->Arg(1000)->Arg(10000);

static void BM_MatchRedisGlob(benchmark::State& state) {
  string random_val = GetRandomHex(state.range(0));
  const char* pattern = "*foobar*";
  while (state.KeepRunning()) {
    DoNotOptimize(
        stringmatchlen(pattern, strlen(pattern), random_val.c_str(), random_val.size(), 0));
  }
}
BENCHMARK(BM_MatchRedisGlob)->Arg(1000)->Arg(10000);

static void BM_MatchRedisGlob2(benchmark::State& state) {
  string random_val = GetRandomHex(state.range(0));
  const char* pattern = "bull:*:meta";
  while (state.KeepRunning()) {
    DoNotOptimize(
        stringmatchlen(pattern, strlen(pattern), random_val.c_str(), random_val.size(), 0));
  }
}
BENCHMARK(BM_MatchRedisGlob2)->Arg(32)->Arg(1000)->Arg(10000);

static void BM_MatchData(benchmark::State& state) {
  vector<string> keys(5000);
  for (unsigned i = 0; i < keys.size(); ++i) {
    keys[i] = GetRandomHex(80);
  }
  string_view pattern =
      "*2addb1c3-eae5-5265-ac8e-9fc9106dda8d*77de68daecd823babbb58edb1c8e14d7106e83bb"sv;
  if (state.range(0) == 1) {
    GlobMatcher matcher(pattern, true);
    while (state.KeepRunning()) {
      for (const auto& key : keys) {
        DoNotOptimize(matcher.Matches(key));
      }
    }
  } else {
    while (state.KeepRunning()) {
      for (const auto& key : keys) {
        DoNotOptimize(stringmatchlen(pattern.data(), pattern.size(), key.c_str(), key.size(), 0));
      }
    }
  }
}
BENCHMARK(BM_MatchData)->ArgName("algo")->Arg(0)->Arg(1);

#ifdef USE_RE2
static void BM_MatchRe2(benchmark::State& state) {
  string random_val = GetRandomHex(state.range(0));
  re2::RE2 re(".*foobar.*", re2::RE2::Latin1);
  CHECK(re.ok());

  while (state.KeepRunning()) {
    DoNotOptimize(re2::RE2::FullMatch(random_val, re));
  }
}
BENCHMARK(BM_MatchRe2)->Arg(1000)->Arg(10000);
#endif

#ifdef USE_PCRE2

pair<pcre2_code*, pcre2_match_data*> create_pcre2(const char* pattern) {
  int errnum;
  PCRE2_SIZE erroffset;
  pcre2_code* re =
      pcre2_compile((PCRE2_SPTR)pattern, PCRE2_ZERO_TERMINATED, 0, &errnum, &erroffset, nullptr);
  CHECK(re);
  CHECK_EQ(0, pcre2_jit_compile(re, PCRE2_JIT_COMPLETE));

  pcre2_match_data* match_data = pcre2_match_data_create_from_pattern(re, NULL);
  return {re, match_data};
}

int pcre2_do_match(string_view str, pcre2_code* re, pcre2_match_data* match_data) {
  int rc = pcre2_jit_match(re, (PCRE2_SPTR)str.data(), str.size(), 0,
                           PCRE2_ANCHORED | PCRE2_ENDANCHORED, match_data, NULL);
  return rc;
}

static void BM_MatchPcre2Jit(benchmark::State& state) {
  string random_val = GetRandomHex(state.range(0));
  auto [re, match_data] = create_pcre2(".*foobar.*");
  const char sample[] = "aaaaaaaaaaaaafoobar";
  int rc = pcre2_do_match(sample, re, match_data);
  CHECK_EQ(1, rc);

  while (state.KeepRunning()) {
    rc = pcre2_do_match(random_val, re, match_data);
    CHECK_EQ(PCRE2_ERROR_NOMATCH, rc);
  }
  pcre2_match_data_free(match_data);
  pcre2_code_free(re);
}
BENCHMARK(BM_MatchPcre2Jit)->Arg(32)->Arg(1000)->Arg(10000);

static void BM_MatchPcre2Jit2(benchmark::State& state) {
  string random_val = GetRandomHex(state.range(0));
  auto [re, match_data] = create_pcre2("foo.*bar");

  while (state.KeepRunning()) {
    int rc = pcre2_do_match(random_val, re, match_data);
    CHECK_EQ(PCRE2_ERROR_NOMATCH, rc);
  }
  pcre2_match_data_free(match_data);
  pcre2_code_free(re);
}
BENCHMARK(BM_MatchPcre2Jit2)->Arg(32)->Arg(1000)->Arg(10000);

static void BM_MatchPcre2JitExp(benchmark::State& state) {
  string exponent_pattern = "a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*b";
  string str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
  auto [re, match_data] = create_pcre2(exponent_pattern.c_str());
  while (state.KeepRunning()) {
    int rc = pcre2_do_match(str, re, match_data);
    CHECK_EQ(PCRE2_ERROR_NOMATCH, rc);
  }
  pcre2_match_data_free(match_data);
  pcre2_code_free(re);
}
BENCHMARK(BM_MatchPcre2JitExp);

#endif

}  // namespace dfly
