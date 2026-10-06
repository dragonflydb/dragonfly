// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "common/dtoa.h"

#include <cmath>
#include <cstdlib>
#include <cstring>
#include <random>
#include <string>
#include <vector>

#include "base/gtest.h"
#include "base/logging.h"

using namespace std;

namespace cmn {

namespace {

string Format(double d, bool exp_plus_sign = true) {
  char buf[kMaxDoubleStrLen];
  size_t len = FormatDoubleShortest(d, exp_plus_sign, buf);
  EXPECT_EQ(len, strlen(buf));
  return string(buf, len);
}

}  // namespace

TEST(DtoaTest, Basic) {
  EXPECT_EQ("0.1", Format(0.1));
  EXPECT_EQ("0.2", Format(0.2));
  EXPECT_EQ("0.8", Format(0.8));
  EXPECT_EQ("1.1", Format(1.1));
  EXPECT_EQ("-1.1", Format(-1.1));
  EXPECT_EQ("123.45", Format(123.45));
  EXPECT_EQ("0.30000000000000004", Format(0.1 + 0.2));
  EXPECT_EQ("1e-23", Format(1e-23));
}

TEST(DtoaTest, SpecialValues) {
  EXPECT_EQ("0", Format(0.0));
  EXPECT_EQ("0", Format(-0.0));
  EXPECT_EQ("inf", Format(INFINITY));
  EXPECT_EQ("-inf", Format(-INFINITY));
  EXPECT_EQ("nan", Format(NAN));
  EXPECT_EQ("nan", Format(-NAN));
  EXPECT_EQ("5e-324", Format(5e-324));
  EXPECT_EQ("-5e-324", Format(-5e-324));
  EXPECT_EQ("2.2250738585072014e-308", Format(2.2250738585072014e-308));
  EXPECT_EQ("1.7976931348623157e+308", Format(1.7976931348623157e308));
}

TEST(DtoaTest, Integers) {
  EXPECT_EQ("1", Format(1.0));
  EXPECT_EQ("-42", Format(-42.0));
  EXPECT_EQ("1000", Format(1000.0));
  EXPECT_EQ("1000000000000000", Format(1e15));
  EXPECT_EQ("9007199254740991", Format(9007199254740991.0));

  // 2^53 and above are not handled by the integer fast path.
  EXPECT_EQ("9007199254740992", Format(9007199254740992.0));
  EXPECT_EQ("9007199254740994", Format(9007199254740994.0));
  EXPECT_EQ("111111111111111110000", Format(111111111111111111111.0));
}

// Fixed notation is used for decimal exponents in [-6, 21).
TEST(DtoaTest, NotationBoundaries) {
  EXPECT_EQ("0.000001", Format(1e-6));
  EXPECT_EQ("0.0000015", Format(1.5e-6));
  EXPECT_EQ("1e-7", Format(1e-7));
  EXPECT_EQ("1.5e-7", Format(1.5e-7));
  EXPECT_EQ("100000000000000000000", Format(1e20));
  EXPECT_EQ("1e+21", Format(1e21));
  EXPECT_EQ("1.1111111111111111e+21", Format(1111111111111111111111.0));
  EXPECT_EQ("1.5e+300", Format(1.5e300));
}

TEST(DtoaTest, NoExpPlusSign) {
  EXPECT_EQ("1e21", Format(1e21, false));
  EXPECT_EQ("1.5e300", Format(1.5e300, false));
  EXPECT_EQ("1e-7", Format(1e-7, false));
  EXPECT_EQ("0.1", Format(0.1, false));
}

TEST(DtoaTest, LongestOutput) {
  string res = Format(-1.2345678901234567e-6);
  EXPECT_EQ("-0.0000012345678901234567", res);
  EXPECT_EQ(kMaxDoubleStrLen - 1, res.size());
}

// Every finite double must be printed within kMaxDoubleStrLen and parse back to itself.
TEST(DtoaTest, RoundTrip) {
  mt19937_64 rng(42);
  for (unsigned i = 0; i < 200'000; ++i) {
    uint64_t bits = rng();
    double d;
    memcpy(&d, &bits, sizeof(d));
    if (!isfinite(d))
      continue;

    string res = Format(d);
    ASSERT_LT(res.size(), kMaxDoubleStrLen);
    ASSERT_EQ(d, strtod(res.c_str(), nullptr)) << res;
  }
}

namespace {

enum class Dist { kRandomBits, kUniform, kShortDecimal, kInteger, kScore };

vector<double> MakeValues(Dist dist) {
  mt19937_64 rng(7);
  uniform_real_distribution<double> unif(0, 1e9);
  vector<double> values;
  while (values.size() < 1024) {
    double d = 0;
    switch (dist) {
      case Dist::kRandomBits: {
        uint64_t bits = rng();
        memcpy(&d, &bits, sizeof(d));
        if (!isfinite(d))
          continue;
        break;
      }
      case Dist::kUniform:
        d = unif(rng);
        break;
      case Dist::kShortDecimal:  // e.g. 123.45
        d = double(rng() % 100000) / 100;
        break;
      case Dist::kInteger:
        d = double(rng() % 1000000000);
        break;
      case Dist::kScore:  // e.g. 0.1234
        d = double(rng() % 10000) / 10000;
        break;
    }
    values.push_back(d);
  }
  return values;
}

void BM_FormatDouble(benchmark::State& state) {
  vector<double> values = MakeValues(Dist(state.range(0)));
  char buf[kMaxDoubleStrLen];
  for (auto _ : state) {
    for (double d : values) {
      benchmark::DoNotOptimize(FormatDoubleShortest(d, true, buf));
      benchmark::ClobberMemory();
    }
  }
  state.SetItemsProcessed(state.iterations() * values.size());
}
BENCHMARK(BM_FormatDouble)
    ->ArgName("dist")
    ->Arg(int(Dist::kRandomBits))
    ->Arg(int(Dist::kUniform))
    ->Arg(int(Dist::kShortDecimal))
    ->Arg(int(Dist::kInteger))
    ->Arg(int(Dist::kScore));

}  // namespace

}  // namespace cmn
