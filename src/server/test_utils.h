// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <gmock/gmock.h>

#include "io/io.h"
#include "server/conn_context.h"
#include "server/main_service.h"
#include "server/memcache_parser.h"
#include "server/redis_parser.h"
#include "util/proactor_pool.h"

namespace dfly {

class RespMatcher {
 public:
  RespMatcher(std::string_view val, RespExpr::Type t = RespExpr::STRING) : type_(t), exp_str_(val) {
  }

  RespMatcher(int64_t val, RespExpr::Type t = RespExpr::INT64) : type_(t), exp_int_(val) {
  }

  using is_gtest_matcher = void;

  bool MatchAndExplain(const RespExpr& e, testing::MatchResultListener*) const;

  void DescribeTo(std::ostream* os) const;

  void DescribeNegationTo(std::ostream* os) const;

 private:
  RespExpr::Type type_;

  std::string exp_str_;
  int64_t exp_int_;
};

class RespTypeMatcher {
 public:
  RespTypeMatcher(RespExpr::Type type) : type_(type) {
  }

  using is_gtest_matcher = void;

  bool MatchAndExplain(const RespExpr& e, testing::MatchResultListener*) const;

  void DescribeTo(std::ostream* os) const;

  void DescribeNegationTo(std::ostream* os) const;

 private:
  RespExpr::Type type_;
};

inline ::testing::PolymorphicMatcher<RespMatcher> StrArg(std::string_view str) {
  return ::testing::MakePolymorphicMatcher(RespMatcher(str));
}

inline ::testing::PolymorphicMatcher<RespMatcher> ErrArg(std::string_view str) {
  return ::testing::MakePolymorphicMatcher(RespMatcher(str, RespExpr::ERROR));
}

inline ::testing::PolymorphicMatcher<RespMatcher> IntArg(int64_t ival) {
  return ::testing::MakePolymorphicMatcher(RespMatcher(ival));
}

inline ::testing::PolymorphicMatcher<RespMatcher> ArrLen(size_t len) {
  return ::testing::MakePolymorphicMatcher(RespMatcher(len, RespExpr::ARRAY));
}

inline ::testing::PolymorphicMatcher<RespTypeMatcher> ArgType(RespExpr::Type t) {
  return ::testing::MakePolymorphicMatcher(RespTypeMatcher(t));
}

inline bool operator==(const RespExpr& left, const char* s) {
  return left.type == RespExpr::STRING && ToSV(left.GetBuf()) == s;
}

void PrintTo(const RespExpr::Vec& vec, std::ostream* os);

MATCHER_P(RespEq, val, "") {
  return ::testing::ExplainMatchResult(::testing::ElementsAre(StrArg(val)), arg, result_listener);
}

std::vector<int64_t> ToIntArr(const RespVec& vec);

class BaseFamilyTest : public ::testing::Test {
 protected:
  BaseFamilyTest();
  ~BaseFamilyTest();

  void SetUp() override;
  void TearDown() override;

 protected:
  struct TestConnWrapper {
    ::io::StringSink sink;  // holds the response blob

    std::unique_ptr<Connection> dummy_conn;

    ConnectionContext cmd_cntx;
    std::vector<std::unique_ptr<std::string>> tmp_str_vec;

    std::unique_ptr<RedisParser> parser;

    TestConnWrapper(Protocol proto);
    ~TestConnWrapper();

    CmdArgVec Args(std::initializer_list<std::string_view> list);

    RespVec ParseResponse();
  };

  RespVec Run(std::initializer_list<std::string_view> list);
  RespVec Run(std::string_view id, std::initializer_list<std::string_view> list);

  using MCResponse = std::vector<std::string>;
  MCResponse RunMC(MemcacheParser::CmdType cmd_type, std::string_view key, std::string_view value,
                   uint32_t flags = 0, std::chrono::seconds ttl = std::chrono::seconds{});
  MCResponse RunMC(MemcacheParser::CmdType cmd_type, std::string_view key = std::string_view{});
  MCResponse GetMC(MemcacheParser::CmdType cmd_type, std::initializer_list<std::string_view> list);

  int64_t CheckedInt(std::initializer_list<std::string_view> list);

  bool IsLocked(DbIndex db_index, std::string_view key) const;
  ConnectionContext::DebugInfo GetDebugInfo(const std::string& id) const;

  ConnectionContext::DebugInfo GetDebugInfo() const {
    return GetDebugInfo("IO0");
  }

  TestConnWrapper* AddFindConn(Protocol proto, std::string_view id);

  // ts is ms
  void UpdateTime(uint64_t ms);
  std::string GetId() const;

  std::unique_ptr<util::ProactorPool> pp_;
  std::unique_ptr<Service> service_;
  EngineShardSet* ess_ = nullptr;
  unsigned num_threads_ = 3;

  absl::flat_hash_map<std::string, std::unique_ptr<TestConnWrapper>> connections_;
  ::boost::fibers::mutex mu_;
  ConnectionContext::DebugInfo last_cmd_dbg_info_;
};

}  // namespace dfly
