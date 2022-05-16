// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <gmock/gmock.h>

#include "facade/memcache_parser.h"
#include "facade/redis_parser.h"
#include "io/io.h"
#include "server/conn_context.h"
#include "server/main_service.h"
#include "util/proactor_pool.h"

namespace dfly {
using namespace facade;

class BaseFamilyTest : public ::testing::Test {
 protected:
  BaseFamilyTest();
  ~BaseFamilyTest();

  static void SetUpTestSuite();

  void SetUp() override;
  void TearDown() override;

 protected:
  struct TestConnWrapper {
    ::io::StringSink sink;  // holds the response blob

    std::unique_ptr<facade::Connection> dummy_conn;

    ConnectionContext cmd_cntx;
    std::vector<std::unique_ptr<std::string>> tmp_str_vec;

    std::unique_ptr<RedisParser> parser;

    TestConnWrapper(Protocol proto);
    ~TestConnWrapper();

    CmdArgVec Args(std::initializer_list<std::string_view> list);

    RespVec ParseResponse();
  };

  RespExpr Run(std::initializer_list<std::string_view> list);
  RespExpr Run(std::string_view id, std::initializer_list<std::string_view> list);

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
  static std::vector<std::string> StrArray(const RespExpr& expr);

  // ts is ms
  void UpdateTime(uint64_t ms);

  std::string GetId() const;

  std::unique_ptr<util::ProactorPool> pp_;
  std::unique_ptr<Service> service_;
  unsigned num_threads_ = 3;

  absl::flat_hash_map<std::string, std::unique_ptr<TestConnWrapper>> connections_;
  ::boost::fibers::mutex mu_;
  ConnectionContext::DebugInfo last_cmd_dbg_info_;
  uint64_t expire_now_;
  std::vector<RespVec*> resp_vec_;
};

}  // namespace dfly
