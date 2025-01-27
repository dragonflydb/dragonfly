// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <gmock/gmock.h>

#include "facade/dragonfly_connection.h"
#include "facade/memcache_parser.h"
#include "facade/redis_parser.h"
#include "io/io.h"
#include "server/conn_context.h"
#include "server/main_service.h"
#include "server/transaction.h"
#include "util/proactor_pool.h"

namespace dfly {
using namespace facade;
using util::fb2::Fiber;
using util::fb2::Launch;

// Test hook defined in common.cc.
void TEST_InvalidateLockTagOptions();

class TestConnection : public facade::Connection {
 public:
  explicit TestConnection(Protocol protocol);
  std::string RemoteEndpointStr() const override;

  void SendPubMessageAsync(PubMessage pmsg) final;

  void SendInvalidationMessageAsync(InvalidationMessage msg) final;

  bool IsPrivileged() const override {
    return is_privileged_;
  }
  void SetPrivileged(bool is_privileged) {
    is_privileged_ = is_privileged;
  }

  std::vector<PubMessage> messages;

  std::vector<InvalidationMessage> invalidate_messages;

 private:
  bool is_privileged_ = false;
};

// The TransactionSuspension class is designed to facilitate the temporary suspension of commands
// executions. When the 'start' method is invoked, it enforces the suspension of other
// transactions by acquiring a global shard lock. Conversely, invoking the 'terminate' method
// releases the global shard lock, enabling all transactions in the queue to resume execution.
class TransactionSuspension {
 public:
  void Start();
  void Terminate();

 private:
  boost::intrusive_ptr<dfly::Transaction> transaction_;
};

class BaseFamilyTest : public ::testing::Test {
 protected:
  BaseFamilyTest();
  ~BaseFamilyTest();

  static void SetUpTestSuite();

  void SetUp() override;
  void TearDown() override;

 protected:
  class TestConnWrapper;

  RespExpr Run(std::initializer_list<const std::string_view> list) {
    return Run(ArgSlice{list.begin(), list.size()});
  }

  // Runs the command in a mocked privileged connection
  // Use for running commands which are allowed only when using admin connection.
  RespExpr RunPrivileged(std::initializer_list<const std::string_view> list);

  RespExpr Run(ArgSlice list);
  RespExpr Run(absl::Span<std::string> list);

  RespExpr Run(std::string_view id, ArgSlice list);

  using MCResponse = std::vector<std::string>;
  MCResponse RunMC(MemcacheParser::CmdType cmd_type, std::string_view key, std::string_view value,
                   uint32_t flags = 0, std::chrono::seconds ttl = std::chrono::seconds{});
  MCResponse RunMC(MemcacheParser::CmdType cmd_type, std::string_view key = std::string_view{});
  MCResponse GetMC(MemcacheParser::CmdType cmd_type, std::initializer_list<std::string_view> list);

  int64_t CheckedInt(std::initializer_list<std::string_view> list) {
    return CheckedInt(ArgSlice{list.begin(), list.size()});
  }
  int64_t CheckedInt(ArgSlice list);
  std::string CheckedString(ArgSlice list);

  void ResetService();

  void ShutdownService();

  void InitWithDbFilename();
  void CleanupSnapshots();

  bool IsLocked(DbIndex db_index, std::string_view key) const;
  ConnectionContext::DebugInfo GetDebugInfo(const std::string& id) const;

  ConnectionContext::DebugInfo GetDebugInfo() const {
    return GetDebugInfo("IO0");
  }

  TestConnWrapper* AddFindConn(Protocol proto, std::string_view id);
  static std::vector<std::string> StrArray(const RespExpr& expr);

  Metrics GetMetrics() const {
    return service_->server_family().GetMetrics(&namespaces->GetDefaultNamespace());
  }

  void ClearMetrics();

  void AdvanceTime(int64_t ms) {
    TEST_current_time_ms += ms;
  }

  // Wait for a locked key to unlock. Aborts after timeout seconds passed.
  void WaitUntilLocked(DbIndex db_index, std::string_view key, double timeout = 3);

  // Wait until condition_cb returns true or timeout reached. Returns condition_cb value
  bool WaitUntilCondition(std::function<bool()> condition_cb,
                          std::chrono::milliseconds timeout_ms = 100ms);

  std::string GetId() const;
  size_t SubscriberMessagesLen(std::string_view conn_id) const;

  size_t InvalidationMessagesLen(std::string_view conn_id) const;

  const facade::Connection::PubMessage& GetPublishedMessage(std::string_view conn_id,
                                                            size_t index) const;

  const facade::Connection::InvalidationMessage& GetInvalidationMessage(std::string_view conn_id,
                                                                        size_t index) const;

  static std::vector<LockFp> GetLastFps();
  static void ExpectConditionWithinTimeout(const std::function<bool()>& condition,
                                           absl::Duration timeout = absl::Seconds(10));
  util::fb2::Fiber ExpectConditionWithSuspension(const std::function<bool()>& condition);
  util::fb2::Fiber ExpectUsedKeys(const std::vector<std::string_view>& keys);

  static unsigned NumLocked();

  static void SetTestFlag(std::string_view flag_name, std::string_view new_value);

  const acl::AclFamily* TestInitAclFam();

  std::unique_ptr<util::ProactorPool> pp_;
  std::unique_ptr<Service> service_;
  unsigned num_threads_ = 3;

  absl::flat_hash_map<std::string, std::unique_ptr<TestConnWrapper>> connections_;
  util::fb2::Mutex mu_;
  ConnectionContext::DebugInfo last_cmd_dbg_info_;

  std::vector<RespVec*> resp_vec_;
  bool single_response_ = true;
  util::fb2::Fiber watchdog_fiber_;
  util::fb2::Done watchdog_done_;
};

std::ostream& operator<<(std::ostream& os, const DbStats& stats);

}  // namespace dfly
