// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/test_utils.h"

extern "C" {
#include "redis/zmalloc.h"
}

#include <absl/flags/reflection.h>
#include <absl/strings/match.h>
#include <absl/strings/str_split.h>
#include <mimalloc.h>

#include "base/flags.h"
#include "base/logging.h"
#include "base/stl_util.h"
#include "facade/dragonfly_connection.h"
#include "util/fibers/pool.h"

using namespace std;

ABSL_DECLARE_FLAG(string, dbfilename);
ABSL_FLAG(bool, force_epoll, false, "If true, uses epoll api instead iouring to run tests");

namespace dfly {

std::ostream& operator<<(std::ostream& os, ArgSlice list) {
  os << "[";
  if (!list.empty()) {
    std::for_each(list.begin(), list.end() - 1, [&os](const auto& val) { os << val << ", "; });
    os << (*(list.end() - 1));
  }
  return os << "]";
}

std::ostream& operator<<(std::ostream& os, const DbStats& stats) {
  os << "keycount: " << stats.key_count << ", tiered_size: " << stats.tiered_size
     << ", tiered_entries: " << stats.tiered_entries << "\n";

  return os;
}

extern unsigned kInitSegmentLog;

using MP = MemcacheParser;
using namespace util;
using namespace testing;

static vector<string> SplitLines(const std::string& src) {
  vector<string> res = absl::StrSplit(src, "\r\n");
  if (res.back().empty())
    res.pop_back();
  for (auto& v : res) {
    absl::StripAsciiWhitespace(&v);
  }
  return res;
}

TestConnection::TestConnection(Protocol protocol, io::StringSink* sink)
    : facade::Connection(protocol, nullptr, nullptr, nullptr), sink_(sink) {
}

void TestConnection::SendPubMessageAsync(PubMessage pmsg) {
  messages.push_back(move(pmsg));
}

class BaseFamilyTest::TestConnWrapper {
 public:
  TestConnWrapper(Protocol proto);
  ~TestConnWrapper();

  CmdArgVec Args(ArgSlice list);

  RespVec ParseResponse(bool fully_consumed);

  // returns: type(pmessage), pattern, channel, message.
  const facade::Connection::PubMessage& GetPubMessage(size_t index) const;

  ConnectionContext* cmd_cntx() {
    return &cmd_cntx_;
  }

  StringVec SplitLines() const {
    return dfly::SplitLines(sink_.str());
  }

  void ClearSink() {
    sink_.Clear();
  }

  TestConnection* conn() {
    return dummy_conn_.get();
  }

 private:
  ::io::StringSink sink_;  // holds the response blob

  std::unique_ptr<TestConnection> dummy_conn_;

  ConnectionContext cmd_cntx_;
  std::vector<std::unique_ptr<std::string>> tmp_str_vec_;

  std::unique_ptr<RedisParser> parser_;
};

BaseFamilyTest::TestConnWrapper::TestConnWrapper(Protocol proto)
    : dummy_conn_(new TestConnection(proto, &sink_)), cmd_cntx_(&sink_, dummy_conn_.get()) {
}

BaseFamilyTest::TestConnWrapper::~TestConnWrapper() {
}

BaseFamilyTest::BaseFamilyTest() {
}

BaseFamilyTest::~BaseFamilyTest() {
  for (auto* v : resp_vec_)
    delete v;
}

void BaseFamilyTest::SetUpTestSuite() {
  kInitSegmentLog = 1;

  absl::SetFlag(&FLAGS_dbfilename, "");
  init_zmalloc_threadlocal(mi_heap_get_backing());

  // TODO: go over all env variables starting with FLAGS_ and make sure they are in the below list.
  static constexpr const char* kEnvFlags[] = {"cluster_mode", "lock_on_hashtags"};
  for (string_view flag : kEnvFlags) {
    const char* value = getenv(absl::StrCat("FLAGS_", flag).data());
    if (value != nullptr) {
      SetTestFlag(flag, value);
    }
  }
}

void BaseFamilyTest::SetUp() {
  if (absl::GetFlag(FLAGS_force_epoll)) {
    pp_.reset(fb2::Pool::Epoll(num_threads_));
  } else {
    pp_.reset(fb2::Pool::IOUring(16, num_threads_));
  }
  pp_->Run();
  service_.reset(new Service{pp_.get()});

  Service::InitOpts opts;
  opts.disable_time_update = true;
  service_->Init(nullptr, {}, opts);

  TEST_current_time_ms = absl::GetCurrentTimeNanos() / 1000000;
  auto cb = [&](EngineShard* s) { s->db_slice().UpdateExpireBase(TEST_current_time_ms - 1000, 0); };
  shard_set->RunBriefInParallel(cb);

  const TestInfo* const test_info = UnitTest::GetInstance()->current_test_info();
  LOG(INFO) << "Starting " << test_info->name();
}

unsigned BaseFamilyTest::NumLocked() {
  atomic_uint count = 0;
  shard_set->RunBriefInParallel([&](EngineShard* shard) {
    for (const auto& db : shard->db_slice().databases()) {
      if (db == nullptr) {
        continue;
      }
      count += db->trans_locks.size();
    }
  });
  return count;
}

void BaseFamilyTest::TearDown() {
  CHECK_EQ(NumLocked(), 0U);

  service_->Shutdown();
  service_.reset();
  pp_->Stop();

  const TestInfo* const test_info = UnitTest::GetInstance()->current_test_info();
  LOG(INFO) << "Finishing " << test_info->name();
}

void BaseFamilyTest::WaitUntilLocked(DbIndex db_index, string_view key, double timeout) {
  auto step = 50us;
  auto timeout_micro = chrono::duration_cast<chrono::microseconds>(1000ms * timeout);
  int64_t steps = timeout_micro.count() / step.count();
  do {
    ThisFiber::SleepFor(step);
  } while (!IsLocked(db_index, key) && --steps > 0);
  CHECK(IsLocked(db_index, key));
}

RespExpr BaseFamilyTest::Run(ArgSlice list) {
  if (!ProactorBase::IsProactorThread()) {
    return pp_->at(0)->Await([&] { return this->Run(list); });
  }

  return Run(GetId(), list);
}

RespExpr BaseFamilyTest::RunAdmin(std::initializer_list<const std::string_view> list) {
  if (!ProactorBase::IsProactorThread()) {
    return pp_->at(0)->Await([&] { return this->RunAdmin(list); });
  }
  string id = GetId();
  TestConnWrapper* conn_wrapper = AddFindConn(Protocol::REDIS, id);
  // Before running the command set the connection as admin connection
  conn_wrapper->conn()->SetAdmin(true);
  auto res = Run(id, ArgSlice{list.begin(), list.size()});
  // After running the command set the connection as non admin connection
  // because the connction is returned to the poll. This way the next call to Run from the same
  // thread will not have the connection set as admin.
  conn_wrapper->conn()->SetAdmin(false);
  return res;
}

RespExpr BaseFamilyTest::Run(absl::Span<std::string> span) {
  vector<string_view> sv_vec(span.size());
  for (unsigned i = 0; i < span.size(); ++i) {
    sv_vec[i] = span[i];
  }
  return Run(sv_vec);
}

RespExpr BaseFamilyTest::Run(std::string_view id, ArgSlice slice) {
  TestConnWrapper* conn_wrapper = AddFindConn(Protocol::REDIS, id);

  CmdArgVec args = conn_wrapper->Args(slice);

  auto* context = conn_wrapper->cmd_cntx();

  DCHECK(context->transaction == nullptr) << id;

  service_->DispatchCommand(CmdArgList{args}, context);

  DCHECK(context->transaction == nullptr);

  auto cmd = absl::AsciiStrToUpper(slice.front());
  if (cmd == "EVAL" || cmd == "EVALSHA" || cmd == "EXEC") {
    shard_set->AwaitRunningOnShardQueue([](auto*) {});  // Wait for async UnlockMulti.
  }

  unique_lock lk(mu_);
  last_cmd_dbg_info_ = context->last_command_debug;

  RespVec vec = conn_wrapper->ParseResponse(single_response_);
  if (vec.size() == 1)
    return vec.front();
  RespVec* new_vec = new RespVec(vec);
  resp_vec_.push_back(new_vec);
  RespExpr e;
  e.type = RespExpr::ARRAY;
  e.u = new_vec;

  return e;
}

auto BaseFamilyTest::RunMC(MP::CmdType cmd_type, string_view key, string_view value, uint32_t flags,
                           chrono::seconds ttl) -> MCResponse {
  if (!ProactorBase::IsProactorThread()) {
    return pp_->at(0)->Await([&] { return this->RunMC(cmd_type, key, value, flags, ttl); });
  }

  MP::Command cmd;
  cmd.type = cmd_type;
  cmd.key = key;
  cmd.flags = flags;
  cmd.bytes_len = value.size();
  cmd.expire_ts = ttl.count();

  TestConnWrapper* conn = AddFindConn(Protocol::MEMCACHE, GetId());

  auto* context = conn->cmd_cntx();

  DCHECK(context->transaction == nullptr);

  service_->DispatchMC(cmd, value, context);

  DCHECK(context->transaction == nullptr);

  return conn->SplitLines();
}

auto BaseFamilyTest::RunMC(MP::CmdType cmd_type, std::string_view key) -> MCResponse {
  if (!ProactorBase::IsProactorThread()) {
    return pp_->at(0)->Await([&] { return this->RunMC(cmd_type, key); });
  }

  MP::Command cmd;
  cmd.type = cmd_type;
  cmd.key = key;
  TestConnWrapper* conn = AddFindConn(Protocol::MEMCACHE, GetId());

  auto* context = conn->cmd_cntx();

  service_->DispatchMC(cmd, string_view{}, context);

  return conn->SplitLines();
}

auto BaseFamilyTest::GetMC(MP::CmdType cmd_type, std::initializer_list<std::string_view> list)
    -> MCResponse {
  CHECK_GT(list.size(), 0u);
  CHECK(base::_in(cmd_type, {MP::GET, MP::GAT, MP::GETS, MP::GATS}));

  if (!ProactorBase::IsProactorThread()) {
    return pp_->at(0)->Await([&] { return this->GetMC(cmd_type, list); });
  }

  MP::Command cmd;
  cmd.type = cmd_type;
  auto src = list.begin();
  cmd.key = *src++;
  for (; src != list.end(); ++src) {
    cmd.keys_ext.push_back(*src);
  }

  TestConnWrapper* conn = AddFindConn(Protocol::MEMCACHE, GetId());

  auto* context = conn->cmd_cntx();

  service_->DispatchMC(cmd, string_view{}, context);

  return conn->SplitLines();
}

int64_t BaseFamilyTest::CheckedInt(ArgSlice list) {
  RespExpr resp = Run(list);
  if (resp.type == RespExpr::INT64) {
    return get<int64_t>(resp.u);
  }
  if (resp.type == RespExpr::NIL) {
    return INT64_MIN;
  }

  CHECK_EQ(RespExpr::STRING, int(resp.type)) << list;
  string_view sv = ToSV(resp.GetBuf());
  int64_t res;
  CHECK(absl::SimpleAtoi(sv, &res)) << "|" << sv << "|";
  return res;
}

string BaseFamilyTest::CheckedString(ArgSlice list) {
  RespExpr resp = Run(list);
  CHECK_EQ(RespExpr::STRING, int(resp.type)) << list;
  return string{ToSV(resp.GetBuf())};
}

CmdArgVec BaseFamilyTest::TestConnWrapper::Args(ArgSlice list) {
  CHECK_NE(0u, list.size());

  CmdArgVec res;
  string* str = new string;

  // I compact all the arguments together on purpose.
  // This way I check that arguments handling works well without c-string endings.
  for (auto v : list) {
    str->append(v);
  }
  tmp_str_vec_.emplace_back(str);
  size_t offset = 0;
  for (auto v : list) {
    if (v.empty()) {
      res.push_back(MutableSlice{});
    } else {
      res.emplace_back(str->data() + offset, v.size());
      offset += v.size();
    }
  }

  return res;
}

RespVec BaseFamilyTest::TestConnWrapper::ParseResponse(bool fully_consumed) {
  tmp_str_vec_.emplace_back(new string{sink_.str()});
  auto& s = *tmp_str_vec_.back();
  auto buf = RespExpr::buffer(&s);

  auto s_copy = s;

  uint32_t consumed = 0;
  parser_.reset(new RedisParser{false});  // Client mode.
  RespVec res;
  RedisParser::Result st = parser_->Parse(buf, &consumed, &res);

  CHECK_EQ(RedisParser::OK, st) << " response: \"" << s_copy << "\" (" << s_copy.size()
                                << " chars)";
  if (fully_consumed) {
    DCHECK_EQ(consumed, s.size()) << s;
  }
  return res;
}

const facade::Connection::PubMessage& BaseFamilyTest::TestConnWrapper::GetPubMessage(
    size_t index) const {
  CHECK_LT(index, dummy_conn_->messages.size());
  return dummy_conn_->messages[index];
}

bool BaseFamilyTest::IsLocked(DbIndex db_index, std::string_view key) const {
  ShardId sid = Shard(key, shard_set->size());
  KeyLockArgs args;
  args.db_index = db_index;
  args.args = ArgSlice{&key, 1};
  args.key_step = 1;
  bool is_open = pp_->at(sid)->AwaitBrief(
      [args] { return EngineShard::tlocal()->db_slice().CheckLock(IntentLock::EXCLUSIVE, args); });
  return !is_open;
}

string BaseFamilyTest::GetId() const {
  int32 id = ProactorBase::GetIndex();
  CHECK_GE(id, 0);
  return absl::StrCat("IO", id);
}

size_t BaseFamilyTest::SubscriberMessagesLen(string_view conn_id) const {
  auto it = connections_.find(conn_id);
  if (it == connections_.end())
    return 0;

  return it->second->conn()->messages.size();
}

const facade::Connection::PubMessage& BaseFamilyTest::GetPublishedMessage(string_view conn_id,
                                                                          size_t index) const {
  auto it = connections_.find(conn_id);
  CHECK(it != connections_.end());

  return it->second->GetPubMessage(index);
}

ConnectionContext::DebugInfo BaseFamilyTest::GetDebugInfo(const std::string& id) const {
  auto it = connections_.find(id);
  CHECK(it != connections_.end());

  return it->second->cmd_cntx()->last_command_debug;
}

auto BaseFamilyTest::AddFindConn(Protocol proto, std::string_view id) -> TestConnWrapper* {
  unique_lock lk(mu_);

  auto [it, inserted] = connections_.emplace(id, nullptr);

  if (inserted) {
    it->second.reset(new TestConnWrapper(proto));
  } else {
    it->second->ClearSink();
  }
  return it->second.get();
}

vector<string> BaseFamilyTest::StrArray(const RespExpr& expr) {
  CHECK(expr.type == RespExpr::ARRAY || expr.type == RespExpr::NIL_ARRAY);
  if (expr.type == RespExpr::NIL_ARRAY)
    return vector<string>{};

  const RespVec* src = get<RespVec*>(expr.u);
  vector<string> res(src->size());
  for (size_t i = 0; i < src->size(); ++i) {
    res[i] = ToSV(src->at(i).GetBuf());
  }

  return res;
}

void BaseFamilyTest::SetTestFlag(string_view flag_name, string_view new_value) {
  auto* flag = absl::FindCommandLineFlag(flag_name);
  CHECK_NE(flag, nullptr);
  string error;
  CHECK(flag->ParseFrom(new_value, &error)) << "Error: " << error;
}

}  // namespace dfly
