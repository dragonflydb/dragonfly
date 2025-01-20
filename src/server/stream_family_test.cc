// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/stream_family.h"

#include "base/flags.h"
#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"
#include "server/command_registry.h"
#include "server/test_utils.h"

using namespace testing;
using namespace std;
using namespace util;

ABSL_DECLARE_FLAG(bool, stream_rdb_encode_v2);

namespace dfly {

const auto kMatchNil = ArgType(RespExpr::NIL);

class StreamFamilyTest : public BaseFamilyTest {
 protected:
};

TEST_F(StreamFamilyTest, Add) {
  auto resp = Run({"xadd", "key", "*", "field", "value"});
  ASSERT_THAT(resp, ArgType(RespExpr::STRING));
  string id = string(ToSV(resp.GetBuf()));
  EXPECT_THAT(id, EndsWith("-0"));

  resp = Run({"xrange", "null", "-", "+"});
  EXPECT_THAT(resp, ArrLen(0));

  resp = Run({"xrange", "key", "-", "+"});
  EXPECT_THAT(resp, ArrLen(2));
  auto sub_arr = resp.GetVec();
  EXPECT_THAT(sub_arr, ElementsAre(id, ArrLen(2)));

  resp = Run({"xlen", "key"});
  EXPECT_THAT(resp, IntArg(1));

  resp = Run({"xadd", "key", "badid", "f1", "val1"});
  EXPECT_THAT(resp, ErrArg("Invalid stream ID"));

  resp = Run({"xadd", "key", "nomkstream", "*", "field2", "value2"});
  ASSERT_THAT(resp, ArgType(RespExpr::STRING));

  resp = Run({"xadd", "noexist", "nomkstream", "*", "field", "value"});
  EXPECT_THAT(resp, kMatchNil);
}

TEST_F(StreamFamilyTest, AddExtended) {
  auto resp0 = Run({"xadd", "key", "5", "f1", "v1", "f2", "v2"});
  EXPECT_EQ(resp0, "5-0");
  resp0 = Run({"xrange", "key", "5-0", "5-0"});
  EXPECT_THAT(resp0, ArrLen(2));
  auto sub_arr = resp0.GetVec();
  EXPECT_THAT(sub_arr, ElementsAre("5-0", ArrLen(4)));
  sub_arr = sub_arr[1].GetVec();
  EXPECT_THAT(sub_arr, ElementsAre("f1", "v1", "f2", "v2"));

  auto resp1 = Run({"xadd", "key", "maxlen", "1", "*", "field1", "val1"});
  string id1 = string(ToSV(resp1.GetBuf()));

  auto resp2 = Run({"xadd", "key", "maxlen", "1", "*", "field2", "val2"});
  string id2 = string(ToSV(resp2.GetBuf()));

  EXPECT_THAT(Run({"xlen", "key"}), IntArg(1));
  EXPECT_THAT(Run({"xrange", "key", id1, id1}), ArrLen(0));

  auto resp3 = Run({"xadd", "key", id2, "f1", "val1"});
  EXPECT_THAT(resp3, ErrArg("equal or smaller than"));

  Run({"xadd", "key2", "5-0", "field", "val"});
  Run({"xadd", "key2", "6-0", "field1", "val1"});
  Run({"xadd", "key2", "7-0", "field2", "val2"});
  auto resp = Run({"xadd", "key2", "minid", "6", "*", "field3", "val3"});
  EXPECT_THAT(Run({"xlen", "key2"}), IntArg(3));
  EXPECT_THAT(Run({"xrange", "key2", "5-0", "5-0"}), ArrLen(0));

  for (int i = 0; i < 700; i++) {
    Run({"xadd", "key3", "*", "field", "val"});
  }
  resp = Run({"xadd", "key3", "maxlen", "~", "500", "*", "field", "val"});
  EXPECT_THAT(Run({"xlen", "key3"}), IntArg(501));
  for (int i = 0; i < 700; i++) {
    Run({"xadd", "key4", "*", "field", "val"});
  }
  resp = Run({"xadd", "key4", "maxlen", "~", "500", "limit", "100", "*", "field", "val"});
  EXPECT_THAT(Run({"xlen", "key4"}), IntArg(601));
}

TEST_F(StreamFamilyTest, XrangeRangeAutocomplete) {
  Run({"xadd", "mystream", "1609459200000-0", "0", "0"});
  Run({"xadd", "mystream", "1609459200001-0", "1", "1"});
  Run({"xadd", "mystream", "1609459200001-1", "2", "2"});
  Run({"xadd", "mystream", "1609459200002-0", "3", "3"});
  auto resp = Run({"xrange", "mystream", "1609459200000", "1609459200001"});
  EXPECT_THAT(resp, RespElementsAre(RespElementsAre("1609459200000-0", RespElementsAre("0", "0")),
                                    RespElementsAre("1609459200001-0", RespElementsAre("1", "1")),
                                    RespElementsAre("1609459200001-1", RespElementsAre("2", "2"))));
  resp = Run({"xrange", "mystream", "1609459200000", "(1609459200001"});
  EXPECT_THAT(resp, RespElementsAre(RespElementsAre("1609459200000-0", RespElementsAre("0", "0")),
                                    RespElementsAre("1609459200001-0", RespElementsAre("1", "1")),
                                    RespElementsAre("1609459200001-1", RespElementsAre("2", "2"))));
}

TEST_F(StreamFamilyTest, Range) {
  Run({"xadd", "key", "1-*", "f1", "v1"});
  Run({"xadd", "key", "1-*", "f2", "v2"});
  auto resp = Run({"xrange", "key", "-", "+"});
  EXPECT_THAT(resp, ArrLen(2));
  auto sub_arr = resp.GetVec();
  EXPECT_THAT(sub_arr, ElementsAre(ArrLen(2), ArrLen(2)));
  auto sub0 = sub_arr[0].GetVec();
  auto sub1 = sub_arr[1].GetVec();
  EXPECT_THAT(sub0, ElementsAre("1-0", ArrLen(2)));
  EXPECT_THAT(sub1, ElementsAre("1-1", ArrLen(2)));

  resp = Run({"xrevrange", "key", "+", "-"});
  sub_arr = resp.GetVec();
  sub0 = sub_arr[0].GetVec();
  sub1 = sub_arr[1].GetVec();
  EXPECT_THAT(sub0, ElementsAre("1-1", ArrLen(2)));
  EXPECT_THAT(sub1, ElementsAre("1-0", ArrLen(2)));
}

TEST_F(StreamFamilyTest, GroupCreate) {
  auto resp = Run({"xadd", "key", "1-*", "f1", "v1"});
  EXPECT_EQ(resp, "1-0");
  resp = Run({"xgroup", "create", "key", "grname", "1"});
  EXPECT_EQ(resp, "OK");
  resp = Run({"xgroup", "create", "test", "test", "0"});
  EXPECT_THAT(resp, ErrArg("requires the key to exist"));
  resp = Run({"xgroup", "create", "test", "test", "0", "MKSTREAM"});
  EXPECT_THAT(resp, "OK");
  resp = Run({"xgroup", "create", "test", "test", "0", "MKSTREAM"});
  EXPECT_THAT(resp, ErrArg("BUSYGROUP"));
}

TEST_F(StreamFamilyTest, XRead) {
  Run({"xadd", "foo", "1-*", "k1", "v1"});
  Run({"xadd", "foo", "1-*", "k2", "v2"});
  Run({"xadd", "foo", "1-*", "k3", "v3"});
  Run({"xadd", "bar", "1-*", "k4", "v4"});
  EXPECT_EQ(GetMetrics().shard_stats.tx_optimistic_total, 4u);

  // Receive all records from a single stream, in a single hop
  auto resp = Run({"xread", "streams", "foo", "0"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("foo", ArrLen(3)));
  EXPECT_EQ(GetMetrics().shard_stats.tx_optimistic_total, 5u);

  // Receive all records from both streams.
  resp = Run({"xread", "streams", "foo", "bar", "0", "0"});

  // 2 results
  ASSERT_THAT(resp, RespArray(ElementsAre(ArrLen(2), ArrLen(2))));
  ASSERT_THAT(resp.GetVec()[0], RespArray(ElementsAre("foo", ArrLen(3))));
  ASSERT_THAT(resp.GetVec()[1], RespArray(ElementsAre("bar", ArrLen(1))));

  // Order of the requested streams is maintained.
  resp = Run({"xread", "streams", "bar", "foo", "0", "0"});
  ASSERT_THAT(resp, RespArray(ElementsAre(ArrLen(2), ArrLen(2))));
  ASSERT_THAT(resp.GetVec()[0], RespArray(ElementsAre("bar", ArrLen(1))));
  ASSERT_THAT(resp.GetVec()[1], RespArray(ElementsAre("foo", ArrLen(3))));

  // Limit count.
  resp = Run({"xread", "count", "1", "streams", "foo", "bar", "0", "0"});
  ASSERT_THAT(resp, RespArray(ElementsAre(ArrLen(2), ArrLen(2))));
  ASSERT_THAT(resp.GetVec()[0], RespArray(ElementsAre("foo", ArrLen(1))));
  ASSERT_THAT(resp.GetVec()[1], RespArray(ElementsAre("bar", ArrLen(1))));

  // Read from ID.
  resp = Run({"xread", "count", "10", "streams", "foo", "bar", "1-1", "2-0"});
  // Note when the response has length 1, Run returns the first element.
  EXPECT_THAT(resp.GetVec(), ElementsAre("foo", ArrLen(1)));
  EXPECT_THAT(resp.GetVec()[1].GetVec()[0].GetVec(), ElementsAre("1-2", ArrLen(2)));

  // Stream not found.
  resp = Run({"xread", "streams", "foo", "notfound", "0", "0"});
  // Note when the response has length 1, Run returns the first element.
  EXPECT_THAT(resp.GetVec(), ElementsAre("foo", ArrLen(3)));

  // Not found.
  resp = Run({"xread", "streams", "notfound", "0"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL_ARRAY));
}

TEST_F(StreamFamilyTest, XReadGroup) {
  Run({"xadd", "foo", "1-*", "k1", "v1"});
  Run({"xadd", "foo", "1-*", "k2", "v2"});
  Run({"xadd", "foo", "1-*", "k3", "v3"});
  Run({"xadd", "bar", "1-*", "k4", "v4"});

  Run({"xadd", "mystream", "1-*", "k1", "v1"});
  Run({"xadd", "mystream", "1-*", "k2", "v2"});
  Run({"xadd", "mystream", "1-*", "k3", "v3"});

  Run({"xgroup", "create", "foo", "group", "0"});
  Run({"xgroup", "create", "bar", "group", "0"});

  // consumer PEL is empty, so resp should have empty list
  auto resp = Run({"xreadgroup", "group", "group", "alice", "streams", "foo", "0"});
  EXPECT_THAT(resp, RespArray(ElementsAre("foo", ArrLen(0))));

  // should return unread entries with key "foo"
  resp = Run({"xreadgroup", "group", "group", "alice", "streams", "foo", ">"});
  // only "foo" key entries are read
  EXPECT_THAT(resp, RespArray(ElementsAre("foo", ArrLen(3))));

  Run({"xadd", "foo", "1-*", "k5", "v5"});
  resp = Run({"xreadgroup", "group", "group", "alice", "streams", "bar", "foo", ">", ">"});
  EXPECT_THAT(resp, RespArray(ElementsAre(ArrLen(2), ArrLen(2))));

  EXPECT_THAT(resp.GetVec()[0].GetVec()[1].GetVec()[0], RespArray(ElementsAre("1-0", ArrLen(2))));
  EXPECT_THAT(resp.GetVec()[1].GetVec()[1].GetVec()[0], RespArray(ElementsAre("1-3", ArrLen(2))));

  // now we can specify id for "foo" and it fetches from alice's consumer PEL
  resp = Run({"xreadgroup", "group", "group", "alice", "streams", "foo", "0"});
  EXPECT_THAT(resp.GetVec()[1], ArrLen(4));

  // now ">" gives nil
  resp = Run({"xreadgroup", "group", "group", "alice", "streams", "foo", ">"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL_ARRAY));

  // count limits the fetched entries
  resp = Run(
      {"xreadgroup", "group", "group", "alice", "count", "2", "streams", "foo", "bar", "0", "0"});
  EXPECT_THAT(resp, RespArray(ElementsAre(ArrLen(2), ArrLen(2))));
  EXPECT_THAT(resp.GetVec()[0].GetVec(), ElementsAre("foo", ArrLen(2)));
  EXPECT_THAT(resp.GetVec()[1].GetVec(), ElementsAre("bar", ArrLen(1)));

  // bob will not get entries of alice
  resp = Run({"xreadgroup", "group", "group", "bob", "streams", "foo", "0"});
  EXPECT_THAT(resp, RespArray(ElementsAre("foo", ArrLen(0))));

  resp = Run({"xinfo", "groups", "foo"});
  // 2 consumers created
  EXPECT_THAT(resp.GetVec()[3], IntArg(2));
  // check last_delivery_id
  EXPECT_THAT(resp.GetVec()[7], "1-3");

  // Noack
  Run({"xadd", "foo", "1-*", "k6", "v6"});
  resp = Run({"xreadgroup", "group", "group", "bob", "noack", "streams", "foo", ">"});
  // check basic results
  EXPECT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec(), ElementsAre("foo", ArrLen(1)));
  // Entry is not inserted in Bob's consumer PEL.
  resp = Run({"xreadgroup", "group", "group", "bob", "streams", "foo", "0"});
  EXPECT_THAT(resp, RespArray(ElementsAre("foo", ArrLen(0))));

  // No Group
  resp = Run({"xreadgroup", "group", "nogroup", "alice", "streams", "foo", "0"});
  EXPECT_THAT(
      resp,
      ErrArg("No such key 'foo' or consumer group 'nogroup' in XREADGROUP with GROUP option"));

  // '>' gives the null array result if group doesn't exist
  resp = Run({"xreadgroup", "group", "group", "alice", "streams", "mystream", ">"});
  EXPECT_THAT(
      resp,
      ErrArg("No such key 'mystream' or consumer group 'group' in XREADGROUP with GROUP option"));

  Run({"xadd", "foo", "1-*", "k7", "v7"});
  resp = Run({"xreadgroup", "group", "group", "alice", "streams", "mystream", "foo", ">", ">"});
  // returns no group error as "group" was not created for mystream.
  EXPECT_THAT(
      resp,
      ErrArg("No such key 'mystream' or consumer group 'group' in XREADGROUP with GROUP option"));

  // returns no group error when key doesn't exists
  // this is how Redis' behave
  resp = Run({"xreadgroup", "group", "group", "consumer", "count", "10", "block", "5000", "streams",
              "nostream", ">"});
  EXPECT_THAT(
      resp,
      ErrArg("No such key 'nostream' or consumer group 'group' in XREADGROUP with GROUP option"));

  // block on empty stream via xgroup create.
  Run({"xgroup", "create", "emptystream", "group", "0", "mkstream"});
  auto before = absl::Now();
  resp = Run({"xreadgroup", "group", "group", "consumer", "count", "10", "block", "1000", "streams",
              "emptystream", ">"});
  EXPECT_GE(absl::Now() - before, absl::Seconds(1));
  EXPECT_THAT(resp, ArgType(RespExpr::NIL_ARRAY));
}

TEST_F(StreamFamilyTest, XReadBlock) {
  Run({"xadd", "foo", "1-*", "k1", "v1"});
  Run({"xadd", "foo", "1-*", "k2", "v2"});
  Run({"xadd", "foo", "1-*", "k3", "v3"});
  Run({"xadd", "bar", "1-*", "k4", "v4"});

  // Receive all records from both streams.
  auto resp = Run({"xread", "block", "100", "streams", "foo", "bar", "0", "0"});
  EXPECT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec()[0].GetVec(), ElementsAre("foo", ArrLen(3)));
  EXPECT_THAT(resp.GetVec()[1].GetVec(), ElementsAre("bar", ArrLen(1)));

  // Timeout.
  resp = Run({"xread", "block", "1", "streams", "foo", "$"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL_ARRAY));

  // Timeout again, on two steams
  resp = Run({"xread", "block", "1", "streams", "foo", "bar", "$", "$"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL_ARRAY));

  // Run XREAD BLOCK from 2 fibers.
  RespExpr resp0, resp1;
  auto fb0 = pp_->at(0)->LaunchFiber(Launch::dispatch, [&] {
    resp0 = Run({"xread", "block", "0", "streams", "foo", "$"});
  });
  auto fb1 = pp_->at(1)->LaunchFiber(Launch::dispatch, [&] {
    resp1 = Run({"xread", "block", "0", "streams", "foo", "bar", "$", "$"});
  });
  ThisFiber::SleepFor(50us);

  resp = pp_->at(1)->Await([&] { return Run("xadd", {"xadd", "foo", "1-*", "k5", "v5"}); });

  fb0.Join();
  fb1.Join();

  // Both xread calls should have been unblocked.
  //
  // Note when the response has length 1, Run returns the first element.
  EXPECT_THAT(resp0.GetVec(), ElementsAre("foo", ArrLen(1)));
  EXPECT_THAT(resp1.GetVec(), ElementsAre("foo", ArrLen(1)));
}

TEST_F(StreamFamilyTest, XReadGroupBlockwithoutBlock) {
  Run({"xadd", "foo", "1-*", "k1", "v1"});
  Run({"xadd", "foo", "1-*", "k2", "v2"});
  Run({"xadd", "foo", "1-*", "k3", "v3"});
  Run({"xadd", "bar", "1-*", "k4", "v4"});

  Run({"xgroup", "create", "foo", "group", "0"});
  Run({"xgroup", "create", "bar", "group", "0"});

  // Receive all records from both streams.
  auto resp = Run(
      {"xreadgroup", "group", "group", "alice", "block", "100", "streams", "foo", "bar", ">", ">"});
  EXPECT_THAT(resp, RespArray(ElementsAre(ArrLen(2), ArrLen(2))));
  EXPECT_THAT(resp.GetVec()[0].GetVec(), ElementsAre("foo", ArrLen(3)));
  EXPECT_THAT(resp.GetVec()[1].GetVec(), ElementsAre("bar", ArrLen(1)));
}

TEST_F(StreamFamilyTest, XReadGroupBlock) {
  Run({"xgroup", "create", "foo", "group", "0", "MKSTREAM"});
  Run({"xgroup", "create", "bar", "group", "0", "MKSTREAM"});

  // Timeout
  auto resp = Run(
      {"xreadgroup", "group", "group", "alice", "block", "1", "streams", "foo", "bar", ">", ">"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL_ARRAY));

  // Run XREADGROUP BLOCK from 2 fibers.
  RespExpr resp0, resp1;
  auto fb0 = pp_->at(0)->LaunchFiber(Launch::dispatch, [&] {
    resp0 = Run(
        {"xreadgroup", "group", "group", "alice", "block", "0", "streams", "foo", "bar", ">", ">"});
  });
  auto fb1 = pp_->at(1)->LaunchFiber(Launch::dispatch, [&] {
    resp1 = Run(
        {"xreadgroup", "group", "group", "alice", "block", "0", "streams", "foo", "bar", ">", ">"});
  });
  ThisFiber::SleepFor(50us);

  pp_->at(1)->Await([&] { return Run("xadd", {"xadd", "foo", "1-*", "k5", "v5"}); });
  // Only one xreadgroup call should have been unblocked.

  ThisFiber::SleepFor(50us);
  pp_->at(1)->Await([&] { return Run("xadd", {"xadd", "bar", "1-*", "k5", "v5"}); });
  // The second one should be unblocked
  ThisFiber::SleepFor(50us);

  fb0.Join();
  fb1.Join();

  if (resp0.GetVec()[0].GetString() == "foo") {
    EXPECT_THAT(resp0.GetVec(), ElementsAre("foo", ArrLen(1)));
    EXPECT_THAT(resp1.GetVec(), ElementsAre("bar", ArrLen(1)));
  } else {
    EXPECT_THAT(resp1.GetVec(), ElementsAre("foo", ArrLen(1)));
    EXPECT_THAT(resp0.GetVec(), ElementsAre("bar", ArrLen(1)));
  }

  // Call XGROUP DESTROY while blocking
  Run({"xgroup", "create", "to-delete", "to-delete", "0", "MKSTREAM"});
  fb0 = pp_->at(1)->LaunchFiber(Launch::dispatch, [&] {
    resp0 = Run({"xreadgroup", "group", "to-delete", "consumer", "block", "0", "streams",
                 "to-delete", ">"});
  });

  Run({"xgroup", "destroy", "to-delete", "to-delete"});
  fb0.Join();
  EXPECT_THAT(resp0, ErrArg("consumer group this client was blocked on no longer exists"));
}

TEST_F(StreamFamilyTest, XReadInvalidArgs) {
  // Invalid COUNT value.
  auto resp = Run({"xread", "count", "invalid", "streams", "s1", "s2", "0", "0"});
  EXPECT_THAT(resp, ErrArg("not an integer or out of range"));

  // Missing COUNT value.
  resp = Run({"xread", "count"});
  EXPECT_THAT(resp, ErrArg("wrong number of arguments for 'xread' command"));

  // Invalid BLOCK value.
  resp = Run({"xread", "block", "invalid", "streams", "s1", "s2", "0", "0"});
  EXPECT_THAT(resp, ErrArg("not an integer or out of range"));

  // Missing BLOCK value.
  resp = Run({"xread", "block", "streams", "s1", "s2", "0", "0"});
  EXPECT_THAT(resp, ErrArg("not an integer or out of range"));

  // Missing STREAMS.
  resp = Run({"xread", "count", "5"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

  // Unbalanced list of streams.
  resp = Run({"xread", "count", "invalid", "streams", "s1", "s2", "0", "0"});
  EXPECT_THAT(resp, ErrArg("value is not an integer"));

  // Wrong type.
  Run({"set", "foo", "v"});
  resp = Run({"xread", "streams", "foo", "0"});
  EXPECT_THAT(resp, ErrArg("key holding the wrong kind of value"));
}

TEST_F(StreamFamilyTest, XReadGroupInvalidArgs) {
  Run({"xgroup", "create", "group", "foo", "0", "mkstream"});
  // Invalid COUNT value.
  auto resp =
      Run({"xreadgroup", "group", "group", "alice", "count", "invalid", "streams", "foo", "0"});
  EXPECT_THAT(resp, ErrArg("not an integer or out of range"));

  // Invalid "stream" instead of GROUP.
  resp = Run({"xreadgroup", "stream", "group", "alice", "count", "1", "streams", "foo", "0"});
  EXPECT_THAT(resp, ErrArg("Missing 'GROUP' in 'XREADGROUP' command"));

  // Missing streams.
  resp = Run({"xreadgroup", "group", "group", "alice", "streams"});
  EXPECT_THAT(resp, ErrArg("wrong number of arguments for 'xreadgroup' command"));

  // Missing consumer.
  resp = Run({"xreadgroup", "group", "group", "streams", "foo", "0"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

  // Missing block value.
  resp = Run({"xreadgroup", "group", "group", "alice", "block", "streams", "foo", "0"});
  EXPECT_THAT(resp, ErrArg("not an integer or out of range"));

  // Invalid block value.
  resp = Run({"xreadgroup", "group", "group", "alice", "block", "invalid", "streams", "foo", "0"});
  EXPECT_THAT(resp, ErrArg("not an integer or out of range"));

  // Unbalanced list of streams.
  resp = Run({"xreadgroup", "group", "group", "alice", "streams", "s1", "s2", "s3", "0", "0"});
  EXPECT_THAT(resp, ErrArg("Unbalanced 'xreadgroup' list of streams: for each stream key an ID or "
                           "'>' must be specified"));

  resp = Run({"XREAD", "COUNT", "1", "STREAMS", "mystream"});
  ASSERT_THAT(resp, ErrArg("Unbalanced 'xread' list of streams: for each stream key an ID or '$' "
                           "must be specified"));
}

TEST_F(StreamFamilyTest, XReadGroupEmpty) {
  Run({"XADD", "stream", "*", "foo", "bar"});
  Run({"XGROUP", "CREATE", "stream", "group", "0"});
  auto resp = Run({"XREADGROUP", "GROUP", "group", "consumer1", "STREAMS", "stream", "0"});
  EXPECT_THAT(resp, ArrLen(2));
}

// todo: ASAN fails heres on arm
#ifndef SANITIZERS
TEST_F(StreamFamilyTest, Issue854) {
  auto resp = Run({"xgroup", "help"});
  EXPECT_THAT(resp, ArgType(RespExpr::ARRAY));

  resp = Run({"eval", "redis.call('xgroup', 'help')", "0"});
  EXPECT_THAT(resp, ErrArg("is not allowed"));
}
#endif

TEST_F(StreamFamilyTest, XGroupConsumer) {
  Run({"xgroup", "create", "foo", "group", "$", "MKSTREAM"});
  auto resp = Run({"xgroup", "createconsumer", "foo", "group", "bob"});
  EXPECT_THAT(resp, IntArg(1));
  Run({"xgroup", "createconsumer", "foo", "group", "alice"});
  resp = Run({"xinfo", "groups", "foo"});
  EXPECT_THAT(resp.GetVec()[3], IntArg(2));
  Run({"xgroup", "delconsumer", "foo", "group", "alice"});
  resp = Run({"xinfo", "groups", "foo"});
  EXPECT_THAT(resp.GetVec()[3], IntArg(1));

  resp = Run({"xgroup", "createconsumer", "foo", "group", "alice"});
  EXPECT_THAT(resp, IntArg(1));

  // ensure createconsumer doesn't create consumer that already exists
  resp = Run({"xgroup", "createconsumer", "foo", "group", "alice"});
  EXPECT_THAT(resp, IntArg(0));

  // nogrouperror
  resp = Run({"xgroup", "createconsumer", "foo", "not-exists", "alice"});
  EXPECT_THAT(resp, ErrArg("NOGROUP"));
}

TEST_F(StreamFamilyTest, Xclaim) {
  Run({"xadd", "foo", "1-0", "k1", "v1"});
  Run({"xadd", "foo", "1-1", "k2", "v2"});
  Run({"xadd", "foo", "1-2", "k3", "v3"});
  Run({"xadd", "foo", "1-3", "k4", "v4"});

  // create a group for foo stream
  Run({"xgroup", "create", "foo", "group", "0"});
  // alice consume all the stream entries
  Run({"xreadgroup", "group", "group", "alice", "streams", "foo", ">"});

  // bob claims alice's two pending stream entries
  auto resp = Run({"xclaim", "foo", "group", "bob", "0", "1-2", "1-3"});
  EXPECT_THAT(resp, RespArray(ElementsAre(
                        RespArray(ElementsAre("1-2", RespArray(ElementsAre("k3", "v3")))),
                        RespArray(ElementsAre("1-3", RespArray(ElementsAre("k4", "v4")))))));

  // bob really have these claimed entries
  resp = Run({"xreadgroup", "group", "group", "bob", "streams", "foo", "0"});
  EXPECT_THAT(resp,
              RespArray(ElementsAre(
                  "foo", RespArray(ElementsAre(
                             RespArray(ElementsAre("1-2", RespArray(ElementsAre("k3", "v3")))),
                             RespArray(ElementsAre("1-3", RespArray(ElementsAre("k4", "v4")))))))));

  // alice no longer have those entries
  resp = Run({"xreadgroup", "group", "group", "alice", "streams", "foo", "0"});
  EXPECT_THAT(resp,
              RespArray(ElementsAre(
                  "foo", RespArray(ElementsAre(
                             RespArray(ElementsAre("1-0", RespArray(ElementsAre("k1", "v1")))),
                             RespArray(ElementsAre("1-1", RespArray(ElementsAre("k2", "v2")))))))));

  // xclaim ensures that entries before the min-idle-time are not claimed by bob
  resp = Run({"xclaim", "foo", "group", "bob", "3600000", "1-0"});
  EXPECT_THAT(resp, ArrLen(0));
  resp = Run({"xreadgroup", "group", "group", "alice", "streams", "foo", "0"});
  EXPECT_THAT(resp,
              RespArray(ElementsAre(
                  "foo", RespArray(ElementsAre(
                             RespArray(ElementsAre("1-0", RespArray(ElementsAre("k1", "v1")))),
                             RespArray(ElementsAre("1-1", RespArray(ElementsAre("k2", "v2")))))))));

  Run({"xadd", "foo", "1-4", "k5", "v5"});
  Run({"xreadgroup", "group", "group", "alice", "streams", "foo", ">"});
  // xclaim returns only claimed ids when justid is set
  resp = Run({"xclaim", "foo", "group", "bob", "0", "1-0", "1-4", "justid"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("1-0", "1-4"));

  Run({"xadd", "foo", "1-5", "k6", "v6"});
  // bob should claim the id forcefully even if it is not yet present in group pel
  resp = Run({"xclaim", "foo", "group", "bob", "0", "1-5", "force", "justid"});
  EXPECT_THAT(resp.GetString(), "1-5");
  resp = Run({"xreadgroup", "group", "group", "bob", "streams", "foo", "0"});
  EXPECT_THAT(resp.GetVec()[1].GetVec()[4].GetVec(),
              ElementsAre("1-5", RespArray(ElementsAre("k6", "v6"))));

  TEST_current_time_ms += 2000;
  resp = Run({"xclaim", "foo", "group", "alice", "0", "1-4", "TIME",
              absl::StrCat(TEST_current_time_ms - 500), "justid"});
  EXPECT_THAT(resp.GetString(), "1-4");
  // min idle time is exceeded for this entry
  resp = Run({"xclaim", "foo", "group", "bob", "600", "1-4"});
  EXPECT_THAT(resp, ArrLen(0));
  resp = Run({"xclaim", "foo", "group", "bob", "400", "1-4", "justid"});
  EXPECT_THAT(resp.GetString(), "1-4");

  //  test RETRYCOUNT
  Run({"xadd", "foo", "1-6", "k7", "v7"});
  resp = Run({"xclaim", "foo", "group", "bob", "0", "1-6", "force", "justid", "retrycount", "5"});
  EXPECT_THAT(resp.GetString(), "1-6");
  resp = Run({"xpending", "foo", "group", "1-6", "1-6", "1"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("1-6", "bob", ArgType(RespExpr::INT64), IntArg(5)));

  // test LASTID
  Run({"xreadgroup", "group", "group", "bob", "count", "2", "streams", "foo", ">"});
  Run({"xclaim", "foo", "group", "alice", "0", "1-6", "LASTID", "1-4"});
  resp = Run({"xinfo", "groups", "foo"});
  EXPECT_EQ(resp.GetVec()[7], "1-6");

  Run({"xclaim", "foo", "group", "bob", "0", "1-6", "LASTID", "1-9"});
  resp = Run({"xinfo", "groups", "foo"});
  EXPECT_EQ(resp.GetVec()[7], "1-9");
}

TEST_F(StreamFamilyTest, XTrim) {
  Run({"xadd", "foo", "1-*", "k", "v"});
  Run({"xadd", "foo", "1-*", "k", "v"});
  Run({"xadd", "foo", "1-*", "k", "v"});
  Run({"xadd", "foo", "1-*", "k", "v"});

  // Trim to maxlen 2, 2 entries should have been deleted with 2 entries remaining.
  auto resp = Run({"xtrim", "foo", "maxlen", "2"});
  EXPECT_THAT(resp, IntArg(2));
  resp = Run({"xlen", "foo"});
  EXPECT_THAT(resp, IntArg(2));

  Run({"xadd", "foo", "1-*", "k", "v"});
  Run({"xadd", "foo", "1-*", "k", "v"});

  // Trim messages whose ID is before 1-4, 2 entries should have been deleted with
  // 2 entries remaining.
  resp = Run({"xtrim", "foo", "minid", "1-4"});
  EXPECT_THAT(resp, IntArg(2));
  resp = Run({"xlen", "foo"});
  EXPECT_THAT(resp, IntArg(2));

  // Trim no changes needed.
  resp = Run({"xtrim", "foo", "maxlen", "5"});
  EXPECT_THAT(resp, IntArg(0));
  resp = Run({"xlen", "foo"});
  EXPECT_THAT(resp, IntArg(2));

  Run({"xadd", "foo", "1-*", "k", "v"});
  Run({"xadd", "foo", "1-*", "k", "v"});

  // Trim exact.
  resp = Run({"xtrim", "foo", "maxlen", "=", "2"});
  EXPECT_THAT(resp, IntArg(2));
  resp = Run({"xlen", "foo"});
  EXPECT_THAT(resp, IntArg(2));

  Run({"xadd", "foo", "1-*", "k", "v"});
  Run({"xadd", "foo", "1-*", "k", "v"});

  // Trim approx.
  resp = Run({"xtrim", "foo", "maxlen", "~", "2"});
  EXPECT_THAT(resp, IntArg(0));
  resp = Run({"xlen", "foo"});
  EXPECT_THAT(resp, IntArg(4));

  // Trim stream not found should return no entries.
  resp = Run({"xtrim", "notfound", "maxlen", "5"});
  EXPECT_THAT(resp, IntArg(0));
}

TEST_F(StreamFamilyTest, XTrimInvalidArgs) {
  // Missing threshold.
  auto resp = Run({"xtrim", "foo"});
  EXPECT_THAT(resp, ErrArg("wrong number of arguments"));
  resp = Run({"xtrim", "foo", "maxlen"});
  EXPECT_THAT(resp, ErrArg("wrong number of arguments"));
  resp = Run({"xtrim", "foo", "minid"});
  EXPECT_THAT(resp, ErrArg("wrong number of arguments"));

  // Invalid threshold.
  resp = Run({"xtrim", "foo", "maxlen", "nan"});
  EXPECT_THAT(resp, ErrArg("not an integer or out of range"));
  resp = Run({"xtrim", "foo", "maxlen", "-1"});
  EXPECT_THAT(resp, ErrArg("not an integer or out of range"));
  resp = Run({"xtrim", "foo", "minid", "nan"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

  // Limit with non-approx.
  resp = Run({"xtrim", "foo", "maxlen", "2", "limit", "5"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

  // Include both maxlen and minid.
  resp = Run({"xtrim", "foo", "maxlen", "2", "minid", "1-1"});
  EXPECT_THAT(resp, ErrArg("MAXLEN and MINID options at the same time are not compatible"));
  resp = Run({"xtrim", "foo", "minid", "1-1", "maxlen", "2"});
  EXPECT_THAT(resp, ErrArg("MAXLEN and MINID options at the same time are not compatible"));

  // Invalid limit.
  resp = Run({"xtrim", "foo", "maxlen", "~", "2", "limit", "nan"});
  EXPECT_THAT(resp, ErrArg("syntax error"));
}
TEST_F(StreamFamilyTest, XPending) {
  Run({"xadd", "foo", "1-0", "k1", "v1"});
  Run({"xadd", "foo", "1-1", "k2", "v2"});
  Run({"xadd", "foo", "1-2", "k3", "v3"});

  // create a group for foo stream
  Run({"xgroup", "create", "foo", "group", "0"});
  // alice consume all the stream entries
  Run({"xreadgroup", "group", "group", "alice", "streams", "foo", ">"});
  // bob doesn't have pending entries
  Run({"xgroup", "createconsumer", "foo", "group", "bob"});

  // XPending should print 4 entries
  auto resp = Run({"xpending", "foo", "group"});
  EXPECT_THAT(resp, RespArray(ElementsAre(
                        IntArg(3), "1-0", "1-2",
                        RespArray(ElementsAre(RespArray(ElementsAre("alice", IntArg(3))))))));

  resp = Run({"xpending", "foo", "group", "-", "+", "10"});
  EXPECT_THAT(resp,
              RespArray(ElementsAre(
                  RespArray(ElementsAre("1-0", "alice", ArgType(RespExpr::INT64), IntArg(1))),
                  RespArray(ElementsAre("1-1", "alice", ArgType(RespExpr::INT64), IntArg(1))),
                  RespArray(ElementsAre("1-2", "alice", ArgType(RespExpr::INT64), IntArg(1))))));

  // only return a single entry
  resp = Run({"xpending", "foo", "group", "-", "+", "1"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("1-0", "alice", ArgType(RespExpr::INT64), IntArg(1)));

  // Bob read a new entry
  Run({"xadd", "foo", "1-3", "k4", "v4"});
  Run({"xreadgroup", "group", "group", "bob", "streams", "foo", ">"});
  // Bob now has` an entry in his pending list
  resp = Run({"xpending", "foo", "group", "-", "+", "10", "bob"});
  EXPECT_THAT(resp.GetVec(), ElementsAre("1-3", "bob", ArgType(RespExpr::INT64), IntArg(1)));

  Run({"xadd", "foo", "1-4", "k5", "v5"});
  TEST_current_time_ms = 100;
  Run({"xreadgroup", "group", "group", "bob", "streams", "foo", ">"});
  TEST_current_time_ms += 3000;

  // min-idle-time is exceeding the delivery time of last inserted entry
  resp = Run({"xpending", "foo", "group", "IDLE", "4000", "-", "+", "10"});
  EXPECT_THAT(resp, ArrLen(0));
}

TEST_F(StreamFamilyTest, XPendingInvalidArgs) {
  Run({"xadd", "foo", "1-0", "k1", "v1"});
  Run({"xadd", "foo", "1-1", "k2", "v2"});

  auto resp = Run({"xpending", "unknown", "group"});
  EXPECT_THAT(resp, ErrArg("no such key"));

  // group doesn't exist
  resp = Run({"xpending", "foo", "group"});
  EXPECT_THAT(resp, ErrArg("NOGROUP"));

  Run({"xgroup", "create", "foo", "group", "0"});
  // start end count not provided
  resp = Run({"xpending", "foo", "group", "IDLE", "0"});
  EXPECT_THAT(resp, ErrArg("wrong number of arguments"));

  // count not provided
  resp = Run({"xpending", "foo", "group", "-", "+"});
  EXPECT_THAT(resp, ErrArg("wrong number of arguments"));
}

TEST_F(StreamFamilyTest, XPendingEmpty) {
  Run({"XADD", "stream", "*", "foo", "bar"});
  Run({"XADD", "stream", "*", "foo", "bar"});
  Run({"XGROUP", "CREATE", "stream", "group", "0"});
  auto resp = Run({"XPENDING", "stream", "group"});
  EXPECT_THAT(resp, RespArray(ElementsAre(IntArg(0), kMatchNil, kMatchNil, kMatchNil)));
}

TEST_F(StreamFamilyTest, XAck) {
  Run({"xadd", "foo", "1-0", "k0", "v0"});
  Run({"xadd", "foo", "1-1", "k1", "v1"});
  Run({"xadd", "foo", "1-2", "k2", "v2"});
  Run({"xadd", "foo", "1-3", "k3", "v3"});
  Run({"xgroup", "create", "foo", "cgroup", "0"});
  Run({"xreadgroup", "group", "cgroup", "consumer", "count", "4", "streams", "foo", ">"});

  // PEL of cgroup now has 4 messages.
  // Acknowledge a message that exists.
  auto resp = Run({"xack", "foo", "cgroup", "1-0"});
  EXPECT_THAT(resp, IntArg(1));

  // acknowledge a message from non-existing stream.
  resp = Run({"xack", "nosuchstream", "cgroup", "1-0"});
  EXPECT_THAT(resp, IntArg(0));

  // acknowledge a message for a non-existing consumer group.
  resp = Run({"xack", "foo", "nosuchcgroup", "1-0"});
  EXPECT_THAT(resp, IntArg(0));

  // Verifies message id 1-0 gets removed from PEL.
  resp = Run({"xreadgroup", "group", "cgroup", "consumer", "streams", "foo", "0"});
  EXPECT_THAT(resp,
              RespArray(ElementsAre(
                  "foo", RespArray(ElementsAre(
                             RespArray(ElementsAre("1-1", RespArray(ElementsAre("k1", "v1")))),
                             RespArray(ElementsAre("1-2", RespArray(ElementsAre("k2", "v2")))),
                             RespArray(ElementsAre("1-3", RespArray(ElementsAre("k3", "v3")))))))));

  // acknowledge a message that doesn't exist
  resp = Run({"xack", "foo", "cgroup", "1-9"});
  EXPECT_THAT(resp, IntArg(0));

  // Verifies no message gets removed from PEL.
  resp = Run({"xreadgroup", "group", "cgroup", "consumer", "streams", "foo", "0"});
  EXPECT_THAT(resp,
              RespArray(ElementsAre(
                  "foo", RespArray(ElementsAre(
                             RespArray(ElementsAre("1-1", RespArray(ElementsAre("k1", "v1")))),
                             RespArray(ElementsAre("1-2", RespArray(ElementsAre("k2", "v2")))),
                             RespArray(ElementsAre("1-3", RespArray(ElementsAre("k3", "v3")))))))));

  // acknowledge another message that exists and one non-existing message.
  resp = Run({"xack", "foo", "cgroup", "1-3", "1-9"});
  EXPECT_THAT(resp, IntArg(1));

  // Verifies only "1-3" gets removed from PEL.
  resp = Run({"xreadgroup", "group", "cgroup", "consumer", "streams", "foo", "0"});
  EXPECT_THAT(resp,
              RespArray(ElementsAre(
                  "foo", RespArray(ElementsAre(
                             RespArray(ElementsAre("1-1", RespArray(ElementsAre("k1", "v1")))),
                             RespArray(ElementsAre("1-2", RespArray(ElementsAre("k2", "v2")))))))));

  // acknowledge all the existing messages left.
  resp = Run({"xack", "foo", "cgroup", "1-1", "1-2"});
  EXPECT_THAT(resp, IntArg(2));

  // Verifies that PEL is empty.
  resp = Run({"xreadgroup", "group", "cgroup", "consumer", "streams", "foo", "0"});
  EXPECT_THAT(resp, RespArray(ElementsAre("foo", ArrLen(0))));
}

TEST_F(StreamFamilyTest, XInfoGroups) {
  Run({"del", "mystream"});
  Run({"xgroup", "create", "mystream", "mygroup", "$", "MKSTREAM"});

  // non-existent-stream
  auto resp = Run({"xinfo", "groups", "non-existent-stream"});
  EXPECT_THAT(resp, ErrArg("no such key"));

  // group with no consumers
  resp = Run({"xinfo", "groups", "mystream"});
  EXPECT_THAT(resp, ArrLen(12));
  EXPECT_THAT(resp.GetVec(),
              ElementsAre("name", "mygroup", "consumers", IntArg(0), "pending", IntArg(0),
                          "last-delivered-id", "0-0", "entries-read", kMatchNil, "lag", IntArg(0)));

  // group with multiple consumers
  Run({"xgroup", "createconsumer", "mystream", "mygroup", "consumer1"});
  Run({"xgroup", "createconsumer", "mystream", "mygroup", "consumer2"});
  resp = Run({"xinfo", "groups", "mystream"});
  EXPECT_THAT(resp, ArrLen(12));
  EXPECT_THAT(resp.GetVec()[3], IntArg(2));

  // group with lag
  Run({"xadd", "mystream", "1-0", "test-field-1", "test-value-1"});
  Run({"xadd", "mystream", "2-0", "test-field-2", "test-value-2"});
  resp = Run({"xinfo", "groups", "mystream"});
  EXPECT_THAT(resp.GetVec()[11], IntArg(2));
  EXPECT_THAT(resp.GetVec()[7], "0-0");

  // group with no lag, before ack
  Run({"xreadgroup", "group", "mygroup", "consumer1", "STREAMS", "mystream", ">"});
  resp = Run({"xinfo", "groups", "mystream"});
  EXPECT_THAT(resp.GetVec(),
              ElementsAre("name", "mygroup", "consumers", IntArg(2), "pending", IntArg(2),
                          "last-delivered-id", "2-0", "entries-read", IntArg(2), "lag", IntArg(0)));

  // after ack
  Run({"xack", "mystream", "mygroup", "1-0"});
  Run({"xack", "mystream", "mygroup", "2-0"});
  resp = Run({"xinfo", "groups", "mystream"});
  EXPECT_THAT(resp.GetVec(),
              ElementsAre("name", "mygroup", "consumers", IntArg(2), "pending", IntArg(0),
                          "last-delivered-id", "2-0", "entries-read", IntArg(2), "lag", IntArg(0)));
}

TEST_F(StreamFamilyTest, XInfoConsumers) {
  Run({"del", "mystream"});
  Run({"xgroup", "create", "mystream", "mygroup", "$", "MKSTREAM"});

  // no consumer
  auto resp = Run({"xinfo", "consumers", "mystream", "mygroup"});
  EXPECT_THAT(resp, ArrLen(0));

  // invalid key
  resp = Run({"xinfo", "consumers", "non-existent-stream", "mygroup"});
  EXPECT_THAT(resp, ErrArg("no such key"));

  // invalid group
  resp = Run({"xinfo", "consumers", "mystream", "non-existent-group"});
  EXPECT_THAT(resp, ErrArg("NOGROUP"));

  Run({"xgroup", "createconsumer", "mystream", "mygroup", "first-consumer"});
  Run({"xgroup", "createconsumer", "mystream", "mygroup", "second-consumer"});
  resp = Run({"xinfo", "consumers", "mystream", "mygroup"});
  EXPECT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec()[0], ArrLen(8));
  EXPECT_THAT(resp.GetVec()[1], ArrLen(8));
  EXPECT_THAT(resp.GetVec()[0].GetVec()[1], "first-consumer");
  EXPECT_THAT(resp.GetVec()[1].GetVec()[1], "second-consumer");

  Run({"xadd", "mystream", "1-0", "test-field-1", "test-value-1"});
  Run({"xreadgroup", "group", "mygroup", "consumer1", "STREAMS", "mystream", ">"});
  resp = Run({"xinfo", "consumers", "mystream", "mygroup"});
  // pending for first-consumer
  EXPECT_THAT(resp.GetVec()[0].GetVec()[3], IntArg(1));
  // pending for second-consumer
  EXPECT_THAT(resp.GetVec()[1].GetVec()[3], IntArg(0));
}

TEST_F(StreamFamilyTest, XAutoClaim) {
  Run({"xadd", "foo", "1-0", "k1", "v1"});
  Run({"xadd", "foo", "1-1", "k2", "v2"});
  Run({"xadd", "foo", "1-2", "k3", "v3"});
  Run({"xadd", "foo", "1-3", "k4", "v4"});

  // create a group for foo stream
  Run({"xgroup", "create", "foo", "group", "0"});
  // alice consume all the stream entries
  Run({"xreadgroup", "group", "group", "alice", "streams", "foo", ">"});

  // bob claims alice's two pending stream entries
  // testing the mandatory command options.
  auto resp = Run({"xautoclaim", "foo", "group", "bob", "0", "1-2"});
  EXPECT_THAT(
      resp,
      RespArray(ElementsAre(
          "0-0",
          RespArray(ElementsAre(RespArray(ElementsAre("1-2", RespArray(ElementsAre("k3", "v3")))),
                                RespArray(ElementsAre("1-3", RespArray(ElementsAre("k4", "v4")))))),
          RespArray(ElementsAre()))));

  // bob really has these claimed entries
  resp = Run({"xreadgroup", "group", "group", "bob", "streams", "foo", "0"});
  EXPECT_THAT(resp,
              RespArray(ElementsAre(
                  "foo", RespArray(ElementsAre(
                             RespArray(ElementsAre("1-2", RespArray(ElementsAre("k3", "v3")))),
                             RespArray(ElementsAre("1-3", RespArray(ElementsAre("k4", "v4")))))))));

  // alice no longer have those entries
  resp = Run({"xreadgroup", "group", "group", "alice", "streams", "foo", "0"});
  EXPECT_THAT(resp,
              RespArray(ElementsAre(
                  "foo", RespArray(ElementsAre(
                             RespArray(ElementsAre("1-0", RespArray(ElementsAre("k1", "v1")))),
                             RespArray(ElementsAre("1-1", RespArray(ElementsAre("k2", "v2")))))))));

  // xautoclaim ensures that entries before the min-idle-time are not claimed by bob
  resp = Run({"xautoclaim", "foo", "group", "bob", "3600000", "0-0"});
  EXPECT_THAT(resp,
              RespArray(ElementsAre("0-0", RespArray(ElementsAre()), RespArray(ElementsAre()))));

  Run({"xadd", "foo", "1-4", "k5", "v5"});
  Run({"xreadgroup", "group", "group", "alice", "streams", "foo", ">"});
  // xautoclaim returns only claimed ids when justid is set
  resp = Run({"xautoclaim", "foo", "group", "bob", "0", "0-0", "justid"});
  EXPECT_THAT(
      resp, RespArray(ElementsAre("0-0", RespArray(ElementsAre("1-0", "1-1", "1-2", "1-3", "1-4")),
                                  RespArray(ElementsAre()))));

  Run({"xadd", "foo", "1-5", "k6", "v6"});
  Run({"xadd", "foo", "1-6", "k7", "v7"});
  Run({"xreadgroup", "group", "group", "alice", "streams", "foo", ">"});
  // test count and end_id
  resp = Run({"xautoclaim", "foo", "group", "bob", "0", "1-5", "count", "1", "justid"});
  EXPECT_THAT(
      resp, RespArray(ElementsAre("1-6", RespArray(ElementsAre("1-5")), RespArray(ElementsAre()))));

  resp = Run({"xautoclaim", "foo", "group", "bob", "0", "1-6", "count", "1", "justid"});
  EXPECT_THAT(
      resp, RespArray(ElementsAre("0-0", RespArray(ElementsAre("1-6")), RespArray(ElementsAre()))));

  resp = Run({"xautoclaim", "foo", "group", "bob", "0", "1-10", "count", "1", "justid"});
  EXPECT_THAT(resp,
              RespArray(ElementsAre("0-0", RespArray(ElementsAre()), RespArray(ElementsAre()))));

  // if a message being claimed is deleted, it should be listed separately.
  Run({"xdel", "foo", "1-2", "1-4"});
  resp = Run({"xautoclaim", "foo", "group", "alice", "0", "0-0", "justid"});
  EXPECT_THAT(
      resp, RespArray(ElementsAre("0-0", RespArray(ElementsAre("1-0", "1-1", "1-3", "1-5", "1-6")),
                                  RespArray(ElementsAre("1-2", "1-4")))));
}

TEST_F(StreamFamilyTest, XInfoStream) {
  Run({"del", "mystream"});
  Run({"xgroup", "create", "mystream", "mygroup", "$", "MKSTREAM"});
  Run({"xgroup", "createconsumer", "mystream", "mygroup", "first-consumer"});

  // invalid key
  auto resp = Run({"xinfo", "stream", "non-existent-stream"});
  EXPECT_THAT(resp, ErrArg("no such key"));

  // invalid args
  resp = Run({"xinfo", "stream", "mystream", "extra-arg"});
  EXPECT_THAT(
      resp,
      ErrArg("unknown subcommand or wrong number of arguments for 'STREAM'. Try XINFO HELP."));
  resp = Run({"xinfo", "stream", "mystream", "full", "count"});
  EXPECT_THAT(
      resp,
      ErrArg("unknown subcommand or wrong number of arguments for 'STREAM'. Try XINFO HELP."));
  resp = Run({"xinfo", "stream", "mystream", "full", "count", "a"});
  EXPECT_THAT(resp, ErrArg("value is not an integer or out of range"));

  // no message in stream
  resp = Run({"xinfo", "stream", "mystream"});
  EXPECT_THAT(resp, ArrLen(20));
  EXPECT_THAT(
      resp.GetVec(),
      ElementsAre("length", IntArg(0), "radix-tree-keys", IntArg(0), "radix-tree-nodes", IntArg(1),
                  "last-generated-id", "0-0", "max-deleted-entry-id", "0-0", "entries-added",
                  IntArg(0), "recorded-first-entry-id", "0-0", "groups", IntArg(1), "first-entry",
                  ArgType(RespExpr::NIL_ARRAY), "last-entry", ArgType(RespExpr::NIL_ARRAY)));

  Run({"xadd", "mystream", "1-1", "message", "one"});
  Run({"xadd", "mystream", "2-1", "message", "two"});
  Run({"xadd", "mystream", "3-1", "message", "three"});
  Run({"xadd", "mystream", "4-1", "message", "four"});
  Run({"xadd", "mystream", "5-1", "message", "five"});
  Run({"xadd", "mystream", "6-1", "message", "six"});
  Run({"xadd", "mystream", "7-1", "message", "seven"});
  Run({"xadd", "mystream", "8-1", "message", "eight"});
  Run({"xadd", "mystream", "9-1", "message", "nine"});
  Run({"xadd", "mystream", "10-1", "message", "ten"});
  Run({"xadd", "mystream", "11-1", "message", "eleven"});
  resp = Run({"xinfo", "stream", "mystream"});
  EXPECT_THAT(resp.GetVec(),
              ElementsAre("length", IntArg(11), "radix-tree-keys", IntArg(1), "radix-tree-nodes",
                          IntArg(2), "last-generated-id", "11-1", "max-deleted-entry-id", "0-0",
                          "entries-added", IntArg(11), "recorded-first-entry-id", "1-1", "groups",
                          IntArg(1), "first-entry", ArrLen(2), "last-entry", ArrLen(2)));
  EXPECT_THAT(resp.GetVec()[17].GetVec()[0], "1-1");
  EXPECT_THAT(resp.GetVec()[17].GetVec()[1].GetVec(), ElementsAre("message", "one"));
  EXPECT_THAT(resp.GetVec()[19].GetVec()[0], "11-1");
  EXPECT_THAT(resp.GetVec()[19].GetVec()[1].GetVec(), ElementsAre("message", "eleven"));

  // full - default
  resp = Run({"xinfo", "stream", "mystream", "full"});
  EXPECT_THAT(resp, ArrLen(18));
  EXPECT_THAT(resp.GetVec()[15], ArrLen(10));
  EXPECT_THAT(resp.GetVec()[17], ArrLen(1));
  EXPECT_THAT(resp.GetVec()[17].GetVec()[0], ArrLen(14));
  EXPECT_THAT(resp.GetVec(),
              ElementsAre("length", IntArg(11), "radix-tree-keys", IntArg(1), "radix-tree-nodes",
                          IntArg(2), "last-generated-id", "11-1", "max-deleted-entry-id", "0-0",
                          "entries-added", IntArg(11), "recorded-first-entry-id", "1-1", "entries",
                          ArrLen(10), "groups", ArrLen(1)));
  EXPECT_THAT(resp.GetVec()[17].GetVec()[0].GetVec(),
              ElementsAre("name", "mygroup", "last-delivered-id", "0-0", "entries-read", kMatchNil,
                          "lag", IntArg(11), "pel-count", IntArg(0), "pending", ArrLen(0),
                          "consumers", ArrLen(1)));
  EXPECT_THAT(resp.GetVec()[17].GetVec()[0].GetVec()[13].GetVec()[0].GetVec(),
              ElementsAre("name", "first-consumer", "seen-time", ArgType(RespExpr::INT64),
                          "active-time", IntArg(-1), "pel-count", IntArg(0), "pending", ArrLen(0)));

  // full with count less than number of messages in stream
  resp = Run({"xinfo", "stream", "mystream", "full", "count", "5"});
  EXPECT_THAT(resp.GetVec()[15], ArrLen(5));

  // full with count exceeding number of messages in stream
  resp = Run({"xinfo", "stream", "mystream", "full", "count", "12"});
  EXPECT_THAT(resp.GetVec()[15], ArrLen(11));

  // full - all messages
  resp = Run({"xinfo", "stream", "mystream", "full", "count", "0"});
  EXPECT_THAT(resp.GetVec()[15], ArrLen(11));

  // read message
  Run({"xreadgroup", "group", "mygroup", "first-consumer", "STREAMS", "mystream", ">"});
  resp = Run({"xinfo", "stream", "mystream", "full", "count", "0"});
  EXPECT_THAT(resp.GetVec()[15], ArrLen(11));
  // group
  EXPECT_THAT(resp.GetVec()[17].GetVec()[0].GetVec()[5], IntArg(11));   // entries-read
  EXPECT_THAT(resp.GetVec()[17].GetVec()[0].GetVec()[7], IntArg(0));    // lag
  EXPECT_THAT(resp.GetVec()[17].GetVec()[0].GetVec()[9], IntArg(11));   // pel-count
  EXPECT_THAT(resp.GetVec()[17].GetVec()[0].GetVec()[11], ArrLen(11));  // pending list
  // consumer
  EXPECT_THAT(resp.GetVec()[17].GetVec()[0].GetVec()[13].GetVec()[0].GetVec()[7],
              IntArg(11));  // pel-count
  EXPECT_THAT(resp.GetVec()[17].GetVec()[0].GetVec()[13].GetVec()[0].GetVec()[9],
              ArrLen(11));  // pending list

  // delete message
  Run({"xdel", "mystream", "1-1"});
  resp = Run({"xinfo", "stream", "mystream"});
  EXPECT_THAT(resp.GetVec(),
              ElementsAre("length", IntArg(10), "radix-tree-keys", IntArg(1), "radix-tree-nodes",
                          IntArg(2), "last-generated-id", "11-1", "max-deleted-entry-id", "1-1",
                          "entries-added", IntArg(11), "recorded-first-entry-id", "2-1", "groups",
                          IntArg(1), "first-entry", ArrLen(2), "last-entry", ArrLen(2)));
  EXPECT_THAT(resp.GetVec()[17].GetVec()[0], "2-1");
  EXPECT_THAT(resp.GetVec()[17].GetVec()[1].GetVec(), ElementsAre("message", "two"));
  EXPECT_THAT(resp.GetVec()[19].GetVec()[0], "11-1");
  EXPECT_THAT(resp.GetVec()[19].GetVec()[1].GetVec(), ElementsAre("message", "eleven"));

  resp = Run({"xinfo", "stream", "mystream", "full", "count", "0"});
  EXPECT_THAT(resp.GetVec()[15], ArrLen(10));
  EXPECT_THAT(resp.GetVec(),
              ElementsAre("length", IntArg(10), "radix-tree-keys", IntArg(1), "radix-tree-nodes",
                          IntArg(2), "last-generated-id", "11-1", "max-deleted-entry-id", "1-1",
                          "entries-added", IntArg(11), "recorded-first-entry-id", "2-1", "entries",
                          ArrLen(10), "groups", ArrLen(1)));
  EXPECT_THAT(resp.GetVec()[17].GetVec()[0].GetVec(),
              ElementsAre("name", "mygroup", "last-delivered-id", "11-1", "entries-read",
                          IntArg(11), "lag", IntArg(0), "pel-count", IntArg(11), "pending",
                          ArrLen(11), "consumers", ArrLen(1)));
  EXPECT_THAT(
      resp.GetVec()[17].GetVec()[0].GetVec()[13].GetVec()[0].GetVec(),
      ElementsAre("name", "first-consumer", "seen-time", ArgType(RespExpr::INT64), "active-time",
                  ArgType(RespExpr::INT64), "pel-count", IntArg(11), "pending", ArrLen(11)));
}

TEST_F(StreamFamilyTest, AutoClaimPelItemsFromAnotherConsumer) {
  auto resp = Run({"xadd", "mystream", "*", "a", "1"});
  string id1 = resp.GetString();
  resp = Run({"xadd", "mystream", "*", "b", "2"});
  string id2 = resp.GetString();
  resp = Run({"xadd", "mystream", "*", "c", "3"});
  string id3 = resp.GetString();
  resp = Run({"xadd", "mystream", "*", "d", "4"});
  string id4 = resp.GetString();

  Run({"XGROUP", "CREATE", "mystream", "mygroup", "0"});

  // Consumer 1 reads item 1 from the stream without acknowledgements.
  // Consumer 2 then claims pending item 1 from the PEL of consumer 1
  resp = Run(
      {"XREADGROUP", "GROUP", "mygroup", "consumer1", "COUNT", "1", "STREAMS", "mystream", ">"});

  auto match_a1 = RespElementsAre("a", "1");
  ASSERT_THAT(resp, RespElementsAre("mystream", RespElementsAre(RespElementsAre(id1, match_a1))));

  AdvanceTime(200);  // Advance time to greater time than the idle time in the autoclaim (10)
  resp = Run({"XAUTOCLAIM", "mystream", "mygroup", "consumer2", "10", "-", "COUNT", "1"});

  EXPECT_THAT(resp, RespElementsAre("0-0", ArrLen(1), ArrLen(0)));
  EXPECT_THAT(resp.GetVec()[1], RespElementsAre(RespElementsAre(id1, match_a1)));

  Run({"XREADGROUP", "GROUP", "mygroup", "consumer1", "COUNT", "3", "STREAMS", "mystream", ">"});
  AdvanceTime(200);

  // Delete item 2 from the stream.Now consumer 1 has PEL that contains
  // only item 3. Try to use consumer 2 to claim the deleted item 2
  // from the PEL of consumer 1, this should return nil
  resp = Run({"XDEL", "mystream", id2});
  ASSERT_THAT(resp, IntArg(1));

  // id1 and id3 are self - claimed here but not id2('count' was set to 3)
  // we make sure id2 is indeed skipped(the cursor points to id4)
  resp = Run({"XAUTOCLAIM", "mystream", "mygroup", "consumer2", "10", "-", "COUNT", "3"});
  auto match_id1_a1 = RespElementsAre(id1, match_a1);
  auto match_id3_c3 = RespElementsAre(id3, RespElementsAre("c", "3"));
  ASSERT_THAT(resp, RespElementsAre(id4, RespElementsAre(match_id1_a1, match_id3_c3),
                                    RespElementsAre(id2)));
  // Delete item 3 from the stream.Now consumer 1 has PEL that is empty.
  // Try to use consumer 2 to claim the deleted item 3 from the PEL
  // of consumer 1, this should return nil
  AdvanceTime(200);

  ASSERT_THAT(Run({"XDEL", "mystream", id4}), IntArg(1));

  // id1 and id3 are self - claimed here but not id2 and id4('count' is default 100)
  // we also test the JUSTID modifier here.note that, when using JUSTID,
  // deleted entries are returned in reply(consistent with XCLAIM).
  resp = Run({"XAUTOCLAIM", "mystream", "mygroup", "consumer2", "10", "-", "JUSTID"});
  ASSERT_THAT(resp, RespElementsAre("0-0", RespElementsAre(id1, id3), RespElementsAre(id4)));
}

TEST_F(StreamFamilyTest, AutoClaimDelCount) {
  Run({"xadd", "x", "1-0", "f", "v"});
  Run({"xadd", "x", "2-0", "f", "v"});
  Run({"xadd", "x", "3-0", "f", "v"});
  Run({"XGROUP", "CREATE", "x", "grp", "0"});
  auto resp = Run({"XREADGROUP", "GROUP", "grp", "Alice", "STREAMS", "x", ">"});

  auto m1 = RespElementsAre("1-0", _);
  auto m2 = RespElementsAre("2-0", _);
  auto m3 = RespElementsAre("3-0", _);
  EXPECT_THAT(resp, RespElementsAre("x", RespElementsAre(m1, m2, m3)));

  EXPECT_THAT(Run({"XDEL", "x", "1-0"}), IntArg(1));
  EXPECT_THAT(Run({"XDEL", "x", "2-0"}), IntArg(1));

  resp = Run({"XAUTOCLAIM", "x", "grp", "Bob", "0", "0-0", "COUNT", "1"});
  EXPECT_THAT(resp, RespElementsAre("2-0", ArrLen(0), RespElementsAre("1-0")));

  resp = Run({"XAUTOCLAIM", "x", "grp", "Bob", "0", "2-0", "COUNT", "1"});
  EXPECT_THAT(resp, RespElementsAre("3-0", ArrLen(0), RespElementsAre("2-0")));

  resp = Run({"XAUTOCLAIM", "x", "grp", "Bob", "0", "3-0", "COUNT", "1"});
  EXPECT_THAT(resp, RespElementsAre(
                        "0-0", RespElementsAre(RespElementsAre("3-0", RespElementsAre("f", "v"))),
                        ArrLen(0)));
  resp = Run({"xpending", "x", "grp", "-", "+", "10", "Alice"});
  EXPECT_THAT(resp, ArrLen(0));

  resp = Run({"XAUTOCLAIM", "x", "grp", "Bob", "0", "3-0", "COUNT", "704505322"});
  EXPECT_THAT(resp, ErrArg("COUNT"));
}

TEST_F(StreamFamilyTest, XAddMaxSeq) {
  Run({"XADD", "x", "1-18446744073709551615", "f1", "v1"});
  auto resp = Run({"XADD", "x", "1-*", "f2", "v2"});
  EXPECT_THAT(resp, ErrArg("The ID specified in XADD is equal or smaller"));
}

TEST_F(StreamFamilyTest, XsetIdSmallerMaxDeleted) {
  Run({"XADD", "x", "1-1", "a", "1"});
  Run({"XADD", "x", "1-2", "b", "2"});
  Run({"XADD", "x", "1-3", "c", "3"});
  Run({"XDEL", "x", "1-2"});
  Run({"XDEL", "x", "1-3"});
  auto resp = Run({"XINFO", "stream", "x"});
  ASSERT_THAT(resp, ArgType(RespExpr::ARRAY));
  auto vec = resp.GetVec();
  string max_del_id;
  for (unsigned i = 0; i < vec.size(); i += 2) {
    if (vec[i] == "max-deleted-entry-id") {
      max_del_id = vec[i + 1].GetString();
      break;
    }
  }
  EXPECT_EQ(max_del_id, "1-3");

  resp = Run({"XSETID", "x", "1-2"});
  ASSERT_THAT(resp, ErrArg("smaller"));
}

TEST_F(StreamFamilyTest, SeenActiveTime) {
  TEST_current_time_ms = 1000;

  Run({"XGROUP", "CREATE", "mystream", "mygroup", "$", "MKSTREAM"});
  Run({"XREADGROUP", "GROUP", "mygroup", "Alice", "COUNT", "1", "STREAMS", "mystream", ">"});
  AdvanceTime(100);
  auto resp = Run({"xinfo", "consumers", "mystream", "mygroup"});
  EXPECT_THAT(resp, RespElementsAre("name", "Alice", "pending", IntArg(0), "idle", IntArg(100),
                                    "inactive", IntArg(-1)));

  Run({"XADD", "mystream", "*", "f", "v"});
  Run({"XREADGROUP", "GROUP", "mygroup", "Alice", "COUNT", "1", "STREAMS", "mystream", ">"});
  AdvanceTime(50);

  resp = Run({"xinfo", "consumers", "mystream", "mygroup"});
  EXPECT_THAT(resp, RespElementsAre("name", "Alice", "pending", IntArg(1), "idle", IntArg(50),
                                    "inactive", IntArg(50)));
  AdvanceTime(100);
  resp = Run({"XREADGROUP", "GROUP", "mygroup", "Alice", "COUNT", "1", "STREAMS", "mystream", ">"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL_ARRAY));

  resp = Run({"xinfo", "consumers", "mystream", "mygroup"});

  // Idle is 0 because XREADGROUP just run, but inactive continues clocking because nothing was
  // read.
  EXPECT_THAT(resp, RespElementsAre("name", "Alice", "pending", IntArg(1), "idle", IntArg(0),
                                    "inactive", IntArg(150)));

  // Serialize/deserialize.
  resp = Run({"XINFO", "STREAM", "mystream", "FULL"});
  auto groups = resp.GetVec()[17];
  auto consumers = groups.GetVec()[0].GetVec()[13].GetVec()[0];
  EXPECT_THAT(consumers, RespElementsAre("name", "Alice", "seen-time", IntArg(1250), "active-time",
                                         IntArg(1100), "pel-count", IntArg(1), "pending", _));

  absl::SetFlag(&FLAGS_stream_rdb_encode_v2, true);
  resp = Run({"DUMP", "mystream"});
  Run({"del", "mystream"});
  resp = Run({"RESTORE", "mystream", "0", resp.GetString()});
  EXPECT_EQ(resp, "OK");
  resp = Run({"XINFO", "STREAM", "mystream", "FULL"});
  groups = resp.GetVec()[17];
  consumers = groups.GetVec()[0].GetVec()[13].GetVec()[0];
  EXPECT_THAT(consumers, RespElementsAre("name", "Alice", "seen-time", IntArg(1250), "active-time",
                                         IntArg(1100), "pel-count", IntArg(1), "pending", _));
}

}  // namespace dfly
