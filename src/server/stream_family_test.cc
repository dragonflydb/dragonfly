// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/stream_family.h"

#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"
#include "server/command_registry.h"
#include "server/test_utils.h"

using namespace testing;
using namespace std;
using namespace util;

namespace dfly {

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
  EXPECT_THAT(resp, ErrArg("no such key"));
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
  Run({"xadd", "key", "1-*", "f1", "v1"});
  auto resp = Run({"xgroup", "create", "key", "grname", "1"});
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

  // Receive all records from both streams.
  auto resp = Run({"xread", "streams", "foo", "bar", "0", "0"});
  EXPECT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec()[0].GetVec(), ElementsAre("foo", ArrLen(3)));
  EXPECT_THAT(resp.GetVec()[1].GetVec(), ElementsAre("bar", ArrLen(1)));

  // Order of the requested streams is maintained.
  resp = Run({"xread", "streams", "bar", "foo", "0", "0"});
  EXPECT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec()[0].GetVec(), ElementsAre("bar", ArrLen(1)));
  EXPECT_THAT(resp.GetVec()[1].GetVec(), ElementsAre("foo", ArrLen(3)));

  // Limit count.
  resp = Run({"xread", "count", "1", "streams", "foo", "bar", "0", "0"});
  EXPECT_THAT(resp.GetVec()[0].GetVec(), ElementsAre("foo", ArrLen(1)));
  EXPECT_THAT(resp.GetVec()[1].GetVec(), ElementsAre("bar", ArrLen(1)));

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

  Run({"xadd", "mystream", "k1", "v1"});
  Run({"xadd", "mystream", "k2", "v2"});
  Run({"xadd", "mystream", "k3", "v3"});

  Run({"xgroup", "create", "foo", "group", "0"});
  Run({"xgroup", "create", "bar", "group", "0"});

  // consumer PEL is empty, so resp should have empty list
  auto resp = Run({"xreadgroup", "group", "group", "alice", "streams", "foo", "0"});
  EXPECT_THAT(resp, ArrLen(0));

  // should return unread entries with key "foo"
  resp = Run({"xreadgroup", "group", "group", "alice", "streams", "foo", ">"});
  // only "foo" key entries are read
  EXPECT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec()[1], ArrLen(3));

  Run({"xadd", "foo", "1-*", "k5", "v5"});
  resp = Run({"xreadgroup", "group", "group", "alice", "streams", "bar", "foo", ">", ">"});
  EXPECT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec()[0].GetVec(), ElementsAre("bar", ArrLen(1)));
  EXPECT_THAT(resp.GetVec()[0].GetVec()[1].GetVec()[0].GetVec(), ElementsAre("1-0", ArrLen(2)));
  EXPECT_THAT(resp.GetVec()[1].GetVec(), ElementsAre("foo", ArrLen(1)));
  EXPECT_THAT(resp.GetVec()[1].GetVec()[1].GetVec()[0].GetVec(), ElementsAre("1-3", ArrLen(2)));

  // now we can specify id for "foo" and it fetches from alice's consumer PEL
  resp = Run({"xreadgroup", "group", "group", "alice", "streams", "foo", "0"});
  EXPECT_THAT(resp.GetVec()[1], ArrLen(4));

  // now ">" gives nil
  resp = Run({"xreadgroup", "group", "group", "alice", "streams", "foo", ">"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL_ARRAY));

  // count limits the fetched entries
  resp = Run(
      {"xreadgroup", "group", "group", "alice", "count", "2", "streams", "foo", "bar", "0", "0"});
  EXPECT_THAT(resp.GetVec()[0].GetVec(), ElementsAre("foo", ArrLen(2)));
  EXPECT_THAT(resp.GetVec()[1].GetVec(), ElementsAre("bar", ArrLen(1)));

  // bob will not get entries of alice
  resp = Run({"xreadgroup", "group", "group", "bob", "streams", "foo", "0"});
  EXPECT_THAT(resp, ArrLen(0));

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
  EXPECT_THAT(resp, ArrLen(0));

  // No Group
  resp = Run({"xreadgroup", "group", "nogroup", "alice", "streams", "foo", "0"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL_ARRAY));

  // '>' gives the null array result if group doesn't exist
  resp = Run({"xreadgroup", "group", "group", "alice", "streams", "mystream", ">"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL_ARRAY));

  Run({"xadd", "foo", "1-*", "k7", "v7"});
  resp = Run({"xreadgroup", "group", "group", "alice", "streams", "mystream", "foo", ">", ">"});
  // Only entries of 'foo' is read
  EXPECT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec(), ElementsAre("foo", ArrLen(1)));
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
  resp = Run({"xread", "block", "1", "streams", "foo", "bar", "$", "$"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL_ARRAY));

  // Run XREAD BLOCK from 2 fibers.
  RespExpr resp0, resp1;
  auto fb0 = pp_->at(0)->LaunchFiber(Launch::dispatch, [&] {
    resp0 = Run({"xread", "block", "0", "streams", "foo", "bar", "$", "$"});
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

TEST_F(StreamFamilyTest, XReadGroupBlock) {
  Run({"xadd", "foo", "1-*", "k1", "v1"});
  Run({"xadd", "foo", "1-*", "k2", "v2"});
  Run({"xadd", "foo", "1-*", "k3", "v3"});
  Run({"xadd", "bar", "1-*", "k4", "v4"});

  Run({"xgroup", "create", "foo", "group", "0"});
  Run({"xgroup", "create", "bar", "group", "0"});

  // Receive all records from both streams.
  auto resp = Run(
      {"xreadgroup", "group", "group", "alice", "block", "100", "streams", "foo", "bar", ">", ">"});
  EXPECT_THAT(resp, ArrLen(2));
  EXPECT_THAT(resp.GetVec()[0].GetVec(), ElementsAre("foo", ArrLen(3)));
  EXPECT_THAT(resp.GetVec()[1].GetVec(), ElementsAre("bar", ArrLen(1)));

  // Timeout
  resp = Run(
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

  resp = pp_->at(1)->Await([&] { return Run("xadd", {"xadd", "foo", "1-*", "k5", "v5"}); });

  fb0.Join();
  fb1.Join();

  // Both xread calls should have been unblocked.
  //
  // Note when the response has length 1, Run returns the first element.
  EXPECT_THAT(resp0.GetVec(), ElementsAre("foo", ArrLen(1)));
  EXPECT_THAT(resp1.GetVec(), ElementsAre("foo", ArrLen(1)));
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
  resp = Run({"xread", "count", "invalid", "streams", "s1", "s2", "s3", "0", "0"});
  EXPECT_THAT(resp, ErrArg("syntax error"));

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
  EXPECT_THAT(resp, ErrArg("syntax error"));
}

TEST_F(StreamFamilyTest, Issue854) {
  auto resp = Run({"xgroup", "help"});
  EXPECT_THAT(resp, ArgType(RespExpr::ARRAY));

  resp = Run({"eval", "redis.call('xgroup', 'help')", "0"});
  EXPECT_THAT(resp, ErrArg("is not allowed"));
}

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
                          "last-delivered-id", "0-0", "entries-read", IntArg(0), "lag", IntArg(0)));

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
  // TODO : xack command not available now, uncomment this test when available
  // Run({"xack", "mystream", "mygroup", "1-0"});
  // Run({"xack", "mystream", "mygroup", "2-0"});
  // resp = Run({"xinfo", "groups", "mystream"});
  // EXPECT_THAT(resp.GetVec(), ElementsAre("name", "mygroup", "consumers", IntArg(2), "pending",
  // IntArg(0), "last-delivered-id", "2-0", "entries-read", IntArg(2), "lag", IntArg(0)));
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
  EXPECT_THAT(resp.GetVec()[0], ArrLen(6));
  EXPECT_THAT(resp.GetVec()[1], ArrLen(6));
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
}  // namespace dfly
