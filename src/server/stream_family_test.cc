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

TEST_F(StreamFamilyTest, Issue854) {
  auto resp = Run({"xgroup", "help"});
  EXPECT_THAT(resp, ArgType(RespExpr::ARRAY));

  resp = Run({"eval", "redis.call('xgroup', 'help')", "0"});
  EXPECT_THAT(resp, ErrArg("is not allowed"));
}

}  // namespace dfly
