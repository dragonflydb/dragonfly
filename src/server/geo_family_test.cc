// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/geo_family.h"

#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"
#include "server/command_registry.h"
#include "server/test_utils.h"

using namespace testing;
using namespace std;
using namespace util;

namespace dfly {

class GeoFamilyTest : public BaseFamilyTest {
 protected:
};

TEST_F(GeoFamilyTest, GeoAdd) {
  EXPECT_EQ(2, CheckedInt({"geoadd", "Sicily", "13.361389", "38.115556", "Palermo", "15.087269",
                           "37.502669", "Catania"}));
  EXPECT_EQ(0, CheckedInt({"geoadd", "Sicily", "13.361389", "38.115556", "Palermo", "15.087269",
                           "37.502669", "Catania"}));
  auto resp = Run({"geohash", "Sicily", "Palermo", "Catania"});
  EXPECT_THAT(resp, RespArray(ElementsAre("sqc8b49rny0", "sqdtr74hyu0")));
}

TEST_F(GeoFamilyTest, GeoAddOptions) {
  EXPECT_EQ(2, CheckedInt({"geoadd", "Sicily", "13.361389", "38.115556", "Palermo", "15.087269",
                           "37.502669", "Catania"}));

  // add 1 + update 1 + XX
  EXPECT_EQ(0, CheckedInt({"geoadd", "Sicily", "XX", "15.361389", "38.115556", "Palermo",
                           "15.554167", "38.193611", "Messina"}));
  auto resp = Run({"geopos", "Sicily", "Palermo", "Messina"});
  EXPECT_THAT(
      resp, RespArray(ElementsAre(RespArray(ElementsAre("15.361389219760895", "38.1155563954963")),
                                  ArgType(RespExpr::NIL))));

  // add 1 + update 1 + NX
  EXPECT_EQ(1, CheckedInt({"geoadd", "Sicily", "NX", "18.361389", "38.115556", "Palermo", "15.2875",
                           "37.069167", "Syracuse"}));
  resp = Run({"geopos", "Sicily", "Palermo", "Syracuse"});
  EXPECT_THAT(resp, RespArray(ElementsAre(
                        RespArray(ElementsAre("15.361389219760895", "38.1155563954963")),
                        RespArray(ElementsAre("15.287499725818634", "37.06916773705567")))));

  // add 1 + update 1 CH
  EXPECT_EQ(2, CheckedInt({"geoadd", "Sicily", "CH", "18.361389", "38.115556", "Palermo",
                           "12.434167", "37.798056", "Marsala"}));
  resp = Run({"geopos", "Sicily", "Palermo", "Marsala"});
  EXPECT_THAT(resp, RespArray(ElementsAre(
                        RespArray(ElementsAre("18.361386358737946", "38.1155563954963")),
                        RespArray(ElementsAre("12.43416577577591", "37.7980572230775")))));

  // update 1 + CH + XX
  EXPECT_EQ(1, CheckedInt({"geoadd", "Sicily", "CH", "XX", "10.361389", "38.115556", "Palermo"}));
  resp = Run({"geopos", "Sicily", "Palermo"});
  EXPECT_THAT(resp, RespArray(ElementsAre("10.361386835575104", "38.1155563954963")));

  // add 1 + CH + NX
  EXPECT_EQ(1, CheckedInt({"geoadd", "Sicily", "CH", "NX", "14.25", "37.066667", "Gela"}));
  resp = Run({"geopos", "Sicily", "Gela"});
  EXPECT_THAT(resp, RespArray(ElementsAre("14.249999821186066", "37.06666596727141")));

  // add 1 + XX + NX
  resp = Run({"geoadd", "Sicily", "XX", "NX", "14.75", "36.933333", "Ragusa"});
  EXPECT_THAT(resp, ErrArg("XX and NX options at the same time are not compatible"));

  // incorrect number of args
  resp = Run({"geoadd", "Sicily", "14.75", "36.933333", "Ragusa", "10.23"});
  EXPECT_THAT(resp, ErrArg("syntax error"));
}

TEST_F(GeoFamilyTest, GeoPos) {
  EXPECT_EQ(1, CheckedInt({"geoadd", "Sicily", "13.361389", "38.115556", "Palermo"}));
  auto resp = Run({"geopos", "Sicily", "Palermo", "NonExisting"});
  EXPECT_THAT(
      resp, RespArray(ElementsAre(RespArray(ElementsAre("13.361389338970184", "38.1155563954963")),
                                  ArgType(RespExpr::NIL))));
}

TEST_F(GeoFamilyTest, GeoPosWrongType) {
  Run({"set", "x", "value"});
  EXPECT_THAT(Run({"geopos", "x", "Sicily", "Palermo"}), ErrArg("WRONGTYPE"));
}

TEST_F(GeoFamilyTest, GeoDist) {
  EXPECT_EQ(2, CheckedInt({"geoadd", "Sicily", "13.361389", "38.115556", "Palermo", "15.087269",
                           "37.502669", "Catania"}));
  auto resp = Run({"geodist", "Sicily", "Palermo", "Catania"});
  EXPECT_EQ(resp, "166274.15156960033");

  resp = Run({"geodist", "Sicily", "Palermo", "Catania", "km"});
  EXPECT_EQ(resp, "166.27415156960032");

  resp = Run({"geodist", "Sicily", "Palermo", "Catania", "MI"});
  EXPECT_EQ(resp, "103.31822459492733");

  resp = Run({"geodist", "Sicily", "Palermo", "Catania", "FT"});
  EXPECT_EQ(resp, "545518.8699790037");

  resp = Run({"geodist", "Sicily", "Foo", "Bar"});
  EXPECT_THAT(resp, ArgType(RespExpr::NIL));
}

TEST_F(GeoFamilyTest, GeoSearch) {
  EXPECT_EQ(10, CheckedInt({"geoadd",  "Europe",    "13.4050", "52.5200", "Berlin",   "3.7038",
                            "40.4168", "Madrid",    "9.1427",  "38.7369", "Lisbon",   "2.3522",
                            "48.8566", "Paris",     "16.3738", "48.2082", "Vienna",   "4.8952",
                            "52.3702", "Amsterdam", "10.7522", "59.9139", "Oslo",     "23.7275",
                            "37.9838", "Athens",    "19.0402", "47.4979", "Budapest", "6.2603",
                            "53.3498", "Dublin"}));

  auto resp = Run({"GEOSEARCH", "Europe", "FROMLONLAT", "13.4050", "52.5200", "BYRADIUS", "500",
                   "KM", "WITHCOORD", "WITHDIST", "WITHHASH"});
  EXPECT_THAT(
      resp,
      RespArray(ElementsAre(
          RespArray(ElementsAre("Berlin", DoubleArg(0.00017343178521311378), "3673983950397063",
                                RespArray(ElementsAre(DoubleArg(13.4050), DoubleArg(52.5200))))),
          RespArray(ElementsAre("Dublin", DoubleArg(487.5619030644293), "3678981558208417",
                                RespArray(ElementsAre(DoubleArg(6.2603), DoubleArg(53.3498))))))));

  resp = Run({"GEOSEARCH", "invalid_key", "FROMMEMBER", "Madrid", "BYRADIUS", "700", "KM",
              "WITHCOORD", "WITHDIST"});
  EXPECT_THAT(resp.GetVec().empty(), true);

  resp = Run({"GEOSEARCH", "Europe", "FROMMEMBER", "invalid_member", "BYRADIUS", "700", "KM",
              "WITHCOORD", "WITHDIST"});
  EXPECT_THAT(resp, ErrArg("could not decode requested zset member"));

  resp = Run({"GEOSEARCH", "America", "FROMLONLAT", "13.4050", "52.5200", "BYBOX", "1000", "1000",
              "KM", "WITHCOORD", "WITHDIST"});
  EXPECT_THAT(resp.GetVec().empty(), true);

  resp = Run({"GEOSEARCH", "Europe", "FROMLONLAT", "130.4050", "52.5200", "BYBOX", "10", "10", "KM",
              "WITHCOORD", "WITHDIST"});
  EXPECT_THAT(resp.GetVec().empty(), true);

  resp = Run({"GEOSEARCH", "Europe", "FROMLONLAT", "13.4050", "52.5200", "BYBOX", "1000", "1000",
              "KM", "WITHCOORD", "WITHDIST"});
  EXPECT_THAT(
      resp,
      RespArray(ElementsAre(
          RespArray(ElementsAre("Vienna", DoubleArg(523.6926930553866),
                                RespArray(ElementsAre(DoubleArg(16.3738), DoubleArg(48.2082))))),
          RespArray(ElementsAre("Berlin", DoubleArg(0.00017343178521311378),
                                RespArray(ElementsAre(DoubleArg(13.4050), DoubleArg(52.5200))))),
          RespArray(ElementsAre("Dublin", DoubleArg(487.5619030644293),
                                RespArray(ElementsAre(DoubleArg(6.2603), DoubleArg(53.3498))))))));

  resp = Run({"GEOSEARCH", "Europe", "FROMLONLAT", "13.4050", "52.5200", "BYRADIUS", "500", "KM",
              "COUNT", "3", "WITHCOORD", "WITHDIST"});
  EXPECT_THAT(
      resp,
      RespArray(ElementsAre(
          RespArray(ElementsAre("Berlin", DoubleArg(0.00017343178521311378),
                                RespArray(ElementsAre(DoubleArg(13.4050), DoubleArg(52.5200))))),
          RespArray(ElementsAre("Dublin", DoubleArg(487.5619030644293),
                                RespArray(ElementsAre(DoubleArg(6.2603), DoubleArg(53.3498))))))));

  resp = Run({"GEOSEARCH", "Europe", "FROMLONLAT", "13.4050", "52.5200", "BYRADIUS", "500", "KM",
              "DESC", "WITHCOORD", "WITHDIST"});
  EXPECT_THAT(
      resp,
      RespArray(ElementsAre(
          RespArray(ElementsAre("Dublin", DoubleArg(487.5619030644293),
                                RespArray(ElementsAre(DoubleArg(6.2603), DoubleArg(53.3498))))),
          RespArray(ElementsAre("Berlin", DoubleArg(0.00017343178521311378),
                                RespArray(ElementsAre(DoubleArg(13.4050), DoubleArg(52.5200))))))));

  resp = Run({"GEOSEARCH", "Europe", "FROMMEMBER", "Madrid", "BYRADIUS", "700", "KM", "WITHCOORD",
              "WITHDIST"});
  EXPECT_THAT(
      resp,
      RespArray(ElementsAre(
          RespArray(ElementsAre("Madrid", "0",
                                RespArray(ElementsAre(DoubleArg(3.7038), DoubleArg(40.4168))))),
          RespArray(ElementsAre("Lisbon", DoubleArg(502.20769462704084),
                                RespArray(ElementsAre(DoubleArg(9.1427), DoubleArg(38.7369))))))));

  resp = Run({"GEOSEARCH", "Europe", "FROMMEMBER", "Madrid", "BYRADIUS", "700", "KM"});
  EXPECT_THAT(resp, RespArray(ElementsAre("Madrid", "Lisbon")));
}

TEST_F(GeoFamilyTest, GeoRadiusByMember) {
  EXPECT_EQ(10, CheckedInt({"geoadd",  "Europe",    "13.4050", "52.5200", "Berlin",   "3.7038",
                            "40.4168", "Madrid",    "9.1427",  "38.7369", "Lisbon",   "2.3522",
                            "48.8566", "Paris",     "16.3738", "48.2082", "Vienna",   "4.8952",
                            "52.3702", "Amsterdam", "10.7522", "59.9139", "Oslo",     "23.7275",
                            "37.9838", "Athens",    "19.0402", "47.4979", "Budapest", "6.2603",
                            "53.3498", "Dublin"}));

  auto resp = Run({"GEORADIUSBYMEMBER", "invalid_key", "Madrid", "900", "KM"});
  EXPECT_THAT(resp.GetVec().empty(), true);

  resp = Run({"GEORADIUSBYMEMBER", "invalid_key", "Madrid", "900", "KM", "STORE", "store_key"});
  EXPECT_THAT(resp.GetVec().empty(), true);

  resp = Run({"GEORADIUSBYMEMBER", "Europe", "invalid_mem", "900", "KM", "STORE", "store_key"});
  EXPECT_THAT(resp, ErrArg("could not decode requested zset member"));

  resp = Run({"GEORADIUSBYMEMBER", "Europe", "Madrid", "700", "KM", "WITHCOORD", "WITHDIST"});
  EXPECT_THAT(
      resp,
      RespArray(ElementsAre(
          RespArray(ElementsAre(
              "Madrid", "0", RespArray(ElementsAre("3.7038007378578186", "40.416799319406216")))),
          RespArray(
              ElementsAre("Lisbon", "502.20769462704084",
                          RespArray(ElementsAre("9.142698347568512", "38.736900197448534")))))));

  EXPECT_EQ(
      2, CheckedInt({"GEORADIUSBYMEMBER", "Europe", "Madrid", "700", "KM", "STORE", "store_key"}));
  resp = Run({"ZRANGE", "store_key", "0", "-1"});
  EXPECT_THAT(resp, RespArray(ElementsAre("Madrid", "Lisbon")));
  resp = Run({"ZRANGE", "store_key", "0", "-1", "WITHSCORES"});
  EXPECT_THAT(resp,
              RespArray(ElementsAre("Madrid", "3471766229222696", "Lisbon", "3473121093062745")));

  EXPECT_EQ(2, CheckedInt({"GEORADIUSBYMEMBER", "Europe", "Madrid", "700", "KM", "STOREDIST",
                           "store_dist_key"}));
  resp = Run({"ZRANGE", "store_dist_key", "0", "-1", "WITHSCORES"});
  EXPECT_THAT(resp, RespArray(ElementsAre("Madrid", "0", "Lisbon", "502.20769462704084")));

  resp = Run(
      {"GEORADIUSBYMEMBER", "Europe", "Madrid", "900", "KM", "STORE", "store_key", "WITHCOORD"});
  EXPECT_THAT(resp, ErrArg("ERR STORE option in GEORADIUS is not compatible with WITHDIST, "
                           "WITHHASH and WITHCOORDS options"));
}

}  // namespace dfly
