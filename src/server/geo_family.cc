// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/strings/ascii.h>

extern "C" {
#include "redis/geo.h"
#include "redis/geohash.h"
#include "redis/geohash_helper.h"
#include "redis/redis_aux.h"
#include "redis/util.h"
#include "redis/zmalloc.h"
}

#include "base/logging.h"
#include "core/sorted_map.h"
#include "facade/cmd_arg_parser.h"
#include "facade/error.h"
#include "server/acl/acl_commands_def.h"
#include "server/command_families.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/family_utils.h"
#include "server/transaction.h"
#include "server/zset_family.h"

namespace rng = std::ranges;

namespace dfly {

using namespace std;
using namespace facade;
namespace {

using CI = CommandId;

enum Errors {
  INVALID_LONG_LAT = CmdArgParser::CUSTOM_ERROR,
};

const char kNxXxErr[] = "XX and NX options at the same time are not compatible";
constexpr char kFromMemberLonglatErr[] =
    "FROMMEMBER and FROMLONLAT options at the same time are not compatible";
constexpr char kByRadiusBoxErr[] = "BYRADIUS and BYBOX options at the same time are not compatible";
constexpr char kAscDescErr[] = "ASC and DESC options at the same time are not compatible";
constexpr char kStoreTypeErr[] = "STORE and STOREDIST options at the same time are not compatible";
const char kStoreCompatRadErr[] =
    "STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORDS options";
const char kStoreCompatByMemberErr[] =
    "STORE option in GEORADIUSBYMEMBER is not compatible with WITHDIST, WITHHASH and WITHCOORDS "
    "options";
const char kMemberNotFound[] = "could not decode requested zset member";
const char kInvalidUnit[] = "unsupported unit provided. please use M, KM, FT, MI";
const char kCountError[] = "ERR COUNT must be > 0";
constexpr string_view kGeoAlphabet = "0123456789bcdefghjkmnpqrstuvwxyz"sv;

using MScoreResponse = std::vector<std::optional<double>>;

using ScoredMember = std::pair<std::string, double>;
using ScoredArray = std::vector<ScoredMember>;
using ScoredMemberView = std::pair<double, std::string_view>;
using ScoredMemberSpan = absl::Span<const ScoredMemberView>;

struct GeoPoint {
  double longitude;
  double latitude;
  double dist;
  double score;
  std::string member;
  GeoPoint() : longitude(0.0), latitude(0.0), dist(0.0), score(0.0){};
  GeoPoint(double _longitude, double _latitude, double _dist, double _score,
           const std::string& _member)
      : longitude(_longitude), latitude(_latitude), dist(_dist), score(_score), member(_member){};
};
using GeoArray = std::vector<GeoPoint>;

enum class Sorting { kUnsorted, kAsc, kDesc };
enum class GeoStoreType { kNoStore, kStoreHash, kStoreDist };
enum class GeoSearchSource { kMember, kLonLat };
struct GeoSearchOpts {
  GeoSearchOpts() = default;

  explicit GeoSearchOpts(bool allow_store, string_view count_err = {})
      : allow_store(allow_store), count_err(count_err) {
  }

  double conversion = 0;
  uint64_t count = std::numeric_limits<uint64_t>::max();
  Sorting sorting = Sorting::kUnsorted;
  bool any = 0;
  bool withdist = 0;
  bool withcoord = 0;
  bool withhash = 0;
  GeoStoreType store = GeoStoreType::kNoStore;
  string_view store_key;
  bool allow_store = false;
  string_view count_err;

  bool HasWithStatement() const {
    return withdist || withcoord || withhash;
  }
};

struct GeoSearchParse {
  GeoSearchOpts geo_ops;
  optional<GeoSearchSource> source;
  string_view member;
  GeoShape shape{};
};

bool ValidateLongLat(double longitude, double latitude) {
  return std::isfinite(longitude) && std::isfinite(latitude) && longitude >= GEO_LONG_MIN &&
         longitude <= GEO_LONG_MAX && latitude >= GEO_LAT_MIN && latitude <= GEO_LAT_MAX;
}

void ParseLongLat(CmdArgParser* parser, double lonlat[2]) {
  std::tie(lonlat[0], lonlat[1]) = parser->Next<double, double>();

  if (!ValidateLongLat(lonlat[0], lonlat[1])) {
    parser->Report(Errors::INVALID_LONG_LAT);
  }
}

bool ParseLongLat(string_view lon, string_view lat, std::pair<double, double>* res) {
  if (!ParseDouble(lon, &res->first))
    return false;

  if (!ParseDouble(lat, &res->second))
    return false;

  return ValidateLongLat(res->first, res->second);
}

bool ScoreToLongLat(const std::optional<double>& val, double* xy) {
  if (!val.has_value())
    return false;

  double score = *val;

  GeoHashBits hash = {.bits = (uint64_t)score, .step = GEO_STEP_MAX};

  return geohashDecodeToLongLatType(hash, xy) == 1;
}

bool ToAsciiGeoHash(const std::optional<double>& val, array<char, 12>* buf) {
  if (!val.has_value())
    return false;

  double score = *val;

  GeoHashBits hash = {.bits = (uint64_t)score, .step = GEO_STEP_MAX};

  double xy[2];
  if (!geohashDecodeToLongLatType(hash, xy)) {
    return false;
  }

  /* Re-encode */
  GeoHashRange r[2];
  r[0].min = -180;
  r[0].max = 180;
  r[1].min = -90;
  r[1].max = 90;

  geohashEncode(&r[0], &r[1], xy[0], xy[1], 26, &hash);

  for (int i = 0; i < 11; i++) {
    int idx;
    if (i == 10) {
      /* We have just 52 bits, but the API used to output
       * an 11 bytes geohash. For compatibility we assume
       * zero. */
      idx = 0;
    } else {
      idx = (hash.bits >> (52 - ((i + 1) * 5))) % kGeoAlphabet.size();
    }
    (*buf)[i] = kGeoAlphabet[idx];
  }
  (*buf)[11] = '\0';

  return true;
}

// Unit token (M/KM/FT/MI) -> meters-per-unit factor.
double ParseGeoUnit(std::string_view arg, facade::RuleError& err) {
  const string unit = absl::AsciiStrToUpper(arg);
  if (unit == "M")
    return 1;
  if (unit == "KM")
    return 1000;
  if (unit == "FT")
    return 0.3048;
  if (unit == "MI")
    return 1609.34;
  err = {true, kInvalidUnit};
  return -1;
}

void ParseCircularShape(CmdArgParser* parser, GeoShape* shape, GeoSearchOpts* geo_ops) {
  shape->t.radius = parser->Next<double>();
  geo_ops->conversion = shape->conversion = parser->Next(ParseGeoUnit);
  shape->type = CIRCULAR_TYPE;
}

bool HandleGeoParserFinalize(const GeoShape& shape, CmdArgParser* parser,
                             CommandContext* cmd_cntx) {
  if (parser->Finalize()) {
    return false;
  }

  auto error = parser->TakeError();
  // INVALID_LONG_LAT is a geo-specific code reported without a message (it aliases CUSTOM_ERROR);
  // a real custom message (e.g. from a parser or ReportCustom) is surfaced verbatim via
  // MakeReply().
  if (error.custom_msg.empty() && error.type == Errors::INVALID_LONG_LAT) {
    string err =
        absl::StrCat("-ERR invalid longitude,latitude pair ", shape.xy[0], ",", shape.xy[1]);
    cmd_cntx->SendError(err, kSyntaxErrType);
    return true;
  }
  cmd_cntx->SendError(error.MakeReply());
  return true;
}

void ParseCount(CmdArgParser* parser, GeoSearchOpts* geo_ops, string_view err_msg = {}) {
  geo_ops->count = parser->Next<uint64_t>(err_msg);
  geo_ops->any = parser->Check("ANY");
}

void ParseGeoResultCount(CmdArgParser* parser, GeoSearchOpts* geo_ops) {
  ParseCount(parser, geo_ops, geo_ops->count_err);
}

void ParseGeoResultOptions(CmdArgParser* parser, GeoSearchOpts* geo_ops) {
  static constexpr auto kGrammar = Compile(Options(
      OneOf(kAscDescErr,
            Map(&GeoSearchOpts::sorting, "ASC", Sorting::kAsc, "DESC", Sorting::kDesc)),
      Action("COUNT", ParseGeoResultCount), Exist("WITHCOORD", &GeoSearchOpts::withcoord),
      Exist("WITHDIST", &GeoSearchOpts::withdist), Exist("WITHHASH", &GeoSearchOpts::withhash),
      If(&GeoSearchOpts::allow_store,
         OneOf(kStoreTypeErr,
               TagValue("STORE", &GeoSearchOpts::store, GeoStoreType::kStoreHash,
                        &GeoSearchOpts::store_key),
               TagValue("STOREDIST", &GeoSearchOpts::store, GeoStoreType::kStoreDist,
                        &GeoSearchOpts::store_key)))));
  kGrammar.Apply(parser, geo_ops);
}

void ParseGeoSearchFromMember(CmdArgParser* parser, GeoSearchParse* opts) {
  opts->source = GeoSearchSource::kMember;
  opts->member = parser->Next<string_view>();
}

void ParseLongLat(CmdArgParser* parser, GeoSearchParse* opts) {
  opts->source = GeoSearchSource::kLonLat;
  ParseLongLat(parser, opts->shape.xy);
}

void ParseGeoSearchByRadius(CmdArgParser* parser, GeoSearchParse* opts) {
  GeoShape& shape = opts->shape;
  shape.t.radius = parser->Next<double>();
  opts->geo_ops.conversion = shape.conversion = parser->Next(ParseGeoUnit);
  shape.type = CIRCULAR_TYPE;
}

void ParseGeoSearchByBox(CmdArgParser* parser, GeoSearchParse* opts) {
  GeoShape& shape = opts->shape;
  std::tie(shape.t.r.width, shape.t.r.height) = parser->Next<double, double>();
  opts->geo_ops.conversion = shape.conversion = parser->Next(ParseGeoUnit);
  shape.type = RECTANGLE_TYPE;
}

void ParseGeoSearchCount(CmdArgParser* parser, GeoSearchParse* opts) {
  ParseCount(parser, &opts->geo_ops);
}

constexpr auto kGeoSearchGrammar = Compile(Options(
    OneOf(kFromMemberLonglatErr, Action("FROMMEMBER", ParseGeoSearchFromMember),
          Action<GeoSearchParse>("FROMLONLAT", ParseLongLat)),
    OneOf(kByRadiusBoxErr, Action("BYRADIUS", ParseGeoSearchByRadius),
          Action("BYBOX", ParseGeoSearchByBox)),
    Into(&GeoSearchParse::geo_ops, OneOf(kAscDescErr, Map(&GeoSearchOpts::sorting, "ASC",
                                                          Sorting::kAsc, "DESC", Sorting::kDesc))),
    Action("COUNT", ParseGeoSearchCount),
    Into(&GeoSearchParse::geo_ops, Exist("WITHCOORD", &GeoSearchOpts::withcoord)),
    Into(&GeoSearchParse::geo_ops, Exist("WITHDIST", &GeoSearchOpts::withdist)),
    Into(&GeoSearchParse::geo_ops, Exist("WITHHASH", &GeoSearchOpts::withhash))));

void CmdGeoAdd(CmdArgParser parser, CommandContext* cmd_cntx) {
  string_view key = parser.Next();

  ZSetFamily::ZParams zparams;
  for (;;) {
    if (parser.Check("XX")) {
      zparams.flags |= ZADD_IN_XX;  // update only
    } else if (parser.Check("NX")) {
      zparams.flags |= ZADD_IN_NX;  // add new only.
    } else if (parser.Check("CH")) {
      zparams.ch = true;
    } else {
      break;
    }
  }

  auto* builder = cmd_cntx->rb();
  auto args = parser.RemainingRange();
  if (args.empty() || args.size() % 3 != 0) {
    builder->SendError(kSyntaxErr);
    return;
  }

  if ((zparams.flags & ZADD_IN_NX) && (zparams.flags & ZADD_IN_XX)) {
    builder->SendError(kNxXxErr);
    return;
  }

  absl::InlinedVector<ScoredMemberView, 4> members;
  for (size_t i = 0; i < args.size(); i += 3) {
    string_view longitude = args[i];
    string_view latitude = args[i + 1];
    string_view member = args[i + 2];

    pair<double, double> longlat;

    if (!ParseLongLat(longitude, latitude, &longlat)) {
      string err = absl::StrCat("-ERR invalid longitude,latitude pair ", longitude, ",", latitude,
                                ",", member);

      return builder->SendError(err, kSyntaxErrType);
    }

    /* Turn the coordinates into the score of the element. */
    GeoHashBits hash;
    geohashEncodeWGS84(longlat.first, longlat.second, GEO_STEP_MAX, &hash);
    GeoHashFix52Bits bits = geohashAlign52Bits(hash);

    members.emplace_back(bits, member);
  }
  DCHECK(cmd_cntx->tx());

  absl::Span memb_sp{members.data(), members.size()};
  ZSetFamily::ZAddGeneric(key, zparams, memb_sp, cmd_cntx);
}

void CmdGeoHash(CmdArgParser parser, CommandContext* cmd_cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());

  OpResult<MScoreResponse> result =
      ZSetFamily::ZGetMembers(parser.UnparsedArgs(), cmd_cntx->tx(), rb);

  if (result.status() == OpStatus::WRONG_TYPE) {
    return rb->SendError(kWrongTypeErr);
  }

  RedisReplyBuilder::ArrayScope scope{rb, result->size()};
  array<char, 12> buf;
  for (const auto& p : result.value()) {
    if (ToAsciiGeoHash(p, &buf)) {
      rb->SendBulkString(string_view{buf.data(), buf.size() - 1});
    } else {
      rb->SendNull();
    }
  }
}

void CmdGeoPos(CmdArgParser parser, CommandContext* cmd_cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());

  OpResult<MScoreResponse> result =
      ZSetFamily::ZGetMembers(parser.UnparsedArgs(), cmd_cntx->tx(), rb);

  if (result.status() != OpStatus::OK) {
    return rb->SendError(result.status());
  }

  RedisReplyBuilder::ArrayScope scope{rb, result->size()};
  double xy[2];
  for (const auto& p : result.value()) {
    if (ScoreToLongLat(p, xy)) {
      rb->StartArray(2);
      rb->SendDouble(xy[0]);
      rb->SendDouble(xy[1]);
    } else {
      rb->SendNull();
    }
  }
}

void CmdGeoDist(CmdArgParser parser, CommandContext* cmd_cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx->rb());

  // GEODIST key member1 member2 [unit]. The unit is trailing/optional, so it can't be part of the
  // [key, members...] view passed to ZGetMembers; read the fixed args explicitly.
  std::array<std::string_view, 3> key_and_members;  // {key, member1, member2}
  for (std::string_view& arg : key_and_members)
    arg = parser.Next();

  // Optional trailing unit; Finalize() rejects both a missing required arg and any trailing args.
  double distance_multiplier = parser.NextOrDefault(ParseGeoUnit, 1.0);
  if (!parser.Finalize()) {
    return rb->SendError(parser.TakeError().MakeReply());
  }

  OpResult<MScoreResponse> result = ZSetFamily::ZGetMembers(
      facade::ParsedArgs{absl::MakeConstSpan(key_and_members)}, cmd_cntx->tx(), rb);

  if (result.status() != OpStatus::OK) {
    return rb->SendError(result.status());
  }

  const MScoreResponse& arr = result.value();

  if (arr.size() != 2) {
    return rb->SendError(kSyntaxErr);
  }

  double xyxy[4];  // 2 pairs of score holding 2 locations
  for (size_t i = 0; i < arr.size(); i++) {
    if (!ScoreToLongLat(arr[i], xyxy + (i * 2))) {
      return rb->SendNull();
    }
  }

  return rb->SendDouble(geohashGetDistance(xyxy[0], xyxy[1], xyxy[2], xyxy[3]) /
                        distance_multiplier);
}

namespace {
std::vector<ZSetFamily::ZRangeSpec> GetGeoRangeSpec(const GeoHashRadius& n) {
  array<GeoHashBits, 9> neighbors;
  unsigned int last_processed = 0;

  neighbors[0] = n.hash;
  neighbors[1] = n.neighbors.north;
  neighbors[2] = n.neighbors.south;
  neighbors[3] = n.neighbors.east;
  neighbors[4] = n.neighbors.west;
  neighbors[5] = n.neighbors.north_east;
  neighbors[6] = n.neighbors.north_west;
  neighbors[7] = n.neighbors.south_east;
  neighbors[8] = n.neighbors.south_west;

  // Get range_specs for neighbors (*and* our own hashbox)
  std::vector<ZSetFamily::ZRangeSpec> range_specs;
  for (unsigned int i = 0; i < neighbors.size(); i++) {
    if (HASHISZERO(neighbors[i])) {
      continue;
    }

    // When a huge Radius (in the 5000 km range or more) is used,
    // adjacent neighbors can be the same, leading to duplicated
    // elements. Skip every range which is the same as the one
    // processed previously.
    if (last_processed && neighbors[i].bits == neighbors[last_processed].bits &&
        neighbors[i].step == neighbors[last_processed].step) {
      continue;
    }

    GeoHashFix52Bits min, max;
    scoresOfGeoHashBox(neighbors[i], &min, &max);

    ZSetFamily::ScoreInterval si;
    si.first = ZSetFamily::Bound{static_cast<double>(min), false};
    si.second = ZSetFamily::Bound{static_cast<double>(max), true};

    ZSetFamily::RangeParams range_params;
    range_params.interval_type = ZSetFamily::RangeParams::IntervalType::SCORE;
    range_params.with_scores = true;
    range_specs.emplace_back(si, range_params);

    last_processed = i;
  }
  return range_specs;
}

void SortIfNeeded(GeoArray* ga, Sorting sorting, uint64_t count) {
  if (sorting == Sorting::kUnsorted) {
    if (count && ga->size() > count) {
      ga->resize(count);
    }
    return;
  }

  auto comparator = [&](const GeoPoint& a, const GeoPoint& b) {
    if (sorting == Sorting::kAsc) {
      return a.dist < b.dist;
    } else {
      DCHECK(sorting == Sorting::kDesc);
      return a.dist > b.dist;
    }
  };

  if (count > 0) {
    count = std::min(count, static_cast<uint64_t>(ga->size()));
    std::partial_sort(ga->begin(), ga->begin() + count, ga->end(), comparator);
    ga->resize(count);
  } else {
    rng::sort(*ga, comparator);
  }
}

void GeoSearchStoreGeneric(Transaction* tx, facade::SinkReplyBuilder* builder,
                           const GeoShape& shape_ref, string_view key, optional<string_view> member,
                           const GeoSearchOpts& geo_ops) {
  GeoShape* shape = &(const_cast<GeoShape&>(shape_ref));
  auto* rb = static_cast<RedisReplyBuilder*>(builder);

  ShardId from_shard = Shard(key, shard_set->size());

  if (member) {
    // get shape.xy from member
    OpResult<double> member_score;
    auto cb = [&](Transaction* t, EngineShard* shard) {
      if (shard->shard_id() == from_shard) {
        member_score = ZSetFamily::OpScore(t->GetOpArgs(shard), key, *member);
      }
      return OpStatus::OK;
    };
    tx->Execute(std::move(cb), false);
    auto member_sts = member_score.status();
    if (member_sts != OpStatus::OK) {
      tx->Conclude();
      switch (member_sts) {
        case OpStatus::WRONG_TYPE:
          return builder->SendError(kWrongTypeErr);
        case OpStatus::KEY_NOTFOUND:
          return rb->StartArray(0);
        case OpStatus::MEMBER_NOTFOUND:
          return builder->SendError(kMemberNotFound);
        default:
          return builder->SendError(member_sts);
      }
    }
    ScoreToLongLat(*member_score, shape->xy);
  } else {
    // verify key is valid
    OpResult<void> result;
    auto cb = [&](Transaction* t, EngineShard* shard) {
      if (shard->shard_id() == from_shard) {
        result = ZSetFamily::OpKeyExisted(t->GetOpArgs(shard), key);
      }
      return OpStatus::OK;
    };
    tx->Execute(std::move(cb), false);
    auto result_sts = result.status();
    if (result_sts != OpStatus::OK) {
      tx->Conclude();
      switch (result_sts) {
        case OpStatus::WRONG_TYPE:
          return builder->SendError(kWrongTypeErr);
        case OpStatus::KEY_NOTFOUND:
          return rb->StartArray(0);
        default:
          return builder->SendError(result_sts);
      }
    }
  }
  DCHECK(shape->xy[0] >= -180.0 && shape->xy[0] <= 180.0);
  DCHECK(shape->xy[1] >= -90.0 && shape->xy[1] <= 90.0);

  // query
  GeoHashRadius georadius = geohashCalculateAreasByShapeWGS84(shape);
  GeoArray ga;
  auto range_specs = GetGeoRangeSpec(georadius);
  // get all the matching members and add them to the potential result list
  vector<OpResult<vector<ScoredArray>>> result_arrays;
  auto cb = [&](Transaction* t, EngineShard* shard) {
    auto res_it = ZSetFamily::OpRanges(range_specs, t->GetOpArgs(shard), key);
    if (res_it) {
      result_arrays.emplace_back(res_it);
    }
    return OpStatus::OK;
  };

  tx->Execute(std::move(cb), geo_ops.store == GeoStoreType::kNoStore);

  // filter potential result list
  double xy[2];
  double distance;
  unsigned long limit = geo_ops.any ? geo_ops.count : 0;
  for (auto& result_array : result_arrays) {
    for (auto& arr : *result_array) {
      for (auto& p : arr) {
        if (geoWithinShape(shape, p.second, xy, &distance) == 0) {
          ga.emplace_back(xy[0], xy[1], distance, p.second, p.first);
          if (limit > 0 && ga.size() >= limit)
            break;
        }
      }
    }
  }

  // sort and trim by count
  SortIfNeeded(&ga, geo_ops.sorting, geo_ops.count);

  if (geo_ops.store == GeoStoreType::kNoStore) {
    // case 1: read mode
    // case 2: write mode, kNoStore
    // generate reply array withdist, withcoords, withhash
    int record_size = 1;
    if (geo_ops.withdist) {
      record_size++;
    }
    if (geo_ops.withhash) {
      record_size++;
    }
    if (geo_ops.withcoord) {
      record_size++;
    }

    RedisReplyBuilder::ArrayScope scope{rb, ga.size()};
    for (const auto& p : ga) {
      // [member, dist, x, y, hash]
      if (geo_ops.HasWithStatement()) {
        rb->StartArray(record_size);
      }
      rb->SendBulkString(p.member);
      if (geo_ops.withdist) {
        rb->SendDouble(p.dist / geo_ops.conversion);
      }
      if (geo_ops.withhash) {
        rb->SendDouble(p.score);
      }
      if (geo_ops.withcoord) {
        rb->StartArray(2);
        rb->SendDouble(p.longitude);
        rb->SendDouble(p.latitude);
      }
    }
  } else {
    // case 3: write mode, !kNoStore
    DCHECK(geo_ops.store == GeoStoreType::kStoreDist || geo_ops.store == GeoStoreType::kStoreHash);
    ShardId dest_shard = Shard(geo_ops.store_key, shard_set->size());
    DVLOG(1) << "store shard:" << dest_shard << ", key " << geo_ops.store_key;

    OpResult<ZSetFamily::AddResult> add_result;
    vector<ScoredMemberView> smvec;
    for (const auto& p : ga) {
      if (geo_ops.store == GeoStoreType::kStoreDist) {
        smvec.emplace_back(p.dist / geo_ops.conversion, p.member);
      } else {
        DCHECK(geo_ops.store == GeoStoreType::kStoreHash);
        smvec.emplace_back(p.score, p.member);
      }
    }

    auto store_cb = [&](Transaction* t, EngineShard* shard) {
      if (shard->shard_id() == dest_shard) {
        ZSetFamily::ZParams zparams;
        zparams.override = true;
        add_result = ZSetFamily::OpAdd(t->GetOpArgs(shard), zparams, geo_ops.store_key,
                                       ScoredMemberSpan{smvec})
                         .value();
      }
      return OpStatus::OK;
    };

    tx->Execute(std::move(store_cb), true);

    rb->SendLong(smvec.size());
  }
}

}  // namespace

void CmdGeoSearch(CmdArgParser parser, CommandContext* cmd_cntx) {
  auto* builder = cmd_cntx->rb();

  string_view key = parser.Next();
  auto st = kGeoSearchGrammar.Apply(&parser);

  if (HandleGeoParserFinalize(st.shape, &parser, cmd_cntx)) {
    return;
  }

  // check mandatory options
  if (!st.source || st.shape.type == 0) {
    return builder->SendError(kSyntaxErr);
  } else if (st.geo_ops.count == 0) {
    return builder->SendError(kCountError);
  }

  st.geo_ops.count = (st.geo_ops.count == UINT64_MAX) ? 0 : st.geo_ops.count;
  optional<string_view> member;
  if (*st.source == GeoSearchSource::kMember) {
    member = st.member;
  }
  GeoSearchStoreGeneric(cmd_cntx->tx(), builder, st.shape, key, member, st.geo_ops);
}

void GeoRadiusByMemberGeneric(CmdArgParser parser, CommandContext* cmd_cntx, bool read_only) {
  GeoShape shape = {};
  GeoSearchOpts geo_ops{!read_only, kSyntaxErr};
  // parse arguments
  string_view key = parser.Next();
  // member to latlong, set shape.xy
  string_view member = parser.Next();

  auto* builder = cmd_cntx->rb();
  ParseCircularShape(&parser, &shape, &geo_ops);

  ParseGeoResultOptions(&parser, &geo_ops);

  if (HandleGeoParserFinalize(shape, &parser, cmd_cntx)) {
    return;
  }

  if (geo_ops.count == 0) {
    return builder->SendError(kCountError);
  } else if (geo_ops.HasWithStatement() && geo_ops.store != GeoStoreType::kNoStore) {
    return builder->SendError(kStoreCompatByMemberErr);
  }

  geo_ops.count = (geo_ops.count == UINT64_MAX) ? 0 : geo_ops.count;
  GeoSearchStoreGeneric(cmd_cntx->tx(), builder, shape, key, optional<string_view>{member},
                        geo_ops);
}

void GeoRadiusGeneric(CmdArgParser parser, CommandContext* cmd_cntx, bool read_only) {
  GeoShape shape = {};
  GeoSearchOpts geo_ops{!read_only};

  auto* builder = cmd_cntx->rb();

  string_view key = parser.Next();
  ParseLongLat(&parser, shape.xy);
  ParseCircularShape(&parser, &shape, &geo_ops);

  ParseGeoResultOptions(&parser, &geo_ops);

  if (HandleGeoParserFinalize(shape, &parser, cmd_cntx)) {
    return;
  }

  if (geo_ops.count == 0) {
    return builder->SendError(kCountError);
  }

  if (geo_ops.HasWithStatement() && geo_ops.store != GeoStoreType::kNoStore) {
    return builder->SendError(kStoreCompatRadErr);
  }

  geo_ops.count = (geo_ops.count == UINT64_MAX) ? 0 : geo_ops.count;
  GeoSearchStoreGeneric(cmd_cntx->tx(), builder, shape, key, nullopt, geo_ops);
}

void CmdGeoRadiusByMember(CmdArgParser parser, CommandContext* cmd_cntx) {
  GeoRadiusByMemberGeneric(parser, cmd_cntx, false);
}

void CmdGeoRadiusByMemberRO(CmdArgParser parser, CommandContext* cmd_cntx) {
  GeoRadiusByMemberGeneric(parser, cmd_cntx, true);
}

void CmdGeoRadius(CmdArgParser parser, CommandContext* cmd_cntx) {
  GeoRadiusGeneric(parser, cmd_cntx, false);
}

void CmdGeoRadiusRO(CmdArgParser parser, CommandContext* cmd_cntx) {
  GeoRadiusGeneric(parser, cmd_cntx, true);
}

}  // namespace

#define HFUNC(x) SetHandler(&Cmd##x)

void RegisterGeoFamily(CommandRegistry* registry) {
  registry->StartFamily(acl::GEO);
  *registry << CI{"GEOADD", CO::JOURNALED | CO::DENYOOM, -5, 1, 1}.HFUNC(GeoAdd)
            << CI{"GEOHASH", CO::READONLY, -2, 1, 1}.HFUNC(GeoHash)
            << CI{"GEOPOS", CO::READONLY, -2, 1, 1}.HFUNC(GeoPos)
            << CI{"GEODIST", CO::READONLY, -4, 1, 1}.HFUNC(GeoDist)
            << CI{"GEOSEARCH", CO::READONLY, -7, 1, 1}.HFUNC(GeoSearch)
            << CI{"GEORADIUSBYMEMBER", CO::JOURNALED | CO::STORE_LAST_KEY, -5, 1, 1}.HFUNC(
                   GeoRadiusByMember)
            << CI{"GEORADIUSBYMEMBER_RO", CO::READONLY, -5, 1, 1}.HFUNC(GeoRadiusByMemberRO)
            << CI{"GEORADIUS", CO::JOURNALED | CO::STORE_LAST_KEY, -6, 1, 1}.HFUNC(GeoRadius)
            << CI{"GEORADIUS_RO", CO::READONLY, -6, 1, 1}.HFUNC(GeoRadiusRO);
}

}  // namespace dfly
