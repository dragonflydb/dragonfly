// Copyright 2025, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/geo_family.h"

#include "server/acl/acl_commands_def.h"
#include "server/zset_family.h"

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
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/family_utils.h"
#include "server/transaction.h"

namespace dfly {

using namespace std;
using namespace facade;
using absl::SimpleAtoi;
namespace {

using CI = CommandId;

enum Errors {
  INVALID_LONG_LAT = CmdArgParser::ErrorType::CUSTOM_ERROR,
  INVALID_UNIT = INVALID_LONG_LAT + 1,
};

const char kNxXxErr[] = "XX and NX options at the same time are not compatible";
const char kFromMemberLonglatErr[] =
    "FROMMEMBER and FROMLONLAT options at the same time are not compatible";
const char kByRadiusBoxErr[] = "BYRADIUS and BYBOX options at the same time are not compatible";
const char kAscDescErr[] = "ASC and DESC options at the same time are not compatible";
const char kStoreTypeErr[] = "STORE and STOREDIST options at the same time are not compatible";
const char kStoreCompatErr[] =
    "STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORDS options";
const char kMemberNotFound[] = "could not decode requested zset member";
const char kInvalidUnit[] = "unsupported unit provided. please use M, KM, FT, MI";
constexpr string_view kGeoAlphabet = "0123456789bcdefghjkmnpqrstuvwxyz"sv;

enum class Type {
  FROMMEMBER,
  FROMLONLAT,
  BYRADIUS,
  BYBOX,
  ASC,
  DESC,
  COUNT,
  WITHCOORD,
  WITHDIST,
  WITHHASH,

  STORE,
  STOREDIST
};

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

enum class Sorting { kUnsorted, kAsc, kDesc, kError };
enum class GeoStoreType { kNoStore, kStoreHash, kStoreDist, kError };
struct GeoSearchOpts {
  double conversion = 0;
  uint64_t count = 0;
  Sorting sorting = Sorting::kUnsorted;
  bool any = 0;
  bool withdist = 0;
  bool withcoord = 0;
  bool withhash = 0;
  GeoStoreType store = GeoStoreType::kNoStore;
  string_view store_key;

  bool HasWithStatement() const {
    return withdist || withcoord || withhash;
  }
};

bool ValidateLongLat(double longitude, double latitude) {
  return !(longitude < GEO_LONG_MIN || longitude > GEO_LONG_MAX || latitude < GEO_LAT_MIN ||
           latitude > GEO_LAT_MAX);
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

double ExtractUnit(CmdArgParser* parser) {
  auto unit = parser->TryMapNext("M", 1.0, "KM", 1000.0, "FT", 0.3048, "MI", 1609.34);
  if (!unit)
    parser->Report(Errors::INVALID_UNIT);
  return unit.value_or(-1);
}

double ExtractUnit(std::string_view arg) {
  const string unit = absl::AsciiStrToUpper(arg);
  if (unit == "M") {
    return 1;
  } else if (unit == "KM") {
    return 1000;
  } else if (unit == "FT") {
    return 0.3048;
  } else if (unit == "MI") {
    return 1609.34;
  } else {
    return -1;
  }
}

}  // namespace

void GeoFamily::GeoAdd(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);

  ZSetFamily::ZParams zparams;
  size_t i = 1;
  for (; i < args.size(); ++i) {
    string cur_arg = absl::AsciiStrToUpper(ArgS(args, i));

    if (cur_arg == "XX") {
      zparams.flags |= ZADD_IN_XX;  // update only
    } else if (cur_arg == "NX") {
      zparams.flags |= ZADD_IN_NX;  // add new only.
    } else if (cur_arg == "CH") {
      zparams.ch = true;
    } else {
      break;
    }
  }

  auto* builder = cmd_cntx.rb;
  args.remove_prefix(i);
  if (args.empty() || args.size() % 3 != 0) {
    builder->SendError(kSyntaxErr);
    return;
  }

  if ((zparams.flags & ZADD_IN_NX) && (zparams.flags & ZADD_IN_XX)) {
    builder->SendError(kNxXxErr);
    return;
  }

  absl::InlinedVector<ScoredMemberView, 4> members;
  for (i = 0; i < args.size(); i += 3) {
    string_view longitude = ArgS(args, i);
    string_view latitude = ArgS(args, i + 1);
    string_view member = ArgS(args, i + 2);

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
  DCHECK(cmd_cntx.tx);

  absl::Span memb_sp{members.data(), members.size()};
  ZSetFamily::ZAddGeneric(key, zparams, memb_sp, cmd_cntx.tx, builder);
}

void GeoFamily::GeoHash(CmdArgList args, const CommandContext& cmd_cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);

  OpResult<MScoreResponse> result = ZSetFamily::ZGetMembers(args, cmd_cntx.tx, rb);

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

void GeoFamily::GeoPos(CmdArgList args, const CommandContext& cmd_cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);

  OpResult<MScoreResponse> result = ZSetFamily::ZGetMembers(args, cmd_cntx.tx, rb);

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

void GeoFamily::GeoDist(CmdArgList args, const CommandContext& cmd_cntx) {
  double distance_multiplier = 1;
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);

  if (args.size() == 4) {
    string_view unit = ArgS(args, 3);
    distance_multiplier = ExtractUnit(unit);
    args.remove_suffix(1);
    if (distance_multiplier < 0) {
      return rb->SendError(kInvalidUnit);
    }
  } else if (args.size() != 3) {
    return rb->SendError(kSyntaxErr);
  }

  OpResult<MScoreResponse> result = ZSetFamily::ZGetMembers(args, cmd_cntx.tx, rb);

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
  if (sorting == Sorting::kUnsorted)
    return;

  auto comparator = [&](const GeoPoint& a, const GeoPoint& b) {
    if (sorting == Sorting::kAsc) {
      return a.dist < b.dist;
    } else {
      DCHECK(sorting == Sorting::kDesc);
      return a.dist > b.dist;
    }
  };

  if (count > 0) {
    count = std::min(count, ga->size());
    std::partial_sort(ga->begin(), ga->begin() + count, ga->end(), comparator);
    ga->resize(count);
  } else {
    std::sort(ga->begin(), ga->end(), comparator);
  }
}

void GeoSearchStoreGeneric(Transaction* tx, facade::SinkReplyBuilder* builder,
                           const GeoShape& shape_ref, string_view key, string_view member,
                           const GeoSearchOpts& geo_ops) {
  GeoShape* shape = &(const_cast<GeoShape&>(shape_ref));
  auto* rb = static_cast<RedisReplyBuilder*>(builder);

  ShardId from_shard = Shard(key, shard_set->size());

  if (!member.empty()) {
    // get shape.xy from member
    OpResult<double> member_score;
    auto cb = [&](Transaction* t, EngineShard* shard) {
      if (shard->shard_id() == from_shard) {
        member_score = ZSetFamily::OpScore(t->GetOpArgs(shard), key, member);
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

void GeoFamily::GeoSearch(CmdArgList args, const CommandContext& cmd_cntx) {
  GeoShape shape = {};
  GeoSearchOpts geo_ops;
  string_view member;

  // FROMMEMBER or FROMLONLAT is set
  int from_set = 0;
  // BYRADIUS or BYBOX is set
  int by_set = 0;
  auto* builder = cmd_cntx.rb;

  CmdArgParser parser(args);
  string_view key = parser.Next();

  while (parser.HasNext()) {
    auto type = parser.MapNext(
        "FROMMEMBER", Type::FROMMEMBER, "FROMLONLAT", Type::FROMLONLAT, "BYRADIUS", Type::BYRADIUS,
        "BYBOX", Type::BYBOX, "ASC", Type::ASC, "DESC", Type::DESC, "COUNT", Type::COUNT,
        "WITHCOORD", Type::WITHCOORD, "WITHDIST", Type::WITHDIST, "WITHHASH", Type::WITHHASH);

    switch (type) {
      case Type::FROMMEMBER:
        ++from_set;
        member = parser.Next();
        break;
      case Type::FROMLONLAT: {
        ++from_set;
        ParseLongLat(&parser, shape.xy);
        break;
      }
      case Type::BYRADIUS:
        ++by_set;
        shape.t.radius = parser.Next<double>();
        shape.conversion = ExtractUnit(&parser);
        geo_ops.conversion = shape.conversion;
        shape.type = CIRCULAR_TYPE;
        break;
      case Type::BYBOX: {
        ++by_set;
        std::tie(shape.t.r.width, shape.t.r.height) = parser.Next<double, double>();
        shape.conversion = ExtractUnit(&parser);
        geo_ops.conversion = shape.conversion;
        shape.type = RECTANGLE_TYPE;
        break;
      }
      case Type::ASC:
        geo_ops.sorting = geo_ops.sorting == Sorting::kUnsorted ? Sorting::kAsc : Sorting::kError;
        break;
      case Type::DESC:
        geo_ops.sorting = geo_ops.sorting == Sorting::kUnsorted ? Sorting::kDesc : Sorting::kError;
        break;
      case Type::COUNT:
        geo_ops.count = parser.Next<uint64_t>();
        geo_ops.any = parser.Check("ANY");
        break;
      case Type::WITHCOORD:
        geo_ops.withcoord = true;
        break;
      case Type::WITHDIST:
        geo_ops.withdist = true;
        break;
      case Type::WITHHASH:
        geo_ops.withhash = true;
        break;
      default:
        return builder->SendError(kSyntaxErr);
    }
  }

  if (!parser.Finalize()) {
    auto error = parser.Error();
    switch (error->type) {
      case Errors::INVALID_LONG_LAT: {
        string err =
            absl::StrCat("-ERR invalid longitude,latitude pair ", shape.xy[0], ",", shape.xy[1]);
        return builder->SendError(err, kSyntaxErrType);
      }
      case Errors::INVALID_UNIT:
        return builder->SendError("Unsupported unit provided. please use M, KM, FT, MI",
                                  kSyntaxErrType);
      default:
        return builder->SendError(error->MakeReply());
    }
  }

  // check mandatory options
  if (from_set == 0 || by_set == 0) {
    return builder->SendError(kSyntaxErr);
  } else if (from_set > 1) {
    return builder->SendError(kFromMemberLonglatErr);
  } else if (by_set > 1) {
    return builder->SendError(kByRadiusBoxErr);
  } else if (geo_ops.sorting == Sorting::kError) {
    return builder->SendError(kAscDescErr);
  }

  GeoSearchStoreGeneric(cmd_cntx.tx, builder, shape, key, member, geo_ops);
}

void GeoFamily::GeoRadiusByMember(CmdArgList args, const CommandContext& cmd_cntx) {
  GeoShape shape = {};
  GeoSearchOpts geo_ops;
  // parse arguments
  string_view key = ArgS(args, 0);
  // member to latlong, set shape.xy
  string_view member = ArgS(args, 1);

  auto* builder = cmd_cntx.rb;
  if (!ParseDouble(ArgS(args, 2), &shape.t.radius)) {
    return builder->SendError(kInvalidFloatErr);
  }
  string_view unit = ArgS(args, 3);
  shape.conversion = ExtractUnit(unit);
  geo_ops.conversion = shape.conversion;
  if (shape.conversion == -1) {
    return builder->SendError("unsupported unit provided. please use M, KM, FT, MI");
  }
  shape.type = CIRCULAR_TYPE;

  for (size_t i = 4; i < args.size(); ++i) {
    string cur_arg = absl::AsciiStrToUpper(ArgS(args, i));

    if (cur_arg == "ASC") {
      if (geo_ops.sorting != Sorting::kUnsorted) {
        return builder->SendError(kAscDescErr);
      } else {
        geo_ops.sorting = Sorting::kAsc;
      }
    } else if (cur_arg == "DESC") {
      if (geo_ops.sorting != Sorting::kUnsorted) {
        return builder->SendError(kAscDescErr);
      } else {
        geo_ops.sorting = Sorting::kDesc;
      }
    } else if (cur_arg == "COUNT") {
      if (i + 1 < args.size() && absl::SimpleAtoi(ArgS(args, i + 1), &geo_ops.count)) {
        i++;
      } else {
        return builder->SendError(kSyntaxErr);
      }
      if (i + 1 < args.size() && ArgS(args, i + 1) == "ANY") {
        geo_ops.any = true;
        i++;
      }
    } else if (cur_arg == "WITHCOORD") {
      if (geo_ops.store != GeoStoreType::kNoStore) {
        return builder->SendError(kStoreCompatErr);
      }
      geo_ops.withcoord = true;
    } else if (cur_arg == "WITHDIST") {
      if (geo_ops.store != GeoStoreType::kNoStore) {
        return builder->SendError(kStoreCompatErr);
      }
      geo_ops.withdist = true;
    } else if (cur_arg == "WITHHASH") {
      if (geo_ops.store != GeoStoreType::kNoStore) {
        return builder->SendError(kStoreCompatErr);
      }
      geo_ops.withhash = true;
    } else if (cur_arg == "STORE") {
      if (geo_ops.store != GeoStoreType::kNoStore) {
        return builder->SendError(kStoreTypeErr);
      } else if (geo_ops.withcoord || geo_ops.withdist || geo_ops.withhash) {
        return builder->SendError(kStoreCompatErr);
      }
      if (i + 1 < args.size()) {
        geo_ops.store_key = ArgS(args, i + 1);
        geo_ops.store = GeoStoreType::kStoreHash;
        i++;
      } else {
        return builder->SendError(kSyntaxErr);
      }
    } else if (cur_arg == "STOREDIST") {
      if (geo_ops.store != GeoStoreType::kNoStore) {
        return builder->SendError(kStoreTypeErr);
      } else if (geo_ops.withcoord || geo_ops.withdist || geo_ops.withhash) {
        return builder->SendError(kStoreCompatErr);
      }
      if (i + 1 < args.size()) {
        geo_ops.store_key = ArgS(args, i + 1);
        geo_ops.store = GeoStoreType::kStoreDist;
        i++;
      } else {
        return builder->SendError(kSyntaxErr);
      }
    } else {
      return builder->SendError(kSyntaxErr);
    }
  }
  // parsing completed

  GeoSearchStoreGeneric(cmd_cntx.tx, builder, shape, key, member, geo_ops);
}

void GeoFamily::GeoRadius(CmdArgList args, const CommandContext& cmd_cntx) {
  GeoShape shape = {};
  GeoSearchOpts geo_ops;

  auto* builder = cmd_cntx.rb;

  CmdArgParser parser(args);

  string_view key = parser.Next();
  ParseLongLat(&parser, shape.xy);
  shape.t.radius = parser.Next<double>();
  shape.conversion = ExtractUnit(&parser);
  geo_ops.conversion = shape.conversion;
  shape.type = CIRCULAR_TYPE;

  while (parser.HasNext()) {
    auto type =
        parser.MapNext("STORE", Type::STORE, "STOREDIST", Type::STOREDIST, "ASC", Type::ASC, "DESC",
                       Type::DESC, "COUNT", Type::COUNT, "WITHCOORD", Type::WITHCOORD, "WITHDIST",
                       Type::WITHDIST, "WITHHASH", Type::WITHHASH);

    switch (type) {
      case Type::STORE:
        geo_ops.store_key = parser.Next();
        geo_ops.store = geo_ops.store == GeoStoreType::kNoStore ? GeoStoreType::kStoreHash
                                                                : GeoStoreType::kError;
        break;
      case Type::STOREDIST:
        geo_ops.store_key = parser.Next();
        geo_ops.store = geo_ops.store == GeoStoreType::kNoStore ? GeoStoreType::kStoreDist
                                                                : GeoStoreType::kError;
        break;
      case Type::ASC:
        geo_ops.sorting = geo_ops.sorting == Sorting::kUnsorted ? Sorting::kAsc : Sorting::kError;
        break;
      case Type::DESC:
        geo_ops.sorting = geo_ops.sorting == Sorting::kUnsorted ? Sorting::kDesc : Sorting::kError;
        break;
      case Type::COUNT:
        geo_ops.count = parser.Next<uint64_t>();
        geo_ops.any = parser.Check("ANY");
        break;
      case Type::WITHCOORD:
        geo_ops.withcoord = true;
        break;
      case Type::WITHDIST:
        geo_ops.withdist = true;
        break;
      case Type::WITHHASH:
        geo_ops.withhash = true;
        break;
      default:
        // If MapNext failed, it means an unknown option was provided or
        // an option requiring an argument was missing its argument.
        // The parser has already recorded the error.
        DCHECK(parser.HasError());
        break;
    }
  }

  if (!parser.Finalize()) {
    auto error = parser.Error();

    switch (error->type) {
      case Errors::INVALID_LONG_LAT: {
        string err =
            absl::StrCat("-ERR invalid longitude,latitude pair ", shape.xy[0], ",", shape.xy[1]);
        return builder->SendError(err, kSyntaxErrType);
      }
      case Errors::INVALID_UNIT:
        return builder->SendError("Unsupported unit provided. please use M, KM, FT, MI",
                                  kSyntaxErrType);
      default:
        return builder->SendError(error->MakeReply());
    }
  }

  if (geo_ops.sorting == Sorting::kError) {
    return builder->SendError(kAscDescErr);
  } else if (geo_ops.store == GeoStoreType::kError) {
    return builder->SendError(kStoreTypeErr);
  }

  GeoSearchStoreGeneric(cmd_cntx.tx, builder, shape, key, "", geo_ops);
}

#define HFUNC(x) SetHandler(&GeoFamily::x)

namespace acl {
constexpr uint32_t kGeoAdd = WRITE | GEO | SLOW;
constexpr uint32_t kGeoHash = READ | GEO | SLOW;
constexpr uint32_t kGeoPos = READ | GEO | SLOW;
constexpr uint32_t kGeoDist = READ | GEO | SLOW;
constexpr uint32_t kGeoSearch = READ | GEO | SLOW;
constexpr uint32_t kGeoRadiusByMember = WRITE | GEO | SLOW;
constexpr uint32_t kGeoRadius = WRITE | GEO | SLOW;
}  // namespace acl

void GeoFamily::Register(CommandRegistry* registry) {
  registry->StartFamily();
  *registry << CI{"GEOADD", CO::FAST | CO::WRITE | CO::DENYOOM, -5, 1, 1, acl::kGeoAdd}.HFUNC(
                   GeoAdd)
            << CI{"GEOHASH", CO::FAST | CO::READONLY, -2, 1, 1, acl::kGeoHash}.HFUNC(GeoHash)
            << CI{"GEOPOS", CO::FAST | CO::READONLY, -2, 1, 1, acl::kGeoPos}.HFUNC(GeoPos)
            << CI{"GEODIST", CO::READONLY, -4, 1, 1, acl::kGeoDist}.HFUNC(GeoDist)
            << CI{"GEOSEARCH", CO::READONLY, -7, 1, 1, acl::kGeoSearch}.HFUNC(GeoSearch)
            << CI{"GEORADIUSBYMEMBER",    CO::WRITE | CO::STORE_LAST_KEY, -5, 1, 1,
                  acl::kGeoRadiusByMember}
                   .HFUNC(GeoRadiusByMember)
            << CI{"GEORADIUS", CO::WRITE | CO::STORE_LAST_KEY, -6, 1, 1, acl::kGeoRadius}.HFUNC(
                   GeoRadius);
}

}  // namespace dfly
