// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "server/debugcmd.h"

#define HUF_STATIC_LINKING_ONLY

extern "C" {
#include "huff/hist.h"
#include "huff/huf.h"
#include "redis/redis_aux.h"
}

#include <absl/cleanup/cleanup.h>
#include <absl/random/random.h>
#include <absl/strings/escaping.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <lz4.h>
#include <zdict.h>
#include <zstd.h>

#include <filesystem>

#include "base/flags.h"
#include "base/logging.h"
#include "core/compact_object.h"
#include "core/huff_coder.h"
#include "core/qlist.h"
#include "core/sorted_map.h"
#include "core/string_map.h"
#include "core/string_set.h"
#include "facade/cmd_arg_parser.h"
#include "server/blocking_controller.h"
#include "server/container_utils.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/main_service.h"
#include "server/multi_command_squasher.h"
#include "server/rdb_load.h"
#include "server/server_state.h"
#include "server/string_family.h"
#include "server/transaction.h"
using namespace std;

ABSL_DECLARE_FLAG(string, dir);
ABSL_DECLARE_FLAG(string, dbfilename);
ABSL_DECLARE_FLAG(bool, df_snapshot_format);

namespace dfly {

using namespace util;
using boost::intrusive_ptr;
using namespace facade;
using absl::StrAppend;
using absl::StrCat;

namespace {

struct ObjInfo {
  unsigned type = 0;
  unsigned encoding;
  unsigned bucket_id = 0;
  unsigned slot_id = 0;

  // for lists - how many nodes do they have.
  unsigned num_nodes = 0;
  unsigned num_compressed = 0;

  enum LockStatus { NONE, S, X } lock_status = NONE;

  int64_t ttl = INT64_MAX;
  optional<uint32_t> external_len;

  bool has_sec_precision = false;
  bool found = false;
};

struct ValueCompressInfo {
  size_t raw_size = 0;
  size_t compressed_size = 0;
};

std::string GenerateValue(size_t val_size, bool random_value, absl::InsecureBitGen* gen) {
  if (random_value) {
    return GetRandomHex(*gen, val_size);
  } else {
    return string(val_size, 'x');
  }
}

tuple<const CommandId*, absl::InlinedVector<string, 5>> GeneratePopulateCommand(
    string_view type, std::string key, size_t val_size, bool random_value, uint32_t elements,
    const CommandRegistry& registry, absl::InsecureBitGen* gen) {
  absl::InlinedVector<string, 5> args;
  args.push_back(std::move(key));

  const CommandId* cid = nullptr;
  if (type == "STRING") {
    cid = registry.Find("SET");
    args.push_back(GenerateValue(val_size, random_value, gen));
  } else if (type == "LIST") {
    cid = registry.Find("LPUSH");
    for (uint32_t i = 0; i < elements; ++i) {
      args.push_back(GenerateValue(val_size, random_value, gen));
    }
  } else if (type == "SET") {
    cid = registry.Find("SADD");
    for (size_t i = 0; i < elements; ++i) {
      args.push_back(GenerateValue(val_size, random_value, gen));
    }
  } else if (type == "HASH") {
    cid = registry.Find("HSET");
    for (size_t i = 0; i < elements; ++i) {
      args.push_back(GenerateValue(val_size / 2, random_value, gen));
      args.push_back(GenerateValue(val_size / 2, random_value, gen));
    }
  } else if (type == "ZSET") {
    cid = registry.Find("ZADD");
    for (size_t i = 0; i < elements; ++i) {
      args.push_back(StrCat((*gen)() % val_size));
      args.push_back(GenerateValue(val_size, random_value, gen));
    }
  } else if (type == "JSON") {
    cid = registry.Find("JSON.MERGE");
    args.push_back("$");

    string json = "{";
    for (size_t i = 0; i < elements; ++i) {
      absl::StrAppend(&json, "\"", GenerateValue(val_size / 2, random_value, gen), "\":\"",
                      GenerateValue(val_size / 2, random_value, gen), "\",");
    }
    json[json.size() - 1] = '}';  // Replace last ',' with '}'
    args.push_back(json);
  } else if (type == "STREAM") {
    cid = registry.Find("XADD");
    args.push_back("*");
    for (size_t i = 0; i < elements; ++i) {
      args.push_back(GenerateValue(val_size / 2, random_value, gen));
      args.push_back(GenerateValue(val_size / 2, random_value, gen));
    }
  }

  return {cid, args};
}

struct ObjHist {
  base::Histogram key_len;
  base::Histogram val_len;    // overall malloc-used size of the value.
  base::Histogram card;       // for sets, hashmaps etc - it's number of entries.
  base::Histogram entry_len;  // for sets, hashmaps etc - it's the length of each entry.
  base::Histogram listpack;   // for listpack encodings - the malloc used of the listpack.
};

// Returns number of O(1) steps executed.
unsigned AddObjHist(PrimeIterator it, ObjHist* hist) {
  using namespace container_utils;
  const PrimeValue& pv = it->second;
  size_t val_len = 0;
  unsigned steps = 1;

  auto per_entry_cb = [&](ContainerEntry entry) {
    if (entry.value) {
      val_len += entry.length;
      hist->entry_len.Add(entry.length);
    } else {
      val_len += 8;  // size of long
    }
    ++steps;
    return true;
  };

  hist->key_len.Add(it->first.Size());

  if (pv.ObjType() == OBJ_LIST) {
    IterateList(pv, per_entry_cb, 0, -1);
    if (pv.Encoding() == kEncodingQL2) {
      const QList* ql = static_cast<QList*>(pv.RObjPtr());
      val_len = ql->MallocUsed(true);
    }
  } else if (pv.ObjType() == OBJ_ZSET) {
    IterateSortedSet(pv.GetRobjWrapper(),
                     [&](ContainerEntry entry, double) { return per_entry_cb(entry); });
    val_len = 0;  // reset - will be calculated below.
    if (pv.Encoding() == OBJ_ENCODING_LISTPACK) {
      hist->listpack.Add(pv.MallocUsed());
    }
  } else if (pv.ObjType() == OBJ_SET) {
    IterateSet(pv, per_entry_cb);
    val_len = 0;  // reset - will be calculated below.
    if (pv.Encoding() == kEncodingIntSet) {
      hist->listpack.Add(pv.MallocUsed());
    }
  } else if (pv.ObjType() == OBJ_HASH) {
    IterateMap(pv, [&](ContainerEntry key, ContainerEntry value) {
      hist->entry_len.Add(key.length + value.length);
      steps += 2;
      return true;
    });
    if (pv.Encoding() == kEncodingListPack) {
      hist->listpack.Add(pv.MallocUsed());
    }
  }
  // TODO: streams

  if (val_len == 0) {
    // Fallback
    val_len = pv.MallocUsed(true);
  }

  hist->val_len.Add(val_len);

  if (pv.ObjType() != OBJ_STRING && pv.ObjType() != OBJ_JSON)
    hist->card.Add(pv.Size());

  return steps;
}

// ObjType -> ObjHist
//
using ObjHistMap = absl::flat_hash_map<unsigned, unique_ptr<ObjHist>>;

void MergeObjHistMap(ObjHistMap&& src, ObjHistMap* dest) {
  for (auto& [obj_type, src_hist] : src) {
    auto& dest_hist = (*dest)[obj_type];
    if (!dest_hist) {
      dest_hist = std::move(src_hist);
    } else {
      dest_hist->key_len.Merge(src_hist->key_len);
      dest_hist->val_len.Merge(src_hist->val_len);
      dest_hist->card.Merge(src_hist->card);
      dest_hist->entry_len.Merge(src_hist->entry_len);
      dest_hist->listpack.Merge(src_hist->listpack);
    }
  }
}

void DoBuildObjHist(EngineShard* shard, ConnectionContext* cntx, ObjHistMap* obj_hist_map) {
  auto& db_slice = cntx->ns->GetDbSlice(shard->shard_id());
  unsigned steps = 0;

  for (unsigned i = 0; i < db_slice.db_array_size(); ++i) {
    DbTable* dbt = db_slice.GetDBTable(i);
    if (dbt == nullptr)
      continue;
    PrimeTable::Cursor cursor;
    do {
      cursor = dbt->prime.Traverse(cursor, [&](PrimeIterator it) {
        unsigned obj_type = it->second.ObjType();
        auto& hist_ptr = (*obj_hist_map)[obj_type];
        if (!hist_ptr) {
          hist_ptr.reset(new ObjHist);
        }
        steps += AddObjHist(it, hist_ptr.get());
      });

      if (steps >= 20000) {
        steps = 0;
        ThisFiber::Yield();
      }
    } while (cursor);
  }
}

struct HufHist {
  static constexpr unsigned kMaxSymbol = 255;
  array<unsigned, kMaxSymbol + 1> hist;  // histogram of symbols.
  unsigned max_symbol = 0;               // what is the max symbol of the histogram.

  HufHist() {
    hist.fill(0);
  }

  void Merge(const HufHist& other) {
    max_symbol = std::max(max_symbol, other.max_symbol);
    for (unsigned i = 0; i <= max_symbol; ++i) {
      hist[i] += other.hist[i];
    }
  }
};

void DoComputeHist(CompactObjType type, EngineShard* shard, ConnectionContext* cntx,
                   HufHist* dest) {
  auto& db_slice = cntx->ns->GetDbSlice(shard->shard_id());
  DbTable* dbt = db_slice.GetDBTable(cntx->db_index());
  CHECK(dbt);

  PrimeTable::Cursor cursor;
  unsigned steps = 0;
  string scratch;
  constexpr size_t kMaxLen = 512;
  PrimeTable& table = dbt->prime;

  do {
    cursor = table.Traverse(cursor, [&](PrimeIterator it) {
      scratch.clear();
      if (type == kInvalidCompactObjType) {  // KEYSPACE
        if (it->first.MallocUsed() > 0) {
          it->first.GetString(&scratch);
        }
      } else if (type == OBJ_STRING && it->second.ObjType() == OBJ_STRING) {
        if (it->first.MallocUsed() > 0) {
          it->second.GetString(&scratch);
        }
      } else if (type == OBJ_ZSET && it->second.ObjType() == OBJ_ZSET) {
        container_utils::IterateSortedSet(
            it->second.GetRobjWrapper(), [&](container_utils::ContainerEntry entry, double) {
              if (entry.value) {
                HIST_add(dest->hist.data(), entry.value, entry.length);
              }
              return true;
            });
      } else if (type == OBJ_LIST && it->second.ObjType() == OBJ_LIST) {
        container_utils::IterateList(it->second, [&](container_utils::ContainerEntry entry) {
          if (entry.value) {
            HIST_add(dest->hist.data(), entry.value, entry.length);
          }
          return true;
        });
      } else if (type == OBJ_HASH && it->second.ObjType() == OBJ_HASH) {
        container_utils::IterateMap(it->second, [&](container_utils::ContainerEntry key,
                                                    container_utils::ContainerEntry value) {
          if (key.value) {
            HIST_add(dest->hist.data(), key.value, key.length);
          }
          if (value.value) {
            HIST_add(dest->hist.data(), value.value, value.length);
          }
          return true;
        });
      }

      if (scratch.size() > 0) {
        size_t len = std::min(scratch.size(), kMaxLen);
        HIST_add(dest->hist.data(), scratch.data(), len);
      }
    });

    if (steps >= 20000) {
      steps = 0;
      ThisFiber::Yield();
    }
  } while (cursor);
  dest->max_symbol = HufHist::kMaxSymbol;
  while (dest->max_symbol && dest->hist[dest->max_symbol] == 0)
    --dest->max_symbol;
}

ObjInfo InspectOp(ConnectionContext* cntx, string_view key) {
  auto& db_slice = cntx->ns->GetCurrentDbSlice();
  auto db_index = cntx->db_index();
  auto [pt, exp_t] = db_slice.GetTables(db_index);

  PrimeIterator it = pt->Find(key);
  ObjInfo oinfo;
  if (IsValid(it)) {
    const PrimeValue& pv = it->second;

    oinfo.found = true;
    oinfo.type = pv.ObjType();
    oinfo.encoding = pv.Encoding();
    oinfo.bucket_id = it.bucket_id();
    oinfo.slot_id = it.slot_id();

    if (pv.ObjType() == OBJ_LIST && pv.Encoding() == kEncodingQL2) {
      const QList* qlist = static_cast<const QList*>(pv.RObjPtr());
      oinfo.num_nodes = qlist->node_count();
      auto* node = qlist->Head();

      while (node) {
        if (node->encoding == QUICKLIST_NODE_ENCODING_LZF) {
          ++oinfo.num_compressed;
        }
        node = node->next;
      }
    }

    if (pv.IsExternal()) {
      oinfo.external_len.emplace(pv.GetExternalSlice().second);
    }

    if (pv.HasExpire()) {
      ExpireIterator exp_it = exp_t->Find(it->first);
      CHECK(!exp_it.is_done());

      time_t exp_time = db_slice.ExpireTime(exp_it);
      oinfo.ttl = exp_time - GetCurrentTimeMs();
      oinfo.has_sec_precision = exp_it->second.is_second_precision();
    }
  }

  if (!db_slice.CheckLock(IntentLock::EXCLUSIVE, db_index, key)) {
    oinfo.lock_status =
        db_slice.CheckLock(IntentLock::SHARED, db_index, key) ? ObjInfo::S : ObjInfo::X;
  }

  return oinfo;
}

OpResult<ValueCompressInfo> EstimateCompression(ConnectionContext* cntx, string_view key) {
  auto& db_slice = cntx->ns->GetCurrentDbSlice();
  auto db_index = cntx->db_index();
  auto [pt, exp_t] = db_slice.GetTables(db_index);

  PrimeIterator it = pt->Find(key);
  if (!IsValid(it)) {
    return OpStatus::KEY_NOTFOUND;
  }

  // Only strings are supported right now.
  if (it->second.ObjType() != OBJ_STRING && it->second.ObjType() != OBJ_LIST) {
    return OpStatus::WRONG_TYPE;
  }
  ValueCompressInfo info;

  if (it->second.ObjType() == OBJ_LIST) {
    if (it->second.Encoding() != kEncodingQL2) {
      return OpStatus::WRONG_TYPE;
    }

    const QList* src = static_cast<const QList*>(it->second.RObjPtr());
    info.raw_size = src->MallocUsed(true);
    QList qlist(-2, 1);
    auto copy_cb = [&](QList::Entry entry) {
      qlist.Push(entry.view(), QList::HEAD);
      return true;
    };
    src->Iterate(copy_cb, 0, -1);
    info.compressed_size = qlist.MallocUsed(true);
    return info;
  }

  string scratch;
  string_view value = it->second.GetSlice(&scratch);

  info.raw_size = value.size();
  info.compressed_size = info.raw_size;

  if (info.raw_size >= 32) {
    size_t compressed_size = ZSTD_compressBound(value.size());
    unique_ptr<char[]> compressed(new char[compressed_size]);
    info.compressed_size =
        ZSTD_compress(compressed.get(), compressed_size, value.data(), value.size(), 5);
  }

  return info;
};

const char* EncodingName(unsigned obj_type, unsigned encoding) {
  switch (obj_type) {
    case OBJ_STRING:
      return "raw";
    case OBJ_LIST:
      switch (encoding) {
        case kEncodingQL2:
        case OBJ_ENCODING_QUICKLIST:
          return "quicklist";
      }
      break;
    case OBJ_SET:
      ABSL_FALLTHROUGH_INTENDED;
    case OBJ_ZSET:
      ABSL_FALLTHROUGH_INTENDED;
    case OBJ_HASH:
      switch (encoding) {
        case kEncodingIntSet:
          return "intset";
        case kEncodingStrMap2:
          return "dense_set";
        case OBJ_ENCODING_SKIPLIST:  // we kept the old enum for zset
          return "btree";
        case OBJ_ENCODING_LISTPACK:
          ABSL_FALLTHROUGH_INTENDED;
        case kEncodingListPack:
          return "listpack";
      }
      break;
    case OBJ_JSON:
      switch (encoding) {
        case kEncodingJsonCons:
          return "jsoncons";
        case kEncodingJsonFlat:
          return "jsonflat";
      }
      break;
    case OBJ_STREAM:
      return "stream";
  }
  return "unknown";
}

struct IOStat {
  uint64_t conn_received = 0;
  uint64_t curr_conn_count = 0;
  uint64_t cmd_total = 0, pipelined_cmd_total = 0;
  size_t io_read_bytes = 0;
  uint64_t io_reads_total = 0;

  void From(const facade::FacadeStats& fs);
  void Print(RedisReplyBuilder* rb) const;

  IOStat& operator-=(const IOStat& other);
};

void IOStat::From(const facade::FacadeStats& fs) {
  conn_received = fs.conn_stats.conn_received_cnt;
  curr_conn_count = fs.conn_stats.num_conns_main;
  cmd_total = fs.conn_stats.command_cnt_main;
  pipelined_cmd_total = fs.conn_stats.pipelined_cmd_cnt;
  io_read_bytes = fs.conn_stats.io_read_bytes;
  io_reads_total = fs.conn_stats.io_read_cnt;
}

void IOStat::Print(RedisReplyBuilder* rb) const {
  rb->StartCollection(6, RedisReplyBuilder::CollectionType::MAP);
  rb->SendSimpleString("connections_received");
  rb->SendLong(conn_received);
  rb->SendSimpleString("current_conn_count");
  rb->SendLong(curr_conn_count);
  rb->SendSimpleString("commands_total");
  rb->SendLong(cmd_total);
  rb->SendSimpleString("pipelined_commands_total");
  rb->SendLong(pipelined_cmd_total);
  rb->SendSimpleString("io_read_bytes");
  rb->SendLong(io_read_bytes);
  rb->SendSimpleString("io_reads_total");
  rb->SendLong(io_reads_total);
}

IOStat& IOStat::operator-=(const IOStat& other) {
  conn_received -= other.conn_received;
  curr_conn_count -= other.curr_conn_count;
  cmd_total -= other.cmd_total;
  pipelined_cmd_total -= other.pipelined_cmd_total;
  io_read_bytes -= other.io_read_bytes;
  io_reads_total -= other.io_reads_total;

  return *this;
}

}  // namespace

DebugCmd::DebugCmd(ServerFamily* owner, cluster::ClusterFamily* cf, ConnectionContext* cntx)
    : sf_(*owner), cf_(*cf), cntx_(cntx) {
}

void DebugCmd::Run(CmdArgList args, facade::SinkReplyBuilder* builder) {
  string subcmd = absl::AsciiStrToUpper(ArgS(args, 0));
  if (subcmd == "HELP") {
    string_view help_arr[] = {
        "DEBUG <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        "EXEC",
        "    Show the descriptors of the MULTI/EXEC transactions that were processed by ",
        "    the server. For each EXEC/i descriptor, 'i' is the number of shards it touches. ",
        "    Each descriptor details the commands it contained followed by number of their ",
        "    arguments. Each descriptor is prefixed by its frequency count",
        "OBJECT <key> [COMPRESS]",
        "    Show low-level info about `key` and associated value.",
        "RELOAD [option ...]",
        "    Save the RDB on disk and reload it back to memory. Valid <option> values:",
        "    * NOSAVE: the database will be loaded from an existing RDB file.",
        "    Examples:",
        "    * DEBUG RELOAD NOSAVE: replace the current database with the contents of an",
        "      existing RDB file.",
        "REPLICA PAUSE/RESUME",
        "    Stops replica from reconnecting to master, or resumes",
        "MIGRATION PAUSE/RESUME",
        "    Stops/resumes incoming migration process only in the SYNC state",
        "REPLICA OFFSET",
        "    Return sync id and array of number of journal commands executed for each replica flow",
        "WATCHED",
        "    Shows the watched keys as a result of BLPOP and similar operations.",
        "POPULATE <count> [prefix] [size] [RAND] [SLOTS start end] [TYPE type] [ELEMENTS elements]"
        "[EXPIRE start end]",
        "    Create <count> string keys named key:<num> with value value:<num>.",
        "    If <prefix> is specified then it is used instead of the 'key' prefix.",
        "    If <size> is specified then X character is concatenated multiple times to value:<num>",
        "    to meet value size.",
        "    If RAND is specified then value will be set to random hex string in specified size.",
        "    If SLOTS is specified then create keys only in given slots range.",
        "    TYPE specifies data type (must be STRING/LIST/SET/HASH/ZSET/JSON), default STRING.",
        "    ELEMENTS specifies how many sub elements if relevant (like entries in a list / set).",
        "    EXPIRE specifies key expire ttl range.",
        "OBJHIST",
        "    Prints histogram of object sizes.",
        "STACKTRACE",
        "    Prints the stacktraces of all current fibers to the logs.",
        "SHARDS",
        "    Prints memory usage and key stats per shard, as well as min/max indicators.",
        "TOPK ON [min_freq] | OFF [max_keys]",
        "    Turns on or off sampling of topk keys. Provides top keys with at least <min_freq> ",
        "    during the sampling period. The results are returned in descending order of frequency",
        "    when calling TOPK OFF command.",
        "KEYS ON | OFF",
        "    Turns on/off counting of unique keys. Results are returned when calling ",
        "    KEYS OFF command.",
        "TX",
        "    Performs transaction analysis per shard.",
        "TRAFFIC <path> | [STOP]",
        "    Starts traffic logging to the specified path. If path is not specified,",
        "    traffic logging is stopped.",
        "RECVSIZE [<tid> | ENABLE | DISABLE]",
        "    Prints the histogram of the received request sizes on the given thread",
        "COMPRESSION [IMPORT <bintable> | EXPORT | SET <bintable>] [type]",
        "    Estimate the compressibility of values of the given type. if no type is given, ",
        "    checks compressibility of keys. If IN is specified, then the provided ",
        "    bintable is used to check compressibility. If OUT is specified, then ",
        "    the serialized table is printed as well",
        "IOSTATS [PS]",
        "    Prints IO stats per thread. If PS is specified, prints thread-level stats ",
        "    per second.",
        "HELP",
        "    Prints this help.",
    };
    auto* rb = static_cast<RedisReplyBuilder*>(builder);
    return rb->SendSimpleStrArr(help_arr);
  }

  VLOG(1) << "subcmd " << subcmd;

  if (subcmd == "POPULATE") {
    return Populate(args, builder);
  }

  if (subcmd == "RELOAD") {
    return Reload(args, builder);
  }

  if (subcmd == "REPLICA" && args.size() == 2) {
    return Replica(args, builder);
  }

  if (subcmd == "MIGRATION" && args.size() == 2) {
    return Migration(args, builder);
  }

  if (subcmd == "WATCHED") {
    return Watched(builder);
  }

  if (subcmd == "OBJECT" && args.size() >= 2) {
    string_view key = ArgS(args, 1);
    args.remove_prefix(2);
    return Inspect(key, args, builder);
  }

  if (subcmd == "TX") {
    return TxAnalysis(builder);
  }

  if (subcmd == "OBJHIST") {
    return ObjHist(builder);
  }

  if (subcmd == "STACKTRACE") {
    return Stacktrace(builder);
  }

  if (subcmd == "SHARDS") {
    return Shards(builder);
  }

  if (subcmd == "EXEC") {
    return Exec(builder);
  }

  if (subcmd == "TRAFFIC") {
    return LogTraffic(args.subspan(1), builder);
  }

  if (subcmd == "RECVSIZE" && args.size() == 2) {
    return RecvSize(ArgS(args, 1), builder);
  }

  if (subcmd == "TOPK" && args.size() >= 2) {
    return Topk(args.subspan(1), builder);
  }

  if (subcmd == "KEYS" && args.size() >= 2) {
    return Keys(args.subspan(1), builder);
  }

  if (subcmd == "COMPRESSION") {
    return Compression(args.subspan(1), builder);
  }

  if (subcmd == "IOSTATS") {
    return IOStats(args.subspan(1), builder);
  }
  string reply = UnknownSubCmd(subcmd, "DEBUG");
  return builder->SendError(reply, kSyntaxErrType);
}

void DebugCmd::Shutdown() {
  // disable traffic logging
  shard_set->pool()->AwaitFiberOnAll([](auto*) { facade::Connection::StopTrafficLogging(); });
}

void DebugCmd::Reload(CmdArgList args, facade::SinkReplyBuilder* builder) {
  bool save = true;

  for (size_t i = 1; i < args.size(); ++i) {
    string opt = absl::AsciiStrToUpper(ArgS(args, i));
    VLOG(1) << "opt " << opt;

    if (opt == "NOSAVE") {
      save = false;
    } else {
      return builder->SendError("DEBUG RELOAD only supports the NOSAVE options.");
    }
  }

  if (save) {
    string err_details;
    VLOG(1) << "Performing save";

    GenericError ec = sf_.DoSave();
    if (ec) {
      return builder->SendError(ec.Format());
    }
  }

  string last_save_file = sf_.GetLastSaveInfo().file_name;

  sf_.FlushAll(cntx_->ns);

  if (auto fut_ec = sf_.Load(last_save_file, ServerFamily::LoadExistingKeys::kFail); fut_ec) {
    GenericError ec = fut_ec->Get();
    if (ec) {
      string msg = ec.Format();
      LOG(WARNING) << "Could not load file " << msg;
      return builder->SendError(msg);
    }
  }

  builder->SendOk();
}

void DebugCmd::Replica(CmdArgList args, facade::SinkReplyBuilder* builder) {
  args.remove_prefix(1);

  string opt = absl::AsciiStrToUpper(ArgS(args, 0));

  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  if (opt == "PAUSE" || opt == "RESUME") {
    sf_.PauseReplication(opt == "PAUSE");
    return rb->SendOk();
  } else if (opt == "OFFSET") {
    const auto offset_info = sf_.GetReplicaOffsetInfo();
    if (offset_info) {
      rb->StartArray(2);
      rb->SendBulkString(offset_info.value().sync_id);
      rb->StartArray(offset_info.value().flow_offsets.size());
      for (uint64_t offset : offset_info.value().flow_offsets) {
        rb->SendLong(offset);
      }
      return;
    } else {
      return builder->SendError("I am master");
    }
  }
  return builder->SendError(UnknownSubCmd("replica", "DEBUG"));
}

void DebugCmd::Migration(CmdArgList args, facade::SinkReplyBuilder* builder) {
  args.remove_prefix(1);

  string opt = absl::AsciiStrToUpper(ArgS(args, 0));

  if (opt == "PAUSE" || opt == "RESUME") {
    cf_.PauseAllIncomingMigrations(opt == "PAUSE");
    return builder->SendOk();
  }
  return builder->SendError(UnknownSubCmd("MIGRATION", "DEBUG"));
}

enum PopulateFlag { FLAG_RAND, FLAG_TYPE, FLAG_ELEMENTS, FLAG_SLOT, FLAG_EXPIRE, FLAG_UNKNOWN };

// Populate arguments format:
// required: (total count) (key prefix) (val size)
// optional: [RAND | TYPE typename | ELEMENTS element num | SLOTS (key value)+ | EXPIRE start end]
optional<DebugCmd::PopulateOptions> DebugCmd::ParsePopulateArgs(CmdArgList args,
                                                                facade::SinkReplyBuilder* builder) {
  CmdArgParser parser(args.subspan(1));
  PopulateOptions options;

  options.total_count = parser.Next<uint64_t>();
  options.prefix = parser.NextOrDefault<string_view>("key");
  options.val_size = parser.NextOrDefault<uint32_t>(16);

  while (parser.HasNext()) {
    PopulateFlag flag = parser.MapNext("RAND", FLAG_RAND, "TYPE", FLAG_TYPE, "ELEMENTS",
                                       FLAG_ELEMENTS, "SLOTS", FLAG_SLOT, "EXPIRE", FLAG_EXPIRE);
    switch (flag) {
      case FLAG_RAND:
        options.populate_random_values = true;
        break;
      case FLAG_TYPE:
        options.type = absl::AsciiStrToUpper(parser.Next<string_view>());
        break;
      case FLAG_ELEMENTS:
        options.elements = parser.Next<uint32_t>();
        break;
      case FLAG_SLOT: {
        auto [start, end] = parser.Next<FInt<0, 16383>, FInt<0, 16383>>();
        options.slot_range = cluster::SlotRange{SlotId(start), SlotId(end)};
        break;
      }
      case FLAG_EXPIRE: {
        auto [min_ttl, max_ttl] = parser.Next<uint32_t, uint32_t>();
        if (min_ttl >= max_ttl) {
          builder->SendError(kExpiryOutOfRange);
          (void)parser.Error();
          return nullopt;
        }
        options.expire_ttl_range = std::make_pair(min_ttl, max_ttl);
        break;
      }
      default:
        LOG(FATAL) << "Unexpected flag in PopulateArgs. Args: " << args;
        break;
    }
  }
  if (parser.HasError()) {
    builder->SendError(parser.Error()->MakeReply());
    return nullopt;
  }
  return options;
}

void DebugCmd::Populate(CmdArgList args, facade::SinkReplyBuilder* builder) {
  optional<PopulateOptions> options = ParsePopulateArgs(args, builder);
  if (!options.has_value()) {
    return;
  }
  DCHECK(sf_.AreAllReplicasInStableSync());

  ProactorPool& pp = sf_.service().proactor_pool();
  size_t runners_count = pp.size();
  vector<pair<uint64_t, uint64_t>> ranges(runners_count - 1);
  uint64_t batch_size = options->total_count / runners_count;
  size_t from = 0;
  for (size_t i = 0; i < ranges.size(); ++i) {
    ranges[i].first = from;
    ranges[i].second = batch_size;
    from += batch_size;
  }
  ranges.emplace_back(from, options->total_count - from);

  vector<fb2::Fiber> fb_arr(ranges.size());
  for (size_t i = 0; i < ranges.size(); ++i) {
    auto range = ranges[i];

    // whatever we do, we should not capture i by reference.
    fb_arr[i] = pp.at(i)->LaunchFiber([range, options, this] {
      this->PopulateRangeFiber(range.first, range.second, options.value());
    });
  }
  for (auto& fb : fb_arr)
    fb.Join();

  builder->SendOk();

  DCHECK(sf_.AreAllReplicasInStableSync());
}

void DebugCmd::PopulateRangeFiber(uint64_t from, uint64_t num_of_keys,
                                  const PopulateOptions& options) {
  ThisFiber::SetName("populate_range");
  VLOG(1) << "PopulateRange: " << from << "-" << (from + num_of_keys - 1);

  string key = StrCat(options.prefix, ":");
  size_t prefsize = key.size();
  DbIndex db_indx = cntx_->db_index();
  EngineShardSet& ess = *shard_set;
  std::vector<PopulateBatch> ps(ess.size(), PopulateBatch{db_indx});

  uint64_t index = from;
  uint64_t to = from + num_of_keys;
  uint64_t added = 0;
  while (added < num_of_keys) {
    if ((index >= to) && ((index - to) % options.total_count == 0)) {
      index = index - num_of_keys + options.total_count;
    }
    key.resize(prefsize);  // shrink back

    StrAppend(&key, index);

    if (options.slot_range.has_value()) {
      // Each fiber will add num_of_keys. Keys are in the form of <key_prefix>:<index>
      // We need to make sure that different fibers will not add the same key.
      // Fiber starting <key_prefix>:<from> to <key_prefix>:<from+num_of_keys-1>
      // then continue to <key_prefix>:<from+total_count> to
      // <key_prefix>:<from+total_count+num_of_keys-1> and continue until num_of_keys are added.

      // Add keys only in slot range.
      SlotId sid = KeySlot(key);
      if (sid < options.slot_range->start || sid > options.slot_range->end) {
        ++index;
        continue;
      }
    }
    ShardId sid = Shard(key, ess.size());

    auto& shard_batch = ps[sid];
    shard_batch.index[shard_batch.sz++] = index;
    ++added;
    ++index;

    if (shard_batch.sz == 32) {
      ess.Add(sid, [this, index, options, shard_batch] {
        DoPopulateBatch(options, shard_batch);
        if (index % 50 == 0) {
          ThisFiber::Yield();
        }
      });

      // we capture shard_batch by value so we can override it here.
      shard_batch.sz = 0;
    }
  }

  ess.AwaitRunningOnShardQueue([&](EngineShard* shard) {
    DoPopulateBatch(options, ps[shard->shard_id()]);
    // Debug populate does not use transaction framework therefore we call OnCbFinishBlocking
    // manually after running the callback Note that running debug populate while running
    // flushall/db can cause dcheck fail because the finish cb is executed just when we finish
    // populating the database.
    cntx_->ns->GetDbSlice(shard->shard_id()).OnCbFinishBlocking();
  });
}

void DebugCmd::Exec(facade::SinkReplyBuilder* builder) {
  EngineShardSet& ess = *shard_set;
  fb2::Mutex mu;
  std::map<string, unsigned> freq_cnt;

  ess.pool()->AwaitFiberOnAll([&](auto*) {
    for (const auto& k_v : ServerState::tlocal()->exec_freq_count) {
      unique_lock lk(mu);
      freq_cnt[k_v.first] += k_v.second;
    }
  });

  string res;
  for (const auto& k_v : freq_cnt) {
    StrAppend(&res, k_v.second, ":", k_v.first, "\n");
  }
  StrAppend(&res, "--------------------------\n");

  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  rb->SendVerbatimString(res);
}

void DebugCmd::LogTraffic(CmdArgList args, facade::SinkReplyBuilder* builder) {
  optional<string> path;
  if (args.size() == 1 && absl::AsciiStrToUpper(facade::ToSV(args.front())) != "STOP"sv) {
    path = ArgS(args, 0);
    LOG(INFO) << "Logging to traffic to " << *path << "*.bin";
  } else {
    LOG(INFO) << "Traffic logging stopped";
  }

  shard_set->pool()->AwaitFiberOnAll([path](auto*) {
    if (path)
      facade::Connection::StartTrafficLogging(*path);
    else
      facade::Connection::StopTrafficLogging();
  });
  builder->SendOk();
}

void DebugCmd::Inspect(string_view key, CmdArgList args, facade::SinkReplyBuilder* builder) {
  EngineShardSet& ess = *shard_set;
  ShardId sid = Shard(key, ess.size());
  VLOG(1) << "DebugCmd::Inspect " << key;

  bool check_compression = false;
  if (args.size() == 1) {
    check_compression = absl::AsciiStrToUpper(ArgS(args, 0)) == "COMPRESS";
  }
  string resp;

  if (check_compression) {
    auto cb = [&] { return EstimateCompression(cntx_, key); };
    auto res = ess.Await(sid, std::move(cb));
    if (!res) {
      builder->SendError(res.status());
      return;
    }
    StrAppend(&resp, "raw_size: ", res->raw_size, ", compressed_size: ", res->compressed_size);
    if (res->raw_size > 0) {
      StrAppend(&resp, " ratio: ", static_cast<double>(res->compressed_size) / (res->raw_size));
    }
  } else {
    auto cb = [&] { return InspectOp(cntx_, key); };

    ObjInfo res = ess.Await(sid, std::move(cb));

    if (!res.found) {
      builder->SendError(kKeyNotFoundErr);
      return;
    }

    StrAppend(&resp, "encoding:", EncodingName(res.type, res.encoding),
              " bucket_id:", res.bucket_id);
    StrAppend(&resp, " slot:", res.slot_id, " shard:", sid);

    if (res.ttl != INT64_MAX) {
      StrAppend(&resp, " ttl:", res.ttl, res.has_sec_precision ? "s" : "ms");
    }

    if (res.external_len) {
      StrAppend(&resp, " spill_len:", *res.external_len);
    }

    if (res.num_nodes) {
      // node count
      StrAppend(&resp, " nc:", res.num_nodes);
    }

    if (res.num_compressed) {
      // compressed nodes
      StrAppend(&resp, " cn:", res.num_compressed);
    }

    if (res.lock_status != ObjInfo::NONE) {
      StrAppend(&resp, " lock:", res.lock_status == ObjInfo::X ? "x" : "s");
    }
  }
  builder->SendSimpleString(resp);
}

void DebugCmd::Watched(facade::SinkReplyBuilder* builder) {
  fb2::Mutex mu;

  vector<string> watched_keys;
  vector<string> awaked_trans;

  auto cb = [&](EngineShard* shard) {
    auto* bc = cntx_->ns->GetBlockingController(shard->shard_id());
    if (bc) {
      auto keys = bc->GetWatchedKeys(cntx_->db_index());

      lock_guard lk(mu);
      watched_keys.insert(watched_keys.end(), keys.begin(), keys.end());
      for (auto* tx : bc->awakened_transactions()) {
        awaked_trans.push_back(StrCat("[", shard->shard_id(), "] ", tx->DebugId()));
      }
    }
  };

  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  shard_set->RunBlockingInParallel(cb);
  rb->StartArray(4);
  rb->SendBulkString("awaked");
  rb->SendBulkStrArr(awaked_trans);
  rb->SendBulkString("watched");
  rb->SendBulkStrArr(watched_keys);
}

void DebugCmd::TxAnalysis(facade::SinkReplyBuilder* builder) {
  vector<EngineShard::TxQueueInfo> shard_info(shard_set->size());

  auto cb = [&](EngineShard* shard) {
    auto& info = shard_info[shard->shard_id()];
    info = shard->AnalyzeTxQueue();
  };

  shard_set->RunBriefInParallel(cb);

  string result;
  for (unsigned i = 0; i < shard_set->size(); ++i) {
    const auto& info = shard_info[i];
    StrAppend(&result, "shard", i, ":\n", info.Format(), "\n");
  }
  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  rb->SendVerbatimString(result);
}

void DebugCmd::ObjHist(facade::SinkReplyBuilder* builder) {
  vector<ObjHistMap> obj_hist_map_arr(shard_set->size());

  shard_set->RunBlockingInParallel([&](EngineShard* shard) {
    DoBuildObjHist(shard, cntx_, &obj_hist_map_arr[shard->shard_id()]);
  });

  for (size_t i = shard_set->size() - 1; i > 0; --i) {
    MergeObjHistMap(std::move(obj_hist_map_arr[i]), &obj_hist_map_arr[0]);
  }

  string result;
  absl::StrAppend(&result, "___begin object histogram___\n\n");

  for (auto& [obj_type, hist_ptr] : obj_hist_map_arr[0]) {
    StrAppend(&result, "OBJECT:", ObjTypeToString(obj_type), "\n");
    StrAppend(&result, "________________________________________________________________\n");
    StrAppend(&result, "Key length histogram:\n", hist_ptr->key_len.ToString(), "\n");
    StrAppend(&result, "Values - Total Memory used:\n", hist_ptr->val_len.ToString(), "\n");
    if (hist_ptr->card.count() > 0) {
      StrAppend(&result, "Cardinality histogram (number of elements in sets):\n",
                hist_ptr->card.ToString(), "\n");
    }
    StrAppend(&result, "Items length histogram:\n", hist_ptr->entry_len.ToString(), "\n");
    if (hist_ptr->listpack.count() > 0) {
      StrAppend(&result, "Listpack histogram:\n", hist_ptr->listpack.ToString(), "\n");
    }
  }

  absl::StrAppend(&result, "___end object histogram___\n");
  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  rb->SendVerbatimString(result);
}

void DebugCmd::Stacktrace(facade::SinkReplyBuilder* builder) {
  fb2::Mutex m;
  shard_set->pool()->AwaitFiberOnAll([&m](unsigned index, ProactorBase* base) {
    EngineShard* es = EngineShard::tlocal();
    string txq;
    if (es) {
      EngineShard::TxQueueInfo txq_info = es->AnalyzeTxQueue();
      txq = txq_info.Format();
    }
    std::unique_lock lk(m);
    LOG_IF(INFO, !txq.empty()) << "Shard" << index << ": " << txq;
    fb2::detail::FiberInterface::PrintAllFiberStackTraces();
  });
  base::FlushLogs();
  builder->SendOk();
}

void DebugCmd::Shards(facade::SinkReplyBuilder* builder) {
  struct ShardInfo {
    size_t used_memory = 0;
    size_t key_count = 0;
    size_t expire_count = 0;
    size_t key_reads = 0;
  };

  vector<ShardInfo> infos(shard_set->size());
  shard_set->RunBriefInParallel([&](EngineShard* shard) {
    auto sid = shard->shard_id();
    auto slice_stats = cntx_->ns->GetDbSlice(sid).GetStats();
    auto& stats = infos[sid];

    stats.used_memory = shard->UsedMemory();
    for (const auto& db_stats : slice_stats.db_stats) {
      stats.key_count += db_stats.key_count;
      stats.expire_count += db_stats.expire_count;
    }
    stats.key_reads = slice_stats.events.hits + slice_stats.events.misses;
  });

#define ADD_STAT(i, stat) absl::StrAppend(&out, "shard", i, "_", #stat, ": ", infos[i].stat, "\n");
#define MAXMIN_STAT(stat)                                   \
  {                                                         \
    size_t minv = numeric_limits<size_t>::max();            \
    size_t maxv = 0;                                        \
    for (const auto& info : infos) {                        \
      minv = min(minv, info.stat);                          \
      maxv = max(maxv, info.stat);                          \
    }                                                       \
    absl::StrAppend(&out, "max_", #stat, ": ", maxv, "\n"); \
    absl::StrAppend(&out, "min_", #stat, ": ", minv, "\n"); \
  }

  string out;
  absl::StrAppend(&out, "num_shards: ", shard_set->size(), "\n");

  for (size_t i = 0; i < infos.size(); i++) {
    ADD_STAT(i, used_memory);
    ADD_STAT(i, key_count);
    ADD_STAT(i, expire_count);
    ADD_STAT(i, key_reads);
  }

  MAXMIN_STAT(used_memory);
  MAXMIN_STAT(key_count);
  MAXMIN_STAT(expire_count);
  MAXMIN_STAT(key_reads);

#undef ADD_STAT
#undef MAXMIN_STAT
  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  rb->SendVerbatimString(out);
}

void DebugCmd::RecvSize(string_view param, facade::SinkReplyBuilder* builder) {
  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  uint8_t enable = 2;
  if (absl::EqualsIgnoreCase(param, "ENABLE"))
    enable = 1;
  else if (absl::EqualsIgnoreCase(param, "DISABLE"))
    enable = 0;

  if (enable < 2) {
    shard_set->pool()->AwaitBrief(
        [enable](auto, auto*) { facade::Connection::TrackRequestSize(enable == 1); });
    return rb->SendOk();
  }

  unsigned tid;
  if (!absl::SimpleAtoi(param, &tid) || tid >= shard_set->pool()->size()) {
    return rb->SendError(kUintErr);
  }

  string hist;
  shard_set->pool()->at(tid)->AwaitBrief(
      [&]() { facade::Connection::GetRequestSizeHistogramThreadLocal(&hist); });
  rb->SendVerbatimString(hist);
}

void DebugCmd::Topk(CmdArgList args, facade::SinkReplyBuilder* builder) {
  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  DCHECK_GE(args.size(), 1u);

  string_view subcmd = ArgS(args, 0);
  if (absl::EqualsIgnoreCase(subcmd, "ON")) {
    uint32_t min_freq = 100;
    if (args.size() > 1) {
      if (!absl::SimpleAtoi(ArgS(args, 1), &min_freq))
        return rb->SendError(kUintErr);
    }
    shard_set->RunBriefInParallel([&](EngineShard* es) {
      cntx_->ns->GetDbSlice(es->shard_id()).StartSampleTopK(cntx_->db_index(), min_freq);
    });
    return rb->SendOk();
  }

  if (absl::EqualsIgnoreCase(subcmd, "OFF")) {
    vector<DbSlice::SamplingResult> results(shard_set->size());
    uint32_t max_keys = 50;

    if (args.size() > 1) {
      if (!absl::SimpleAtoi(ArgS(args, 1), &max_keys))
        return rb->SendError(kUintErr);
    }

    shard_set->RunBriefInParallel([&](EngineShard* es) {
      results[es->shard_id()] =
          cntx_->ns->GetDbSlice(es->shard_id()).StopSampleTopK(cntx_->db_index());
    });

    vector<pair<uint64_t, string>> items;

    for (const auto& res : results) {
      for (const auto& k_v : res.top_keys) {
        items.emplace_back(k_v.second, k_v.first);
        push_heap(items.begin(), items.end(), std::greater<>());
        if (items.size() > max_keys) {
          pop_heap(items.begin(), items.end(), std::greater<>());
          items.pop_back();
        }
      }
    }

    rb->StartArray(items.size());
    for (const auto& k_v : items) {
      rb->SendBulkString(StrCat(k_v.second, ":", k_v.first));
    }
    return;
  }

  return rb->SendError(kSyntaxErr);
}

void DebugCmd::Keys(CmdArgList args, facade::SinkReplyBuilder* builder) {
  string_view subcmd = ArgS(args, 0);

  if (absl::EqualsIgnoreCase(subcmd, "ON")) {
    shard_set->RunBriefInParallel([&](EngineShard* es) {
      cntx_->ns->GetDbSlice(es->shard_id()).StartSampleKeys(cntx_->db_index());
    });
    return builder->SendOk();
  }

  if (absl::EqualsIgnoreCase(subcmd, "OFF")) {
    atomic_uint64_t total_keys = 0;
    shard_set->RunBriefInParallel([&](EngineShard* es) {
      size_t res = cntx_->ns->GetDbSlice(es->shard_id()).StopSampleKeys(cntx_->db_index());
      total_keys.fetch_add(res, memory_order_relaxed);
    });

    return builder->SendLong(total_keys.load());
  }

  return builder->SendError(kSyntaxErr);
}

void DebugCmd::Compression(CmdArgList args, facade::SinkReplyBuilder* builder) {
  CompactObjType type = kInvalidCompactObjType;
  CmdArgParser parser(args);
  string bintable;
  bool print_bintable = false;

  if (parser.Check("SET", &bintable)) {
    atomic_bool succeed = true;
    shard_set->RunBriefInParallel([&](EngineShard* shard) {
      if (!CompactObj::InitHuffmanThreadLocal(bintable)) {
        succeed = false;
      }
    });
    return succeed ? builder->SendOk() : builder->SendError("Failed to set bintable");
  }

  if (parser.Check("EXPORT")) {
    print_bintable = true;
  } else {
    parser.Check("IMPORT", &bintable);
  }

  if (parser.HasNext()) {
    string_view type_str = parser.Next();
    type = ObjTypeFromString(type_str);
    if (type == kInvalidCompactObjType) {
      return builder->SendError(kSyntaxErr);
    }
  }

  if (parser.HasError()) {
    return builder->SendError(parser.Error()->MakeReply());
  }

  auto* rb = static_cast<RedisReplyBuilder*>(builder);

  fb2::Mutex mu;
  HufHist hist;
  shard_set->RunBlockingInParallel([&](EngineShard* shard) {
    HufHist local;
    DoComputeHist(type, shard, cntx_, &local);
    std::unique_lock lk(mu);
    hist.Merge(local);
  });

  size_t num_bits = 0, compressed_size = 0, raw_size = 0;

  if (hist.max_symbol) {
    HuffmanEncoder huff_enc;
    string err_msg;

    raw_size = 0;
    for (unsigned i = 0; i <= HufHist::kMaxSymbol; i++) {
      raw_size += hist.hist[i];

      // force non-zero weights for all symbols.
      if (hist.hist[i] == 0)
        hist.hist[i] = 1;
    }

    if (bintable.empty()) {
      if (!huff_enc.Build(hist.hist.data(), HufHist::kMaxSymbol, &err_msg)) {
        return rb->SendError(StrCat("Internal error: ", err_msg));
      }
    } else {
      // Try to read the bintable and create a ctable from it.
      if (!huff_enc.Load(bintable, &err_msg)) {
        return rb->SendError(StrCat("Internal error: ", err_msg));
      }
    }
    num_bits = huff_enc.num_bits();
    compressed_size = huff_enc.EstimateCompressedSize(hist.hist.data(), HufHist::kMaxSymbol);

    if (print_bintable) {
      bintable = huff_enc.Export();
      VLOG(1) << "bintable: " << absl::CHexEscape(bintable);
    } else {
      bintable.clear();
    }
  }

  unsigned map_len = print_bintable ? 6 : 5;

  rb->StartCollection(map_len, RedisReplyBuilder::CollectionType::MAP);
  rb->SendSimpleString("max_symbol");
  rb->SendLong(hist.max_symbol);

  rb->SendSimpleString("max_bits");
  rb->SendLong(num_bits);
  rb->SendSimpleString("raw_size");
  rb->SendLong(raw_size);
  rb->SendSimpleString("compressed_size");
  rb->SendLong(compressed_size);
  rb->SendSimpleString("ratio");
  double ratio = raw_size > 0 ? static_cast<double>(compressed_size) / raw_size : 0;
  rb->SendDouble(ratio);
  if (print_bintable) {
    rb->SendSimpleString("bintable");
    rb->SendBulkString(bintable);
  }
}

void DebugCmd::IOStats(CmdArgList args, facade::SinkReplyBuilder* builder) {
  auto* rb = static_cast<RedisReplyBuilder*>(builder);

  bool per_second = args.size() > 0 && absl::EqualsIgnoreCase(args[0], "PS");
  vector<IOStat> stats(shard_set->pool()->size());

  shard_set->pool()->AwaitBrief(
      [&](unsigned index, ProactorBase*) { stats[index].From(*facade::tl_facade_stats); });

  if (per_second) {
    ThisFiber::SleepFor(1s);
    vector<IOStat> stats2(shard_set->pool()->size());
    shard_set->pool()->AwaitBrief(
        [&](unsigned index, ProactorBase*) { stats2[index].From(*facade::tl_facade_stats); });

    for (size_t i = 0; i < stats.size(); ++i) {
      stats2[i] -= stats[i];
    }
    stats = std::move(stats2);
  }

  rb->StartArray(stats.size());
  for (const auto& stat : stats) {
    stat.Print(rb);
  }
}

void DebugCmd::DoPopulateBatch(const PopulateOptions& options, const PopulateBatch& batch) {
  boost::intrusive_ptr<Transaction> local_tx =
      new Transaction{sf_.service().mutable_registry()->Find("EXEC")};
  local_tx->StartMultiNonAtomic();
  boost::intrusive_ptr<Transaction> stub_tx =
      new Transaction{local_tx.get(), EngineShard::tlocal()->shard_id(), nullopt};

  absl::InlinedVector<string_view, 5> args_view;
  facade::CapturingReplyBuilder crb;
  ConnectionContext local_cntx{cntx_, stub_tx.get()};
  absl::InsecureBitGen gen;
  for (unsigned i = 0; i < batch.sz; ++i) {
    string key = StrCat(options.prefix, ":", batch.index[i]);
    uint32_t elements_left = options.elements;

    while (elements_left) {
      // limit rss grow by 32K by limiting the element count in each command.
      uint32_t max_batch_elements = std::max(32_KB / options.val_size, 1ULL);
      uint32_t populate_elements = std::min(max_batch_elements, elements_left);
      elements_left -= populate_elements;
      auto [cid, args] = GeneratePopulateCommand(options.type, key, options.val_size,
                                                 options.populate_random_values, populate_elements,
                                                 *sf_.service().mutable_registry(), &gen);
      if (!cid) {
        LOG_EVERY_N(WARNING, 10'000) << "Unable to find command, was it renamed?";
        break;
      }

      args_view.clear();
      for (auto& arg : args) {
        args_view.push_back(arg);
      }
      auto args_span = absl::MakeSpan(args_view);
      local_cntx.SwitchTxCmd(cid);
      crb.SetReplyMode(ReplyMode::NONE);
      stub_tx->InitByArgs(cntx_->ns, local_cntx.conn_state.db_index, args_span);

      sf_.service().InvokeCmd(cid, args_span,
                              CommandContext{local_cntx.transaction, &crb, &local_cntx});
    }

    if (options.expire_ttl_range.has_value()) {
      uint32_t start = options.expire_ttl_range->first;
      uint32_t end = options.expire_ttl_range->second;
      uint32_t expire_ttl = rand() % (end - start) + start;
      VLOG(1) << "set key " << key << " expire ttl as " << expire_ttl;
      auto cid = sf_.service().mutable_registry()->Find("EXPIRE");
      absl::InlinedVector<string, 5> args;
      args.push_back(std::move(key));
      args.push_back(to_string(expire_ttl));
      args_view.clear();
      for (auto& arg : args) {
        args_view.push_back(arg);
      }
      auto args_span = absl::MakeSpan(args_view);
      local_cntx.SwitchTxCmd(cid);
      crb.SetReplyMode(ReplyMode::NONE);
      stub_tx->InitByArgs(cntx_->ns, local_cntx.conn_state.db_index, args_span);
      sf_.service().InvokeCmd(cid, args_span,
                              CommandContext{local_cntx.transaction, &crb, &local_cntx});
    }
  }

  local_tx->UnlockMulti();
}

}  // namespace dfly
