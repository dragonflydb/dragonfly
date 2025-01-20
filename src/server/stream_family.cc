// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/stream_family.h"

#include <absl/strings/str_cat.h>

extern "C" {
#include "redis/stream.h"
#include "redis/zmalloc.h"
}

#include "base/logging.h"
#include "facade/cmd_arg_parser.h"
#include "server/acl/acl_commands_def.h"
#include "server/blocking_controller.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"
#include "server/error.h"
#include "server/family_utils.h"
#include "server/transaction.h"

namespace dfly {

using namespace facade;
using namespace std;

StreamMemTracker::StreamMemTracker() {
  start_size_ = zmalloc_used_memory_tl;
}

void StreamMemTracker::UpdateStreamSize(PrimeValue& pv) const {
  const size_t current = zmalloc_used_memory_tl;
  int64_t diff = static_cast<int64_t>(current) - static_cast<int64_t>(start_size_);
  pv.AddStreamSize(diff);
  // Under any flow we must not end up with this special value.
  DCHECK(pv.MallocUsed() != 0);
}

namespace {

struct Record {
  streamID id;
  vector<pair<string, string>> kv_arr;
};

using RecordVec = vector<Record>;

struct ParsedStreamId {
  streamID val;

  // Was an ID different than "ms-*" specified? for XADD only.
  bool has_seq = false;
  // Was an ID different than "*" specified? for XADD only.
  bool id_given = false;

  // Whether to lookup messages after the last ID in the stream. Used for XREAD
  // when using ID '$'.
  bool resolve_last_id = false;
};

struct RangeId {
  ParsedStreamId parsed_id;
  bool exclude = false;
};

enum class TrimStrategy {
  kNone = TRIM_STRATEGY_NONE,
  kMaxLen = TRIM_STRATEGY_MAXLEN,
  kMinId = TRIM_STRATEGY_MINID,
};

struct AddTrimOpts {
  string_view key;
  ParsedStreamId minid;
  uint32_t max_len = kuint32max;
  uint32_t limit = 0;
  TrimStrategy trim_strategy = TrimStrategy::kNone;
  bool trim_approx = false;

  // XADD only.
  ParsedStreamId parsed_id;
  bool no_mkstream = false;
};

struct NACKInfo {
  streamID pel_id;
  string consumer_name;
  size_t delivery_time;
  size_t delivery_count;
};

struct ConsumerInfo {
  string name;
  mstime_t seen_time;
  mstime_t active_time;
  size_t pel_count;
  vector<NACKInfo> pending;
  size_t idle;
};

struct GroupInfo {
  string name;
  size_t consumer_size;
  size_t pending_size;
  streamID last_id;
  int64_t entries_read;
  int64_t lag;
  vector<NACKInfo> stream_nack_vec;
  vector<ConsumerInfo> consumer_info_vec;
};

using GroupInfoVec = vector<GroupInfo>;

struct StreamInfo {
  size_t length;
  size_t radix_tree_keys;
  size_t radix_tree_nodes;
  size_t groups;
  streamID recorded_first_entry_id;
  streamID last_generated_id;
  streamID max_deleted_entry_id;
  size_t entries_added;
  Record first_entry;
  Record last_entry;
  vector<Record> entries;
  GroupInfoVec cgroups;
};

struct RangeOpts {
  ParsedStreamId start;
  ParsedStreamId end;
  bool is_rev = false;
  uint32_t count = kuint32max;

  // readgroup range fields
  streamCG* group = nullptr;
  streamConsumer* consumer = nullptr;
  bool noack = false;
};

struct StreamIDsItem {
  ParsedStreamId id;

  // Readgroup fields - id and group-consumer pair is exclusive.
  streamCG* group = nullptr;
  streamConsumer* consumer = nullptr;
};

struct ReadOpts {
  // Contains a mapping from stream name to the starting stream ID.
  unordered_map<string_view, StreamIDsItem> stream_ids;
  // Contains the maximum number of entries to return for each stream.
  uint32_t count = kuint32max;
  // Contains the time to block waiting for entries, or -1 if should not block.
  int64_t timeout = -1;
  size_t streams_arg = 0;

  // readgroup fields
  bool read_group = false;
  bool serve_history = false;
  string_view group_name;
  string_view consumer_name;
  bool noack = false;
};

const char kInvalidStreamId[] = "Invalid stream ID specified as stream command argument";
const char kXGroupKeyNotFound[] =
    "The XGROUP subcommand requires the key to exist. "
    "Note that for CREATE you may want to use the MKSTREAM option to create "
    "an empty stream automatically.";
const char kSameStreamFound[] = "Same stream specified multiple time";

const uint32_t STREAM_LISTPACK_MAX_SIZE = 1 << 30;
const uint32_t kStreamNodeMaxBytes = 4096;
const uint32_t kStreamNodeMaxEntries = 100;
const uint32_t STREAM_LISTPACK_MAX_PRE_ALLOCATE = 4096;

/* Every stream item inside the listpack, has a flags field that is used to
 * mark the entry as deleted, or having the same field as the "master"
 * entry at the start of the listpack. */
// const uint32_t STREAM_ITEM_FLAG_DELETED = (1 << 0);    /* Entry is deleted. Skip it. */
const uint32_t STREAM_ITEM_FLAG_SAMEFIELDS = (1 << 1); /* Same fields as master entry. */

string StreamIdRepr(const streamID& id) {
  return absl::StrCat(id.ms, "-", id.seq);
};

string NoGroupError(string_view key, string_view cgroup) {
  return absl::StrCat("-NOGROUP No such consumer group '", cgroup, "' for key name '", key, "'");
}

string NoGroupOrKey(string_view key, string_view cgroup, string_view suffix = "") {
  return absl::StrCat("-NOGROUP No such key '", key, "'", " or consumer group '", cgroup, "'",
                      suffix);
}

string LeqTopIdError(string_view cmd_name) {
  return absl::StrCat("The ID specified in ", cmd_name,
                      " is equal or smaller than the target stream top item");
}

inline const uint8_t* SafePtr(MutableSlice field) {
  return field.empty() ? reinterpret_cast<const uint8_t*>("")
                       : reinterpret_cast<const uint8_t*>(field.data());
}

bool ParseID(string_view strid, bool strict, uint64_t missing_seq, ParsedStreamId* dest) {
  if (strid.empty() || strid.size() > 127)
    return false;

  if (strid == "*")
    return true;

  dest->id_given = true;
  dest->has_seq = true;

  /* Handle the "-" and "+" special cases. */
  if (strid == "-" || strid == "+") {
    if (strict)
      return false;

    if (strid == "-") {
      dest->val.ms = 0;
      dest->val.seq = 0;
      return true;
    }

    dest->val.ms = UINT64_MAX;
    dest->val.seq = UINT64_MAX;
    return true;
  }

  /* Parse <ms>-<seq> form. */
  streamID result{.ms = 0, .seq = missing_seq};

  size_t dash_pos = strid.find('-');
  if (!absl::SimpleAtoi(strid.substr(0, dash_pos), &result.ms))
    return false;

  if (dash_pos != string_view::npos) {
    if (dash_pos + 1 == strid.size())
      return false;

    if (dash_pos + 2 == strid.size() && strid[dash_pos + 1] == '*') {
      result.seq = 0;
      dest->has_seq = false;
    } else if (!absl::SimpleAtoi(strid.substr(dash_pos + 1), &result.seq)) {
      return false;
    }
  }

  dest->val = result;

  return true;
}

enum class RangeBoundary { kStart, kEnd };
bool ParseRangeId(string_view id, RangeBoundary type, RangeId* dest) {
  if (id.empty())
    return false;
  if (id[0] == '(') {
    dest->exclude = true;
    id.remove_prefix(1);
  }
  uint64 missing_seq = type == RangeBoundary::kStart ? 0 : -1;
  return ParseID(id, dest->exclude, missing_seq, &dest->parsed_id);
}

/* This is a wrapper function for lpGet() to directly get an integer value
 * from the listpack (that may store numbers as a string), converting
 * the string if needed.
 * The `valid` argument is an optional output parameter to get an indication
 * if the record was valid, when this parameter is NULL, the function will
 * fail with an assertion. */
static inline int64_t lpGetIntegerIfValid(unsigned char* ele, int* valid) {
  int64_t v;
  unsigned char* e = lpGet(ele, &v, NULL);
  if (e == NULL) {
    if (valid)
      *valid = 1;
    return v;
  }
  /* The following code path should never be used for how listpacks work:
   * they should always be able to store an int64_t value in integer
   * encoded form. However the implementation may change. */
  long long ll;
  int ret = string2ll((char*)e, v, &ll);
  if (valid)
    *valid = ret;
  else
    serverAssert(ret != 0);
  v = ll;
  return v;
}

int64_t lpGetInteger(unsigned char* ele) {
  return lpGetIntegerIfValid(ele, NULL);
}

/* Generate the next stream item ID given the previous one. If the current
 * milliseconds Unix time is greater than the previous one, just use this
 * as time part and start with sequence part of zero. Otherwise we use the
 * previous time (and never go backward) and increment the sequence. */
void StreamNextID(uint64_t now_ms, const streamID* last_id, streamID* new_id) {
  if (now_ms > last_id->ms) {
    new_id->ms = now_ms;
    new_id->seq = 0;
  } else {
    *new_id = *last_id;
    streamIncrID(new_id);
  }
}

/* Convert the specified stream entry ID as a 128 bit big endian number, so
 * that the IDs can be sorted lexicographically. */
inline void StreamEncodeID(uint8_t* buf, streamID* id) {
  absl::big_endian::Store64(buf, id->ms);
  absl::big_endian::Store64(buf + 8, id->seq);
}

/* Adds a new item into the stream 's' having the specified number of
 * field-value pairs as specified in 'numfields' and stored into 'argv'.
 * Returns the new entry ID populating the 'added_id' structure.
 *
 * If 'use_id' is not NULL, the ID is not auto-generated by the function,
 * but instead the passed ID is used to add the new entry. In this case
 * adding the entry may fail as specified later in this comment.
 *
 * When 'use_id' is used alongside with a zero 'seq-given', the sequence
 * part of the passed ID is ignored and the function will attempt to use an
 * auto-generated sequence.
 *
 * The function returns 0 if the item was added, this is always true
 * if the ID was generated by the function. However the function may return
 * errors in several cases:
 * 1. If an ID was given via 'use_id', but adding it failed since the
 *    current top ID is greater or equal, it returns EDOM.
 * 2. If a size of a single element or the sum of the elements is too big to
 *    be stored into the stream. it returns ERANGE. */
int StreamAppendItem(stream* s, CmdArgList fields, uint64_t now_ms, streamID* added_id,
                     streamID* use_id, int seq_given) {
  /* Generate the new entry ID. */
  streamID id;
  if (use_id) {
    if (seq_given) {
      id = *use_id;
    } else {
      /* The automatically generated sequence can be either zero (new
       * timestamps) or the incremented sequence of the last ID. In the
       * latter case, we need to prevent an overflow/advancing forward
       * in time. */
      if (s->last_id.ms == use_id->ms) {
        if (s->last_id.seq == UINT64_MAX) {
          return EDOM;
        }
        id = s->last_id;
        id.seq++;
      } else {
        id = *use_id;
      }
    }
  } else {
    StreamNextID(now_ms, &s->last_id, &id);
  }

  /* Check that the new ID is greater than the last entry ID
   * or return an error. Automatically generated IDs might
   * overflow (and wrap-around) when incrementing the sequence
     part. */
  if (streamCompareID(&id, &s->last_id) <= 0) {
    return EDOM;
  }

  /* Avoid overflow when trying to add an element to the stream (listpack
   * can only host up to 32bit length strings, and also a total listpack size
   * can't be bigger than 32bit length. */
  size_t totelelen = 0;
  for (size_t i = 0; i < fields.size(); i++) {
    totelelen += fields[i].size();
  }

  if (totelelen > STREAM_LISTPACK_MAX_SIZE) {
    return ERANGE;
  }

  /* Add the new entry. */
  raxIterator ri;
  raxStart(&ri, s->rax_tree);
  raxSeek(&ri, "$", NULL, 0);

  size_t lp_bytes = 0;      /* Total bytes in the tail listpack. */
  unsigned char* lp = NULL; /* Tail listpack pointer. */

  if (!raxEOF(&ri)) {
    /* Get a reference to the tail node listpack. */
    lp = (uint8_t*)ri.data;
    lp_bytes = lpBytes(lp);
  }
  raxStop(&ri);

  /* We have to add the key into the radix tree in lexicographic order,
   * to do so we consider the ID as a single 128 bit number written in
   * big endian, so that the most significant bytes are the first ones. */
  uint8_t rax_key[16]; /* Key in the radix tree containing the listpack.*/
  streamID master_id;  /* ID of the master entry in the listpack. */

  /* Create a new listpack and radix tree node if needed. Note that when
   * a new listpack is created, we populate it with a "master entry". This
   * is just a set of fields that is taken as references in order to compress
   * the stream entries that we'll add inside the listpack.
   *
   * Note that while we use the first added entry fields to create
   * the master entry, the first added entry is NOT represented in the master
   * entry, which is a stand alone object. But of course, the first entry
   * will compress well because it's used as reference.
   *
   * The master entry is composed like in the following example:
   *
   * +-------+---------+------------+---------+--/--+---------+---------+-+
   * | count | deleted | num-fields | field_1 | field_2 | ... | field_N |0|
   * +-------+---------+------------+---------+--/--+---------+---------+-+
   *
   * count and deleted just represent respectively the total number of
   * entries inside the listpack that are valid, and marked as deleted
   * (deleted flag in the entry flags set). So the total number of items
   * actually inside the listpack (both deleted and not) is count+deleted.
   *
   * The real entries will be encoded with an ID that is just the
   * millisecond and sequence difference compared to the key stored at
   * the radix tree node containing the listpack (delta encoding), and
   * if the fields of the entry are the same as the master entry fields, the
   * entry flags will specify this fact and the entry fields and number
   * of fields will be omitted (see later in the code of this function).
   *
   * The "0" entry at the end is the same as the 'lp-count' entry in the
   * regular stream entries (see below), and marks the fact that there are
   * no more entries, when we scan the stream from right to left. */

  /* First of all, check if we can append to the current macro node or
   * if we need to switch to the next one. 'lp' will be set to NULL if
   * the current node is full. */
  if (lp != NULL) {
    int new_node = 0;
    size_t node_max_bytes = kStreamNodeMaxBytes;
    if (node_max_bytes == 0 || node_max_bytes > STREAM_LISTPACK_MAX_SIZE)
      node_max_bytes = STREAM_LISTPACK_MAX_SIZE;
    if (lp_bytes + totelelen >= node_max_bytes) {
      new_node = 1;
    } else if (kStreamNodeMaxEntries) {
      unsigned char* lp_ele = lpFirst(lp);
      /* Count both live entries and deleted ones. */
      int64_t count = lpGetInteger(lp_ele) + lpGetInteger(lpNext(lp, lp_ele));
      if (count >= kStreamNodeMaxEntries) {
        new_node = 1;
      }
    }

    if (new_node) {
      /* Shrink extra pre-allocated memory */
      lp = lpShrinkToFit(lp);
      if (ri.data != lp)
        raxInsert(s->rax_tree, ri.key, ri.key_len, lp, NULL);
      lp = NULL;
    }
  }

  int flags = 0;
  unsigned numfields = fields.size() / 2;
  if (lp == NULL) {
    master_id = id;
    StreamEncodeID(rax_key, &id);
    /* Create the listpack having the master entry ID and fields.
     * Pre-allocate some bytes when creating listpack to avoid realloc on
     * every XADD. Since listpack.c uses malloc_size, it'll grow in steps,
     * and won't realloc on every XADD.
     * When listpack reaches max number of entries, we'll shrink the
     * allocation to fit the data. */
    size_t prealloc = STREAM_LISTPACK_MAX_PRE_ALLOCATE;

    lp = lpNew(prealloc);
    lp = lpAppendInteger(lp, 1); /* One item, the one we are adding. */
    lp = lpAppendInteger(lp, 0); /* Zero deleted so far. */
    lp = lpAppendInteger(lp, numfields);
    for (int64_t i = 0; i < numfields; i++) {
      MutableSlice field = fields[i * 2];

      lp = lpAppend(lp, SafePtr(field), field.size());
    }
    lp = lpAppendInteger(lp, 0); /* Master entry zero terminator. */
    raxInsert(s->rax_tree, (unsigned char*)&rax_key, sizeof(rax_key), lp, NULL);
    /* The first entry we insert, has obviously the same fields of the
     * master entry. */
    flags |= STREAM_ITEM_FLAG_SAMEFIELDS;
  } else {
    serverAssert(ri.key_len == sizeof(rax_key));
    memcpy(rax_key, ri.key, sizeof(rax_key));

    /* Read the master ID from the radix tree key. */
    streamDecodeID(rax_key, &master_id);
    unsigned char* lp_ele = lpFirst(lp);

    /* Update count and skip the deleted fields. */
    int64_t count = lpGetInteger(lp_ele);
    lp = lpReplaceInteger(lp, &lp_ele, count + 1);
    lp_ele = lpNext(lp, lp_ele); /* seek deleted. */
    lp_ele = lpNext(lp, lp_ele); /* seek master entry num fields. */

    /* Check if the entry we are adding, have the same fields
     * as the master entry. */
    int64_t master_fields_count = lpGetInteger(lp_ele);
    lp_ele = lpNext(lp, lp_ele);
    if (numfields == master_fields_count) {
      int64_t i;
      for (i = 0; i < master_fields_count; i++) {
        MutableSlice field = fields[i * 2];
        int64_t e_len;
        unsigned char buf[LP_INTBUF_SIZE];
        unsigned char* e = lpGet(lp_ele, &e_len, buf);
        /* Stop if there is a mismatch. */
        if (field.size() != (size_t)e_len || memcmp(e, field.data(), e_len) != 0)
          break;
        lp_ele = lpNext(lp, lp_ele);
      }
      /* All fields are the same! We can compress the field names
       * setting a single bit in the flags. */
      if (i == master_fields_count)
        flags |= STREAM_ITEM_FLAG_SAMEFIELDS;
    }
  }

  /* Populate the listpack with the new entry. We use the following
   * encoding:
   *
   * +-----+--------+----------+-------+-------+-/-+-------+-------+--------+
   * |flags|entry-id|num-fields|field-1|value-1|...|field-N|value-N|lp-count|
   * +-----+--------+----------+-------+-------+-/-+-------+-------+--------+
   *
   * However if the SAMEFIELD flag is set, we have just to populate
   * the entry with the values, so it becomes:
   *
   * +-----+--------+-------+-/-+-------+--------+
   * |flags|entry-id|value-1|...|value-N|lp-count|
   * +-----+--------+-------+-/-+-------+--------+
   *
   * The entry-id field is actually two separated fields: the ms
   * and seq difference compared to the master entry.
   *
   * The lp-count field is a number that states the number of listpack pieces
   * that compose the entry, so that it's possible to travel the entry
   * in reverse order: we can just start from the end of the listpack, read
   * the entry, and jump back N times to seek the "flags" field to read
   * the stream full entry. */
  lp = lpAppendInteger(lp, flags);
  lp = lpAppendInteger(lp, id.ms - master_id.ms);
  lp = lpAppendInteger(lp, id.seq - master_id.seq);
  if (!(flags & STREAM_ITEM_FLAG_SAMEFIELDS))
    lp = lpAppendInteger(lp, numfields);
  for (int64_t i = 0; i < numfields; i++) {
    MutableSlice field = fields[i * 2], value = fields[i * 2 + 1];
    if (!(flags & STREAM_ITEM_FLAG_SAMEFIELDS))
      lp = lpAppend(lp, SafePtr(field), field.size());
    lp = lpAppend(lp, SafePtr(value), value.size());
  }
  /* Compute and store the lp-count field. */
  int64_t lp_count = numfields;
  lp_count += 3; /* Add the 3 fixed fields flags + ms-diff + seq-diff. */
  if (!(flags & STREAM_ITEM_FLAG_SAMEFIELDS)) {
    /* If the item is not compressed, it also has the fields other than
     * the values, and an additional num-fields field. */
    lp_count += numfields + 1;
  }
  lp = lpAppendInteger(lp, lp_count);

  /* Insert back into the tree in order to update the listpack pointer. */
  if (ri.data != lp)
    raxInsert(s->rax_tree, (unsigned char*)&rax_key, sizeof(rax_key), lp, NULL);
  s->length++;
  s->entries_added++;
  s->last_id = id;
  if (s->length == 1)
    s->first_id = id;
  if (added_id)
    *added_id = id;

  return 0;
}

/* Create a NACK entry setting the delivery count to 1 and the delivery
 * time to the current time or test-hooked time. The NACK consumer will be
 * set to the one specified as argument of the function. */
streamNACK* StreamCreateNACK(streamConsumer* consumer, uint64_t now_ms) {
  streamNACK* nack = reinterpret_cast<streamNACK*>(zmalloc(sizeof(*nack)));
  nack->delivery_time = now_ms;
  nack->delivery_count = 1;
  nack->consumer = consumer;
  return nack;
}

int StreamTrim(const AddTrimOpts& opts, stream* s) {
  if (!opts.limit) {
    if (opts.trim_strategy == TrimStrategy::kMaxLen) {
      /* Notify xtrim event if needed. */
      return streamTrimByLength(s, opts.max_len, opts.trim_approx);
      // TODO: when replicating, we should propagate it as exact limit in case of trimming.
    } else if (opts.trim_strategy == TrimStrategy::kMinId) {
      return streamTrimByID(s, opts.minid.val, opts.trim_approx);
    }
  } else {
    streamAddTrimArgs trim_args = {};
    trim_args.trim_strategy = static_cast<int>(opts.trim_strategy);
    trim_args.approx_trim = opts.trim_approx;
    trim_args.limit = opts.limit;

    if (opts.trim_strategy == TrimStrategy::kMaxLen) {
      trim_args.maxlen = opts.max_len;
    } else if (opts.trim_strategy == TrimStrategy::kMinId) {
      trim_args.minid = opts.minid.val;
    }
    return streamTrim(s, &trim_args);
  }

  return 0;
}

OpResult<streamID> OpAdd(const OpArgs& op_args, const AddTrimOpts& opts, CmdArgList args) {
  DCHECK(!args.empty() && args.size() % 2 == 0);
  auto& db_slice = op_args.GetDbSlice();
  DbSlice::AddOrFindResult add_res;

  if (opts.no_mkstream) {
    auto res_it = db_slice.FindMutable(op_args.db_cntx, opts.key, OBJ_STREAM);
    if (!res_it) {
      return res_it.status();
    }
    add_res = std::move(*res_it);
  } else {
    auto op_res = db_slice.AddOrFind(op_args.db_cntx, opts.key);
    RETURN_ON_BAD_STATUS(op_res);
    add_res = std::move(*op_res);
  }

  auto& it = add_res.it;

  StreamMemTracker mem_tracker;

  if (add_res.is_new) {
    stream* s = streamNew();
    it->second.InitRobj(OBJ_STREAM, OBJ_ENCODING_STREAM, s);
  } else if (it->second.ObjType() != OBJ_STREAM) {
    return OpStatus::WRONG_TYPE;
  }

  stream* stream_inst = (stream*)it->second.RObjPtr();

  streamID result_id;
  const auto& parsed_id = opts.parsed_id;
  streamID passed_id = parsed_id.val;
  int res = StreamAppendItem(stream_inst, args, op_args.db_cntx.time_now_ms, &result_id,
                             parsed_id.id_given ? &passed_id : nullptr, parsed_id.has_seq);

  if (res != 0) {
    if (res == ERANGE)
      return OpStatus::OUT_OF_RANGE;
    if (res == EDOM)
      return OpStatus::STREAM_ID_SMALL;

    return OpStatus::OUT_OF_MEMORY;
  }

  StreamTrim(opts, stream_inst);

  mem_tracker.UpdateStreamSize(it->second);

  auto blocking_controller = op_args.db_cntx.ns->GetBlockingController(op_args.shard->shard_id());
  if (blocking_controller) {
    blocking_controller->AwakeWatched(op_args.db_cntx.db_index, opts.key);
  }

  return result_id;
}

OpResult<RecordVec> OpRange(const OpArgs& op_args, string_view key, const RangeOpts& opts) {
  auto& db_slice = op_args.GetDbSlice();
  auto res_it = db_slice.FindReadOnly(op_args.db_cntx, key, OBJ_STREAM);
  if (!res_it)
    return res_it.status();

  RecordVec result;

  if (opts.count == 0)
    return result;

  streamIterator si;
  int64_t numfields;
  streamID id;
  const CompactObj& cobj = (*res_it)->second;
  stream* s = (stream*)cobj.RObjPtr();
  streamID sstart = opts.start.val, send = opts.end.val;

  streamIteratorStart(&si, s, &sstart, &send, opts.is_rev);
  while (streamIteratorGetID(&si, &id, &numfields)) {
    Record rec;
    rec.id = id;
    rec.kv_arr.reserve(numfields);
    if (opts.group && streamCompareID(&id, &opts.group->last_id) > 0) {
      if (opts.group->entries_read != SCG_INVALID_ENTRIES_READ &&
          !streamRangeHasTombstones(s, &id, NULL)) {
        /* A valid counter and no future tombstones mean we can
         * increment the read counter to keep tracking the group's
         * progress. */
        opts.group->entries_read++;
      } else if (s->entries_added) {
        /* The group's counter may be invalid, so we try to obtain it. */
        opts.group->entries_read = streamEstimateDistanceFromFirstEverEntry(s, &id);
      }
      opts.group->last_id = id;
    }

    /* Emit the field-value pairs. */
    while (numfields--) {
      unsigned char *key, *value;
      int64_t key_len, value_len;
      streamIteratorGetField(&si, &key, &value, &key_len, &value_len);
      string skey(reinterpret_cast<char*>(key), key_len);
      string sval(reinterpret_cast<char*>(value), value_len);

      rec.kv_arr.emplace_back(std::move(skey), std::move(sval));
    }

    result.push_back(std::move(rec));

    if (opts.group && !opts.noack) {
      unsigned char buf[sizeof(streamID)];
      StreamEncodeID(buf, &id);
      uint64_t now_ms = op_args.db_cntx.time_now_ms;

      /* Try to add a new NACK. Most of the time this will work and
       * will not require extra lookups. We'll fix the problem later
       * if we find that there is already an entry for this ID. */
      streamNACK* nack = StreamCreateNACK(opts.consumer, now_ms);
      int group_inserted = raxTryInsert(opts.group->pel, buf, sizeof(buf), nack, nullptr);

      int consumer_inserted = raxTryInsert(opts.consumer->pel, buf, sizeof(buf), nack, nullptr);

      /* Now we can check if the entry was already busy, and
       * in that case reassign the entry to the new consumer,
       * or update it if the consumer is the same as before. */
      if (group_inserted == 0) {
        streamFreeNACK(nack);
        nack = static_cast<streamNACK*>(raxFind(opts.group->pel, buf, sizeof(buf)));
        DCHECK(nack != raxNotFound);
        raxRemove(nack->consumer->pel, buf, sizeof(buf), NULL);
        LOG_IF(DFATAL, nack->consumer->pel->numnodes == 0) << "Invalid rax state";

        /* Update the consumer and NACK metadata. */
        nack->consumer = opts.consumer;
        nack->delivery_time = now_ms;
        nack->delivery_count = 1;
        /* Add the entry in the new consumer local PEL. */
        raxInsert(opts.consumer->pel, buf, sizeof(buf), nack, NULL);
      } else if (group_inserted == 1 && consumer_inserted == 0) {
        LOG(DFATAL) << "Internal error";
        return OpStatus::SKIPPED;  // ("NACK half-created. Should not be possible.");
      }
      opts.consumer->active_time = now_ms;
    }
    if (opts.count == result.size())
      break;
  }

  streamIteratorStop(&si);

  return result;
}

OpResult<RecordVec> OpRangeFromConsumerPEL(const OpArgs& op_args, string_view key,
                                           const RangeOpts& opts) {
  RecordVec result;

  if (opts.count == 0)
    return result;

  unsigned char start_key[sizeof(streamID)];
  unsigned char end_key[sizeof(streamID)];
  auto sstart = opts.start.val;
  auto send = opts.end.val;

  StreamEncodeID(start_key, &sstart);
  StreamEncodeID(end_key, &send);
  raxIterator ri;

  raxStart(&ri, opts.consumer->pel);
  raxSeek(&ri, ">=", start_key, sizeof(start_key));
  size_t ecount = 0;
  while (raxNext(&ri) && (!opts.count || ecount < opts.count)) {
    if (memcmp(ri.key, &send, ri.key_len) > 0)
      break;
    streamID id;

    streamDecodeID(ri.key, &id);
    RangeOpts ropts;
    ropts.start.val = id;
    ropts.end.val = id;
    auto op_result = OpRange(op_args, key, ropts);
    if (!op_result || !op_result.value().size()) {
      result.push_back(Record{id, vector<pair<string, string>>()});
    } else {
      streamNACK* nack = static_cast<streamNACK*>(ri.data);
      nack->delivery_time = op_args.db_cntx.time_now_ms;
      nack->delivery_count++;
      result.push_back(std::move(op_result.value()[0]));
    }
    ecount++;
  }
  raxStop(&ri);
  return result;
}

namespace {
// Our C-API doesn't use const, so we have to const cast.
// Only intended for read-only functions.
stream* GetReadOnlyStream(const CompactObj& cobj) {
  return const_cast<stream*>((const stream*)cobj.RObjPtr());
}

}  // namespace

// Returns the range response for each stream on this shard in order of
// GetShardArgs.
vector<RecordVec> OpRead(const OpArgs& op_args, const ShardArgs& shard_args, const ReadOpts& opts) {
  DCHECK(!shard_args.Empty());

  RangeOpts range_opts;
  range_opts.count = opts.count;
  range_opts.end = ParsedStreamId{.val = streamID{
                                      .ms = UINT64_MAX,
                                      .seq = UINT64_MAX,
                                  }};

  vector<RecordVec> response(shard_args.Size());
  unsigned index = 0;
  for (string_view key : shard_args) {
    const auto& sitem = opts.stream_ids.at(key);
    auto& dest = response[index++];
    if (!sitem.group && opts.read_group) {
      continue;
    }
    range_opts.start = sitem.id;
    range_opts.group = sitem.group;
    range_opts.consumer = sitem.consumer;
    range_opts.noack = opts.noack;

    OpResult<RecordVec> range_res;

    if (opts.serve_history)
      range_res = OpRangeFromConsumerPEL(op_args, key, range_opts);
    else
      range_res = OpRange(op_args, key, range_opts);
    if (range_res) {
      dest = std::move(range_res.value());
    }
  }

  return response;
}

OpResult<uint32_t> OpLen(const OpArgs& op_args, string_view key) {
  auto& db_slice = op_args.GetDbSlice();
  auto res_it = db_slice.FindReadOnly(op_args.db_cntx, key, OBJ_STREAM);
  if (!res_it)
    return res_it.status();
  const CompactObj& cobj = (*res_it)->second;
  stream* s = (stream*)cobj.RObjPtr();
  return s->length;
}

OpResult<vector<GroupInfo>> OpListGroups(const DbContext& db_cntx, string_view key,
                                         EngineShard* shard) {
  auto& db_slice = db_cntx.GetDbSlice(shard->shard_id());
  auto res_it = db_slice.FindReadOnly(db_cntx, key, OBJ_STREAM);
  if (!res_it)
    return res_it.status();

  vector<GroupInfo> result;
  const CompactObj& cobj = (*res_it)->second;
  stream* s = (stream*)cobj.RObjPtr();

  if (s->cgroups) {
    result.reserve(raxSize(s->cgroups));

    raxIterator ri;
    raxStart(&ri, s->cgroups);
    raxSeek(&ri, "^", NULL, 0);
    while (raxNext(&ri)) {
      streamCG* cg = (streamCG*)ri.data;
      GroupInfo ginfo;
      ginfo.name.assign(reinterpret_cast<char*>(ri.key), ri.key_len);
      ginfo.consumer_size = raxSize(cg->consumers);
      ginfo.pending_size = raxSize(cg->pel);
      ginfo.last_id = cg->last_id;
      ginfo.entries_read = cg->entries_read;
      ginfo.lag = streamCGLag(s, cg);
      result.push_back(std::move(ginfo));
    }
    raxStop(&ri);
  }

  return result;
}

vector<Record> GetStreamRecords(stream* s, streamID start, streamID end, bool reverse,
                                size_t count) {
  streamIterator si;
  int64_t numfields;
  streamID id;
  size_t arraylen = 0;
  vector<Record> records;

  streamIteratorStart(&si, s, &start, &end, reverse);
  while (streamIteratorGetID(&si, &id, &numfields)) {
    Record rec;
    rec.id = id;
    rec.kv_arr.reserve(numfields);

    while (numfields--) {
      unsigned char *key, *value;
      int64_t key_len, value_len;
      streamIteratorGetField(&si, &key, &value, &key_len, &value_len);
      string skey(reinterpret_cast<char*>(key), key_len);
      string sval(reinterpret_cast<char*>(value), value_len);

      rec.kv_arr.emplace_back(std::move(skey), std::move(sval));
    }
    records.push_back(std::move(rec));
    arraylen++;
    if (count && count == arraylen)
      break;
  }

  streamIteratorStop(&si);

  return records;
}

void GetGroupPEL(stream* s, streamCG* cg, long long count, GroupInfo* ginfo) {
  vector<NACKInfo> nack_info_vec;
  long long arraylen_cg_pel = 0;
  raxIterator ri_cg_pel;
  raxStart(&ri_cg_pel, cg->pel);
  raxSeek(&ri_cg_pel, "^", NULL, 0);
  while (raxNext(&ri_cg_pel) && (!count || arraylen_cg_pel < count)) {
    streamNACK* nack = static_cast<streamNACK*>(ri_cg_pel.data);
    NACKInfo nack_info;

    streamID id;
    streamDecodeID(ri_cg_pel.key, &id);
    nack_info.pel_id = id;
    nack_info.consumer_name = nack->consumer->name;
    nack_info.delivery_time = nack->delivery_time;
    nack_info.delivery_count = nack->delivery_count;

    nack_info_vec.push_back(nack_info);
    arraylen_cg_pel++;
  }
  raxStop(&ri_cg_pel);
  ginfo->stream_nack_vec = std::move(nack_info_vec);
}

void GetConsumers(stream* s, streamCG* cg, long long count, GroupInfo* ginfo) {
  vector<ConsumerInfo> consumer_info_vec;
  raxIterator ri_consumers;
  raxStart(&ri_consumers, cg->consumers);
  raxSeek(&ri_consumers, "^", NULL, 0);
  while (raxNext(&ri_consumers)) {
    ConsumerInfo consumer_info;
    streamConsumer* consumer = static_cast<streamConsumer*>(ri_consumers.data);

    LOG_IF(DFATAL, consumer->pel->numnodes == 0) << "Invalid rax state";

    consumer_info.name = consumer->name;
    consumer_info.seen_time = consumer->seen_time;
    consumer_info.active_time = consumer->active_time;
    consumer_info.pel_count = raxSize(consumer->pel);

    /* Consumer PEL */
    long long arraylen_cpel = 0;
    raxIterator ri_cpel;
    raxStart(&ri_cpel, consumer->pel);
    raxSeek(&ri_cpel, "^", NULL, 0);
    vector<NACKInfo> consumer_pel_vec;
    while (raxNext(&ri_cpel) && (!count || arraylen_cpel < count)) {
      NACKInfo nack_info;
      streamNACK* nack = static_cast<streamNACK*>(ri_cpel.data);

      streamID id;
      streamDecodeID(ri_cpel.key, &id);
      nack_info.pel_id = id;
      nack_info.delivery_time = nack->delivery_time;
      nack_info.delivery_count = nack->delivery_count;

      consumer_pel_vec.push_back(nack_info);
      arraylen_cpel++;
    }
    consumer_info.pending = consumer_pel_vec;
    consumer_info_vec.push_back(consumer_info);
    raxStop(&ri_cpel);
  }
  raxStop(&ri_consumers);
  ginfo->consumer_info_vec = std::move(consumer_info_vec);
}

OpResult<StreamInfo> OpStreams(const DbContext& db_cntx, string_view key, EngineShard* shard,
                               int full, size_t count) {
  auto& db_slice = db_cntx.GetDbSlice(shard->shard_id());
  auto res_it = db_slice.FindReadOnly(db_cntx, key, OBJ_STREAM);
  if (!res_it)
    return res_it.status();

  vector<StreamInfo> result;
  const CompactObj& cobj = (*res_it)->second;
  stream* s = (stream*)cobj.RObjPtr();

  StreamInfo sinfo;
  sinfo.length = s->length;

  sinfo.radix_tree_keys = raxSize(s->rax_tree);
  sinfo.radix_tree_nodes = s->rax_tree->numnodes;
  sinfo.last_generated_id = s->last_id;
  sinfo.max_deleted_entry_id = s->max_deleted_entry_id;
  sinfo.entries_added = s->entries_added;
  sinfo.recorded_first_entry_id = s->first_id;
  sinfo.groups = s->cgroups ? raxSize(s->cgroups) : 0;
  sinfo.entries = GetStreamRecords(s, s->first_id, s->last_id, false, count);

  if (full) {
    if (s->cgroups) {
      GroupInfoVec group_info_vec;

      raxIterator ri_cgroups;
      raxStart(&ri_cgroups, s->cgroups);
      raxSeek(&ri_cgroups, "^", NULL, 0);
      while (raxNext(&ri_cgroups)) {
        streamCG* cg = (streamCG*)ri_cgroups.data;
        GroupInfo ginfo;
        ginfo.name.assign(reinterpret_cast<char*>(ri_cgroups.key), ri_cgroups.key_len);
        ginfo.last_id = cg->last_id;
        ginfo.consumer_size = raxSize(cg->consumers);
        ginfo.pending_size = raxSize(cg->pel);
        ginfo.entries_read = cg->entries_read;
        ginfo.lag = streamCGLag(s, cg);
        GetGroupPEL(s, cg, count, &ginfo);
        GetConsumers(s, cg, count, &ginfo);

        group_info_vec.push_back(ginfo);
      }
      raxStop(&ri_cgroups);

      sinfo.cgroups = group_info_vec;
    }
  } else {
    vector<Record> first_entry_vector = GetStreamRecords(s, s->first_id, s->last_id, false, 1);
    if (first_entry_vector.size() != 0) {
      sinfo.first_entry = first_entry_vector.at(0);
    }
    vector<Record> last_entry_vector = GetStreamRecords(s, s->first_id, s->last_id, true, 1);
    if (last_entry_vector.size() != 0) {
      sinfo.last_entry = last_entry_vector.at(0);
    }
  }

  return sinfo;
}

OpResult<vector<ConsumerInfo>> OpConsumers(const DbContext& db_cntx, EngineShard* shard,
                                           string_view stream_name, string_view group_name) {
  auto& db_slice = db_cntx.GetDbSlice(shard->shard_id());
  auto res_it = db_slice.FindReadOnly(db_cntx, stream_name, OBJ_STREAM);
  if (!res_it)
    return res_it.status();

  vector<ConsumerInfo> result;
  const CompactObj& cobj = (*res_it)->second;
  stream* s = GetReadOnlyStream(cobj);
  streamCG* cg = streamLookupCG(s, WrapSds(group_name));
  if (cg == NULL) {
    return OpStatus::INVALID_VALUE;
  }
  result.reserve(raxSize(s->cgroups));

  raxIterator ri;
  raxStart(&ri, cg->consumers);
  raxSeek(&ri, "^", NULL, 0);
  mstime_t now = db_cntx.time_now_ms;
  while (raxNext(&ri)) {
    ConsumerInfo consumer_info;
    streamConsumer* consumer = (streamConsumer*)ri.data;
    mstime_t idle = now - consumer->seen_time;
    if (idle < 0)
      idle = 0;

    consumer_info.name = consumer->name;
    consumer_info.pel_count = raxSize(consumer->pel);
    consumer_info.idle = idle;
    consumer_info.active_time = consumer->active_time;
    result.push_back(std::move(consumer_info));
  }
  raxStop(&ri);
  return result;
}

constexpr uint8_t kCreateOptMkstream = 1 << 0;

struct CreateOpts {
  string_view gname;
  string_view id;
  uint8_t flags = 0;
};

OpStatus OpCreate(const OpArgs& op_args, string_view key, const CreateOpts& opts) {
  auto& db_slice = op_args.GetDbSlice();
  auto res_it = db_slice.FindMutable(op_args.db_cntx, key, OBJ_STREAM);
  int64_t entries_read = SCG_INVALID_ENTRIES_READ;
  StreamMemTracker mem_tracker;
  if (!res_it) {
    if (opts.flags & kCreateOptMkstream) {
      // MKSTREAM is enabled, so create the stream
      res_it = db_slice.AddNew(op_args.db_cntx, key, PrimeValue{}, 0);
      if (!res_it)
        return res_it.status();

      stream* s = streamNew();
      res_it->it->second.InitRobj(OBJ_STREAM, OBJ_ENCODING_STREAM, s);
    } else {
      return res_it.status();
    }
  }

  CompactObj& cobj = res_it->it->second;
  stream* s = (stream*)cobj.RObjPtr();

  streamID id;
  ParsedStreamId parsed_id;
  if (opts.id == "$") {
    id = s->last_id;
  } else {
    if (ParseID(opts.id, true, 0, &parsed_id)) {
      id = parsed_id.val;
    } else {
      return OpStatus::SYNTAX_ERR;
    }
  }

  streamCG* cg = streamCreateCG(s, opts.gname.data(), opts.gname.size(), &id, entries_read);
  mem_tracker.UpdateStreamSize(res_it->it->second);
  return cg ? OpStatus::OK : OpStatus::BUSY_GROUP;
}

struct FindGroupResult {
  stream* s = nullptr;
  streamCG* cg = nullptr;
  DbSlice::AutoUpdater post_updater;
  DbSlice::Iterator it;
};

OpResult<FindGroupResult> FindGroup(const OpArgs& op_args, string_view key, string_view gname,
                                    bool skip_group = true) {
  auto& db_slice = op_args.GetDbSlice();
  auto res_it = db_slice.FindMutable(op_args.db_cntx, key, OBJ_STREAM);
  RETURN_ON_BAD_STATUS(res_it);

  CompactObj& cobj = res_it->it->second;
  auto* s = static_cast<stream*>(cobj.RObjPtr());
  auto* cg = streamLookupCG(s, WrapSds(gname));
  if (skip_group && !cg)
    return OpStatus::SKIPPED;

  return FindGroupResult{s, cg, std::move(res_it->post_updater), res_it->it};
}

// Try to get the consumer. If not found, create a new one.
streamConsumer* FindOrAddConsumer(string_view name, streamCG* cg, uint64_t now_ms) {
  // Try to get the consumer. If not found, create a new one.
  auto cname = WrapSds(name);
  streamConsumer* consumer = streamLookupConsumer(cg, cname);
  if (consumer)
    consumer->seen_time = now_ms;
  else  // TODO: notify xgroup-createconsumer event once we support stream events.
    consumer = StreamCreateConsumer(cg, name, now_ms, SCC_DEFAULT);
  return consumer;
}

constexpr uint8_t kClaimForce = 1 << 0;
constexpr uint8_t kClaimJustID = 1 << 1;
constexpr uint8_t kClaimLastID = 1 << 2;

struct ClaimOpts {
  string_view group;
  string_view consumer;
  int64 min_idle_time;
  int64 delivery_time = -1;
  int retry = -1;
  uint8_t flags = 0;
  int32_t count = 100;      // only for XAUTOCLAIM
  streamID start = {0, 0};  // only for XAUTOCLAIM
  streamID last_id;
};

struct ClaimInfo {
  bool justid = false;
  vector<streamID> ids;
  RecordVec records;
  streamID end_id = {0, 0};      // only for XAUTOCLAIM
  vector<streamID> deleted_ids;  // only for XAUTOCLAIM
};

void AppendClaimResultItem(ClaimInfo& result, stream* s, streamID id) {
  int64_t numfields;
  if (result.justid) {
    result.ids.push_back(id);
    return;
  }
  streamIterator it;
  streamID cid;
  streamIteratorStart(&it, s, &id, &id, 0);
  while (streamIteratorGetID(&it, &cid, &numfields)) {
    Record rec;
    rec.id = cid;
    rec.kv_arr.reserve(numfields);

    /* Emit the field-value pairs. */
    while (numfields--) {
      unsigned char *key, *value;
      int64_t key_len, value_len;
      streamIteratorGetField(&it, &key, &value, &key_len, &value_len);
      string skey(reinterpret_cast<char*>(key), key_len);
      string sval(reinterpret_cast<char*>(value), value_len);

      rec.kv_arr.emplace_back(std::move(skey), std::move(sval));
    }
    result.records.push_back(std::move(rec));
  }
  streamIteratorStop(&it);
}

// XCLAIM key group consumer min-idle-time id
OpResult<ClaimInfo> OpClaim(const OpArgs& op_args, string_view key, const ClaimOpts& opts,
                            absl::Span<streamID> ids) {
  auto cgr_res = FindGroup(op_args, key, opts.group);
  RETURN_ON_BAD_STATUS(cgr_res);

  uint64_t now_ms = op_args.db_cntx.time_now_ms;
  ClaimInfo result;
  result.justid = (opts.flags & kClaimJustID);

  streamID last_id = opts.last_id;
  if (opts.flags & kClaimLastID) {
    if (streamCompareID(&last_id, &cgr_res->cg->last_id) > 0) {
      cgr_res->cg->last_id = last_id;
    }
  }

  StreamMemTracker tracker;

  streamConsumer* consumer = FindOrAddConsumer(opts.consumer, cgr_res->cg, now_ms);

  for (streamID id : ids) {
    std::array<uint8_t, sizeof(streamID)> buf;
    StreamEncodeID(buf.begin(), &id);

    streamNACK* nack = (streamNACK*)raxFind(cgr_res->cg->pel, buf.begin(), sizeof(buf));
    if (!streamEntryExists(cgr_res->s, &id)) {
      if (nack != raxNotFound) {
        /* Release the NACK */
        raxRemove(cgr_res->cg->pel, buf.begin(), sizeof(buf), nullptr);
        raxRemove(nack->consumer->pel, buf.begin(), sizeof(buf), nullptr);
        LOG_IF(DFATAL, nack->consumer->pel->numnodes == 0) << "Invalid rax state";
        streamFreeNACK(nack);
      }
      continue;
    }

    // We didn't find a nack but the FORCE option is given.
    // Create the NACK forcefully.
    if ((opts.flags & kClaimForce) && nack == raxNotFound) {
      /* Create the NACK. */
      nack = StreamCreateNACK(nullptr, now_ms);
      raxInsert(cgr_res->cg->pel, buf.begin(), sizeof(buf), nack, nullptr);
    }

    // We found the nack, continue.
    if (nack != raxNotFound) {
      // First check if the entry id exceeds the `min_idle_time`.
      if (nack->consumer && opts.min_idle_time) {
        mstime_t this_idle = now_ms - nack->delivery_time;
        if (this_idle < opts.min_idle_time) {
          continue;
        }
      }

      // If the entry belongs to the same consumer, we don't have to
      // do anything. Else remove the entry from the old consumer.
      if (nack->consumer != consumer) {
        /* Remove the entry from the old consumer.
         * Note that nack->consumer is NULL if we created the
         * NACK above because of the FORCE option. */
        if (nack->consumer) {
          raxRemove(nack->consumer->pel, buf.begin(), sizeof(buf), nullptr);
          LOG_IF(DFATAL, nack->consumer->pel->numnodes == 0) << "Invalid rax state";
        }
      }
      // Set the delivery time for the entry.
      nack->delivery_time = opts.delivery_time;
      /* Set the delivery attempts counter if given, otherwise
       * autoincrement unless JUSTID option provided */
      if (opts.retry >= 0) {
        nack->delivery_count = opts.retry;
      } else if (!(opts.flags & kClaimJustID)) {
        nack->delivery_count++;
      }
      if (nack->consumer != consumer) {
        /* Add the entry in the new consumer local PEL. */
        raxInsert(consumer->pel, buf.begin(), sizeof(buf), nack, nullptr);
        nack->consumer = consumer;
      }
      consumer->active_time = now_ms;

      /* Send the reply for this entry. */
      AppendClaimResultItem(result, cgr_res->s, id);
      // TODO: propagate this change with streamPropagateXCLAIM
    }
  }
  tracker.UpdateStreamSize(cgr_res->it->second);
  return result;
}

// XGROUP DESTROY key groupname
OpStatus OpDestroyGroup(const OpArgs& op_args, string_view key, string_view gname) {
  auto cgr_res = FindGroup(op_args, key, gname);
  RETURN_ON_BAD_STATUS(cgr_res);
  StreamMemTracker mem_tracker;

  raxRemove(cgr_res->s->cgroups, (uint8_t*)(gname.data()), gname.size(), NULL);
  streamFreeCG(cgr_res->cg);

  mem_tracker.UpdateStreamSize(cgr_res->it->second);

  // Awake readers blocked on this group
  auto blocking_controller = op_args.db_cntx.ns->GetBlockingController(op_args.shard->shard_id());
  if (blocking_controller) {
    blocking_controller->AwakeWatched(op_args.db_cntx.db_index, key);
  }

  return OpStatus::OK;
}

struct GroupConsumerPair {
  streamCG* group;
  streamConsumer* consumer;
};

struct GroupConsumerPairOpts {
  string_view group;
  string_view consumer;
};

// XGROUP CREATECONSUMER key groupname consumername
OpResult<uint32_t> OpCreateConsumer(const OpArgs& op_args, string_view key, string_view gname,
                                    string_view consumer_name) {
  auto cgroup_res = FindGroup(op_args, key, gname);
  RETURN_ON_BAD_STATUS(cgroup_res);

  StreamMemTracker mem_tracker;

  streamConsumer* consumer = StreamCreateConsumer(
      cgroup_res->cg, consumer_name, op_args.db_cntx.time_now_ms, SCC_NO_NOTIFY | SCC_NO_DIRTIFY);

  mem_tracker.UpdateStreamSize(cgroup_res->it->second);
  return consumer ? OpStatus::OK : OpStatus::KEY_EXISTS;
}

// XGROUP DELCONSUMER key groupname consumername
OpResult<uint32_t> OpDelConsumer(const OpArgs& op_args, string_view key, string_view gname,
                                 string_view consumer_name) {
  auto cgroup_res = FindGroup(op_args, key, gname);
  RETURN_ON_BAD_STATUS(cgroup_res);
  StreamMemTracker mem_tracker;

  long long pending = 0;
  streamConsumer* consumer = streamLookupConsumer(cgroup_res->cg, WrapSds(consumer_name));
  if (consumer) {
    pending = raxSize(consumer->pel);
    streamDelConsumer(cgroup_res->cg, consumer);
  }

  mem_tracker.UpdateStreamSize(cgroup_res->it->second);
  return pending;
}

OpStatus OpSetId(const OpArgs& op_args, string_view key, string_view gname, string_view id) {
  auto cgr_res = FindGroup(op_args, key, gname);
  RETURN_ON_BAD_STATUS(cgr_res);

  streamID sid;
  ParsedStreamId parsed_id;
  if (id == "$") {
    sid = cgr_res->s->last_id;
  } else {
    if (ParseID(id, true, 0, &parsed_id)) {
      sid = parsed_id.val;
    } else {
      return OpStatus::SYNTAX_ERR;
    }
  }
  cgr_res->cg->last_id = sid;

  return OpStatus::OK;
}

ErrorReply OpXSetId(const OpArgs& op_args, string_view key, const streamID& sid) {
  auto& db_slice = op_args.GetDbSlice();
  auto res_it = db_slice.FindMutable(op_args.db_cntx, key, OBJ_STREAM);
  if (!res_it)
    return res_it.status();

  StreamMemTracker mem_tracker;

  CompactObj& cobj = res_it->it->second;
  stream* stream_inst = (stream*)cobj.RObjPtr();
  long long entries_added = -1;
  streamID max_xdel_id{0, 0};
  streamID id = sid;

  if (streamCompareID(&id, &stream_inst->max_deleted_entry_id) < 0) {
    return ErrorReply{"The ID specified in XSETID is smaller than current max_deleted_entry_id",
                      "stream_smaller_deleted"};
  }

  /* If the stream has at least one item, we want to check that the user
   * is setting a last ID that is equal or greater than the current top
   * item, otherwise the fundamental ID monotonicity assumption is violated. */
  if (stream_inst->length > 0) {
    streamID maxid;
    streamLastValidID(stream_inst, &maxid);

    if (streamCompareID(&id, &maxid) < 0) {
      return OpStatus::STREAM_ID_SMALL;
    }

    /* If an entries_added was provided, it can't be lower than the length. */
    if (entries_added != -1 && stream_inst->length > uint64_t(entries_added)) {
      return ErrorReply{
          "The entries_added specified in XSETID is smaller than the target stream length",
          "stream_added_small"};
    }
  }

  stream_inst->last_id = sid;
  if (entries_added != -1)
    stream_inst->entries_added = entries_added;
  if (!streamIDEqZero(&max_xdel_id))
    stream_inst->max_deleted_entry_id = max_xdel_id;

  mem_tracker.UpdateStreamSize(cobj);

  return OpStatus::OK;
}

OpResult<uint32_t> OpDel(const OpArgs& op_args, string_view key, absl::Span<streamID> ids) {
  auto& db_slice = op_args.GetDbSlice();
  auto res_it = db_slice.FindMutable(op_args.db_cntx, key, OBJ_STREAM);
  if (!res_it)
    return res_it.status();

  CompactObj& cobj = res_it->it->second;
  stream* stream_inst = (stream*)cobj.RObjPtr();

  uint32_t deleted = 0;
  bool first_entry = false;

  StreamMemTracker tracker;

  for (size_t j = 0; j < ids.size(); j++) {
    streamID id = ids[j];
    if (!streamDeleteItem(stream_inst, &id))
      continue;

    /* We want to know if the first entry in the stream was deleted
     * so we can later set the new one. */
    if (streamCompareID(&id, &stream_inst->first_id) == 0) {
      first_entry = 1;
    }
    /* Update the stream's maximal tombstone if needed. */
    if (streamCompareID(&id, &stream_inst->max_deleted_entry_id) > 0) {
      stream_inst->max_deleted_entry_id = id;
    }
    deleted++;
  }

  /* Update the stream's first ID. */
  if (deleted) {
    if (stream_inst->length == 0) {
      stream_inst->first_id.ms = 0;
      stream_inst->first_id.seq = 0;
    } else if (first_entry) {
      streamGetEdgeID(stream_inst, 1, 1, &stream_inst->first_id);
    }
  }

  tracker.UpdateStreamSize(cobj);
  return deleted;
}

// XACK key groupname id [id ...]
OpResult<uint32_t> OpAck(const OpArgs& op_args, string_view key, string_view gname,
                         absl::Span<streamID> ids) {
  auto res = FindGroup(op_args, key, gname, false);
  RETURN_ON_BAD_STATUS(res);

  if (res->cg == nullptr || res->s == nullptr) {
    return 0;
  }

  int acknowledged = 0;
  StreamMemTracker mem_tracker;
  for (auto& id : ids) {
    unsigned char buf[sizeof(streamID)];
    streamEncodeID(buf, &id);

    // From Redis' xackCommand's implemenation
    // Lookup the ID in the group PEL: it will have a reference to the
    // NACK structure that will have a reference to the consumer, so that
    // we are able to remove the entry from both PELs.
    streamNACK* nack = (streamNACK*)raxFind(res->cg->pel, buf, sizeof(buf));
    if (nack != raxNotFound) {
      raxRemove(res->cg->pel, buf, sizeof(buf), nullptr);
      raxRemove(nack->consumer->pel, buf, sizeof(buf), nullptr);
      streamFreeNACK(nack);
      acknowledged++;
    }
  }
  mem_tracker.UpdateStreamSize(res->it->second);
  return acknowledged;
}

OpResult<ClaimInfo> OpAutoClaim(const OpArgs& op_args, string_view key, const ClaimOpts& opts) {
  auto cgr_res = FindGroup(op_args, key, opts.group, false);
  RETURN_ON_BAD_STATUS(cgr_res);

  stream* stream = cgr_res->s;
  streamCG* group = cgr_res->cg;

  if (stream == nullptr || group == nullptr) {
    return OpStatus::KEY_NOTFOUND;
  }

  StreamMemTracker mem_tracker;

  // from Redis spec on XAutoClaim:
  // https://redis.io/commands/xautoclaim/
  // The maximum number of pending entries that the command scans is the product of
  // multiplying <count>'s value by 10 (hard-coded).
  int64_t attempts = opts.count * 10;

  unsigned char start_key[sizeof(streamID)];
  streamID start_id = opts.start;
  streamEncodeID(start_key, &start_id);
  raxIterator ri;
  raxStart(&ri, group->pel);
  raxSeek(&ri, ">=", start_key, sizeof(start_key));

  ClaimInfo result;
  result.justid = (opts.flags & kClaimJustID);

  uint64_t now_ms = op_args.db_cntx.time_now_ms;
  int count = opts.count;

  streamConsumer* consumer = FindOrAddConsumer(opts.consumer, group, now_ms);

  while (attempts-- && count && raxNext(&ri)) {
    streamNACK* nack = (streamNACK*)ri.data;

    streamID id;
    streamDecodeID(ri.key, &id);

    if (!streamEntryExists(stream, &id)) {
      // TODO: to propagate this change to replica as XCLAIM command
      // - since we delete it from NACK. See streamPropagateXCLAIM call.
      raxRemove(group->pel, ri.key, ri.key_len, nullptr);
      raxRemove(nack->consumer->pel, ri.key, ri.key_len, nullptr);
      streamFreeNACK(nack);
      result.deleted_ids.push_back(id);
      raxSeek(&ri, ">=", ri.key, ri.key_len);

      count--; /* Count is a limit of the command response size. */
      continue;
    }

    if (opts.min_idle_time) {
      mstime_t this_idle = now_ms - nack->delivery_time;
      if (this_idle < opts.min_idle_time)
        continue;
    }

    if (nack->consumer != consumer) {
      /* Remove the entry from the old consumer.
       * Note that nack->consumer is NULL if we created the
       * NACK above because of the FORCE option. */
      if (nack->consumer) {
        raxRemove(nack->consumer->pel, ri.key, ri.key_len, nullptr);
      }
    }

    nack->delivery_time = now_ms;
    if (!result.justid) {
      nack->delivery_count++;
    }

    if (nack->consumer != consumer) {
      raxInsert(consumer->pel, ri.key, ri.key_len, nack, nullptr);
      nack->consumer = consumer;
    }
    consumer->active_time = now_ms;
    AppendClaimResultItem(result, stream, id);
    count--;
    // TODO: propagate xclaim to replica
  }

  raxNext(&ri);
  streamID end_id;
  if (raxEOF(&ri)) {
    end_id.ms = end_id.seq = 0;
  } else {
    streamDecodeID(ri.key, &end_id);
  }
  raxStop(&ri);
  result.end_id = end_id;

  mem_tracker.UpdateStreamSize(cgr_res->it->second);

  return result;
}

struct PendingOpts {
  string_view group_name;
  string_view consumer_name;
  ParsedStreamId start;
  ParsedStreamId end;
  int64_t min_idle_time = 0;
  int64_t count = -1;
};

struct PendingReducedResult {
  uint64_t count = 0;
  streamID start;
  streamID end;
  vector<pair<string_view, uint64_t /* size of consumer pending list*/>> consumer_list;
};

struct PendingExtendedResult {
  streamID start;
  string_view consumer_name;
  uint64_t delivery_count;
  mstime_t elapsed;
};

using PendingExtendedResultList = std::vector<PendingExtendedResult>;
using PendingResult = std::variant<PendingReducedResult, PendingExtendedResultList>;

PendingReducedResult GetPendingReducedResult(streamCG* cg) {
  PendingReducedResult result;
  result.count = raxSize(cg->pel);
  if (!result.count) {
    return result;
  }

  raxIterator ri;

  raxStart(&ri, cg->pel);
  raxSeek(&ri, "^", nullptr, 0);
  raxNext(&ri);
  streamDecodeID(ri.key, &result.start);

  raxSeek(&ri, "$", nullptr, 0);
  raxNext(&ri);
  streamDecodeID(ri.key, &result.end);

  raxStart(&ri, cg->consumers);
  raxSeek(&ri, "^", nullptr, 0);
  while (raxNext(&ri)) {
    streamConsumer* consumer = static_cast<streamConsumer*>(ri.data);
    uint64_t pel_size = raxSize(consumer->pel);
    if (!pel_size)
      continue;

    pair<string_view, uint64_t> item;
    item.first = string_view{consumer->name, sdslen(consumer->name)};
    item.second = pel_size;
    result.consumer_list.push_back(item);
  }
  raxStop(&ri);
  return result;
}

PendingExtendedResultList GetPendingExtendedResult(uint64_t now_ms, streamCG* cg,
                                                   streamConsumer* consumer,
                                                   const PendingOpts& opts) {
  PendingExtendedResultList result;
  rax* pel = consumer ? consumer->pel : cg->pel;
  streamID sstart = opts.start.val, send = opts.end.val;
  unsigned char start_key[sizeof(streamID)];
  unsigned char end_key[sizeof(streamID)];
  raxIterator ri;

  StreamEncodeID(start_key, &sstart);
  StreamEncodeID(end_key, &send);
  raxStart(&ri, pel);
  raxSeek(&ri, ">=", start_key, sizeof(start_key));

  auto count = opts.count;
  while (count && raxNext(&ri)) {
    if (memcmp(ri.key, end_key, ri.key_len) > 0) {
      break;
    }
    streamNACK* nack = static_cast<streamNACK*>(ri.data);

    if (opts.min_idle_time) {
      mstime_t this_idle = now_ms - nack->delivery_time;
      if (this_idle < opts.min_idle_time) {
        continue;
      }
    }

    count--;

    /* Entry ID. */
    streamID id;
    streamDecodeID(ri.key, &id);

    /* Milliseconds elapsed since last delivery. */
    mstime_t elapsed = now_ms - nack->delivery_time;
    if (elapsed < 0) {
      elapsed = 0;
    }

    PendingExtendedResult item = {.start = id,
                                  .consumer_name = nack->consumer->name,
                                  .delivery_count = nack->delivery_count,
                                  .elapsed = elapsed};
    result.push_back(item);
  }
  raxStop(&ri);
  return result;
}

OpResult<PendingResult> OpPending(const OpArgs& op_args, string_view key, const PendingOpts& opts) {
  auto cgroup_res = FindGroup(op_args, key, opts.group_name);
  RETURN_ON_BAD_STATUS(cgroup_res);

  streamConsumer* consumer = nullptr;
  if (!opts.consumer_name.empty()) {
    consumer = streamLookupConsumer(cgroup_res->cg, WrapSds(opts.consumer_name));
  }

  PendingResult result;

  if (opts.count == -1) {
    result = GetPendingReducedResult(cgroup_res->cg);
  } else {
    result = GetPendingExtendedResult(op_args.db_cntx.time_now_ms, cgroup_res->cg, consumer, opts);
  }
  return result;
}

void CreateGroup(facade::CmdArgParser* parser, Transaction* tx, SinkReplyBuilder* builder) {
  auto key = parser->Next();

  CreateOpts opts;
  std::tie(opts.gname, opts.id) = parser->Next<string_view, string_view>();
  if (parser->Check("MKSTREAM")) {
    opts.flags |= kCreateOptMkstream;
  }

  if (auto err = parser->Error(); err)
    return builder->SendError(err->MakeReply());

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpCreate(t->GetOpArgs(shard), key, opts);
  };

  OpStatus result = tx->ScheduleSingleHop(std::move(cb));
  switch (result) {
    case OpStatus::KEY_NOTFOUND:
      return builder->SendError(kXGroupKeyNotFound);
    default:
      builder->SendError(result);
  }
}

void DestroyGroup(facade::CmdArgParser* parser, Transaction* tx, SinkReplyBuilder* builder) {
  auto [key, gname] = parser->Next<string_view, string_view>();

  if (auto err = parser->Error(); err)
    return builder->SendError(err->MakeReply());

  if (parser->HasNext())
    return builder->SendError(UnknownSubCmd("DESTROY", "XGROUP"));

  auto cb = [&, &key = key, &gname = gname](Transaction* t, EngineShard* shard) {
    return OpDestroyGroup(t->GetOpArgs(shard), key, gname);
  };

  OpStatus result = tx->ScheduleSingleHop(std::move(cb));
  switch (result) {
    case OpStatus::OK:
      return builder->SendLong(1);
    case OpStatus::SKIPPED:
      return builder->SendLong(0);
    case OpStatus::KEY_NOTFOUND:
      return builder->SendError(kXGroupKeyNotFound);
    default:
      builder->SendError(result);
  }
}

void CreateConsumer(facade::CmdArgParser* parser, Transaction* tx, SinkReplyBuilder* builder) {
  auto [key, gname, consumer] = parser->Next<string_view, string_view, string_view>();

  if (auto err = parser->Error(); err)
    return builder->SendError(err->MakeReply());

  if (parser->HasNext())
    return builder->SendError(UnknownSubCmd("CREATECONSUMER", "XGROUP"));

  auto cb = [&, &key = key, &gname = gname, &consumer = consumer](Transaction* t,
                                                                  EngineShard* shard) {
    return OpCreateConsumer(t->GetOpArgs(shard), key, gname, consumer);
  };
  OpResult<uint32_t> result = tx->ScheduleSingleHopT(cb);

  switch (result.status()) {
    case OpStatus::OK:
      return builder->SendLong(1);
    case OpStatus::KEY_EXISTS:
      return builder->SendLong(0);
    case OpStatus::SKIPPED:
      return builder->SendError(NoGroupError(key, gname));
    case OpStatus::KEY_NOTFOUND:
      return builder->SendError(kXGroupKeyNotFound);
    default:
      builder->SendError(result.status());
  }
}

void DelConsumer(facade::CmdArgParser* parser, Transaction* tx, SinkReplyBuilder* builder) {
  auto [key, gname, consumer] = parser->Next<string_view, string_view, string_view>();

  if (auto err = parser->Error(); err)
    return builder->SendError(err->MakeReply());

  if (parser->HasNext())
    return builder->SendError(UnknownSubCmd("DELCONSUMER", "XGROUP"));

  auto cb = [&, &key = key, &gname = gname, &consumer = consumer](Transaction* t,
                                                                  EngineShard* shard) {
    return OpDelConsumer(t->GetOpArgs(shard), key, gname, consumer);
  };

  OpResult<uint32_t> result = tx->ScheduleSingleHopT(cb);

  switch (result.status()) {
    case OpStatus::OK:
      return builder->SendLong(*result);
    case OpStatus::SKIPPED:
      return builder->SendError(NoGroupError(key, gname));
    case OpStatus::KEY_NOTFOUND:
      return builder->SendError(kXGroupKeyNotFound);
    default:
      builder->SendError(result.status());
  }
}

void SetId(facade::CmdArgParser* parser, Transaction* tx, SinkReplyBuilder* builder) {
  auto [key, gname, id] = parser->Next<string_view, string_view, string_view>();

  while (parser->HasNext()) {
    if (parser->Check("ENTRIESREAD")) {
      // TODO: to support ENTRIESREAD.
      return builder->SendError(kSyntaxErr);
    } else {
      return builder->SendError(kSyntaxErr);
    }
  }

  if (auto err = parser->Error(); err)
    return builder->SendError(err->MakeReply());

  auto cb = [&, &key = key, &gname = gname, &id = id](Transaction* t, EngineShard* shard) {
    return OpSetId(t->GetOpArgs(shard), key, gname, id);
  };

  OpStatus result = tx->ScheduleSingleHop(std::move(cb));
  switch (result) {
    case OpStatus::SKIPPED:
      return builder->SendError(NoGroupError(key, gname));
    case OpStatus::KEY_NOTFOUND:
      return builder->SendError(kXGroupKeyNotFound);
    default:
      builder->SendError(result);
  }
}

void XGroupHelp(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view help_arr[] = {"XGROUP <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                            "CREATE <key> <groupname> <id|$> [option]",
                            "    Create a new consumer group. Options are:",
                            "    * MKSTREAM",
                            "      Create the empty stream if it does not exist.",
                            "    * ENTRIESREAD entries_read",
                            "      Set the group's entries_read counter (internal use).",
                            "CREATECONSUMER <key> <groupname> <consumer>",
                            "    Create a new consumer in the specified group.",
                            "DELCONSUMER <key> <groupname> <consumer>",
                            "    Remove the specified consumer.",
                            "DESTROY <key> <groupname>",
                            "    Remove the specified group.",
                            "SETID <key> <groupname> <id|$> [ENTRIESREAD entries_read]",
                            "    Set the current group ID and entries_read counter.",
                            "HELP",
                            "    Print this help."};
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  return rb->SendSimpleStrArr(help_arr);
}

OpResult<int64_t> OpTrim(const OpArgs& op_args, const AddTrimOpts& opts) {
  auto& db_slice = op_args.GetDbSlice();
  auto res_it = db_slice.FindMutable(op_args.db_cntx, opts.key, OBJ_STREAM);
  if (!res_it) {
    if (res_it.status() == OpStatus::KEY_NOTFOUND) {
      return 0;
    }
    return res_it.status();
  }

  StreamMemTracker mem_tracker;

  CompactObj& cobj = res_it->it->second;
  stream* s = (stream*)cobj.RObjPtr();

  auto res = StreamTrim(opts, s);

  mem_tracker.UpdateStreamSize(cobj);
  return res;
}

optional<pair<AddTrimOpts, unsigned>> ParseAddOrTrimArgsOrReply(CmdArgList args, bool is_xadd,
                                                                SinkReplyBuilder* builder) {
  AddTrimOpts opts;
  opts.key = ArgS(args, 0);

  unsigned id_indx = 1;
  for (; id_indx < args.size(); ++id_indx) {
    string arg = absl::AsciiStrToUpper(ArgS(args, id_indx));
    size_t remaining_args = args.size() - id_indx - 1;

    if (is_xadd && arg == "NOMKSTREAM") {
      opts.no_mkstream = true;
    } else if ((arg == "MAXLEN" || arg == "MINID") && remaining_args >= 1) {
      if (opts.trim_strategy != TrimStrategy::kNone) {
        builder->SendError("MAXLEN and MINID options at the same time are not compatible",
                           kSyntaxErr);
        return std::nullopt;
      }

      if (arg == "MAXLEN") {
        opts.trim_strategy = TrimStrategy::kMaxLen;
      } else {
        opts.trim_strategy = TrimStrategy::kMinId;
      }

      id_indx++;
      arg = ArgS(args, id_indx);
      if (remaining_args >= 2 && arg == "~") {
        opts.trim_approx = true;
        id_indx++;
        arg = ArgS(args, id_indx);
      } else if (remaining_args >= 2 && arg == "=") {
        opts.trim_approx = false;
        id_indx++;
        arg = ArgS(args, id_indx);
      }

      if (opts.trim_strategy == TrimStrategy::kMaxLen && !absl::SimpleAtoi(arg, &opts.max_len)) {
        builder->SendError(kInvalidIntErr);
        return std::nullopt;
      }
      if (opts.trim_strategy == TrimStrategy::kMinId && !ParseID(arg, false, 0, &opts.minid)) {
        builder->SendError(kSyntaxErr);
        return std::nullopt;
      }
    } else if (arg == "LIMIT" && remaining_args >= 1 && opts.trim_strategy != TrimStrategy::kNone) {
      if (!opts.trim_approx) {
        builder->SendError(kSyntaxErr);
        return std::nullopt;
      }
      ++id_indx;
      if (!absl::SimpleAtoi(ArgS(args, id_indx), &opts.limit)) {
        builder->SendError(kSyntaxErr);
        return std::nullopt;
      }
    } else if (is_xadd) {
      // There are still remaining field args.
      break;
    } else {
      builder->SendError(kSyntaxErr);
      return std::nullopt;
    }
  }

  return make_pair(opts, id_indx);
}

struct StreamReplies {
  explicit StreamReplies(SinkReplyBuilder* rb) : rb{static_cast<RedisReplyBuilder*>(rb)} {
    DCHECK(dynamic_cast<RedisReplyBuilder*>(rb));
  }

  void SendRecord(const Record& record) const {
    rb->StartArray(2);
    rb->SendBulkString(StreamIdRepr(record.id));
    rb->StartArray(record.kv_arr.size() * 2);
    for (const auto& k_v : record.kv_arr) {
      rb->SendBulkString(k_v.first);
      rb->SendBulkString(k_v.second);
    }
  }

  void SendIDs(absl::Span<const streamID> ids) const {
    rb->StartArray(ids.size());
    for (auto id : ids)
      rb->SendBulkString(StreamIdRepr(id));
  }

  void SendRecords(absl::Span<const Record> records) const {
    rb->StartArray(records.size());
    for (const auto& record : records)
      SendRecord(record);
  }

  void SendStreamRecords(string_view key, absl::Span<const Record> records) const {
    rb->SendBulkString(key);
    SendRecords(records);
  }

  void SendClaimInfo(const ClaimInfo& ci) const {
    if (ci.justid) {
      SendIDs(ci.ids);
    } else {
      SendRecords(ci.records);
    }
  }

  RedisReplyBuilder* rb;
};

std::optional<ReadOpts> ParseReadArgsOrReply(CmdArgList args, bool read_group,
                                             SinkReplyBuilder* builder) {
  size_t streams_count = 0;

  ReadOpts opts;
  opts.read_group = read_group;
  size_t id_indx = 0;

  if (opts.read_group) {
    string arg = absl::AsciiStrToUpper(ArgS(args, id_indx));

    if (arg.size() - 1 < 2) {
      builder->SendError(kSyntaxErr);
      return std::nullopt;
    }

    if (arg != "GROUP") {
      const auto m = "Missing 'GROUP' in 'XREADGROUP' command";
      builder->SendError(m, kSyntaxErr);
      return std::nullopt;
    }
    id_indx++;
    opts.group_name = ArgS(args, id_indx);
    opts.consumer_name = ArgS(args, ++id_indx);
    id_indx++;
  }

  for (; id_indx < args.size(); ++id_indx) {
    string arg = absl::AsciiStrToUpper(ArgS(args, id_indx));

    bool remaining_args = args.size() - id_indx - 1 > 0;
    if (arg == "BLOCK" && remaining_args) {
      id_indx++;
      arg = ArgS(args, id_indx);
      if (!absl::SimpleAtoi(arg, &opts.timeout)) {
        builder->SendError(kInvalidIntErr);
        return std::nullopt;
      }
    } else if (arg == "COUNT" && remaining_args) {
      id_indx++;
      arg = ArgS(args, id_indx);
      if (!absl::SimpleAtoi(arg, &opts.count)) {
        builder->SendError(kInvalidIntErr);
        return std::nullopt;
      }
    } else if (opts.read_group && arg == "NOACK") {
      opts.noack = true;
    } else if (arg == "STREAMS" && remaining_args) {
      opts.streams_arg = id_indx + 1;

      size_t pair_count = args.size() - opts.streams_arg;
      if ((pair_count % 2) != 0) {
        const char* cmd_name = read_group ? "xreadgroup" : "xread";
        const char* symbol = read_group ? ">" : "$";
        const auto msg = absl::StrCat("Unbalanced '", cmd_name,
                                      "' list of streams: for each stream key an ID or '", symbol,
                                      "' must be specified");
        builder->SendError(msg, kSyntaxErr);
        return std::nullopt;
      }
      streams_count = pair_count / 2;
      break;
    } else {
      builder->SendError(kSyntaxErr);
      return std::nullopt;
    }
  }

  // STREAMS option is required.
  if (opts.streams_arg == 0) {
    builder->SendError(kSyntaxErr);
    return std::nullopt;
  }

  // Parse the stream IDs.
  for (size_t i = opts.streams_arg + streams_count; i < args.size(); i++) {
    string_view key = ArgS(args, i - streams_count);
    string_view idstr = ArgS(args, i);

    StreamIDsItem sitem;
    ParsedStreamId id;

    if (idstr == "$") {
      // Set ID to 0 so if the ID cannot be resolved (when the stream doesn't
      // exist) it takes the first entry added.
      if (opts.read_group) {
        builder->SendError("The $ can be specified only when calling XREAD.", kSyntaxErr);
        return std::nullopt;
      }
      id.val.ms = 0;
      id.val.seq = 0;
      id.resolve_last_id = true;
      sitem.id = id;
      auto [_, is_inserted] = opts.stream_ids.emplace(key, sitem);
      if (!is_inserted) {
        builder->SendError(kSameStreamFound);
        return std::nullopt;
      }
      continue;
    }

    if (idstr == ">") {
      if (!opts.read_group) {
        builder->SendError(
            "The > ID can be specified only when calling XREADGROUP using the GROUP <group> "
            "<consumer> option.",
            kSyntaxErr);
        return std::nullopt;
      }
      id.val.ms = UINT64_MAX;
      id.val.seq = UINT64_MAX;
      sitem.id = id;
      auto [_, is_inserted] = opts.stream_ids.emplace(key, sitem);
      if (!is_inserted) {
        builder->SendError(kSameStreamFound);
        return std::nullopt;
      }
      continue;
    }

    if (!ParseID(idstr, true, 0, &id)) {
      builder->SendError(kInvalidStreamId, kSyntaxErrType);
      return std::nullopt;
    }

    // We only include messages with IDs greater than start so increment the
    // starting ID.
    streamIncrID(&id.val);
    sitem.id = id;
    auto [_, is_inserted] = opts.stream_ids.emplace(key, sitem);
    if (!is_inserted) {
      builder->SendError(kSameStreamFound);
      return std::nullopt;
    }
  }
  return opts;
}

void XRangeGeneric(std::string_view key, std::string_view start, std::string_view end,
                   CmdArgList args, bool is_rev, Transaction* tx, SinkReplyBuilder* builder) {
  RangeOpts range_opts;
  RangeId rs, re;
  if (!ParseRangeId(start, RangeBoundary::kStart, &rs) ||
      !ParseRangeId(end, RangeBoundary::kEnd, &re)) {
    return builder->SendError(kInvalidStreamId, kSyntaxErrType);
  }

  if (rs.exclude && streamIncrID(&rs.parsed_id.val) != C_OK) {
    return builder->SendError("invalid start ID for the interval", kSyntaxErrType);
  }

  if (re.exclude && streamDecrID(&re.parsed_id.val) != C_OK) {
    return builder->SendError("invalid end ID for the interval", kSyntaxErrType);
  }

  if (args.size() > 0) {
    if (args.size() != 2) {
      return builder->SendError(WrongNumArgsError("XRANGE"), kSyntaxErrType);
    }

    string opt = absl::AsciiStrToUpper(ArgS(args, 0));
    string_view val = ArgS(args, 1);

    if (opt != "COUNT" || !absl::SimpleAtoi(val, &range_opts.count)) {
      return builder->SendError(kSyntaxErr);
    }
  }

  range_opts.start = rs.parsed_id;
  range_opts.end = re.parsed_id;
  range_opts.is_rev = is_rev;

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpRange(t->GetOpArgs(shard), key, range_opts);
  };

  OpResult<RecordVec> result = tx->ScheduleSingleHopT(cb);
  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  if (result) {
    SinkReplyBuilder::ReplyAggregator agg(builder);
    StreamReplies{builder}.SendRecords(*result);
    return;
  }

  if (result.status() == OpStatus::KEY_NOTFOUND) {
    return rb->SendEmptyArray();
  }
  return builder->SendError(result.status());
}

void XReadBlock(ReadOpts* opts, Transaction* tx, SinkReplyBuilder* builder,
                ConnectionContext* cntx) {
  // If BLOCK is not set just return an empty array as there are no resolvable
  // entries.
  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  if (opts->timeout == -1 || tx->IsMulti()) {
    // Close the transaction and release locks.
    tx->Conclude();
    return rb->SendNullArray();
  }

  auto wcb = [](Transaction* t, EngineShard* shard) { return t->GetShardArgs(shard->shard_id()); };

  auto tp = (opts->timeout) ? chrono::steady_clock::now() + chrono::milliseconds(opts->timeout)
                            : Transaction::time_point::max();

  const auto key_checker = [opts](EngineShard* owner, const DbContext& context, Transaction* tx,
                                  std::string_view key) -> bool {
    auto& db_slice = context.GetDbSlice(owner->shard_id());
    auto res_it = db_slice.FindReadOnly(context, key, OBJ_STREAM);
    if (!res_it.ok())
      return false;

    StreamIDsItem& sitem = opts->stream_ids.at(key);
    if (sitem.id.val.ms != UINT64_MAX && sitem.id.val.seq != UINT64_MAX)
      return true;

    const CompactObj& cobj = (*res_it)->second;
    stream* s = GetReadOnlyStream(cobj);
    streamID last_id = s->last_id;
    if (s->length) {
      streamLastValidID(s, &last_id);
    }

    // Update group pointer and check it's validity
    if (opts->read_group) {
      sitem.group = streamLookupCG(s, WrapSds(opts->group_name));
      if (!sitem.group)
        return true;  // abort
    }

    return streamCompareID(&last_id, &sitem.group->last_id) > 0;
  };

  if (auto status = tx->WaitOnWatch(tp, std::move(wcb), key_checker, &cntx->blocked, &cntx->paused);
      status != OpStatus::OK)
    return rb->SendNullArray();

  // Resolve the entry in the woken key. Note this must not use OpRead since
  // only the shard that contains the woken key blocks for the awoken
  // transaction to proceed.
  OpResult<RecordVec> result;
  std::string key;
  auto range_cb = [&](Transaction* t, EngineShard* shard) {
    if (auto wake_key = t->GetWakeKey(shard->shard_id()); wake_key) {
      RangeOpts range_opts;
      range_opts.end = ParsedStreamId{.val = streamID{
                                          .ms = UINT64_MAX,
                                          .seq = UINT64_MAX,
                                      }};
      StreamIDsItem& sitem = opts->stream_ids.at(*wake_key);
      range_opts.start = sitem.id;

      // Expect group to exist? No guarantees from transactional framework
      if (opts->read_group && !sitem.group) {
        result = OpStatus::INVALID_VALUE;
        return OpStatus::OK;
      }

      if (sitem.id.val.ms == UINT64_MAX || sitem.id.val.seq == UINT64_MAX) {
        range_opts.start.val = sitem.group->last_id;  // only for '>'
        streamIncrID(&range_opts.start.val);
      }

      range_opts.group = sitem.group;

      // Update consumer
      if (sitem.group) {
        range_opts.consumer =
            FindOrAddConsumer(opts->consumer_name, sitem.group, GetCurrentTimeMs());
      }

      range_opts.noack = opts->noack;
      if (sitem.consumer) {
        if (sitem.consumer->pel->numnodes == 0) {
          LOG(DFATAL) << "Internal error when accessing consumer data, seen_time "
                      << sitem.consumer->seen_time;
          result = OpStatus::CANCELLED;
          return OpStatus::OK;
        }
      }
      result = OpRange(t->GetOpArgs(shard), *wake_key, range_opts);
      key = *wake_key;
    }
    return OpStatus::OK;
  };
  tx->Execute(std::move(range_cb), true);

  if (result) {
    SinkReplyBuilder::ReplyAggregator agg(rb);
    if (opts->read_group && rb->IsResp3()) {
      rb->StartCollection(1, RedisReplyBuilder::CollectionType::MAP);
    } else {
      rb->StartArray(1);
      rb->StartArray(2);
    }
    return StreamReplies{rb}.SendStreamRecords(key, *result);
  } else if (result.status() == OpStatus::INVALID_VALUE) {
    return rb->SendError("NOGROUP the consumer group this client was blocked on no longer exists");
  }
  return rb->SendNullArray();
}

void XReadGeneric2(CmdArgList args, bool read_group, Transaction* tx, SinkReplyBuilder* builder,
                   ConnectionContext* cntx) {
  optional<ReadOpts> opts = ParseReadArgsOrReply(args, read_group, builder);
  if (!opts)
    return;

  // Determine if streams have entries or any error occured
  AggregateValue<optional<facade::ErrorReply>> err;
  atomic_bool have_entries = false;

  // With a single shard we can call OpRead in a single hop, falling back to
  // avoid concluding if no entries are available.
  bool try_fastread = tx->GetUniqueShardCnt() == 1;
  vector<RecordVec> fastread_prefetched;

  auto cb = [&](auto* tx, auto* es) -> Transaction::RunnableResult {
    auto op_args = tx->GetOpArgs(es);
    for (string_view skey : tx->GetShardArgs(es->shard_id())) {
      if (auto res = HasEntries2(op_args, skey, &*opts); holds_alternative<facade::ErrorReply>(res))
        err = get<facade::ErrorReply>(res);
      else if (holds_alternative<bool>(res) && get<bool>(res))
        have_entries.store(true, memory_order_relaxed);
    }

    if (try_fastread) {
      if (have_entries.load(memory_order_relaxed))
        fastread_prefetched = OpRead(tx->GetOpArgs(es), tx->GetShardArgs(es->shard_id()), *opts);
      else
        return {OpStatus::OK, Transaction::RunnableResult::AVOID_CONCLUDING};
    }
    return OpStatus::OK;
  };
  tx->Execute(cb, try_fastread);

  if (err) {
    tx->Conclude();
    return builder->SendError(**err);
  }

  if (!have_entries.load(memory_order_relaxed))
    return XReadBlock(&*opts, tx, builder, cntx);

  vector<vector<RecordVec>> xread_resp;
  if (try_fastread && have_entries.load(memory_order_relaxed)) {
    xread_resp = {std::move(fastread_prefetched)};
  } else {
    xread_resp.resize(shard_set->size());
    auto read_cb = [&](Transaction* t, EngineShard* shard) {
      ShardId sid = shard->shard_id();
      xread_resp[sid] = OpRead(t->GetOpArgs(shard), t->GetShardArgs(sid), *opts);
      return OpStatus::OK;
    };
    tx->Execute(std::move(read_cb), true);
  }

  // Count number of streams and merge final results in correct order
  int resolved_streams = 0;
  vector<RecordVec> results(opts->stream_ids.size());
  for (size_t i = 0; i < xread_resp.size(); i++) {
    vector<RecordVec>& sub_results = xread_resp[i];
    ShardId sid = xread_resp.size() < shard_set->size() ? tx->GetUniqueShard() : i;
    if (!tx->IsActive(sid)) {
      DCHECK(sub_results.empty());
      continue;
    }

    ShardArgs shard_args = tx->GetShardArgs(sid);
    DCHECK_EQ(shard_args.Size(), sub_results.size());

    auto shard_args_it = shard_args.begin();
    for (size_t j = 0; j < sub_results.size(); j++, ++shard_args_it) {
      if (sub_results[j].empty())
        continue;

      resolved_streams++;
      results[shard_args_it.index() - opts->streams_arg] = std::move(sub_results[j]);
    }
  }

  // Send all results back
  auto* rb = static_cast<RedisReplyBuilder*>(builder);
  SinkReplyBuilder::ReplyAggregator agg(builder);
  if (opts->read_group) {
    if (rb->IsResp3()) {
      rb->StartCollection(opts->stream_ids.size(), RedisReplyBuilder::CollectionType::MAP);
      for (size_t i = 0; i < opts->stream_ids.size(); i++) {
        string_view key = ArgS(args, i + opts->streams_arg);
        StreamReplies{builder}.SendStreamRecords(key, results[i]);
      }
    } else {
      rb->StartArray(opts->stream_ids.size());
      for (size_t i = 0; i < opts->stream_ids.size(); i++) {
        string_view key = ArgS(args, i + opts->streams_arg);
        rb->StartArray(2);
        StreamReplies{builder}.SendStreamRecords(key, results[i]);
      }
    }
  } else {
    rb->StartArray(resolved_streams);
    for (size_t i = 0; i < results.size(); i++) {
      if (results[i].empty())
        continue;
      string_view key = ArgS(args, i + opts->streams_arg);
      rb->StartArray(2);
      StreamReplies{builder}.SendStreamRecords(key, results[i]);
    }
  }
}

void HelpSubCmd(facade::CmdArgParser* parser, Transaction* tx, SinkReplyBuilder* builder) {
  XGroupHelp(parser->Tail(), CommandContext{tx, builder, nullptr});
}

bool ParseXpendingOptions(CmdArgList& args, PendingOpts& opts, SinkReplyBuilder* builder) {
  size_t id_indx = 0;
  string arg = absl::AsciiStrToUpper(ArgS(args, id_indx));

  if (arg == "IDLE" && args.size() > 4) {
    id_indx++;
    if (!absl::SimpleAtoi(ArgS(args, id_indx), &opts.min_idle_time)) {
      builder->SendError(kInvalidIntErr, kSyntaxErrType);
      return false;
    }
    // Ignore negative min_idle_time
    opts.min_idle_time = std::max(opts.min_idle_time, static_cast<int64_t>(0));
    args.remove_prefix(2);
    id_indx = 0;
  }
  if (args.size() < 3) {
    builder->SendError(WrongNumArgsError("XPENDING"), kSyntaxErrType);
    return false;
  }

  // Parse start and end
  RangeId rs, re;
  string_view start = ArgS(args, id_indx);
  id_indx++;
  string_view end = ArgS(args, id_indx);
  if (!ParseRangeId(start, RangeBoundary::kStart, &rs) ||
      !ParseRangeId(end, RangeBoundary::kEnd, &re)) {
    builder->SendError(kInvalidStreamId, kSyntaxErrType);
    return false;
  }

  if (rs.exclude && streamIncrID(&rs.parsed_id.val) != C_OK) {
    builder->SendError("invalid start ID for the interval", kSyntaxErrType);
    return false;
  }

  if (re.exclude && streamDecrID(&re.parsed_id.val) != C_OK) {
    builder->SendError("invalid end ID for the interval", kSyntaxErrType);
    return false;
  }
  id_indx++;
  opts.start = rs.parsed_id;
  opts.end = re.parsed_id;

  // Parse count
  if (!absl::SimpleAtoi(ArgS(args, id_indx), &opts.count)) {
    builder->SendError(kInvalidIntErr, kSyntaxErrType);
    return false;
  }

  // Ignore negative count value
  opts.count = std::max(opts.count, static_cast<int64_t>(0));
  if (args.size() - id_indx - 1) {
    id_indx++;
    opts.consumer_name = ArgS(args, id_indx);
  }
  return true;
}

}  // namespace

void StreamFamily::XAdd(CmdArgList args, const CommandContext& cmd_cntx) {
  auto parse_resp = ParseAddOrTrimArgsOrReply(args, true, cmd_cntx.rb);
  if (!parse_resp) {
    return;
  }

  auto add_opts = parse_resp->first;
  auto id_indx = parse_resp->second;

  args.remove_prefix(id_indx);
  if (args.size() < 2 || args.size() % 2 == 0) {
    return cmd_cntx.rb->SendError(WrongNumArgsError("XADD"), kSyntaxErrType);
  }

  string_view id = ArgS(args, 0);

  if (!ParseID(id, true, 0, &add_opts.parsed_id)) {
    return cmd_cntx.rb->SendError(kInvalidStreamId, kSyntaxErrType);
  }

  args.remove_prefix(1);
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpAdd(t->GetOpArgs(shard), add_opts, args);
  };

  OpResult<streamID> add_result = cmd_cntx.tx->ScheduleSingleHopT(cb);
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);

  if (add_result) {
    return rb->SendBulkString(StreamIdRepr(*add_result));
  }

  if (add_result == OpStatus::KEY_NOTFOUND) {
    return rb->SendNull();
  }

  if (add_result.status() == OpStatus::STREAM_ID_SMALL) {
    return cmd_cntx.rb->SendError(LeqTopIdError("XADD"));
  }

  return cmd_cntx.rb->SendError(add_result.status());
}

absl::InlinedVector<streamID, 8> GetXclaimIds(CmdArgList& args) {
  size_t i;
  absl::InlinedVector<streamID, 8> ids;
  for (i = 0; i < args.size(); ++i) {
    ParsedStreamId parsed_id;
    string_view str_id = ArgS(args, i);
    if (!ParseID(str_id, true, 0, &parsed_id)) {
      if (i > 0) {
        break;
      }
      return ids;
    }
    ids.push_back(parsed_id.val);
  }
  args.remove_prefix(i);
  return ids;
}

bool ParseXclaimOptions(CmdArgList& args, ClaimOpts& opts, SinkReplyBuilder* builder) {
  for (size_t i = 0; i < args.size(); ++i) {
    string arg = absl::AsciiStrToUpper(ArgS(args, i));
    bool remaining_args = args.size() - i - 1 > 0;

    if (remaining_args) {
      if (arg == "IDLE") {
        arg = ArgS(args, ++i);
        if (!absl::SimpleAtoi(arg, &opts.delivery_time)) {
          builder->SendError(kInvalidIntErr);
          return false;
        }
        continue;
      } else if (arg == "TIME") {
        arg = ArgS(args, ++i);
        if (!absl::SimpleAtoi(arg, &opts.delivery_time)) {
          builder->SendError(kInvalidIntErr);
          return false;
        }
        continue;
      } else if (arg == "RETRYCOUNT") {
        arg = ArgS(args, ++i);
        if (!absl::SimpleAtoi(arg, &opts.retry)) {
          builder->SendError(kInvalidIntErr);
          return false;
        }
        continue;
      } else if (arg == "LASTID") {
        opts.flags |= kClaimLastID;
        arg = ArgS(args, ++i);
        ParsedStreamId parsed_id;
        if (ParseID(arg, true, 0, &parsed_id)) {
          opts.last_id = parsed_id.val;
        } else {
          builder->SendError(kInvalidStreamId, kSyntaxErrType);
          return false;
        }
        continue;
      }
    }
    if (arg == "FORCE") {
      opts.flags |= kClaimForce;
    } else if (arg == "JUSTID") {
      opts.flags |= kClaimJustID;
    } else {
      builder->SendError("Unknown argument given for XCLAIM command", kSyntaxErr);
      return false;
    }
  }
  return true;
}

void StreamFamily::XClaim(CmdArgList args, const CommandContext& cmd_cntx) {
  ClaimOpts opts;
  string_view key = ArgS(args, 0);
  opts.group = ArgS(args, 1);
  opts.consumer = ArgS(args, 2);
  if (!absl::SimpleAtoi(ArgS(args, 3), &opts.min_idle_time)) {
    return cmd_cntx.rb->SendError(kSyntaxErr);
  }
  // Ignore negative min-idle-time
  opts.min_idle_time = std::max(opts.min_idle_time, static_cast<int64>(0));
  args.remove_prefix(4);

  auto ids = GetXclaimIds(args);
  if (ids.empty()) {
    // No ids given.
    return cmd_cntx.rb->SendError(kInvalidStreamId, kSyntaxErrType);
  }

  // parse the options
  if (!ParseXclaimOptions(args, opts, cmd_cntx.rb))
    return;

  if (uint64_t now = cmd_cntx.tx->GetDbContext().time_now_ms;
      opts.delivery_time < 0 || static_cast<uint64_t>(opts.delivery_time) > now)
    opts.delivery_time = now;

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpClaim(t->GetOpArgs(shard), key, opts, absl::Span{ids.data(), ids.size()});
  };
  OpResult<ClaimInfo> result = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));
  if (!result) {
    cmd_cntx.rb->SendError(result.status());
    return;
  }

  StreamReplies{cmd_cntx.rb}.SendClaimInfo(result.value());
}

void StreamFamily::XDel(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);
  args.remove_prefix(1);

  absl::InlinedVector<streamID, 8> ids(args.size());

  for (size_t i = 0; i < args.size(); ++i) {
    ParsedStreamId parsed_id;
    string_view str_id = ArgS(args, i);
    if (!ParseID(str_id, true, 0, &parsed_id)) {
      return cmd_cntx.rb->SendError(kInvalidStreamId, kSyntaxErrType);
    }
    ids[i] = parsed_id.val;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpDel(t->GetOpArgs(shard), key, absl::Span{ids.data(), ids.size()});
  };

  OpResult<uint32_t> result = cmd_cntx.tx->ScheduleSingleHopT(cb);
  if (result || result.status() == OpStatus::KEY_NOTFOUND) {
    return cmd_cntx.rb->SendLong(*result);
  }

  cmd_cntx.rb->SendError(result.status());
}

void StreamFamily::XGroup(CmdArgList args, const CommandContext& cmd_cntx) {
  facade::CmdArgParser parser{args};

  auto sub_cmd_func = parser.MapNext("HELP", &HelpSubCmd, "CREATE", &CreateGroup, "DESTROY",
                                     &DestroyGroup, "CREATECONSUMER", &CreateConsumer,
                                     "DELCONSUMER", &DelConsumer, "SETID", &SetId);

  if (auto err = parser.Error(); err)
    return cmd_cntx.rb->SendError(err->MakeReply());

  sub_cmd_func(&parser, cmd_cntx.tx, cmd_cntx.rb);
}

void StreamFamily::XInfo(CmdArgList args, const CommandContext& cmd_cntx) {
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  string sub_cmd = absl::AsciiStrToUpper(ArgS(args, 0));

  if (sub_cmd == "HELP") {
    string_view help_arr[] = {"CONSUMERS <key> <groupname>",
                              "    Show consumers of <groupname>.",
                              "GROUPS <key>",
                              "    Show the stream consumer groups.",
                              "STREAM <key> [FULL [COUNT <count>]",
                              "    Show information about the stream.",
                              "HELP",
                              "    Prints this help."};
    return rb->SendSimpleStrArr(help_arr);
  }

  if (args.size() >= 2) {
    string_view key = ArgS(args, 1);
    ShardId sid = Shard(key, shard_set->size());

    if (sub_cmd == "GROUPS") {
      // We do not use transactional xemantics for xinfo since it's informational command.
      auto cb = [&]() {
        EngineShard* shard = EngineShard::tlocal();
        DbContext db_context{cmd_cntx.conn_cntx->ns, cmd_cntx.conn_cntx->db_index(),
                             GetCurrentTimeMs()};
        return OpListGroups(db_context, key, shard);
      };

      OpResult<vector<GroupInfo>> result = shard_set->Await(sid, std::move(cb));
      if (result) {
        rb->StartArray(result->size());
        for (const auto& ginfo : *result) {
          string last_id = StreamIdRepr(ginfo.last_id);

          rb->StartCollection(6, RedisReplyBuilder::MAP);
          rb->SendBulkString("name");
          rb->SendBulkString(ginfo.name);
          rb->SendBulkString("consumers");
          rb->SendLong(ginfo.consumer_size);
          rb->SendBulkString("pending");
          rb->SendLong(ginfo.pending_size);
          rb->SendBulkString("last-delivered-id");
          rb->SendBulkString(last_id);
          rb->SendBulkString("entries-read");
          if (ginfo.entries_read != SCG_INVALID_ENTRIES_READ) {
            rb->SendLong(ginfo.entries_read);
          } else {
            rb->SendNull();
          }
          rb->SendBulkString("lag");
          if (ginfo.lag != SCG_INVALID_LAG) {
            rb->SendLong(ginfo.lag);
          } else {
            rb->SendNull();
          }
        }
        return;
      }
      return rb->SendError(result.status());
    } else if (sub_cmd == "STREAM") {
      int full = 0;
      size_t count = 10;  // default count for xinfo streams

      if (args.size() == 4 || args.size() > 5) {
        return rb->SendError(
            "unknown subcommand or wrong number of arguments for 'STREAM'. Try XINFO HELP.");
      }

      if (args.size() >= 3) {
        full = 1;
        string full_arg = absl::AsciiStrToUpper(ArgS(args, 2));
        if (full_arg != "FULL") {
          return rb->SendError(
              "unknown subcommand or wrong number of arguments for 'STREAM'. Try XINFO HELP.");
        }
        if (args.size() > 3) {
          string count_arg = absl::AsciiStrToUpper(ArgS(args, 3));
          string_view count_value_arg = ArgS(args, 4);
          if (count_arg != "COUNT") {
            return rb->SendError(
                "unknown subcommand or wrong number of arguments for 'STREAM'. Try XINFO HELP.");
          }

          if (!absl::SimpleAtoi(count_value_arg, &count)) {
            return rb->SendError(kInvalidIntErr);
          }
        }
      }

      auto cb = [&]() {
        EngineShard* shard = EngineShard::tlocal();
        return OpStreams(
            DbContext{cmd_cntx.conn_cntx->ns, cmd_cntx.conn_cntx->db_index(), GetCurrentTimeMs()},
            key, shard, full, count);
      };

      OpResult<StreamInfo> sinfo = shard_set->Await(sid, std::move(cb));
      if (sinfo) {
        if (full) {
          rb->StartCollection(9, RedisReplyBuilder::MAP);
        } else {
          rb->StartCollection(10, RedisReplyBuilder::MAP);
        }

        rb->SendBulkString("length");
        rb->SendLong(sinfo->length);

        rb->SendBulkString("radix-tree-keys");
        rb->SendLong(sinfo->radix_tree_keys);

        rb->SendBulkString("radix-tree-nodes");
        rb->SendLong(sinfo->radix_tree_nodes);

        rb->SendBulkString("last-generated-id");
        rb->SendBulkString(StreamIdRepr(sinfo->last_generated_id));

        rb->SendBulkString("max-deleted-entry-id");
        rb->SendBulkString(StreamIdRepr(sinfo->max_deleted_entry_id));

        rb->SendBulkString("entries-added");
        rb->SendLong(sinfo->entries_added);

        rb->SendBulkString("recorded-first-entry-id");
        rb->SendBulkString(StreamIdRepr(sinfo->recorded_first_entry_id));

        if (full) {
          rb->SendBulkString("entries");
          StreamReplies{rb}.SendRecords(sinfo->entries);

          rb->SendBulkString("groups");
          rb->StartArray(sinfo->cgroups.size());
          for (const auto& ginfo : sinfo->cgroups) {
            rb->StartCollection(7, RedisReplyBuilder::MAP);

            rb->SendBulkString("name");
            rb->SendBulkString(ginfo.name);

            rb->SendBulkString("last-delivered-id");
            rb->SendBulkString(StreamIdRepr(ginfo.last_id));

            rb->SendBulkString("entries-read");
            if (ginfo.entries_read != SCG_INVALID_ENTRIES_READ) {
              rb->SendLong(ginfo.entries_read);
            } else {
              rb->SendNull();
            }
            rb->SendBulkString("lag");
            if (ginfo.lag != SCG_INVALID_LAG) {
              rb->SendLong(ginfo.lag);
            } else {
              rb->SendNull();
            }

            rb->SendBulkString("pel-count");
            rb->SendLong(ginfo.pending_size);

            rb->SendBulkString("pending");
            rb->StartArray(ginfo.stream_nack_vec.size());
            for (const auto& pending_info : ginfo.stream_nack_vec) {
              rb->StartArray(4);
              rb->SendBulkString(StreamIdRepr(pending_info.pel_id));
              rb->SendBulkString(pending_info.consumer_name);
              rb->SendLong(pending_info.delivery_time);
              rb->SendLong(pending_info.delivery_count);
            }

            rb->SendBulkString("consumers");
            rb->StartArray(ginfo.consumer_info_vec.size());
            for (const auto& consumer_info : ginfo.consumer_info_vec) {
              rb->StartCollection(5, RedisReplyBuilder::MAP);

              rb->SendBulkString("name");
              rb->SendBulkString(consumer_info.name);

              rb->SendBulkString("seen-time");
              rb->SendLong(consumer_info.seen_time);

              rb->SendBulkString("active-time");
              rb->SendLong(consumer_info.active_time);

              rb->SendBulkString("pel-count");
              rb->SendLong(consumer_info.pel_count);

              rb->SendBulkString("pending");
              if (consumer_info.pending.size() == 0) {
                rb->SendEmptyArray();
              } else {
                rb->StartArray(consumer_info.pending.size());
              }
              for (const auto& pending : consumer_info.pending) {
                rb->StartArray(3);

                rb->SendBulkString(StreamIdRepr(pending.pel_id));
                rb->SendLong(pending.delivery_time);
                rb->SendLong(pending.delivery_count);
              }
            }
          }
        } else {
          rb->SendBulkString("groups");
          rb->SendLong(sinfo->groups);

          rb->SendBulkString("first-entry");
          if (sinfo->first_entry.kv_arr.size() != 0) {
            StreamReplies{rb}.SendRecord(sinfo->first_entry);
          } else {
            rb->SendNullArray();
          }

          rb->SendBulkString("last-entry");
          if (sinfo->last_entry.kv_arr.size() != 0) {
            StreamReplies{rb}.SendRecord(sinfo->last_entry);
          } else {
            rb->SendNullArray();
          }
        }
        return;
      }
      return rb->SendError(sinfo.status());
    } else if (sub_cmd == "CONSUMERS") {
      string_view stream_name = ArgS(args, 1);
      string_view group_name = ArgS(args, 2);
      auto cb = [&]() {
        return OpConsumers(
            DbContext{cmd_cntx.conn_cntx->ns, cmd_cntx.conn_cntx->db_index(), GetCurrentTimeMs()},
            EngineShard::tlocal(), stream_name, group_name);
      };

      OpResult<vector<ConsumerInfo>> result = shard_set->Await(sid, std::move(cb));
      if (result) {
        rb->StartArray(result->size());
        int64_t now_ms = GetCurrentTimeMs();
        for (const auto& consumer_info : *result) {
          int64_t active = consumer_info.active_time;
          int64_t inactive = active != -1 ? now_ms - active : -1;

          rb->StartCollection(4, RedisReplyBuilder::MAP);
          rb->SendBulkString("name");
          rb->SendBulkString(consumer_info.name);
          rb->SendBulkString("pending");
          rb->SendLong(consumer_info.pel_count);
          rb->SendBulkString("idle");
          rb->SendLong(consumer_info.idle);
          rb->SendBulkString("inactive");
          rb->SendLong(inactive);
        }
        return;
      }
      if (result.status() == OpStatus::INVALID_VALUE) {
        return rb->SendError(NoGroupError(stream_name, group_name));
      }
      return rb->SendError(result.status());
    }
  }
  return rb->SendError(UnknownSubCmd(sub_cmd, "XINFO"));
}

void StreamFamily::XLen(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);
  auto cb = [&](Transaction* t, EngineShard* shard) { return OpLen(t->GetOpArgs(shard), key); };

  OpResult<uint32_t> result = cmd_cntx.tx->ScheduleSingleHopT(cb);
  if (result || result.status() == OpStatus::KEY_NOTFOUND) {
    return cmd_cntx.rb->SendLong(*result);
  }

  return cmd_cntx.rb->SendError(result.status());
}

void StreamFamily::XPending(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);
  PendingOpts opts;
  opts.group_name = ArgS(args, 1);
  args.remove_prefix(2);

  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);
  if (!args.empty() && !ParseXpendingOptions(args, opts, rb)) {
    return;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpPending(t->GetOpArgs(shard), key, opts);
  };
  OpResult<PendingResult> op_result = cmd_cntx.tx->ScheduleSingleHopT(cb);
  if (!op_result) {
    if (op_result.status() == OpStatus::SKIPPED)
      return rb->SendError(NoGroupError(key, opts.group_name));
    return rb->SendError(op_result.status());
  }
  const PendingResult& result = op_result.value();

  if (std::holds_alternative<PendingReducedResult>(result)) {
    const auto& res = std::get<PendingReducedResult>(result);
    rb->StartArray(4);
    rb->SendLong(res.count);
    if (res.count) {
      rb->SendBulkString(StreamIdRepr(res.start));
      rb->SendBulkString(StreamIdRepr(res.end));
      rb->StartArray(res.consumer_list.size());

      for (auto& [consumer_name, count] : res.consumer_list) {
        rb->StartArray(2);
        rb->SendBulkString(consumer_name);
        rb->SendLong(count);
      }
    } else {
      for (unsigned j = 0; j < 3; ++j)
        rb->SendNull();
    }
  } else {
    const auto& res = std::get<PendingExtendedResultList>(result);
    if (!res.size()) {
      return rb->SendEmptyArray();
    }

    rb->StartArray(res.size());
    for (auto& item : res) {
      rb->StartArray(4);
      rb->SendBulkString(StreamIdRepr(item.start));
      rb->SendBulkString(item.consumer_name);
      rb->SendLong(item.elapsed);
      rb->SendLong(item.delivery_count);
    }
  }
}

void StreamFamily::XRange(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = args[0];
  string_view start = args[1];
  string_view end = args[2];

  XRangeGeneric(key, start, end, args.subspan(3), false, cmd_cntx.tx, cmd_cntx.rb);
}

void StreamFamily::XRevRange(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = args[0];
  string_view start = args[1];
  string_view end = args[2];

  XRangeGeneric(key, end, start, args.subspan(3), true, cmd_cntx.tx, cmd_cntx.rb);
}

variant<bool, facade::ErrorReply> HasEntries2(const OpArgs& op_args, string_view skey,
                                              ReadOpts* opts) {
  auto& db_slice = op_args.GetDbSlice();
  auto res_it = db_slice.FindReadOnly(op_args.db_cntx, skey, OBJ_STREAM);
  if (!res_it) {
    if (res_it.status() == OpStatus::WRONG_TYPE)
      return facade::ErrorReply{res_it.status()};
    else if (res_it.status() == OpStatus::KEY_NOTFOUND && opts->read_group)
      return facade::ErrorReply{
          NoGroupOrKey(skey, opts->group_name, " in XREADGROUP with GROUP option")};
    return false;
  }

  const CompactObj& cobj = (*res_it)->second;
  stream* s = GetReadOnlyStream(cobj);

  // Fetch last id
  streamID last_id = s->last_id;
  if (s->length)
    streamLastValidID(s, &last_id);

  // Check requested
  auto& requested_sitem = opts->stream_ids.at(skey);

  // Look up group consumer if needed
  streamCG* group = nullptr;
  streamConsumer* consumer = nullptr;
  if (opts->read_group) {
    group = streamLookupCG(s, WrapSds(opts->group_name));
    if (!group)
      return facade::ErrorReply{
          NoGroupOrKey(skey, opts->group_name, " in XREADGROUP with GROUP option")};

    consumer = FindOrAddConsumer(opts->consumer_name, group, op_args.db_cntx.time_now_ms);

    requested_sitem.group = group;
    requested_sitem.consumer = consumer;

    // If '>' is not provided, consumer PEL is used. So don't need to block.
    if (requested_sitem.id.val.ms != UINT64_MAX || requested_sitem.id.val.seq != UINT64_MAX) {
      opts->serve_history = true;
      return true;
    }

    // we know the requested last_id only when we already have it
    if (streamCompareID(&last_id, &requested_sitem.group->last_id) > 0) {
      requested_sitem.id.val = requested_sitem.group->last_id;
      streamIncrID(&requested_sitem.id.val);
    }
  } else {
    // Resolve $ to the last ID in the stream.
    if (requested_sitem.id.resolve_last_id) {
      requested_sitem.id.val = last_id;
      streamIncrID(&requested_sitem.id.val);  // include id's strictly greater
      requested_sitem.id.resolve_last_id = false;
      return false;
    }
  }

  return streamCompareID(&last_id, &requested_sitem.id.val) >= 0;
}

void StreamFamily::XRead(CmdArgList args, const CommandContext& cmd_cntx) {
  return XReadGeneric2(args, false, cmd_cntx.tx, cmd_cntx.rb, cmd_cntx.conn_cntx);
}

void StreamFamily::XReadGroup(CmdArgList args, const CommandContext& cmd_cntx) {
  return XReadGeneric2(args, true, cmd_cntx.tx, cmd_cntx.rb, cmd_cntx.conn_cntx);
}

void StreamFamily::XSetId(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);
  string_view idstr = ArgS(args, 1);

  ParsedStreamId parsed_id;
  if (!ParseID(idstr, true, 0, &parsed_id)) {
    return cmd_cntx.rb->SendError(kInvalidStreamId, kSyntaxErrType);
  }

  facade::ErrorReply reply(OpStatus::OK);
  auto cb = [&](Transaction* t, EngineShard* shard) {
    reply = OpXSetId(t->GetOpArgs(shard), key, parsed_id.val);
    return OpStatus::OK;
  };

  cmd_cntx.tx->ScheduleSingleHop(std::move(cb));
  if (reply.status == OpStatus::STREAM_ID_SMALL) {
    return cmd_cntx.rb->SendError(LeqTopIdError("XSETID"));
  }
  return cmd_cntx.rb->SendError(reply);
}

void StreamFamily::XTrim(CmdArgList args, const CommandContext& cmd_cntx) {
  auto parse_resp = ParseAddOrTrimArgsOrReply(args, true, cmd_cntx.rb);
  if (!parse_resp) {
    return;
  }

  auto trim_opts = parse_resp->first;

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpTrim(t->GetOpArgs(shard), trim_opts);
  };

  OpResult<int64_t> trim_result = cmd_cntx.tx->ScheduleSingleHopT(cb);
  if (trim_result) {
    return cmd_cntx.rb->SendLong(*trim_result);
  }
  return cmd_cntx.rb->SendError(trim_result.status());
}

void StreamFamily::XAck(CmdArgList args, const CommandContext& cmd_cntx) {
  string_view key = ArgS(args, 0);
  string_view group = ArgS(args, 1);
  args.remove_prefix(2);
  absl::InlinedVector<streamID, 8> ids(args.size());

  for (size_t i = 0; i < args.size(); ++i) {
    ParsedStreamId parsed_id;
    string_view str_id = ArgS(args, i);
    if (!ParseID(str_id, true, 0, &parsed_id)) {
      return cmd_cntx.rb->SendError(kInvalidStreamId, kSyntaxErrType);
    }
    ids[i] = parsed_id.val;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpAck(t->GetOpArgs(shard), key, group, absl::Span{ids.data(), ids.size()});
  };

  OpResult<uint32_t> result = cmd_cntx.tx->ScheduleSingleHopT(cb);
  if (result || result.status() == OpStatus::KEY_NOTFOUND) {
    return cmd_cntx.rb->SendLong(*result);
  }

  cmd_cntx.rb->SendError(result.status());
}

void StreamFamily::XAutoClaim(CmdArgList args, const CommandContext& cmd_cntx) {
  ClaimOpts opts;
  string_view key = ArgS(args, 0);
  opts.group = ArgS(args, 1);
  opts.consumer = ArgS(args, 2);
  auto* rb = static_cast<RedisReplyBuilder*>(cmd_cntx.rb);

  if (!absl::SimpleAtoi(ArgS(args, 3), &opts.min_idle_time)) {
    return rb->SendError(kSyntaxErr);
  }

  opts.min_idle_time = std::max((int64)0, opts.min_idle_time);

  string_view start = ArgS(args, 4);
  RangeId rs;

  if (!ParseRangeId(start, RangeBoundary::kStart, &rs)) {
    return rb->SendError(kSyntaxErr);
  }

  if (rs.exclude && streamDecrID(&rs.parsed_id.val) != C_OK) {
    return rb->SendError("invalid start ID for the interval", kSyntaxErrType);
  }
  opts.start = rs.parsed_id.val;

  for (size_t i = 5; i < args.size(); ++i) {
    string arg = absl::AsciiStrToUpper(ArgS(args, i));

    bool remaining_args = args.size() - i - 1 > 0;

    if (remaining_args) {
      if (arg == "COUNT") {
        arg = ArgS(args, ++i);
        if (!absl::SimpleAtoi(arg, &opts.count)) {
          return rb->SendError(kInvalidIntErr);
        }
        if (opts.count <= 0 || opts.count >= (1L << 18)) {
          return rb->SendError("COUNT must be > 0 and less than 2^18");
        }
        continue;
      }
    }
    if (arg == "JUSTID") {
      opts.flags |= kClaimJustID;
    } else {
      return cmd_cntx.rb->SendError("Unknown argument given for XAUTOCLAIM command", kSyntaxErr);
    }
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpAutoClaim(t->GetOpArgs(shard), key, opts);
  };
  OpResult<ClaimInfo> result = cmd_cntx.tx->ScheduleSingleHopT(std::move(cb));

  if (result.status() == OpStatus::KEY_NOTFOUND) {
    rb->SendError(NoGroupOrKey(key, opts.group));
    return;
  }

  if (!result) {
    rb->SendError(result.status());
    return;
  }

  ClaimInfo cresult = result.value();

  rb->StartArray(3);
  rb->SendBulkString(StreamIdRepr(cresult.end_id));
  StreamReplies{rb}.SendClaimInfo(cresult);
  StreamReplies{rb}.SendIDs(cresult.deleted_ids);
}

#define HFUNC(x) SetHandler(&StreamFamily::x)

namespace acl {
constexpr uint32_t kXAdd = WRITE | STREAM | FAST;
constexpr uint32_t kXClaim = WRITE | FAST;
constexpr uint32_t kXDel = WRITE | STREAM | FAST;
constexpr uint32_t kXGroup = SLOW;
constexpr uint32_t kXInfo = SLOW;
constexpr uint32_t kXLen = READ | STREAM | FAST;
constexpr uint32_t kXPending = READ | STREAM;
constexpr uint32_t kXRange = READ | STREAM | SLOW;
constexpr uint32_t kXRevRange = READ | STREAM | SLOW;
constexpr uint32_t kXRead = READ | STREAM | SLOW | BLOCKING;
constexpr uint32_t kXReadGroup = WRITE | STREAM | SLOW | BLOCKING;
constexpr uint32_t kXSetId = WRITE | STREAM | SLOW;
constexpr uint32_t kXTrim = WRITE | STREAM | SLOW;
constexpr uint32_t kXGroupHelp = READ | STREAM | SLOW;
constexpr uint32_t kXAck = WRITE | STREAM | FAST;
constexpr uint32_t kXAutoClaim = WRITE | STREAM | FAST;
}  // namespace acl

void StreamFamily::Register(CommandRegistry* registry) {
  using CI = CommandId;
  registry->StartFamily();
  constexpr auto kReadFlags = CO::READONLY | CO::BLOCKING | CO::VARIADIC_KEYS;
  *registry << CI{"XADD", CO::WRITE | CO::DENYOOM | CO::FAST, -5, 1, 1, acl::kXAdd}.HFUNC(XAdd)
            << CI{"XCLAIM", CO::WRITE | CO::FAST, -6, 1, 1, acl::kXClaim}.HFUNC(XClaim)
            << CI{"XDEL", CO::WRITE | CO::FAST, -3, 1, 1, acl::kXDel}.HFUNC(XDel)
            << CI{"XGROUP", CO::WRITE | CO::DENYOOM, -3, 2, 2, acl::kXGroup}.HFUNC(XGroup)
            << CI{"XINFO", CO::READONLY | CO::NOSCRIPT, -2, 0, 0, acl::kXInfo}.HFUNC(XInfo)
            << CI{"XLEN", CO::READONLY | CO::FAST, 2, 1, 1, acl::kXLen}.HFUNC(XLen)
            << CI{"XPENDING", CO::READONLY, -2, 1, 1, acl::kXPending}.HFUNC(XPending)
            << CI{"XRANGE", CO::READONLY, -4, 1, 1, acl::kXRange}.HFUNC(XRange)
            << CI{"XREVRANGE", CO::READONLY, -4, 1, 1, acl::kXRevRange}.HFUNC(XRevRange)
            << CI{"XREAD", kReadFlags, -3, 3, 3, acl::kXRead}.HFUNC(XRead)
            << CI{"XREADGROUP", kReadFlags, -6, 6, 6, acl::kXReadGroup}.HFUNC(XReadGroup)
            << CI{"XSETID", CO::WRITE, 3, 1, 1, acl::kXSetId}.HFUNC(XSetId)
            << CI{"XTRIM", CO::WRITE | CO::FAST, -4, 1, 1, acl::kXTrim}.HFUNC(XTrim)
            << CI{"_XGROUP_HELP", CO::NOSCRIPT | CO::HIDDEN, 2, 0, 0, acl::kXGroupHelp}.SetHandler(
                   XGroupHelp)
            << CI{"XACK", CO::WRITE | CO::FAST, -4, 1, 1, acl::kXAck}.HFUNC(XAck)
            << CI{"XAUTOCLAIM", CO::WRITE | CO::FAST, -6, 1, 1, acl::kXAutoClaim}.HFUNC(XAutoClaim);
}

}  // namespace dfly
