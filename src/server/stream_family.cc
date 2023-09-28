// Copyright 2022, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/stream_family.h"

#include <absl/strings/str_cat.h>

extern "C" {
#include "redis/object.h"
#include "redis/stream.h"
#include "redis/zmalloc.h"
}

#include "base/logging.h"
#include "facade/error.h"
#include "server/acl/acl_commands_def.h"
#include "server/blocking_controller.h"
#include "server/command_registry.h"
#include "server/conn_context.h"
#include "server/engine_shard_set.h"
#include "server/server_state.h"
#include "server/transaction.h"

namespace dfly {

using namespace facade;
using namespace std;

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
  bool last_id = false;
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

struct GroupInfo {
  string name;
  size_t consumer_size;
  size_t pending_size;
  streamID last_id;
  int64_t entries_read;
  int64_t lag;
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
const uint32_t STREAM_ITEM_FLAG_NONE = 0;              /* No special flags. */
const uint32_t STREAM_ITEM_FLAG_DELETED = (1 << 0);    /* Entry is deleted. Skip it. */
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

bool ParseRangeId(string_view id, RangeId* dest) {
  if (id.empty())
    return false;
  if (id[0] == '(') {
    dest->exclude = true;
    id.remove_prefix(1);
  }

  return ParseID(id, dest->exclude, 0, &dest->parsed_id);
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
void StreamNextID(const streamID* last_id, streamID* new_id) {
  uint64_t ms = mstime();
  if (ms > last_id->ms) {
    new_id->ms = ms;
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
 * The function returns C_OK if the item was added, this is always true
 * if the ID was generated by the function. However the function may return
 * C_ERR in several cases:
 * 1. If an ID was given via 'use_id', but adding it failed since the
 *    current top ID is greater or equal. errno will be set to EDOM.
 * 2. If a size of a single element or the sum of the elements is too big to
 *    be stored into the stream. errno will be set to ERANGE. */
int StreamAppendItem(stream* s, CmdArgList fields, streamID* added_id, streamID* use_id,
                     int seq_given) {
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
          return C_ERR;
        }
        id = s->last_id;
        id.seq++;
      } else {
        id = *use_id;
      }
    }
  } else {
    StreamNextID(&s->last_id, &id);
  }

  /* Check that the new ID is greater than the last entry ID
   * or return an error. Automatically generated IDs might
   * overflow (and wrap-around) when incrementing the sequence
     part. */
  if (streamCompareID(&id, &s->last_id) <= 0) {
    errno = EDOM;
    return C_ERR;
  }

  /* Avoid overflow when trying to add an element to the stream (listpack
   * can only host up to 32bit length sttrings, and also a total listpack size
   * can't be bigger than 32bit length. */
  size_t totelelen = 0;
  for (size_t i = 0; i < fields.size(); i++) {
    totelelen += fields[i].size();
  }

  if (totelelen > STREAM_LISTPACK_MAX_SIZE) {
    errno = ERANGE;
    return C_ERR;
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
    size_t node_max_bytes = kStreamNodeMaxBytes;
    if (node_max_bytes == 0 || node_max_bytes > STREAM_LISTPACK_MAX_SIZE)
      node_max_bytes = STREAM_LISTPACK_MAX_SIZE;
    if (lp_bytes + totelelen >= node_max_bytes) {
      lp = NULL;
    } else if (kStreamNodeMaxEntries) {
      unsigned char* lp_ele = lpFirst(lp);
      /* Count both live entries and deleted ones. */
      int64_t count = lpGetInteger(lp_ele) + lpGetInteger(lpNext(lp, lp_ele));
      if (count >= kStreamNodeMaxEntries) {
        /* Shrink extra pre-allocated memory */
        lp = lpShrinkToFit(lp);
        if (ri.data != lp)
          raxInsert(s->rax_tree, ri.key, ri.key_len, lp, NULL);
        lp = NULL;
      }
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
  return C_OK;
}

/* Create a NACK entry setting the delivery count to 1 and the delivery
 * time to the current time or test-hooked time. The NACK consumer will be
 * set to the one specified as argument of the function. */
streamNACK* StreamCreateNACK(streamConsumer* consumer) {
  streamNACK* nack = reinterpret_cast<streamNACK*>(zmalloc(sizeof(*nack)));
  nack->delivery_time = GetCurrentTimeMs();
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
  auto& db_slice = op_args.shard->db_slice();
  pair<PrimeIterator, bool> add_res;

  if (opts.no_mkstream) {
    auto res_it = db_slice.Find(op_args.db_cntx, opts.key, OBJ_STREAM);
    if (!res_it) {
      return res_it.status();
    }
    add_res.first = res_it.value();
    add_res.second = false;
  } else {
    try {
      add_res = db_slice.AddOrFind(op_args.db_cntx, opts.key);
    } catch (bad_alloc&) {
      return OpStatus::OUT_OF_MEMORY;
    }
  }

  robj* stream_obj = nullptr;
  PrimeIterator& it = add_res.first;

  if (add_res.second) {  // new key
    stream_obj = createStreamObject();

    it->second.ImportRObj(stream_obj);
  } else {
    if (it->second.ObjType() != OBJ_STREAM)
      return OpStatus::WRONG_TYPE;

    db_slice.PreUpdate(op_args.db_cntx.db_index, it);
  }

  stream* stream_inst = (stream*)it->second.RObjPtr();

  streamID result_id;
  const auto& parsed_id = opts.parsed_id;
  streamID passed_id = parsed_id.val;
  int res = StreamAppendItem(stream_inst, args, &result_id,
                             parsed_id.id_given ? &passed_id : nullptr, parsed_id.has_seq);

  if (res != C_OK) {
    if (errno == ERANGE)
      return OpStatus::OUT_OF_RANGE;
    if (errno == EDOM)
      return OpStatus::STREAM_ID_SMALL;

    return OpStatus::OUT_OF_MEMORY;
  }

  StreamTrim(opts, stream_inst);

  EngineShard* es = op_args.shard;
  if (es->blocking_controller()) {
    es->blocking_controller()->AwakeWatched(op_args.db_cntx.db_index, opts.key);
  }

  return result_id;
}

OpResult<RecordVec> OpRange(const OpArgs& op_args, string_view key, const RangeOpts& opts) {
  auto& db_slice = op_args.shard->db_slice();
  OpResult<PrimeIterator> res_it = db_slice.Find(op_args.db_cntx, key, OBJ_STREAM);
  if (!res_it)
    return res_it.status();

  RecordVec result;

  if (opts.count == 0)
    return result;

  streamIterator si;
  int64_t numfields;
  streamID id;
  CompactObj& cobj = (*res_it)->second;
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

      /* Try to add a new NACK. Most of the time this will work and
       * will not require extra lookups. We'll fix the problem later
       * if we find that there is already an entry for this ID. */
      streamNACK* nack = StreamCreateNACK(opts.consumer);
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
        /* Update the consumer and NACK metadata. */
        nack->consumer = opts.consumer;
        nack->delivery_time = GetCurrentTimeMs();
        nack->delivery_count = 1;
        /* Add the entry in the new consumer local PEL. */
        raxInsert(opts.consumer->pel, buf, sizeof(buf), nack, NULL);
      } else if (group_inserted == 1 && consumer_inserted == 0) {
        return OpStatus::SKIPPED;  // ("NACK half-created. Should not be possible.");
      }
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
      nack->delivery_time = GetCurrentTimeMs();
      nack->delivery_count++;
      result.push_back(std::move(op_result.value()[0]));
    }
    ecount++;
  }
  raxStop(&ri);
  return result;
}

// Returns a map of stream to the ID of the last entry in the stream. Any
// streams not found are omitted from the result.
OpResult<vector<pair<string_view, streamID>>> OpLastIDs(const OpArgs& op_args,
                                                        const ArgSlice& args) {
  DCHECK(!args.empty());

  auto& db_slice = op_args.shard->db_slice();

  vector<pair<string_view, streamID>> last_ids;
  for (string_view key : args) {
    OpResult<PrimeIterator> res_it = db_slice.Find(op_args.db_cntx, key, OBJ_STREAM);
    if (!res_it) {
      if (res_it.status() == OpStatus::KEY_NOTFOUND) {
        continue;
      }
      return res_it.status();
    }

    CompactObj& cobj = (*res_it)->second;
    stream* s = (stream*)cobj.RObjPtr();

    streamID last_id = s->last_id;
    if (s->length) {
      streamLastValidID(s, &last_id);
    }
    last_ids.emplace_back(key, last_id);
  }

  return last_ids;
}

// Returns the range response for each stream on this shard in order of
// GetShardArgs.
vector<RecordVec> OpRead(const OpArgs& op_args, const ArgSlice& args, const ReadOpts& opts) {
  DCHECK(!args.empty());

  RangeOpts range_opts;
  range_opts.count = opts.count;
  range_opts.end = ParsedStreamId{.val = streamID{
                                      .ms = UINT64_MAX,
                                      .seq = UINT64_MAX,
                                  }};

  vector<RecordVec> response(args.size());
  for (size_t i = 0; i < args.size(); ++i) {
    string_view key = args[i];

    auto sitem = opts.stream_ids.at(key);
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
      response[i] = std::move(range_res.value());
    }
  }

  return response;
}

OpResult<uint32_t> OpLen(const OpArgs& op_args, string_view key) {
  auto& db_slice = op_args.shard->db_slice();
  OpResult<PrimeIterator> res_it = db_slice.Find(op_args.db_cntx, key, OBJ_STREAM);
  if (!res_it)
    return res_it.status();
  CompactObj& cobj = (*res_it)->second;
  stream* s = (stream*)cobj.RObjPtr();
  return s->length;
}

OpResult<vector<GroupInfo>> OpListGroups(const DbContext& db_cntx, string_view key,
                                         EngineShard* shard) {
  auto& db_slice = shard->db_slice();
  OpResult<PrimeIterator> res_it = db_slice.Find(db_cntx, key, OBJ_STREAM);
  if (!res_it)
    return res_it.status();

  vector<GroupInfo> result;
  CompactObj& cobj = (*res_it)->second;
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

constexpr uint8_t kCreateOptMkstream = 1 << 0;

struct CreateOpts {
  string_view gname;
  string_view id;
  uint8_t flags = 0;
};

OpStatus OpCreate(const OpArgs& op_args, string_view key, const CreateOpts& opts) {
  auto* shard = op_args.shard;
  auto& db_slice = shard->db_slice();
  OpResult<PrimeIterator> res_it = db_slice.Find(op_args.db_cntx, key, OBJ_STREAM);
  int64_t entries_read = SCG_INVALID_ENTRIES_READ;
  if (!res_it) {
    if (opts.flags & kCreateOptMkstream) {
      // MKSTREAM is enabled, so create the stream
      res_it = db_slice.AddNew(op_args.db_cntx, key, PrimeValue{}, 0);
      if (!res_it)
        return res_it.status();

      robj* stream_obj = createStreamObject();
      (*res_it)->second.ImportRObj(stream_obj);
    } else {
      return res_it.status();
    }
  }

  CompactObj& cobj = (*res_it)->second;
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
  if (cg) {
    return OpStatus::OK;
  }
  return OpStatus::BUSY_GROUP;
}

OpResult<pair<stream*, streamCG*>> FindGroup(const OpArgs& op_args, string_view key,
                                             string_view gname) {
  auto* shard = op_args.shard;
  auto& db_slice = shard->db_slice();
  OpResult<PrimeIterator> res_it = db_slice.Find(op_args.db_cntx, key, OBJ_STREAM);
  if (!res_it)
    return res_it.status();

  CompactObj& cobj = (*res_it)->second;
  pair<stream*, streamCG*> res;
  res.first = (stream*)cobj.RObjPtr();
  shard->tmp_str1 = sdscpylen(shard->tmp_str1, gname.data(), gname.size());
  res.second = streamLookupCG(res.first, shard->tmp_str1);

  return res;
}

constexpr uint8_t kClaimForce = 1 << 0;
constexpr uint8_t kClaimJustID = 1 << 1;

struct ClaimOpts {
  string_view group;
  string_view consumer;
  int64 min_idle_time;
  int64 delivery_time = -1;
  int retry = -1;
  uint8_t flags = 0;
};

struct ClaimInfo {
  bool justid = false;
  vector<streamID> ids;
  RecordVec records;
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
  OpResult<pair<stream*, streamCG*>> cgr_res = FindGroup(op_args, key, opts.group);
  if (!cgr_res)
    return cgr_res.status();
  stream* s = cgr_res->first;
  streamCG* scg = cgr_res->second;
  if (!scg) {
    return OpStatus::SKIPPED;
  }
  streamConsumer* consumer = nullptr;
  auto now = GetCurrentTimeMs();
  ClaimInfo result;
  result.justid = (opts.flags & kClaimJustID);

  for (streamID id : ids) {
    std::array<uint8_t, sizeof(streamID)> buf;
    StreamEncodeID(buf.begin(), &id);

    streamNACK* nack = (streamNACK*)raxFind(scg->pel, buf.begin(), sizeof(buf));
    if (!streamEntryExists(s, &id)) {
      if (nack != raxNotFound) {
        /* Release the NACK */
        raxRemove(scg->pel, buf.begin(), sizeof(buf), nullptr);
        raxRemove(nack->consumer->pel, buf.begin(), sizeof(buf), nullptr);
        streamFreeNACK(nack);
      }
      continue;
    }

    // We didn't find a nack but the FORCE option is given.
    // Create the NACK forcefully.
    if ((opts.flags & kClaimForce) && nack == raxNotFound) {
      /* Create the NACK. */
      nack = streamCreateNACK(nullptr);
      raxInsert(scg->pel, buf.begin(), sizeof(buf), nack, nullptr);
    }

    // We found the nack, continue.
    if (nack != raxNotFound) {
      // First check if the entry id exceeds the `min_idle_time`.
      if (nack->consumer && opts.min_idle_time) {
        mstime_t this_idle = now - nack->delivery_time;
        if (this_idle < opts.min_idle_time) {
          continue;
        }
      }

      // Try to get the consumer. If not found, create a new one.
      op_args.shard->tmp_str1 =
          sdscpylen(op_args.shard->tmp_str1, opts.consumer.data(), opts.consumer.size());
      if ((consumer = streamLookupConsumer(scg, op_args.shard->tmp_str1, SLC_NO_REFRESH)) ==
          nullptr) {
        consumer = streamCreateConsumer(scg, op_args.shard->tmp_str1, nullptr, 0,
                                        SCC_NO_NOTIFY | SCC_NO_DIRTIFY);
      }

      // If the entry belongs to the same consumer, we don't have to
      // do anything. Else remove the entry from the old consumer.
      if (nack->consumer != consumer) {
        /* Remove the entry from the old consumer.
         * Note that nack->consumer is NULL if we created the
         * NACK above because of the FORCE option. */
        if (nack->consumer) {
          raxRemove(nack->consumer->pel, buf.begin(), sizeof(buf), nullptr);
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

      /* Send the reply for this entry. */
      AppendClaimResultItem(result, s, id);
    }
  }
  return result;
}

// XGROUP DESTROY key groupname
OpStatus OpDestroyGroup(const OpArgs& op_args, string_view key, string_view gname) {
  OpResult<pair<stream*, streamCG*>> cgr_res = FindGroup(op_args, key, gname);
  if (!cgr_res)
    return cgr_res.status();

  stream* s = cgr_res->first;
  streamCG* scg = cgr_res->second;

  if (scg) {
    raxRemove(s->cgroups, (uint8_t*)(gname.data()), gname.size(), NULL);
    streamFreeCG(scg);
    return OpStatus::OK;
  }

  return OpStatus::SKIPPED;
}

struct GroupConsumerPair {
  streamCG* group;
  streamConsumer* consumer;
};

struct GroupConsumerPairOpts {
  string_view group;
  string_view consumer;
};

vector<GroupConsumerPair> OpGetGroupConsumerPairs(ArgSlice slice_args, const OpArgs& op_args,
                                                  const GroupConsumerPairOpts& opts) {
  vector<GroupConsumerPair> sid_items(slice_args.size());

  // get group and consumer
  for (size_t i = 0; i < slice_args.size(); i++) {
    string_view key = slice_args[i];
    streamCG* group = nullptr;
    streamConsumer* consumer = nullptr;
    auto group_res = FindGroup(op_args, key, opts.group);
    if (!group_res) {
      continue;
    }
    if (group = group_res->second; !group) {
      continue;
    }

    op_args.shard->tmp_str1 =
        sdscpylen(op_args.shard->tmp_str1, opts.consumer.data(), opts.consumer.size());
    consumer = streamLookupConsumer(group, op_args.shard->tmp_str1, SLC_NO_REFRESH);
    if (!consumer) {
      consumer = streamCreateConsumer(group, op_args.shard->tmp_str1, NULL, 0,
                                      SCC_NO_NOTIFY | SCC_NO_DIRTIFY);
    }
    sid_items[i] = {group, consumer};
  }
  return sid_items;
}

// XGROUP CREATECONSUMER key groupname consumername
OpResult<uint32_t> OpCreateConsumer(const OpArgs& op_args, string_view key, string_view gname,
                                    string_view consumer_name) {
  OpResult<pair<stream*, streamCG*>> cgroup_res = FindGroup(op_args, key, gname);
  if (!cgroup_res)
    return cgroup_res.status();
  streamCG* cg = cgroup_res->second;
  if (cg == nullptr)
    return OpStatus::SKIPPED;

  auto* shard = op_args.shard;
  shard->tmp_str1 = sdscpylen(shard->tmp_str1, consumer_name.data(), consumer_name.size());
  streamConsumer* consumer =
      streamCreateConsumer(cg, shard->tmp_str1, NULL, 0, SCC_NO_NOTIFY | SCC_NO_DIRTIFY);

  if (consumer)
    return OpStatus::OK;
  return OpStatus::KEY_EXISTS;
}

// XGROUP DELCONSUMER key groupname consumername
OpResult<uint32_t> OpDelConsumer(const OpArgs& op_args, string_view key, string_view gname,
                                 string_view consumer_name) {
  OpResult<pair<stream*, streamCG*>> cgroup_res = FindGroup(op_args, key, gname);
  if (!cgroup_res)
    return cgroup_res.status();

  streamCG* cg = cgroup_res->second;
  if (cg == nullptr)
    return OpStatus::SKIPPED;

  long long pending = 0;
  auto* shard = op_args.shard;

  shard->tmp_str1 = sdscpylen(shard->tmp_str1, consumer_name.data(), consumer_name.size());
  streamConsumer* consumer = streamLookupConsumer(cg, shard->tmp_str1, SLC_NO_REFRESH);
  if (consumer) {
    pending = raxSize(consumer->pel);
    streamDelConsumer(cg, consumer);
  }

  return pending;
}

OpStatus OpSetId(const OpArgs& op_args, string_view key, string_view gname, string_view id) {
  OpResult<pair<stream*, streamCG*>> cgr_res = FindGroup(op_args, key, gname);
  if (!cgr_res)
    return cgr_res.status();

  streamCG* cg = cgr_res->second;
  if (cg == nullptr)
    return OpStatus::SKIPPED;

  streamID sid;
  ParsedStreamId parsed_id;
  if (id == "$") {
    sid = cgr_res->first->last_id;
  } else {
    if (ParseID(id, true, 0, &parsed_id)) {
      sid = parsed_id.val;
    } else {
      return OpStatus::SYNTAX_ERR;
    }
  }
  cg->last_id = sid;

  return OpStatus::OK;
}

OpStatus OpSetId2(const OpArgs& op_args, string_view key, const streamID& sid) {
  auto* shard = op_args.shard;
  auto& db_slice = shard->db_slice();
  OpResult<PrimeIterator> res_it = db_slice.Find(op_args.db_cntx, key, OBJ_STREAM);
  if (!res_it)
    return res_it.status();

  CompactObj& cobj = (*res_it)->second;
  stream* stream_inst = (stream*)cobj.RObjPtr();
  long long entries_added = -1;
  streamID max_xdel_id{0, 0};

  /* If the stream has at least one item, we want to check that the user
   * is setting a last ID that is equal or greater than the current top
   * item, otherwise the fundamental ID monotonicity assumption is violated. */
  if (stream_inst->length > 0) {
    streamID maxid;
    streamID id = sid;
    streamLastValidID(stream_inst, &maxid);

    if (streamCompareID(&id, &maxid) < 0) {
      return OpStatus::STREAM_ID_SMALL;
    }

    /* If an entries_added was provided, it can't be lower than the length. */
    if (entries_added != -1 && stream_inst->length > uint64_t(entries_added)) {
      return OpStatus::ENTRIES_ADDED_SMALL;
    }
  }

  stream_inst->last_id = sid;
  if (entries_added != -1)
    stream_inst->entries_added = entries_added;
  if (!streamIDEqZero(&max_xdel_id))
    stream_inst->max_deleted_entry_id = max_xdel_id;

  return OpStatus::OK;
}

OpResult<uint32_t> OpDel(const OpArgs& op_args, string_view key, absl::Span<streamID> ids) {
  auto* shard = op_args.shard;
  auto& db_slice = shard->db_slice();
  OpResult<PrimeIterator> res_it = db_slice.Find(op_args.db_cntx, key, OBJ_STREAM);
  if (!res_it)
    return res_it.status();

  CompactObj& cobj = (*res_it)->second;
  stream* stream_inst = (stream*)cobj.RObjPtr();

  uint32_t deleted = 0;
  bool first_entry = false;

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

  return deleted;
}

// XACK key groupname id [id ...]
OpResult<uint32_t> OpAck(const OpArgs& op_args, string_view key, string_view gname,
                         absl::Span<streamID> ids) {
  OpResult<pair<stream*, streamCG*>> res = FindGroup(op_args, key, gname);
  if (!res)
    return res.status();
  auto [stream_inst, cg] = *res;

  if (cg == nullptr || stream_inst == nullptr) {
    return 0;
  }

  int acknowledged = 0;
  for (auto& id : ids) {
    unsigned char buf[sizeof(streamID)];
    streamEncodeID(buf, &id);

    // From Redis' xackCommand's implemenation
    // Lookup the ID in the group PEL: it will have a reference to the
    // NACK structure that will have a reference to the consumer, so that
    // we are able to remove the entry from both PELs.
    streamNACK* nack = (streamNACK*)raxFind(cg->pel, buf, sizeof(buf));
    if (nack != raxNotFound) {
      raxRemove(cg->pel, buf, sizeof(buf), nullptr);
      raxRemove(nack->consumer->pel, buf, sizeof(buf), nullptr);
      streamFreeNACK(nack);
      acknowledged++;
    }
  }
  return acknowledged;
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

PendingExtendedResultList GetPendingExtendedResult(streamCG* cg, streamConsumer* consumer,
                                                   const PendingOpts& opts) {
  PendingExtendedResultList result;
  rax* pel = consumer ? consumer->pel : cg->pel;
  streamID sstart = opts.start.val, send = opts.end.val;
  auto now = GetCurrentTimeMs();
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
      mstime_t this_idle = now - nack->delivery_time;
      if (this_idle < opts.min_idle_time) {
        continue;
      }
    }

    count--;

    /* Entry ID. */
    streamID id;
    streamDecodeID(ri.key, &id);

    /* Milliseconds elapsed since last delivery. */
    mstime_t elapsed = now - nack->delivery_time;
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
  OpResult<pair<stream*, streamCG*>> cgroup_res = FindGroup(op_args, key, opts.group_name);
  if (!cgroup_res) {
    return cgroup_res.status();
  }

  streamCG* cg = cgroup_res->second;
  if (cg == nullptr) {
    return OpStatus::SKIPPED;
  }

  auto* shard = op_args.shard;
  streamConsumer* consumer = nullptr;
  if (!opts.consumer_name.empty()) {
    shard->tmp_str1 =
        sdscpylen(shard->tmp_str1, opts.consumer_name.data(), opts.consumer_name.size());
    consumer = streamLookupConsumer(cg, shard->tmp_str1, SLC_NO_REFRESH);
  }

  PendingResult result;

  if (opts.count == -1) {
    result = GetPendingReducedResult(cg);
  } else {
    result = GetPendingExtendedResult(cg, consumer, opts);
  }
  return result;
}

void CreateGroup(CmdArgList args, string_view key, ConnectionContext* cntx) {
  if (args.size() < 2)
    return cntx->SendError(UnknownSubCmd("CREATE", "XGROUP"));

  CreateOpts opts;
  opts.gname = ArgS(args, 0);
  opts.id = ArgS(args, 1);
  if (args.size() >= 3) {
    ToUpper(&args[2]);
    if (ArgS(args, 2) == "MKSTREAM")
      opts.flags |= kCreateOptMkstream;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpCreate(t->GetOpArgs(shard), key, opts);
  };

  OpStatus result = cntx->transaction->ScheduleSingleHop(std::move(cb));
  switch (result) {
    case OpStatus::KEY_NOTFOUND:
      return (*cntx)->SendError(kXGroupKeyNotFound);
    default:
      (*cntx)->SendError(result);
  }
}

void DestroyGroup(string_view key, string_view gname, ConnectionContext* cntx) {
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpDestroyGroup(t->GetOpArgs(shard), key, gname);
  };

  OpStatus result = cntx->transaction->ScheduleSingleHop(std::move(cb));
  switch (result) {
    case OpStatus::OK:
      return (*cntx)->SendLong(1);
    case OpStatus::SKIPPED:
      return (*cntx)->SendLong(0);
    case OpStatus::KEY_NOTFOUND:
      return (*cntx)->SendError(kXGroupKeyNotFound);
    default:
      (*cntx)->SendError(result);
  }
}

void CreateConsumer(string_view key, string_view gname, string_view consumer,
                    ConnectionContext* cntx) {
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpCreateConsumer(t->GetOpArgs(shard), key, gname, consumer);
  };
  OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(cb);

  switch (result.status()) {
    case OpStatus::OK:
      return (*cntx)->SendLong(1);
    case OpStatus::KEY_EXISTS:
      return (*cntx)->SendLong(0);
    case OpStatus::SKIPPED:
      return (*cntx)->SendError(NoGroupError(key, gname));
    case OpStatus::KEY_NOTFOUND:
      return (*cntx)->SendError(kXGroupKeyNotFound);
    default:
      (*cntx)->SendError(result.status());
  }
}

void DelConsumer(string_view key, string_view gname, string_view consumer,
                 ConnectionContext* cntx) {
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpDelConsumer(t->GetOpArgs(shard), key, gname, consumer);
  };

  OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(cb);

  switch (result.status()) {
    case OpStatus::OK:
      return (*cntx)->SendLong(*result);
    case OpStatus::SKIPPED:
      return (*cntx)->SendError(NoGroupError(key, gname));
    case OpStatus::KEY_NOTFOUND:
      return (*cntx)->SendError(kXGroupKeyNotFound);
    default:
      (*cntx)->SendError(result.status());
  }
}

void SetId(string_view key, string_view gname, CmdArgList args, ConnectionContext* cntx) {
  string_view id = ArgS(args, 0);

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpSetId(t->GetOpArgs(shard), key, gname, id);
  };

  OpStatus result = cntx->transaction->ScheduleSingleHop(std::move(cb));
  switch (result) {
    case OpStatus::SKIPPED:
      return (*cntx)->SendError(NoGroupError(key, gname));
    case OpStatus::KEY_NOTFOUND:
      return (*cntx)->SendError(kXGroupKeyNotFound);
    default:
      (*cntx)->SendError(result);
  }
}

void XGroupHelp(CmdArgList args, ConnectionContext* cntx) {
  string_view help_arr[] = {
      "CREATE <key> <groupname> <id|$> [option]",
      "    Create a new consumer group. Options are:",
      "    * MKSTREAM",
      "      Create the empty stream if it does not exist.",
      "CREATECONSUMER <key> <groupname> <consumer>",
      "    Create a new consumer in the specified group.",
      "DELCONSUMER <key> <groupname> <consumer>",
      "    Remove the specified consumer.",
      "DESTROY <key> <groupname>",
      "    Remove the specified group.",
      "SETID <key> <groupname> <id|$>",
      "    Set the current group ID.",
  };
  return (*cntx)->SendSimpleStrArr(help_arr);
}

OpResult<int64_t> OpTrim(const OpArgs& op_args, const AddTrimOpts& opts) {
  auto* shard = op_args.shard;
  auto& db_slice = shard->db_slice();
  OpResult<PrimeIterator> res_it = db_slice.Find(op_args.db_cntx, opts.key, OBJ_STREAM);
  if (!res_it) {
    if (res_it.status() == OpStatus::KEY_NOTFOUND) {
      return 0;
    }
    return res_it.status();
  }

  CompactObj& cobj = (*res_it)->second;
  stream* s = (stream*)cobj.RObjPtr();

  return StreamTrim(opts, s);
}

optional<pair<AddTrimOpts, unsigned>> ParseAddOrTrimArgsOrReply(CmdArgList args,
                                                                ConnectionContext* cntx,
                                                                bool is_xadd) {
  AddTrimOpts opts;
  opts.key = ArgS(args, 0);

  unsigned id_indx = 1;
  for (; id_indx < args.size(); ++id_indx) {
    ToUpper(&args[id_indx]);
    string_view arg = ArgS(args, id_indx);

    size_t remaining_args = args.size() - id_indx - 1;

    if (is_xadd && arg == "NOMKSTREAM") {
      opts.no_mkstream = true;
    } else if ((arg == "MAXLEN" || arg == "MINID") && remaining_args >= 1) {
      if (opts.trim_strategy != TrimStrategy::kNone) {
        (*cntx)->SendError("MAXLEN and MINID options at the same time are not compatible",
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
        (*cntx)->SendError(kInvalidIntErr);
        return std::nullopt;
      }
      if (opts.trim_strategy == TrimStrategy::kMinId && !ParseID(arg, false, 0, &opts.minid)) {
        (*cntx)->SendError(kSyntaxErr);
        return std::nullopt;
      }
    } else if (arg == "LIMIT" && remaining_args >= 1 && opts.trim_strategy != TrimStrategy::kNone) {
      if (!opts.trim_approx) {
        (*cntx)->SendError(kSyntaxErr);
        return std::nullopt;
      }
      ++id_indx;
      if (!absl::SimpleAtoi(ArgS(args, id_indx), &opts.limit)) {
        (*cntx)->SendError(kSyntaxErr);
        return std::nullopt;
      }
    } else if (is_xadd) {
      // There are still remaining field args.
      break;
    } else {
      (*cntx)->SendError(kSyntaxErr);
      return std::nullopt;
    }
  }

  return make_pair(opts, id_indx);
}

}  // namespace

void StreamFamily::XAdd(CmdArgList args, ConnectionContext* cntx) {
  auto parse_resp = ParseAddOrTrimArgsOrReply(args, cntx, true);
  if (!parse_resp) {
    return;
  }

  auto add_opts = parse_resp->first;
  auto id_indx = parse_resp->second;

  args.remove_prefix(id_indx);
  if (args.size() < 2 || args.size() % 2 == 0) {
    return (*cntx)->SendError(WrongNumArgsError("XADD"), kSyntaxErrType);
  }

  string_view id = ArgS(args, 0);

  if (!ParseID(id, true, 0, &add_opts.parsed_id)) {
    return (*cntx)->SendError(kInvalidStreamId, kSyntaxErrType);
  }

  args.remove_prefix(1);
  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpAdd(t->GetOpArgs(shard), add_opts, args);
  };

  OpResult<streamID> add_result = cntx->transaction->ScheduleSingleHopT(cb);
  if (add_result) {
    return (*cntx)->SendBulkString(StreamIdRepr(*add_result));
  }

  if (add_result.status() == OpStatus::STREAM_ID_SMALL) {
    return (*cntx)->SendError(
        "The ID specified in XADD is equal or smaller than "
        "the target stream top item");
  }

  return (*cntx)->SendError(add_result.status());
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

void ParseXclaimOptions(CmdArgList& args, ClaimOpts& opts, ConnectionContext* cntx) {
  for (size_t i = 0; i < args.size(); ++i) {
    ToUpper(&args[i]);
    string_view arg = ArgS(args, i);
    bool remaining_args = args.size() - i - 1 > 0;

    if (remaining_args) {
      if (arg == "IDLE") {
        arg = ArgS(args, ++i);
        if (!absl::SimpleAtoi(arg, &opts.delivery_time)) {
          return (*cntx)->SendError(kInvalidIntErr);
        }
        continue;
      } else if (arg == "TIME") {
        arg = ArgS(args, ++i);
        if (!absl::SimpleAtoi(arg, &opts.delivery_time)) {
          return (*cntx)->SendError(kInvalidIntErr);
        }
        continue;
      } else if (arg == "RETRYCOUNT") {
        arg = ArgS(args, ++i);
        if (!absl::SimpleAtoi(arg, &opts.retry)) {
          return (*cntx)->SendError(kInvalidIntErr);
        }
        continue;
      } else if (arg == "LASTID") {
        arg = ArgS(args, ++i);
        // TODO: implement lastID
        continue;
      }
    }
    if (arg == "FORCE") {
      opts.flags |= kClaimForce;
    } else if (arg == "JUSTID") {
      opts.flags |= kClaimJustID;
    } else {
      return (*cntx)->SendError("Unknown argument given for XCLAIM command", kSyntaxErr);
    }
  }
}

void StreamFamily::XClaim(CmdArgList args, ConnectionContext* cntx) {
  ClaimOpts opts;
  string_view key = ArgS(args, 0);
  opts.group = ArgS(args, 1);
  opts.consumer = ArgS(args, 2);
  if (!absl::SimpleAtoi(ArgS(args, 3), &opts.min_idle_time)) {
    return (*cntx)->SendError(kSyntaxErr);
  }
  // Ignore negative min-idle-time
  opts.min_idle_time = std::max(opts.min_idle_time, static_cast<int64>(0));
  args.remove_prefix(4);

  auto ids = GetXclaimIds(args);
  if (ids.empty()) {
    // No ids given.
    return (*cntx)->SendError(kInvalidStreamId, kSyntaxErrType);
  }

  // parse the options
  ParseXclaimOptions(args, opts, cntx);
  if (auto now = GetCurrentTimeMs();
      opts.delivery_time < 0 || static_cast<uint64_t>(opts.delivery_time) > now)
    opts.delivery_time = now;

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpClaim(t->GetOpArgs(shard), key, opts, absl::Span{ids.data(), ids.size()});
  };
  OpResult<ClaimInfo> result = cntx->transaction->ScheduleSingleHopT(cb);
  if (!result) {
    (*cntx)->SendError(result.status());
    return;
  }

  ClaimInfo cresult = result.value();
  if (cresult.justid) {
    (*cntx)->StartArray(cresult.ids.size());
    for (auto id : cresult.ids) {
      (*cntx)->SendBulkString(StreamIdRepr(id));
    }
  } else {
    const RecordVec& crec = cresult.records;
    (*cntx)->StartArray(crec.size());
    for (const auto& item : crec) {
      (*cntx)->StartArray(2);
      (*cntx)->SendBulkString(StreamIdRepr(item.id));
      (*cntx)->StartArray(item.kv_arr.size() * 2);
      for (const auto& [k, v] : item.kv_arr) {
        (*cntx)->SendBulkString(k);
        (*cntx)->SendBulkString(v);
      }
    }
  }
}

void StreamFamily::XDel(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  args.remove_prefix(1);

  absl::InlinedVector<streamID, 8> ids(args.size());

  for (size_t i = 0; i < args.size(); ++i) {
    ParsedStreamId parsed_id;
    string_view str_id = ArgS(args, i);
    if (!ParseID(str_id, true, 0, &parsed_id)) {
      return (*cntx)->SendError(kInvalidStreamId, kSyntaxErrType);
    }
    ids[i] = parsed_id.val;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpDel(t->GetOpArgs(shard), key, absl::Span{ids.data(), ids.size()});
  };

  OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(cb);
  if (result || result.status() == OpStatus::KEY_NOTFOUND) {
    return (*cntx)->SendLong(*result);
  }

  (*cntx)->SendError(result.status());
}

void StreamFamily::XGroup(CmdArgList args, ConnectionContext* cntx) {
  ToUpper(&args[0]);
  string_view sub_cmd = ArgS(args, 0);

  if (args.size() >= 2) {
    string_view key = ArgS(args, 1);
    if (sub_cmd == "CREATE") {
      args.remove_prefix(2);
      return CreateGroup(std::move(args), key, cntx);
    }

    if (sub_cmd == "DESTROY" && args.size() == 3) {
      string_view gname = ArgS(args, 2);
      return DestroyGroup(key, gname, cntx);
    }

    if (sub_cmd == "CREATECONSUMER" && args.size() == 4) {
      string_view gname = ArgS(args, 2);
      string_view cname = ArgS(args, 3);
      return CreateConsumer(key, gname, cname, cntx);
    }

    if (sub_cmd == "DELCONSUMER" && args.size() == 4) {
      string_view gname = ArgS(args, 2);
      string_view cname = ArgS(args, 3);
      return DelConsumer(key, gname, cname, cntx);
    }

    if (sub_cmd == "SETID" && args.size() >= 4) {
      string_view gname = ArgS(args, 2);
      args.remove_prefix(3);
      return SetId(key, gname, std::move(args), cntx);
    }
  }

  return (*cntx)->SendError(UnknownSubCmd(sub_cmd, "XGROUP"));
}

void StreamFamily::XInfo(CmdArgList args, ConnectionContext* cntx) {
  ToUpper(&args[0]);
  string_view sub_cmd = ArgS(args, 0);
  if (sub_cmd == "HELP") {
    string_view help_arr[] = {
        "CONSUMERS <key> <groupname>",
        "    Show consumers of <groupname>.",
        "GROUPS <key>",
        "    Show the stream consumer groups.",
        "STREAM <key> [FULL [COUNT <count>]",
        "    Show information about the stream.",
    };
    return (*cntx)->SendSimpleStrArr(help_arr);
  }

  if (args.size() >= 2) {
    string_view key = ArgS(args, 1);
    ShardId sid = Shard(key, shard_set->size());

    if (sub_cmd == "GROUPS") {
      // We do not use transactional xemantics for xinfo since it's informational command.
      auto cb = [&]() {
        EngineShard* shard = EngineShard::tlocal();
        DbContext db_context{.db_index = cntx->db_index(), .time_now_ms = GetCurrentTimeMs()};
        return OpListGroups(db_context, key, shard);
      };

      OpResult<vector<GroupInfo>> result = shard_set->Await(sid, std::move(cb));
      if (result) {
        (*cntx)->StartArray(result->size());
        for (const auto& ginfo : *result) {
          string last_id = StreamIdRepr(ginfo.last_id);

          (*cntx)->StartCollection(6, RedisReplyBuilder::MAP);
          (*cntx)->SendBulkString("name");
          (*cntx)->SendBulkString(ginfo.name);
          (*cntx)->SendBulkString("consumers");
          (*cntx)->SendLong(ginfo.consumer_size);
          (*cntx)->SendBulkString("pending");
          (*cntx)->SendLong(ginfo.pending_size);
          (*cntx)->SendBulkString("last-delivered-id");
          (*cntx)->SendBulkString(last_id);
          (*cntx)->SendBulkString("entries-read");
          if (ginfo.entries_read != SCG_INVALID_ENTRIES_READ) {
            (*cntx)->SendLong(ginfo.entries_read);
          } else {
            (*cntx)->SendNull();
          }
          (*cntx)->SendBulkString("lag");
          if (ginfo.lag != SCG_INVALID_LAG) {
            (*cntx)->SendLong(ginfo.lag);
          } else {
            (*cntx)->SendNull();
          }
        }
        return;
      }
      return (*cntx)->SendError(result.status());
    }
  }
  return (*cntx)->SendError(UnknownSubCmd(sub_cmd, "XINFO"));
}

void StreamFamily::XLen(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  auto cb = [&](Transaction* t, EngineShard* shard) { return OpLen(t->GetOpArgs(shard), key); };

  OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(cb);
  if (result || result.status() == OpStatus::KEY_NOTFOUND) {
    return (*cntx)->SendLong(*result);
  }

  return (*cntx)->SendError(result.status());
}

bool ParseXpendingOptions(CmdArgList& args, PendingOpts& opts, ConnectionContext* cntx) {
  size_t id_indx = 0;
  ToUpper(&args[id_indx]);
  string_view arg = ArgS(args, id_indx);

  if (arg == "IDLE" && args.size() > 4) {
    id_indx++;
    if (!absl::SimpleAtoi(ArgS(args, id_indx), &opts.min_idle_time)) {
      (*cntx)->SendError(kInvalidIntErr, kSyntaxErrType);
      return false;
    }
    // Ignore negative min_idle_time
    opts.min_idle_time = std::max(opts.min_idle_time, static_cast<int64_t>(0));
    args.remove_prefix(2);
    id_indx = 0;
  }
  if (args.size() < 3) {
    (*cntx)->SendError(WrongNumArgsError("XPENDING"), kSyntaxErrType);
    return false;
  }

  // Parse start and end
  RangeId rs, re;
  string_view start = ArgS(args, id_indx);
  id_indx++;
  string_view end = ArgS(args, id_indx);
  if (!ParseRangeId(start, &rs) || !ParseRangeId(end, &re)) {
    (*cntx)->SendError(kInvalidStreamId, kSyntaxErrType);
    return false;
  }

  if (rs.exclude && streamIncrID(&rs.parsed_id.val) != C_OK) {
    (*cntx)->SendError("invalid start ID for the interval", kSyntaxErrType);
    return false;
  }

  if (re.exclude && streamDecrID(&re.parsed_id.val) != C_OK) {
    (*cntx)->SendError("invalid end ID for the interval", kSyntaxErrType);
    return false;
  }
  id_indx++;
  opts.start = rs.parsed_id;
  opts.end = re.parsed_id;

  // Parse count
  if (!absl::SimpleAtoi(ArgS(args, id_indx), &opts.count)) {
    (*cntx)->SendError(kInvalidIntErr, kSyntaxErrType);
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

void StreamFamily::XPending(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  PendingOpts opts;
  opts.group_name = ArgS(args, 1);
  args.remove_prefix(2);

  if (!args.empty() && !ParseXpendingOptions(args, opts, cntx)) {
    return;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpPending(t->GetOpArgs(shard), key, opts);
  };
  OpResult<PendingResult> op_result = cntx->transaction->ScheduleSingleHopT(cb);
  if (!op_result) {
    if (op_result.status() == OpStatus::SKIPPED)
      return (*cntx)->SendError(NoGroupError(key, opts.group_name));
    return (*cntx)->SendError(op_result.status());
  }
  PendingResult result = op_result.value();

  if (std::holds_alternative<PendingReducedResult>(result)) {
    const auto& res = std::get<PendingReducedResult>(result);
    if (!res.count) {
      return (*cntx)->SendEmptyArray();
    }
    (*cntx)->StartArray(4);
    (*cntx)->SendLong(res.count);
    (*cntx)->SendBulkString(StreamIdRepr(res.start));
    (*cntx)->SendBulkString(StreamIdRepr(res.end));
    (*cntx)->StartArray(res.consumer_list.size());

    for (auto& [consumer_name, count] : res.consumer_list) {
      (*cntx)->StartArray(2);
      (*cntx)->SendBulkString(consumer_name);
      (*cntx)->SendLong(count);
    }
  } else {
    const auto& res = std::get<PendingExtendedResultList>(result);
    if (!res.size()) {
      return (*cntx)->SendEmptyArray();
    }

    (*cntx)->StartArray(res.size());
    for (auto& item : res) {
      (*cntx)->StartArray(4);
      (*cntx)->SendBulkString(StreamIdRepr(item.start));
      (*cntx)->SendBulkString(item.consumer_name);
      (*cntx)->SendLong(item.elapsed);
      (*cntx)->SendLong(item.delivery_count);
    }
  }
}

void StreamFamily::XRange(CmdArgList args, ConnectionContext* cntx) {
  XRangeGeneric(std::move(args), false, cntx);
}

void StreamFamily::XRevRange(CmdArgList args, ConnectionContext* cntx) {
  swap(args[1], args[2]);
  XRangeGeneric(std::move(args), true, cntx);
}

std::optional<ReadOpts> ParseReadArgsOrReply(CmdArgList args, bool read_group,
                                             ConnectionContext* cntx) {
  size_t streams_count = 0;

  ReadOpts opts;
  opts.read_group = read_group;
  size_t id_indx = 0;

  if (opts.read_group) {
    ToUpper(&args[id_indx]);
    string_view arg = ArgS(args, id_indx);

    if (arg.size() - 1 < 2) {
      (*cntx)->SendError(kSyntaxErr);
      return std::nullopt;
    }

    if (arg != "GROUP") {
      const auto m = "Missing 'GROUP' in 'XREADGROUP' command";
      (*cntx)->SendError(m, kSyntaxErr);
      return std::nullopt;
    }
    id_indx++;
    opts.group_name = ArgS(args, id_indx);
    opts.consumer_name = ArgS(args, ++id_indx);
    id_indx++;
  }

  for (; id_indx < args.size(); ++id_indx) {
    ToUpper(&args[id_indx]);
    string_view arg = ArgS(args, id_indx);

    bool remaining_args = args.size() - id_indx - 1 > 0;
    if (arg == "BLOCK" && remaining_args) {
      id_indx++;
      arg = ArgS(args, id_indx);
      if (!absl::SimpleAtoi(arg, &opts.timeout)) {
        (*cntx)->SendError(kInvalidIntErr);
        return std::nullopt;
      }
    } else if (arg == "COUNT" && remaining_args) {
      id_indx++;
      arg = ArgS(args, id_indx);
      if (!absl::SimpleAtoi(arg, &opts.count)) {
        (*cntx)->SendError(kInvalidIntErr);
        return std::nullopt;
      }
    } else if (opts.read_group && arg == "NOACK") {
      opts.noack = true;
    } else if (arg == "STREAMS" && remaining_args) {
      opts.streams_arg = id_indx + 1;

      size_t pair_count = args.size() - opts.streams_arg;
      if ((pair_count % 2) != 0) {
        const auto m = "Unbalanced list of streams: for each stream key an ID must be specified";
        (*cntx)->SendError(m, kSyntaxErr);
        return std::nullopt;
      }
      streams_count = pair_count / 2;
      break;
    } else {
      (*cntx)->SendError(kSyntaxErr);
      return std::nullopt;
    }
  }

  // STREAMS option is required.
  if (opts.streams_arg == 0) {
    (*cntx)->SendError(kSyntaxErr);
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
        (*cntx)->SendError("The $ can be specified only when calling XREAD.", kSyntaxErr);
        return std::nullopt;
      }
      id.val.ms = 0;
      id.val.seq = 0;
      id.last_id = true;
      sitem.id = id;
      auto [_, is_inserted] = opts.stream_ids.emplace(key, sitem);
      if (!is_inserted) {
        (*cntx)->SendError(kSameStreamFound);
        return std::nullopt;
      }
      continue;
    }

    if (idstr == ">") {
      if (!opts.read_group) {
        (*cntx)->SendError(
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
        (*cntx)->SendError(kSameStreamFound);
        return std::nullopt;
      }
      continue;
    }

    if (!ParseID(idstr, true, 0, &id)) {
      (*cntx)->SendError(kInvalidStreamId, kSyntaxErrType);
      return std::nullopt;
    }

    // We only include messages with IDs greater than start so increment the
    // starting ID.
    streamIncrID(&id.val);
    sitem.id = id;
    auto [_, is_inserted] = opts.stream_ids.emplace(key, sitem);
    if (!is_inserted) {
      (*cntx)->SendError(kSameStreamFound);
      return std::nullopt;
    }
  }
  return opts;
}

// Returns the last ID of each stream in the transaction.
OpResult<unordered_map<string_view, streamID>> StreamLastIDs(Transaction* trans) {
  vector<OpResult<vector<pair<string_view, streamID>>>> last_ids_res(shard_set->size());

  auto cb = [&](Transaction* t, EngineShard* shard) {
    ShardId sid = shard->shard_id();
    last_ids_res[sid] = OpLastIDs(t->GetOpArgs(shard), t->GetShardArgs(shard->shard_id()));
    return OpStatus::OK;
  };
  trans->Execute(std::move(cb), false);

  unordered_map<string_view, streamID> last_ids;
  for (auto res : last_ids_res) {
    if (!res) {
      return res.status();
    }

    for (auto& e : *res) {
      last_ids.emplace(e.first, e.second);
    }
  }
  return last_ids;
}

void XReadBlock(ReadOpts opts, ConnectionContext* cntx) {
  // If BLOCK is not set just return an empty array as there are no resolvable
  // entries.
  if (opts.timeout == -1 || cntx->transaction->IsMulti()) {
    // Close the transaction and release locks.
    cntx->transaction->Conclude();
    return (*cntx)->SendNullArray();
  }

  auto wcb = [](Transaction* t, EngineShard* shard) { return t->GetShardArgs(shard->shard_id()); };

  auto tp = (opts.timeout) ? chrono::steady_clock::now() + chrono::milliseconds(opts.timeout)
                           : Transaction::time_point::max();

  bool wait_succeeded = cntx->transaction->WaitOnWatch(tp, std::move(wcb));
  if (!wait_succeeded) {
    return (*cntx)->SendNullArray();
  }

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
      auto sitem = opts.stream_ids.at(*wake_key);
      range_opts.start = sitem.id;
      range_opts.group = sitem.group;
      range_opts.consumer = sitem.consumer;
      range_opts.noack = opts.noack;

      result = OpRange(t->GetOpArgs(shard), *wake_key, range_opts);
      key = *wake_key;
    }
    return OpStatus::OK;
  };
  cntx->transaction->Execute(std::move(range_cb), true);

  if (result) {
    SinkReplyBuilder::ReplyAggregator agg(cntx->reply_builder());

    (*cntx)->StartArray(1);

    (*cntx)->StartArray(2);
    (*cntx)->SendBulkString(key);

    (*cntx)->StartArray(result->size());
    for (const auto& item : *result) {
      (*cntx)->StartArray(2);
      (*cntx)->SendBulkString(StreamIdRepr(item.id));
      (*cntx)->StartArray(item.kv_arr.size() * 2);
      for (const auto& k_v : item.kv_arr) {
        (*cntx)->SendBulkString(k_v.first);
        (*cntx)->SendBulkString(k_v.second);
      }
    }
    return;
  } else {
    return (*cntx)->SendNullArray();
  }
}

// Read entries from given streams
void XReadImpl(CmdArgList args, std::optional<ReadOpts> opts, ConnectionContext* cntx) {
  auto last_ids = StreamLastIDs(cntx->transaction);
  if (!last_ids) {
    // Close the transaction.
    cntx->transaction->Conclude();

    if (last_ids.status() == OpStatus::WRONG_TYPE) {
      cntx->SendError(kWrongTypeErr);
      return;
    }

    return (*cntx)->SendNullArray();
  }

  // Resolve '$' IDs and check if there are any streams with entries that can
  // be resolved without blocking.
  bool block = true;
  for (auto& [stream, requested_sitem] : opts->stream_ids) {
    if (auto last_id_it = last_ids->find(stream); last_id_it != last_ids->end()) {
      streamID last_id = last_id_it->second;

      if (opts->read_group && !requested_sitem.group) {
        // if the group associated with the key is not found,
        // send NoGroupOrKey error.
        // We are simply mimicking Redis' error message here.
        // However, we could actually report more precise error message.
        cntx->transaction->Conclude();
        cntx->SendError(NoGroupOrKey(stream, opts->group_name, " in XREADGROUP with GROUP option"));
        return;
      }

      // Resolve $ to the last ID in the stream.
      if (requested_sitem.id.last_id && !opts->read_group) {
        requested_sitem.id.val = last_id;
        // We only include messages with IDs greater than the last message so
        // increment the ID.
        streamIncrID(&requested_sitem.id.val);
        requested_sitem.id.last_id = false;
        continue;
      }
      if (opts->read_group) {
        // If '>' is not provided, consumer PEL is used. So don't need to block.
        if (requested_sitem.id.val.ms != UINT64_MAX || requested_sitem.id.val.seq != UINT64_MAX) {
          block = false;
          opts->serve_history = true;
          continue;
        }
        requested_sitem.id.val = requested_sitem.group->last_id;
        streamIncrID(&requested_sitem.id.val);
      }

      if (streamCompareID(&last_id, &requested_sitem.id.val) >= 0) {
        block = false;
      }
    } else {
      if (opts->read_group) {
        // if the key is not found, send NoGroupOrKey error.
        // We are simply mimicking Redis' error message here.
        // However, we could actually report more precise error message.
        string error_msg =
            NoGroupOrKey(stream, opts->group_name, " in XREADGROUP with GROUP option");
        cntx->transaction->Conclude();
        cntx->SendError(error_msg);
        return;
      }
    }
  }

  if (block) {
    return XReadBlock(*opts, cntx);
  }

  vector<vector<RecordVec>> xread_resp(shard_set->size());
  auto read_cb = [&](Transaction* t, EngineShard* shard) {
    ShardId sid = shard->shard_id();
    xread_resp[sid] = OpRead(t->GetOpArgs(shard), t->GetShardArgs(shard->shard_id()), *opts);
    return OpStatus::OK;
  };
  cntx->transaction->Execute(std::move(read_cb), true);

  // Merge the results into a single response ordered by stream.
  vector<RecordVec> res(opts->stream_ids.size());
  // Track the number of streams with records as empty streams are excluded from
  // the response.
  int resolved_streams = 0;
  for (ShardId sid = 0; sid < shard_set->size(); ++sid) {
    if (!cntx->transaction->IsActive(sid))
      continue;

    vector<RecordVec>& results = xread_resp[sid];

    ArgSlice slice = cntx->transaction->GetShardArgs(sid);

    DCHECK(!slice.empty());
    DCHECK_EQ(slice.size(), results.size());

    for (size_t i = 0; i < slice.size(); ++i) {
      if (results[i].size() == 0) {
        continue;
      }

      resolved_streams++;

      // Add the stream records ordered by the original stream arguments.
      size_t indx = cntx->transaction->ReverseArgIndex(sid, i);
      res[indx - opts->streams_arg] = std::move(results[i]);
    }
  }

  SinkReplyBuilder::ReplyAggregator agg(cntx->reply_builder());

  (*cntx)->StartArray(resolved_streams);
  for (size_t i = 0; i != res.size(); i++) {
    // Ignore empty streams.
    if (res[i].size() == 0) {
      continue;
    }

    (*cntx)->StartArray(2);
    (*cntx)->SendBulkString(ArgS(args, i + opts->streams_arg));
    (*cntx)->StartArray(res[i].size());
    for (const auto& item : res[i]) {
      (*cntx)->StartArray(2);
      (*cntx)->SendBulkString(StreamIdRepr(item.id));
      (*cntx)->StartArray(item.kv_arr.size() * 2);
      for (const auto& k_v : item.kv_arr) {
        (*cntx)->SendBulkString(k_v.first);
        (*cntx)->SendBulkString(k_v.second);
      }
    }
  }
}

void XReadGeneric(CmdArgList args, bool read_group, ConnectionContext* cntx) {
  auto opts = ParseReadArgsOrReply(args, read_group, cntx);
  if (!opts) {
    return;
  }

  vector<vector<GroupConsumerPair>> res_pairs(shard_set->size());
  auto cb = [&](Transaction* t, EngineShard* shard) {
    auto sid = shard->shard_id();
    auto s_args = t->GetShardArgs(sid);
    GroupConsumerPairOpts gc_opts = {opts->group_name, opts->consumer_name};

    res_pairs[sid] = OpGetGroupConsumerPairs(s_args, t->GetOpArgs(shard), gc_opts);
    return OpStatus::OK;
  };
  cntx->transaction->Schedule();
  if (opts->read_group) {
    // If the command is `XReadGroup`, we need to get
    // the (group, consumer) pairs for each key.
    cntx->transaction->Execute(std::move(cb), false);

    for (size_t i = 0; i < shard_set->size(); i++) {
      auto s_item = res_pairs[i];
      auto s_args = cntx->transaction->GetShardArgs(i);
      if (s_item.size() == 0) {
        continue;
      }
      for (size_t j = 0; j < s_args.size(); j++) {
        string_view key = s_args[j];
        StreamIDsItem& item = opts->stream_ids.at(key);
        item.consumer = s_item[j].consumer;
        item.group = s_item[j].group;
      }
    }
  }
  return XReadImpl(args, opts, cntx);
}

void StreamFamily::XRead(CmdArgList args, ConnectionContext* cntx) {
  return XReadGeneric(args, false, cntx);
}

void StreamFamily::XReadGroup(CmdArgList args, ConnectionContext* cntx) {
  return XReadGeneric(args, true, cntx);
}

void StreamFamily::XSetId(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view idstr = ArgS(args, 1);

  ParsedStreamId parsed_id;
  if (!ParseID(idstr, true, 0, &parsed_id)) {
    return (*cntx)->SendError(kInvalidStreamId, kSyntaxErrType);
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpSetId2(t->GetOpArgs(shard), key, parsed_id.val);
  };

  OpStatus result = cntx->transaction->ScheduleSingleHop(std::move(cb));
  switch (result) {
    case OpStatus::STREAM_ID_SMALL:
      return (*cntx)->SendError(
          "The ID specified in XSETID is smaller than the target stream top item");
    case OpStatus::ENTRIES_ADDED_SMALL:
      return (*cntx)->SendError(
          "The entries_added specified in XSETID is smaller than "
          "the target stream length");
    default:
      return (*cntx)->SendError(result);
  }
}

void StreamFamily::XTrim(CmdArgList args, ConnectionContext* cntx) {
  auto parse_resp = ParseAddOrTrimArgsOrReply(args, cntx, true);
  if (!parse_resp) {
    return;
  }

  auto trim_opts = parse_resp->first;

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpTrim(t->GetOpArgs(shard), trim_opts);
  };

  OpResult<int64_t> trim_result = cntx->transaction->ScheduleSingleHopT(cb);
  if (trim_result) {
    return (*cntx)->SendLong(*trim_result);
  }
  return (*cntx)->SendError(trim_result.status());
}

void StreamFamily::XRangeGeneric(CmdArgList args, bool is_rev, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view start = ArgS(args, 1);
  string_view end = ArgS(args, 2);
  RangeOpts range_opts;
  RangeId rs, re;
  if (!ParseRangeId(start, &rs) || !ParseRangeId(end, &re)) {
    return (*cntx)->SendError(kInvalidStreamId, kSyntaxErrType);
  }

  if (rs.exclude && streamIncrID(&rs.parsed_id.val) != C_OK) {
    return (*cntx)->SendError("invalid start ID for the interval", kSyntaxErrType);
  }

  if (re.exclude && streamDecrID(&re.parsed_id.val) != C_OK) {
    return (*cntx)->SendError("invalid end ID for the interval", kSyntaxErrType);
  }

  if (args.size() > 3) {
    if (args.size() != 5) {
      return (*cntx)->SendError(WrongNumArgsError("XRANGE"), kSyntaxErrType);
    }
    ToUpper(&args[3]);
    string_view opt = ArgS(args, 3);
    string_view val = ArgS(args, 4);

    if (opt != "COUNT" || !absl::SimpleAtoi(val, &range_opts.count)) {
      return (*cntx)->SendError(kSyntaxErr);
    }
  }

  range_opts.start = rs.parsed_id;
  range_opts.end = re.parsed_id;
  range_opts.is_rev = is_rev;

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpRange(t->GetOpArgs(shard), key, range_opts);
  };

  OpResult<RecordVec> result = cntx->transaction->ScheduleSingleHopT(cb);

  if (result) {
    SinkReplyBuilder::ReplyAggregator agg(cntx->reply_builder());

    (*cntx)->StartArray(result->size());
    for (const auto& item : *result) {
      (*cntx)->StartArray(2);
      (*cntx)->SendBulkString(StreamIdRepr(item.id));
      (*cntx)->StartArray(item.kv_arr.size() * 2);
      for (const auto& k_v : item.kv_arr) {
        (*cntx)->SendBulkString(k_v.first);
        (*cntx)->SendBulkString(k_v.second);
      }
    }
    return;
  }

  if (result.status() == OpStatus::KEY_NOTFOUND) {
    return (*cntx)->SendEmptyArray();
  }
  return (*cntx)->SendError(result.status());
}

void StreamFamily::XAck(CmdArgList args, ConnectionContext* cntx) {
  string_view key = ArgS(args, 0);
  string_view group = ArgS(args, 1);
  args.remove_prefix(2);
  absl::InlinedVector<streamID, 8> ids(args.size());

  for (size_t i = 0; i < args.size(); ++i) {
    ParsedStreamId parsed_id;
    string_view str_id = ArgS(args, i);
    if (!ParseID(str_id, true, 0, &parsed_id)) {
      return (*cntx)->SendError(kInvalidStreamId, kSyntaxErrType);
    }
    ids[i] = parsed_id.val;
  }

  auto cb = [&](Transaction* t, EngineShard* shard) {
    return OpAck(t->GetOpArgs(shard), key, group, absl::Span{ids.data(), ids.size()});
  };

  OpResult<uint32_t> result = cntx->transaction->ScheduleSingleHopT(cb);
  if (result || result.status() == OpStatus::KEY_NOTFOUND) {
    return (*cntx)->SendLong(*result);
  }

  (*cntx)->SendError(result.status());
  return;
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
}  // namespace acl

void StreamFamily::Register(CommandRegistry* registry) {
  using CI = CommandId;
  registry->StartFamily();
  *registry
      << CI{"XADD", CO::WRITE | CO::DENYOOM | CO::FAST, -5, 1, 1, 1, acl::kXAdd}.HFUNC(XAdd)
      << CI{"XCLAIM", CO::WRITE | CO::FAST, -6, 1, 1, 1, acl::kXClaim}.HFUNC(XClaim)
      << CI{"XDEL", CO::WRITE | CO::FAST, -3, 1, 1, 1, acl::kXDel}.HFUNC(XDel)
      << CI{"XGROUP", CO::WRITE | CO::DENYOOM, -3, 2, 2, 1, acl::kXGroup}.HFUNC(XGroup)
      << CI{"XINFO", CO::READONLY | CO::NOSCRIPT, -2, 0, 0, 0, acl::kXInfo}.HFUNC(XInfo)
      << CI{"XLEN", CO::READONLY | CO::FAST, 2, 1, 1, 1, acl::kXLen}.HFUNC(XLen)
      << CI{"XPENDING", CO::READONLY, -2, 1, 1, 1, acl::kXPending}.HFUNC(XPending)
      << CI{"XRANGE", CO::READONLY, -4, 1, 1, 1, acl::kXRange}.HFUNC(XRange)
      << CI{"XREVRANGE", CO::READONLY, -4, 1, 1, 1, acl::kXRevRange}.HFUNC(XRevRange)
      << CI{"XREAD",    CO::READONLY | CO::REVERSE_MAPPING | CO::VARIADIC_KEYS, -3, 3, 3, 1,
            acl::kXRead}
             .HFUNC(XRead)
      << CI{"XREADGROUP",    CO::READONLY | CO::REVERSE_MAPPING | CO::VARIADIC_KEYS, -6, 6, 6, 1,
            acl::kXReadGroup}
             .HFUNC(XReadGroup)
      << CI{"XSETID", CO::WRITE, 3, 1, 1, 1, acl::kXSetId}.HFUNC(XSetId)
      << CI{"XTRIM", CO::WRITE | CO::FAST, -4, 1, 1, 1, acl::kXTrim}.HFUNC(XTrim)
      << CI{"_XGROUP_HELP", CO::NOSCRIPT | CO::HIDDEN, 2, 0, 0, 0, acl::kXGroupHelp}.SetHandler(
             XGroupHelp)
      << CI{"XACK", CO::WRITE | CO::FAST, -4, 1, 1, 1, acl::kXAdd}.HFUNC(XAck);
}

}  // namespace dfly
