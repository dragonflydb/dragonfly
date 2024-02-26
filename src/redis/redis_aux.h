#ifndef __REDIS_AUX_H
#define __REDIS_AUX_H

#include "sds.h"

/* redis.h auxiliary definitions */
/* the last one in object.h is OBJ_STREAM and it is 6,
 * this will add enough place for Redis types to grow */
#define OBJ_JSON 15U

/* How many types of objects exist */
#define OBJ_TYPE_MAX 16U

#define HASHTABLE_MIN_FILL 10           /* Minimal hash table fill 10% */
#define HASHTABLE_MAX_LOAD_FACTOR 1.618 /* Maximum hash table load factor. */

/* Redis maxmemory strategies. Instead of using just incremental number
 * for this defines, we use a set of flags so that testing for certain
 * properties common to multiple policies is faster. */
#define MAXMEMORY_FLAG_LRU (1 << 0)
#define MAXMEMORY_FLAG_LFU (1 << 1)
#define MAXMEMORY_FLAG_ALLKEYS (1 << 2)
#define MAXMEMORY_FLAG_NO_SHARED_INTEGERS (MAXMEMORY_FLAG_LRU | MAXMEMORY_FLAG_LFU)

#define LFU_INIT_VAL 5

#define MAXMEMORY_VOLATILE_LRU ((0 << 8) | MAXMEMORY_FLAG_LRU)
#define MAXMEMORY_VOLATILE_LFU ((1 << 8) | MAXMEMORY_FLAG_LFU)
#define MAXMEMORY_VOLATILE_TTL (2 << 8)
#define MAXMEMORY_VOLATILE_RANDOM (3 << 8)
#define MAXMEMORY_ALLKEYS_LRU ((4 << 8) | MAXMEMORY_FLAG_LRU | MAXMEMORY_FLAG_ALLKEYS)
#define MAXMEMORY_ALLKEYS_LFU ((5 << 8) | MAXMEMORY_FLAG_LFU | MAXMEMORY_FLAG_ALLKEYS)
#define MAXMEMORY_ALLKEYS_RANDOM ((6 << 8) | MAXMEMORY_FLAG_ALLKEYS)
#define MAXMEMORY_NO_EVICTION (7 << 8)

#define CONFIG_RUN_ID_SIZE 40U

/* To improve the quality of the LRU approximation we take a set of keys
 * that are good candidate for eviction across performEvictions() calls.
 *
 * Entries inside the eviction pool are taken ordered by idle time, putting
 * greater idle times to the right (ascending order).
 *
 * When an LFU policy is used instead, a reverse frequency indication is used
 * instead of the idle time, so that we still evict by larger value (larger
 * inverse frequency means to evict keys with the least frequent accesses).
 *
 * Empty entries have the key pointer set to NULL. */

typedef struct dict dict;

uint64_t dictSdsHash(const void* key);
int dictSdsKeyCompare(dict* privdata, const void* key1, const void* key2);
void dictSdsDestructor(dict* privdata, void* val);
size_t sdsZmallocSize(sds s);

typedef struct ServerStub {
  int lfu_decay_time; /* LFU counter decay factor. */
  /* should not be used. Use FLAGS_list_max_ziplist_size and FLAGS_list_compress_depth instead. */
  // int list_compress_depth;
  // int list_max_ziplist_size;

  // unused - left so that object.c will compile.
  int maxmemory_policy; /* Policy for key eviction */

  size_t max_map_field_len, max_listpack_map_bytes;

  size_t zset_max_listpack_entries;
  size_t zset_max_listpack_value;

  size_t stream_node_max_bytes;
  long long stream_node_max_entries;
} Server;

extern Server server;

void InitRedisTables();

const char *strEncoding(int encoding);


/* The actual Redis Object */
#define OBJ_STRING 0U    /* String object. */
#define OBJ_LIST 1U      /* List object. */
#define OBJ_SET 2U       /* Set object. */
#define OBJ_ZSET 3U      /* Sorted set object. */
#define OBJ_HASH 4U      /* Hash object. */
#define OBJ_MODULE 5U    /* Module object. */
#define OBJ_STREAM 6U    /* Stream object. */

/* Objects encoding. Some kind of objects like Strings and Hashes can be
 * internally represented in multiple ways. The 'encoding' field of the object
 * is set to one of this fields for this object. */
#define OBJ_ENCODING_RAW 0U     /* Raw representation */
#define OBJ_ENCODING_INT 1U     /* Encoded as integer */
#define OBJ_ENCODING_HT 2U      /* Encoded as hash table */
#define OBJ_ENCODING_ZIPMAP 3U  /* Encoded as zipmap */
#define OBJ_ENCODING_LINKEDLIST 4U /* No longer used: old list encoding. */
#define OBJ_ENCODING_ZIPLIST 5U /* Encoded as ziplist */
#define OBJ_ENCODING_INTSET 6U  /* Encoded as intset */
#define OBJ_ENCODING_SKIPLIST 7U  /* Encoded as skiplist */
#define OBJ_ENCODING_EMBSTR 8U  /* Embedded sds string encoding */
#define OBJ_ENCODING_QUICKLIST 9U /* Encoded as linked list of ziplists */
#define OBJ_ENCODING_STREAM 10U /* Encoded as a radix tree of listpacks */
#define OBJ_ENCODING_LISTPACK 11 /* Encoded as a listpack */
#define OBJ_ENCODING_COMPRESS_INTERNAL 15U  /* Kept as lzf compressed, to pass compressed blob to another thread */

#endif /* __REDIS_AUX_H */
