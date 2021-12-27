#include "redis_aux.h"

#include <string.h>

#include "crc64.h"
#include "object.h"
#include "zmalloc.h"

Server server;

void InitRedisTables() {
  crc64_init();
  server.page_size = sysconf(_SC_PAGESIZE);
  server.zset_max_listpack_entries = 128;
  server.zset_max_listpack_value = 64;
}

// These functions are moved here from server.c
int htNeedsResize(dict* dict) {
  long long size, used;

  size = dictSlots(dict);
  used = dictSize(dict);
  return (size > DICT_HT_INITIAL_SIZE && (used * 100 / size < HASHTABLE_MIN_FILL));
}

uint64_t dictSdsHash(const void* key) {
  return dictGenHashFunction((unsigned char*)key, sdslen((char*)key));
}

// MurmurHash64A for 8 bytes blob.
uint64_t dictPtrHash(const void* key) {
  const uint64_t m = 0xc6a4a7935bd1e995ULL;
  const int r = 47;
  uint64_t h = 120577 ^ (8 * m);
  uint64_t data;
  memcpy(&data, key, 8);
  uint64_t k = data;
  k *= m;
  k ^= k >> r;
  k *= m;
  h ^= k;
  h *= m;

  h ^= h >> r;
  h *= m;
  h ^= h >> r;

  return h;
}

int dictPtrKeyCompare(dict* privdata, const void* key1, const void* key2) {
  return key1 == key2;
}

int dictSdsKeyCompare(dict *d, const void* key1, const void* key2) {
  int l1, l2;
  DICT_NOTUSED(d);

  l1 = sdslen((sds)key1);
  l2 = sdslen((sds)key2);
  if (l1 != l2)
    return 0;
  return memcmp(key1, key2, l1) == 0;
}

void dictSdsDestructor(dict *d, void* val) {
  DICT_NOTUSED(d);

  sdsfree(val);
}

/* Return the size consumed from the allocator, for the specified SDS string,
 * including internal fragmentation. This function is used in order to compute
 * the client output buffer size. */
size_t sdsZmallocSize(sds s) {
  void* sh = sdsAllocPtr(s);
  return zmalloc_size(sh);
}

/* Return 1 if currently we allow dict to expand. Dict may allocate huge
 * memory to contain hash buckets when dict expands, that may lead redis
 * rejects user's requests or evicts some keys, we can stop dict to expand
 * provisionally if used memory will be over maxmemory after dict expands,
 * but to guarantee the performance of redis, we still allow dict to expand
 * if dict load factor exceeds HASHTABLE_MAX_LOAD_FACTOR. */
static int dictExpandAllowed(size_t moreMem, double usedRatio) {
  if (usedRatio <= HASHTABLE_MAX_LOAD_FACTOR) {
    return !overMaxmemoryAfterAlloc(moreMem);
  } else {
    return 1;
  }
}

/* Set dictionary type. Keys are SDS strings, values are not used. */
dictType setDictType = {
    dictSdsHash,       /* hash function */
    NULL,              /* key dup */
    NULL,              /* val dup */
    dictSdsKeyCompare, /* key compare */
    dictSdsDestructor, /* key destructor */
    NULL,              /* val destructor */
    NULL               /* allow to expand */
};

/* Sorted sets hash (note: a skiplist is used in addition to the hash table) */
dictType zsetDictType = {
    dictSdsHash,       /* hash function */
    NULL,              /* key dup */
    NULL,              /* val dup */
    dictSdsKeyCompare, /* key compare */
    NULL,              /* Note: SDS string shared & freed by skiplist */
    NULL,              /* val destructor */
    NULL               /* allow to expand */
};

static void dictObjectDestructor(dict* privdata, void* val) {
  DICT_NOTUSED(privdata);

  if (val == NULL)
    return; /* Lazy freeing will set value to NULL. */
  decrRefCount(val);
}

/* Db->dict, keys are sds strings, vals are Redis objects. */
dictType dbDictType = {
    dictSdsHash,          /* hash function */
    NULL,                 /* key dup */
    NULL,                 /* val dup */
    dictSdsKeyCompare,    /* key compare */
    dictSdsDestructor,    /* key destructor */
    dictObjectDestructor, /* val destructor */
    dictExpandAllowed     /* allow to expand */
};

/* Db->expires */
/* Db->expires stores keys that are contained in redis_dict_.
   Which means that uniqueness of the strings is guaranteed through uniqueness of the pointers
   Therefore, it's enough to compare and hash keys by their addresses without reading the contents
   of the key
*/
dictType keyPtrDictType = {
    dictPtrHash,       /* hash function */
    NULL,              /* key dup */
    NULL,              /* val dup */
    dictPtrKeyCompare, /* key compare */
    NULL,              /* key destructor */
    NULL,              /* val destructor */
    NULL               /* allow to expand */
};
