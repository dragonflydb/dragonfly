#include "redis_aux.h"

#include <string.h>
#include <unistd.h>

#include "crc64.h"
#include "object.h"
#include "zmalloc.h"

Server server;

void InitRedisTables() {
  crc64_init();
  memset(&server, 0, sizeof(server));

  server.page_size = sysconf(_SC_PAGESIZE);

  // been used by t_zset routines that convert listpack to skiplist for cases
  // above these thresholds.
  server.zset_max_listpack_entries = 128;
  server.zset_max_listpack_value = 64;

  // Present so that redis code compiles. However, we ignore this field and instead check against
  // listpack total size in hset_family.cc
  server.hash_max_listpack_entries = 512;
  server.hash_max_listpack_value = 32;  // decreased from redis default 64.

  server.rdb_compression = 1;
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

/* Hash type hash table (note that small hashes are represented with listpacks) */
dictType hashDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    dictSdsDestructor,          /* key destructor */
    dictSdsDestructor,          /* val destructor */
    NULL                        /* allow to expand */
};
