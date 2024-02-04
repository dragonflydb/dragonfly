#include "redis_aux.h"

#include <string.h>
#include <unistd.h>

#include "crc64.h"
#include "endianconv.h"
#include "object.h"
#include "zmalloc.h"

Server server;

void InitRedisTables() {
  crc64_init();
  memset(&server, 0, sizeof(server));

  server.maxmemory_policy = 0;
  server.lfu_decay_time = 0;

  // been used by t_zset routines that convert listpack to skiplist for cases
  // above these thresholds.
  server.zset_max_listpack_entries = 128;
  server.zset_max_listpack_value = 32;

  server.max_map_field_len = 64;
  server.max_listpack_map_bytes = 1024;

  server.stream_node_max_bytes = 4096;
  server.stream_node_max_entries = 100;
}

// These functions are moved here from server.c

uint64_t dictSdsHash(const void* key) {
  return dictGenHashFunction((unsigned char*)key, sdslen((char*)key));
}

int dictSdsKeyCompare(dict* d, const void* key1, const void* key2) {
  int l1, l2;
  DICT_NOTUSED(d);

  l1 = sdslen((sds)key1);
  l2 = sdslen((sds)key2);
  if (l1 != l2)
    return 0;
  return memcmp(key1, key2, l1) == 0;
}

void dictSdsDestructor(dict* d, void* val) {
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

/* Toggle the 64 bit unsigned integer pointed by *p from little endian to
 * big endian */
void memrev64(void* p) {
  unsigned char *x = p, t;

  t = x[0];
  x[0] = x[7];
  x[7] = t;
  t = x[1];
  x[1] = x[6];
  x[6] = t;
  t = x[2];
  x[2] = x[5];
  x[5] = t;
  t = x[3];
  x[3] = x[4];
  x[4] = t;
}

uint64_t intrev64(uint64_t v) {
  memrev64(&v);
  return v;
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

/* Hash type hash table (note that small hashes are represented with listpacks) */
dictType hashDictType = {
    dictSdsHash,       /* hash function */
    NULL,              /* key dup */
    NULL,              /* val dup */
    dictSdsKeyCompare, /* key compare */
    dictSdsDestructor, /* key destructor */
    dictSdsDestructor, /* val destructor */
    NULL               /* allow to expand */
};
