#include "redis_aux.h"

#include <string.h>
#include <unistd.h>

#include "crc64.h"
#include "endianconv.h"
#include "zmalloc.h"

Server server;

void InitRedisTables() {
  crc64_init();
  memset(&server, 0, sizeof(server));

  // been used by t_zset routines that convert listpack to skiplist for cases
  // above these thresholds.
  server.zset_max_listpack_entries = 128;
  server.zset_max_listpack_value = 32;

  server.max_map_field_len = 64;
  server.max_listpack_map_bytes = 1024;

  server.stream_node_max_bytes = 4096;
  server.stream_node_max_entries = 100;
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
