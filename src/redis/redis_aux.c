#include "redis_aux.h"

#include <string.h>
#include <unistd.h>

#include "crc64.h"
#include "endianconv.h"
#include "quicklist.h"
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

// Based on quicklistGetIteratorAtIdx but without allocations
void quicklistInitIterator(quicklistIter* iter, quicklist *quicklist, int direction,
                           const long long idx) {
    quicklistNode *n = NULL;
    unsigned long long accum = 0;
    int forward = idx < 0 ? 0 : 1; /* < 0 -> reverse, 0+ -> forward */
    unsigned long long index = forward ? idx : (-idx) - 1;

    iter->direction = direction;
    iter->quicklist = quicklist;
    iter->current = NULL;
    iter->zi = NULL;

    if (index >= quicklist->count) return;

    /* Seek in the other direction if that way is shorter. */
    int seek_forward = forward;
    unsigned long long seek_index = index;
    if (index > (quicklist->count - 1) / 2) {
        seek_forward = !forward;
        seek_index = quicklist->count - 1 - index;
    }

    n = seek_forward ? quicklist->head : quicklist->tail;
    while (likely(n)) {
        if ((accum + n->count) > seek_index) {
            break;
        } else {
            accum += n->count;
            n = seek_forward ? n->next : n->prev;
        }
    }

    if (!n)
      return;

    iter->current = n;

    /* Fix accum so it looks like we seeked in the other direction. */
    if (seek_forward != forward)
      accum = quicklist->count - n->count - accum;

    if (forward) {
        /* forward = normal head-to-tail offset. */
        iter->offset = index - accum;
    } else {
        /* reverse = need negative offset for tail-to-head, so undo
         * the result of the original index = (-idx) - 1 above. */
        iter->offset = (-index) - 1 + accum;
    }
}