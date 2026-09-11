/* Minimal, self-contained replacement for the sds functions read.c needs
 * (rsdsempty/free/len/avail/catlen/range), backed by plain malloc/realloc/free
 * instead of zmalloc.
 *
 * read.c's redisReader outlives the per-shard mimalloc heap that zmalloc routes
 * through (it's owned by a long-lived ProtocolClient/Replica, torn down only at
 * final process exit, well after EngineShard::DestroyThreadLocal() has already
 * destroyed that heap) -- freeing zmalloc-backed memory that late is a
 * use-after-free. This gives redisReader's own buffer a heap-independent
 * lifetime instead.
 */
#pragma once

#include <stddef.h>

typedef char *rsds;

rsds rsdsempty(void);
void rsdsfree(rsds s);
size_t rsdslen(const rsds s);
size_t rsdsavail(const rsds s);
rsds rsdscatlen(rsds s, const void *t, size_t len);
int rsdsrange(rsds s, long start, long end);
