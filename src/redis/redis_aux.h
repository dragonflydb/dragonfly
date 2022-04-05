#ifndef __REDIS_AUX_H
#define __REDIS_AUX_H

#include "dict.h"
#include "sds.h"

#define HASHTABLE_MIN_FILL        10      /* Minimal hash table fill 10% */
#define HASHTABLE_MAX_LOAD_FACTOR 1.618   /* Maximum hash table load factor. */

/* Redis maxmemory strategies. Instead of using just incremental number
 * for this defines, we use a set of flags so that testing for certain
 * properties common to multiple policies is faster. */
#define MAXMEMORY_FLAG_LRU (1<<0)
#define MAXMEMORY_FLAG_LFU (1<<1)
#define MAXMEMORY_FLAG_ALLKEYS (1<<2)
#define MAXMEMORY_FLAG_NO_SHARED_INTEGERS  (MAXMEMORY_FLAG_LRU|MAXMEMORY_FLAG_LFU)

#define LFU_INIT_VAL 5

#define MAXMEMORY_VOLATILE_LRU ((0<<8)|MAXMEMORY_FLAG_LRU)
#define MAXMEMORY_VOLATILE_LFU ((1<<8)|MAXMEMORY_FLAG_LFU)
#define MAXMEMORY_VOLATILE_TTL (2<<8)
#define MAXMEMORY_VOLATILE_RANDOM (3<<8)
#define MAXMEMORY_ALLKEYS_LRU ((4<<8)|MAXMEMORY_FLAG_LRU|MAXMEMORY_FLAG_ALLKEYS)
#define MAXMEMORY_ALLKEYS_LFU ((5<<8)|MAXMEMORY_FLAG_LFU|MAXMEMORY_FLAG_ALLKEYS)
#define MAXMEMORY_ALLKEYS_RANDOM ((6<<8)|MAXMEMORY_FLAG_ALLKEYS)
#define MAXMEMORY_NO_EVICTION (7<<8)


#define CONFIG_RUN_ID_SIZE 40

#define EVPOOL_CACHED_SDS_SIZE 255
#define EVPOOL_SIZE 16

int htNeedsResize(dict *dict);  // moved from server.cc

/* Hash table types */
extern dictType zsetDictType;
extern dictType setDictType;
extern dictType hashDictType;

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

struct evictionPoolEntry {
    unsigned long long idle;    /* Object idle time (inverse frequency for LFU) */
    sds key;                    /* Key name. */
    sds cached;                 /* Cached SDS object for key name. */
    int dbid;                   /* Key DB number. */
};

uint64_t dictSdsHash(const void *key);
int dictSdsKeyCompare(dict *privdata, const void *key1, const void *key2);
void dictSdsDestructor(dict *privdata, void *val);
size_t sdsZmallocSize(sds s) ;

typedef struct ServerStub {
    // char replid[CONFIG_RUN_ID_SIZE+1]; /* Master replication ID (if master). */
    size_t loading_loaded_bytes;
    size_t page_size;
    long long dirty, master_repl_offset;
    time_t lastsave;
    char* masterhost;

    int rdb_compression;
    int loading;
    int key_load_delay;
    int repl_state;
    int loading_start_time;
    int loading_total_bytes;
    int lastbgsave_status;

    int lfu_decay_time;             /* LFU counter decay factor. */
    /* should not be used. Use FLAGS_list_max_ziplist_size and FLAGS_list_compress_depth instead. */
    // int list_compress_depth;
    // int list_max_ziplist_size;

    unsigned long long maxmemory;   /* Max number of memory bytes to use */
    int maxmemory_policy;           /* Policy for key eviction */

    int rdb_save_incremental_fsync;
    size_t stat_peak_memory;
    size_t hash_max_listpack_entries,
           hash_max_listpack_value;
    size_t zset_max_listpack_entries;
    size_t zset_max_listpack_value;
    int sanitize_dump_payload;      /* Enables deep sanitization for ziplist and listpack in RDB and RESTORE. */
    long long stat_dump_payload_sanitizations; /* Number deep dump payloads integrity validations. */
    off_t loading_rdb_used_mem;
} Server;


extern Server server;

void InitRedisTables();

typedef struct redisObject robj;

#endif /* __REDIS_AUX_H */
