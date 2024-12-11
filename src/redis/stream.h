#ifndef STREAM_H
#define STREAM_H

#include "util.h"
#include "rax.h"
#include "sds.h"
#include "listpack.h"


typedef struct redisObject robj;

/* Stream item ID: a 128 bit number composed of a milliseconds time and
 * a sequence counter. IDs generated in the same millisecond (or in a past
 * millisecond if the clock jumped backward) will use the millisecond time
 * of the latest generated ID and an incremented sequence. */
typedef struct streamID {
    uint64_t ms;        /* Unix time in milliseconds. */
    uint64_t seq;       /* Sequence number. */
} streamID;

typedef struct stream {
    rax *rax_tree;               /* The radix tree holding the stream. */
    uint64_t length;        /* Current number of elements inside this stream. */
    streamID last_id;       /* Zero if there are yet no items. */
    streamID first_id;      /* The first non-tombstone entry, zero if empty. */
    streamID max_deleted_entry_id;  /* The maximal ID that was deleted. */
    uint64_t entries_added; /* All time count of elements added. */
    rax *cgroups;           /* Consumer groups dictionary: name -> streamCG */
} stream;

/* We define an iterator to iterate stream items in an abstract way, without
 * caring about the radix tree + listpack representation. Technically speaking
 * the iterator is only used inside streamReplyWithRange(), so could just
 * be implemented inside the function, but practically there is the AOF
 * rewriting code that also needs to iterate the stream to emit the XADD
 * commands. */
typedef struct streamIterator {
    stream *stream;         /* The stream we are iterating. */
    streamID master_id;     /* ID of the master entry at listpack head. */
    uint64_t master_fields_count;       /* Master entries # of fields. */
    unsigned char *master_fields_start; /* Master entries start in listpack. */
    unsigned char *master_fields_ptr;   /* Master field to emit next. */
    int entry_flags;                    /* Flags of entry we are emitting. */
    int rev;                /* True if iterating end to start (reverse). */
    int skip_tombstones;    /* True if not emitting tombstone entries. */
    uint64_t start_key[2];  /* Start key as 128 bit big endian. */
    uint64_t end_key[2];    /* End key as 128 bit big endian. */
    raxIterator ri;         /* Rax iterator. */
    unsigned char *lp;      /* Current listpack. */
    unsigned char *lp_ele;  /* Current listpack cursor. */
    unsigned char *lp_flags; /* Current entry flags pointer. */
    /* Buffers used to hold the string of lpGet() when the element is
     * integer encoded, so that there is no string representation of the
     * element inside the listpack itself. */
    unsigned char field_buf[LP_INTBUF_SIZE];
    unsigned char value_buf[LP_INTBUF_SIZE];
} streamIterator;

/* Consumer group. */
typedef struct streamCG {
    streamID last_id;       /* Last delivered (not acknowledged) ID for this
                               group. Consumers that will just ask for more
                               messages will served with IDs > than this. */
    long long entries_read; /* In a perfect world (CG starts at 0-0, no dels, no
                               XGROUP SETID, ...), this is the total number of
                               group reads. In the real world, the reasoning behind
                               this value is detailed at the top comment of
                               streamEstimateDistanceFromFirstEverEntry(). */
    rax *pel;               /* Pending entries list. This is a radix tree that
                               has every message delivered to consumers (without
                               the NOACK option) that was yet not acknowledged
                               as processed. The key of the radix tree is the
                               ID as a 64 bit big endian number, while the
                               associated value is a streamNACK structure.*/
    rax *consumers;         /* A radix tree representing the consumers by name
                               and their associated representation in the form
                               of streamConsumer structures. */
} streamCG;

/* A specific consumer in a consumer group.  */
typedef struct streamConsumer {
    mstime_t seen_time;         /* Last time this consumer tried to perform an action (attempted reading/claiming). */
    mstime_t active_time;       /* Last time this consumer was active (successful reading/claiming). */
    sds name;                   /* Consumer name. This is how the consumer
                                   will be identified in the consumer group
                                   protocol. Case sensitive. */
    rax *pel;                   /* Consumer specific pending entries list: all
                                   the pending messages delivered to this
                                   consumer not yet acknowledged. Keys are
                                   big endian message IDs, while values are
                                   the same streamNACK structure referenced
                                   in the "pel" of the consumer group structure
                                   itself, so the value is shared. */
} streamConsumer;

/* Pending (yet not acknowledged) message in a consumer group. */
typedef struct streamNACK {
    mstime_t delivery_time;     /* Last time this message was delivered. */
    uint64_t delivery_count;    /* Number of times this message was delivered.*/
    streamConsumer *consumer;   /* The consumer this message was delivered to
                                   in the last delivery. */
} streamNACK;


typedef struct {
  /* XADD options */
  streamID id;     /* User-provided ID, for XADD only. */
  int id_given;    /* Was an ID different than "*" specified? for XADD only. */
  int seq_given;   /* Was an ID different than "ms-*" specified? for XADD only. */
  int no_mkstream; /* if set to 1 do not create new stream */

  /* XADD + XTRIM common options */
  int trim_strategy;         /* TRIM_STRATEGY_* */
  int trim_strategy_arg_idx; /* Index of the count in MAXLEN/MINID, for rewriting. */
  int approx_trim;           /* If 1 only delete whole radix tree nodes, so
                              * the trim argument is not applied verbatim. */
  long long limit;           /* Maximum amount of entries to trim. If 0, no limitation
                              * on the amount of trimming work is enforced. */
  /* TRIM_STRATEGY_MAXLEN options */
  long long maxlen; /* After trimming, leave stream at this length . */
  /* TRIM_STRATEGY_MINID options */
  streamID minid; /* Trim by ID (No stream entries with ID < 'minid' will remain) */
} streamAddTrimArgs;

/* Prototypes of exported APIs. */
// struct client;

/* Flags for streamCreateConsumer */
#define SCC_DEFAULT       0
#define SCC_NO_NOTIFY     (1<<0) /* Do not notify key space if consumer created */
#define SCC_NO_DIRTIFY    (1<<1) /* Do not dirty++ if consumer created */

#define SCG_INVALID_ENTRIES_READ -1
#define SCG_INVALID_LAG -1

#define TRIM_STRATEGY_NONE 0
#define TRIM_STRATEGY_MAXLEN 1
#define TRIM_STRATEGY_MINID 2

stream *streamNew(void);
void freeStream(stream *s);
// size_t streamReplyWithRange(client *c, stream *s, streamID *start, streamID *end, size_t count, int rev, streamCG *group, streamConsumer *consumer, int flags, streamPropInfo *spi);
void streamIteratorStart(streamIterator *si, stream *s, streamID *start, streamID *end, int rev);
int streamIteratorGetID(streamIterator *si, streamID *id, int64_t *numfields);
void streamIteratorGetField(streamIterator *si, unsigned char **fieldptr, unsigned char **valueptr, int64_t *fieldlen, int64_t *valuelen);
void streamIteratorRemoveEntry(streamIterator *si, streamID *current);
void streamIteratorStop(streamIterator *si);
streamCG *streamLookupCG(stream *s, sds groupname);
streamConsumer *streamLookupConsumer(streamCG *cg, sds name);
streamCG *streamCreateCG(stream *s, const char *name, size_t namelen, streamID *id, long long entries_read);
void streamEncodeID(void *buf, streamID *id);
void streamDecodeID(void *buf, streamID *id);
int streamCompareID(streamID *a, streamID *b);
int streamEntryExists(stream *s, streamID *id);
void streamFreeNACK(streamNACK *na);
int streamIncrID(streamID *id);
int streamDecrID(streamID *id);

int streamValidateListpackIntegrity(unsigned char *lp, size_t size, int deep);
int streamParseID(const robj *o, streamID *id);
robj *createObjectFromStreamID(streamID *id);
int streamAppendItem(stream *s, robj **argv, int64_t numfields, streamID *added_id, streamID *use_id, int seq_given);
int streamDeleteItem(stream *s, streamID *id);
void streamGetEdgeID(stream *s, int first, int skip_tombstones, streamID *edge_id);
long long streamEstimateDistanceFromFirstEverEntry(stream *s, streamID *id);
int64_t streamTrim(stream *s, streamAddTrimArgs *args);
int64_t streamTrimByLength(stream *s, long long maxlen, int approx);
int64_t streamTrimByID(stream *s, streamID minid, int approx);
void streamFreeCG(streamCG *cg);
void streamDelConsumer(streamCG *cg, streamConsumer *consumer);
void streamLastValidID(stream *s, streamID *maxid);
int streamIDEqZero(streamID *id);
int streamRangeHasTombstones(stream *s, streamID *start, streamID *end);
long long streamCGLag(stream *s, streamCG *cg);

#endif
