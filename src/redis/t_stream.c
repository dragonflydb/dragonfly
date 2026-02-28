/*
 * Copyright (c) 2017, Redis Ltd.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <errno.h>
#include <stdio.h>
#include <string.h>

#include "endianconv.h"
#include "stream.h"
#include "redis_aux.h"
#include "zmalloc.h"


/* For stream commands that require multiple IDs
 * when the number of IDs is less than 'STREAMID_STATIC_VECTOR_LEN',
 * avoid malloc allocation.*/
#define STREAMID_STATIC_VECTOR_LEN 8

/* Max pre-allocation for listpack. This is done to avoid abuse of a user
 * setting stream_node_max_bytes to a huge number. */
#define STREAM_LISTPACK_MAX_PRE_ALLOCATE 4096

/* Don't let listpacks grow too big, even if the user config allows it.
 * doing so can lead to an overflow (trying to store more than 32bit length
 * into the listpack header), or actually an assertion since lpInsert
 * will return NULL. */
#define STREAM_LISTPACK_MAX_SIZE (1 << 30)

static void streamFreeCGVoid(void *cg);

/* -----------------------------------------------------------------------
 * Low level stream encoding: a radix tree of listpacks.
 * ----------------------------------------------------------------------- */

/* Create a new stream data structure. */
stream *streamNew(void) {
    stream *s = zmalloc(sizeof(*s));
    s->rax = raxNew();
    s->length = 0;
    s->first_id.ms = 0;
    s->first_id.seq = 0;
    s->last_id.ms = 0;
    s->last_id.seq = 0;
    s->max_deleted_entry_id.seq = 0;
    s->max_deleted_entry_id.ms = 0;
    s->entries_added = 0;
    s->cgroups = NULL; /* Created on demand to save memory when not used. */
    return s;
}

static void lpFreeVoid(void *lp) {
    lpFree(lp);
}

/* Free a stream, including the listpacks stored inside the radix tree. */
void freeStream(stream *s) {
    raxFreeWithCallback(s->rax, lpFreeVoid);
    if (s->cgroups) raxFreeWithCallback(s->cgroups, streamFreeCGVoid);
    zfree(s);
}

static inline int64_t lpGetIntegerIfValid(unsigned char *ele, int *valid) {
    int64_t v;
    unsigned char *e = lpGet(ele, &v, NULL);
    if (e == NULL) {
        if (valid) *valid = 1;
        return v;
    }
    long long ll;
    int ret = string2ll((char *)e, v, &ll);
    if (valid)
        *valid = ret;
    else
        serverAssert(ret != 0);
    v = ll;
    return v;
}

#define lpGetInteger(ele) lpGetIntegerIfValid(ele, NULL)

/* Get an edge streamID of a given listpack.
 * 'master_id' is an input param, used to build the 'edge_id' output param */
/* Convert the specified stream entry ID as a 128 bit big endian number, so
 * that the IDs can be sorted lexicographically. */
static void streamEncodeID(void *buf, streamID *id) {
    uint64_t e[2];
    e[0] = htonu64(id->ms);
    e[1] = htonu64(id->seq);
    memcpy(buf, e, sizeof(e));
}

/* This is the reverse of streamEncodeID(): the decoded ID will be stored
 * in the 'id' structure passed by reference. The buffer 'buf' must point
 * to a 128 bit big-endian encoded ID. */
void streamDecodeID(void *buf, streamID *id) {
    uint64_t e[2];
    memcpy(e, buf, sizeof(e));
    id->ms = ntohu64(e[0]);
    id->seq = ntohu64(e[1]);
}

/* Compare two stream IDs. Return -1 if a < b, 0 if a == b, 1 if a > b. */
int streamCompareID(streamID *a, streamID *b) {
    if (a->ms > b->ms)
        return 1;
    else if (a->ms < b->ms)
        return -1;
    /* The ms part is the same. Check the sequence part. */
    else if (a->seq > b->seq)
        return 1;
    else if (a->seq < b->seq)
        return -1;
    /* Everything is the same: IDs are equal. */
    return 0;
}

/* Retrieves the ID of the stream edge entry. An edge is either the first or
 * the last ID in the stream, and may be a tombstone. To filter out tombstones,
 * set the'skip_tombstones' argument to 1. */
void streamGetEdgeID(stream *s, int first, int skip_tombstones, streamID *edge_id) {
    streamIterator si;
    int64_t numfields;
    streamIteratorStart(&si, s, NULL, NULL, !first);
    si.skip_tombstones = skip_tombstones;
    int found = streamIteratorGetID(&si, edge_id, &numfields);
    if (!found) {
        streamID min_id = {0, 0}, max_id = {UINT64_MAX, UINT64_MAX};
        *edge_id = first ? max_id : min_id;
    }
    streamIteratorStop(&si);
}

/* Initialize the stream iterator, so that we can call iterating functions
 * to get the next items. This requires a corresponding streamIteratorStop()
 * at the end. The 'rev' parameter controls the direction. If it's zero the
 * iteration is from the start to the end element (inclusive), otherwise
 * if rev is non-zero, the iteration is reversed.
 *
 * Once the iterator is initialized, we iterate like this:
 *
 *  streamIterator myiterator;
 *  streamIteratorStart(&myiterator,...);
 *  int64_t numfields;
 *  while(streamIteratorGetID(&myiterator,&ID,&numfields)) {
 *      while(numfields--) {
 *          unsigned char *key, *value;
 *          size_t key_len, value_len;
 *          streamIteratorGetField(&myiterator,&key,&value,&key_len,&value_len);
 *
 *          ... do what you want with key and value ...
 *      }
 *  }
 *  streamIteratorStop(&myiterator); */
void streamIteratorStart(streamIterator *si, stream *s, streamID *start, streamID *end, int rev) {
    /* Initialize the iterator and translates the iteration start/stop
     * elements into a 128 big big-endian number. */
    if (start) {
        streamEncodeID(si->start_key, start);
    } else {
        si->start_key[0] = 0;
        si->start_key[1] = 0;
    }

    if (end) {
        streamEncodeID(si->end_key, end);
    } else {
        si->end_key[0] = UINT64_MAX;
        si->end_key[1] = UINT64_MAX;
    }

    /* Seek the correct node in the radix tree. */
    raxStart(&si->ri, s->rax);
    if (!rev) {
        if (start && (start->ms || start->seq)) {
            raxSeek(&si->ri, "<=", (unsigned char *)si->start_key, sizeof(si->start_key));
            if (raxEOF(&si->ri)) raxSeek(&si->ri, "^", NULL, 0);
        } else {
            raxSeek(&si->ri, "^", NULL, 0);
        }
    } else {
        if (end && (end->ms || end->seq)) {
            raxSeek(&si->ri, "<=", (unsigned char *)si->end_key, sizeof(si->end_key));
            if (raxEOF(&si->ri)) raxSeek(&si->ri, "$", NULL, 0);
        } else {
            raxSeek(&si->ri, "$", NULL, 0);
        }
    }
    si->stream = s;
    si->lp = NULL;           /* There is no current listpack right now. */
    si->lp_ele = NULL;       /* Current listpack cursor. */
    si->rev = rev;           /* Direction, if non-zero reversed, from end to start. */
    si->skip_tombstones = 1; /* By default tombstones aren't emitted. */
}

/* Return 1 and store the current item ID at 'id' if there are still
 * elements within the iteration range, otherwise return 0 in order to
 * signal the iteration terminated. */
int streamIteratorGetID(streamIterator *si, streamID *id, int64_t *numfields) {
    while (1) { /* Will stop when element > stop_key or end of radix tree. */
        /* If the current listpack is set to NULL, this is the start of the
         * iteration or the previous listpack was completely iterated.
         * Go to the next node. */
        if (si->lp == NULL || si->lp_ele == NULL) {
            if (!si->rev && !raxNext(&si->ri))
                return 0;
            else if (si->rev && !raxPrev(&si->ri))
                return 0;
            serverAssert(si->ri.key_len == sizeof(streamID));
            /* Get the master ID. */
            streamDecodeID(si->ri.key,&si->master_id);
            /* Get the master fields count. */
            si->lp = si->ri.data;
            si->lp_ele = lpFirst(si->lp);           /* Seek items count */
            si->lp_ele = lpNext(si->lp,si->lp_ele); /* Seek deleted count. */
            si->lp_ele = lpNext(si->lp,si->lp_ele); /* Seek num fields. */
            si->master_fields_count = lpGetInteger(si->lp_ele);
            si->lp_ele = lpNext(si->lp,si->lp_ele); /* Seek first field. */
            si->master_fields_start = si->lp_ele;
            /* We are now pointing to the first field of the master entry.
             * We need to seek either the first or the last entry depending
             * on the direction of the iteration. */
            if (!si->rev) {
                /* If we are iterating in normal order, skip the master fields
                 * to seek the first actual entry. */
                for (uint64_t i = 0; i < si->master_fields_count; i++)
                    si->lp_ele = lpNext(si->lp,si->lp_ele);
            } else {
                /* If we are iterating in reverse direction, just seek the
                 * last part of the last entry in the listpack (that is, the
                 * fields count). */
                si->lp_ele = lpLast(si->lp);
            }
        } else if (si->rev) {
            /* If we are iterating in the reverse order, and this is not
             * the first entry emitted for this listpack, then we already
             * emitted the current entry, and have to go back to the previous
             * one. */
            int64_t lp_count = lpGetInteger(si->lp_ele);
            while (lp_count--) si->lp_ele = lpPrev(si->lp, si->lp_ele);
            /* Seek lp-count of prev entry. */
            si->lp_ele = lpPrev(si->lp, si->lp_ele);
        }

        /* For every radix tree node, iterate the corresponding listpack,
         * returning elements when they are within range. */
        while (1) {
            if (!si->rev) {
                /* If we are going forward, skip the previous entry
                 * lp-count field (or in case of the master entry, the zero
                 * term field) */
                si->lp_ele = lpNext(si->lp,si->lp_ele);
                if (si->lp_ele == NULL) break;
            } else {
                /* If we are going backward, read the number of elements this
                 * entry is composed of, and jump backward N times to seek
                 * its start. */
                int64_t lp_count = lpGetInteger(si->lp_ele);
                if (lp_count == 0) { /* We reached the master entry. */
                    si->lp = NULL;
                    si->lp_ele = NULL;
                    break;
                }
                while(lp_count--) si->lp_ele = lpPrev(si->lp,si->lp_ele);
            }

            /* Get the flags entry. */
            si->lp_flags = si->lp_ele;
            int64_t flags = lpGetInteger(si->lp_ele);
            si->lp_ele = lpNext(si->lp,si->lp_ele); /* Seek ID. */

            /* Get the ID: it is encoded as difference between the master
             * ID and this entry ID. */
            *id = si->master_id;
            id->ms += lpGetInteger(si->lp_ele);
            si->lp_ele = lpNext(si->lp, si->lp_ele);
            id->seq += lpGetInteger(si->lp_ele);
            si->lp_ele = lpNext(si->lp, si->lp_ele);
            unsigned char buf[sizeof(streamID)];
            streamEncodeID(buf, id);

            /* The number of entries is here or not depending on the
             * flags. */
            if (flags & STREAM_ITEM_FLAG_SAMEFIELDS) {
                *numfields = si->master_fields_count;
            } else {
                *numfields = lpGetInteger(si->lp_ele);
                si->lp_ele = lpNext(si->lp, si->lp_ele);
            }
            serverAssert(*numfields >= 0);

            /* If current >= start, and the entry is not marked as
             * deleted or tombstones are included, emit it. */
            if (!si->rev) {
                if (memcmp(buf,si->start_key,sizeof(streamID)) >= 0 &&
                    (!si->skip_tombstones || !(flags & STREAM_ITEM_FLAG_DELETED)))
                {
                    if (memcmp(buf,si->end_key,sizeof(streamID)) > 0)
                        return 0; /* We are already out of range. */
                    si->entry_flags = flags;
                    if (flags & STREAM_ITEM_FLAG_SAMEFIELDS)
                        si->master_fields_ptr = si->master_fields_start;
                    return 1; /* Valid item returned. */
                }
            } else {
                if (memcmp(buf, si->end_key, sizeof(streamID)) <= 0 &&
                    (!si->skip_tombstones || !(flags & STREAM_ITEM_FLAG_DELETED))) {
                    if (memcmp(buf, si->start_key, sizeof(streamID)) < 0) return 0; /* We are already out of range. */
                    si->entry_flags = flags;
                    if (flags & STREAM_ITEM_FLAG_SAMEFIELDS)
                        si->master_fields_ptr = si->master_fields_start;
                    return 1; /* Valid item returned. */
                }
            }

            /* If we do not emit, we have to discard if we are going
             * forward, or seek the previous entry if we are going
             * backward. */
            if (!si->rev) {
                int64_t to_discard = (flags & STREAM_ITEM_FLAG_SAMEFIELDS) ? *numfields : *numfields * 2;
                for (int64_t i = 0; i < to_discard; i++) si->lp_ele = lpNext(si->lp, si->lp_ele);
            } else {
                int64_t prev_times = 4; /* flag + id ms + id seq + one more to
                                           go back to the previous entry "count"
                                           field. */
                /* If the entry was not flagged SAMEFIELD we also read the
                 * number of fields, so go back one more. */
                if (!(flags & STREAM_ITEM_FLAG_SAMEFIELDS)) prev_times++;
                while (prev_times--) si->lp_ele = lpPrev(si->lp, si->lp_ele);
            }
        }

        /* End of listpack reached. Try the next/prev radix tree node. */
    }
}

/* Get the field and value of the current item we are iterating. This should
 * be called immediately after streamIteratorGetID(), and for each field
 * according to the number of fields returned by streamIteratorGetID().
 * The function populates the field and value pointers and the corresponding
 * lengths by reference, that are valid until the next iterator call, assuming
 * no one touches the stream meanwhile. */
void streamIteratorGetField(streamIterator *si, unsigned char **fieldptr, unsigned char **valueptr, int64_t *fieldlen, int64_t *valuelen) {
    if (si->entry_flags & STREAM_ITEM_FLAG_SAMEFIELDS) {
        *fieldptr = lpGet(si->master_fields_ptr,fieldlen,si->field_buf);
        si->master_fields_ptr = lpNext(si->lp,si->master_fields_ptr);
    } else {
        *fieldptr = lpGet(si->lp_ele, fieldlen, si->field_buf);
        si->lp_ele = lpNext(si->lp, si->lp_ele);
    }
    *valueptr = lpGet(si->lp_ele, valuelen, si->value_buf);
    si->lp_ele = lpNext(si->lp, si->lp_ele);
}

/* Remove the current entry from the stream: can be called after the
 * GetID() API or after any GetField() call, however we need to iterate
 * a valid entry while calling this function. Moreover the function
 * requires the entry ID we are currently iterating, that was previously
 * returned by GetID().
 *
 * Note that after calling this function, next calls to GetField() can't
 * be performed: the entry is now deleted. Instead the iterator will
 * automatically re-seek to the next entry, so the caller should continue
 * with GetID(). */

/* Stop the stream iterator. The only cleanup we need is to free the rax
 * iterator, since the stream iterator itself is supposed to be stack
 * allocated. */
void streamIteratorStop(streamIterator *si) {
    raxStop(&si->ri);
}

static int streamIDEqZero(streamID *id) {
    return !(id->ms || id->seq);
}

/* This function returns a value that is the ID's logical read counter, or its
 * distance (the number of entries) from the first entry ever to have been added
 * to the stream.
 *
 * A counter is returned only in one of the following cases:
 * 1. The ID is the same as the stream's last ID. In this case, the returned
 *    is the same as the stream's entries_added counter.
 * 2. The ID equals that of the currently first entry in the stream, and the
 *    stream has no tombstones. The returned value, in this case, is the result
 *    of subtracting the stream's length from its added_entries, incremented by
 *    one.
 * 3. The ID less than the stream's first current entry's ID, and there are no
 *    tombstones. Here the estimated counter is the result of subtracting the
 *    stream's length from its added_entries.
 * 4. The stream's added_entries is zero, meaning that no entries were ever
 *    added.
 *
 * The special return value of ULLONG_MAX signals that the counter's value isn't
 * obtainable. It is returned in these cases:
 * 1. The provided ID, if it even exists, is somewhere between the stream's
 *    current first and last entries' IDs, or in the future.
 * 2. The stream contains one or more tombstones. */
long long streamEstimateDistanceFromFirstEverEntry(stream *s, streamID *id) {
    /* The counter of any ID in an empty, never-before-used stream is 0. */
    if (!s->entries_added) {
        return 0;
    }

    /* In the empty stream, if the ID is smaller or equal to the last ID,
     * it can set to the current added_entries value. */
    if (!s->length && streamCompareID(id, &s->last_id) < 1) {
        return s->entries_added;
    }

    if (!streamIDEqZero(id) && streamCompareID(id, &s->max_deleted_entry_id) < 0) {
        /* The ID is before the last tombstone, so the counter is unknown. */
        return SCG_INVALID_ENTRIES_READ;
    }

    int cmp_last = streamCompareID(id, &s->last_id);
    if (cmp_last == 0) {
        /* Return the exact counter of the last entry in the stream. */
        return s->entries_added;
    } else if (cmp_last > 0) {
        /* The counter of a future ID is unknown. */
        return SCG_INVALID_ENTRIES_READ;
    }

    int cmp_id_first = streamCompareID(id, &s->first_id);
    int cmp_xdel_first = streamCompareID(&s->max_deleted_entry_id, &s->first_id);
    if (streamIDEqZero(&s->max_deleted_entry_id) || cmp_xdel_first < 0) {
        /* There's definitely no fragmentation ahead. */
        if (cmp_id_first < 0) {
            /* Return the estimated counter. */
            return s->entries_added - s->length;
        } else if (cmp_id_first == 0) {
            /* Return the exact counter of the first entry in the stream. */
            return s->entries_added - s->length + 1;
        }
    }

    /* The ID is either before an XDEL that fragments the stream or an arbitrary
     * ID. Either case, so we can't make a prediction. */
    return SCG_INVALID_ENTRIES_READ;
}

/* Send the stream items in the specified range to the client 'c'. The range
 * the client will receive is between start and end inclusive, if 'count' is
 * non zero, no more than 'count' elements are sent.
 *
 * The 'end' pointer can be NULL to mean that we want all the elements from
 * 'start' till the end of the stream. If 'rev' is non zero, elements are
 * produced in reversed order from end to start.
 *
 * The function returns the number of entries emitted.
 *
 * If group and consumer are not NULL, the function performs additional work:
 * 1. It updates the last delivered ID in the group in case we are
 *    sending IDs greater than the current last ID.
 * 2. If the requested IDs are already assigned to some other consumer, the
 *    function will not return it to the client.
 * 3. An entry in the pending list will be created for every entry delivered
 *    for the first time to this consumer.
 * 4. The group's read counter is incremented if it is already valid and there
 *    are no future tombstones, or is invalidated (set to 0) otherwise. If the
 *    counter is invalid to begin with, we try to obtain it for the last
 *    delivered ID.
 *
 * The behavior may be modified passing non-zero flags:
 *
 * STREAM_RWR_NOACK: Do not create PEL entries, that is, the point "3" above
 *                   is not performed.
 * STREAM_RWR_RAWENTRIES: Do not emit array boundaries, but just the entries,
 *                        and return the number of entries emitted as usually.
 *                        This is used when the function is just used in order
 *                        to emit data and there is some higher level logic.
 *
 * The final argument 'spi' (stream propagation info pointer) is a structure
 * filled with information needed to propagate the command execution to AOF
 * and replicas, in the case a consumer group was passed: we need to generate
 * XCLAIM commands to create the pending list into AOF/replicas in that case.
 *
 * If 'spi' is set to NULL no propagation will happen even if the group was
 * given, but currently such a feature is never used by the code base that
 * will always pass 'spi' and propagate when a group is passed.
 *
 * Note that this function is recursive in certain cases. When it's called
 * with a non NULL group and consumer argument, it may call
 * streamReplyWithRangeFromConsumerPEL() in order to get entries from the
 * consumer pending entries list. However such a function will then call
 * streamReplyWithRange() in order to emit single entries (found in the
 * PEL by ID) to the client. This is the use case for the STREAM_RWR_RAWENTRIES
 * flag.
 */
#define STREAM_RWR_NOACK (1 << 0) /* Do not create entries in the PEL. */
#define STREAM_RWR_RAWENTRIES                                         \
    (1 << 1)                        /* Do not emit protocol for array \
                                       boundaries, just the entries. */
#define STREAM_RWR_HISTORY (1 << 2) /* Only serve consumer local PEL. */


/* -----------------------------------------------------------------------
 * Low level implementation of consumer groups
 * ----------------------------------------------------------------------- */


/* Free a NACK entry. */
void streamFreeNACK(streamNACK *na) {
    zfree(na);
}

/* Free a consumer and associated data structures. Note that this function
 * will not reassign the pending messages associated with this consumer
 * nor will delete them from the stream, so when this function is called
 * to delete a consumer, and not when the whole stream is destroyed, the caller
 * should do some work before. */
static void streamFreeConsumer(streamConsumer *sc) {
    raxFree(sc->pel); /* No value free callback: the PEL entries are shared
                         between the consumer and the main stream PEL. */
    sdsfree(sc->name);
    zfree(sc);
}

/* Used for generic free functions. */
static void streamFreeConsumerVoid(void *sc) {
    streamFreeConsumer((streamConsumer *)sc);
}

/* Create a new consumer group in the context of the stream 's', having the
 * specified name, last server ID and reads counter. If a consumer group with
 * the same name already exists NULL is returned, otherwise the pointer to the
 * consumer group is returned. */
streamCG *streamCreateCG(stream *s, const char *name, size_t namelen, streamID *id, long long entries_read) {
    if (s->cgroups == NULL) s->cgroups = raxNew();
    if (raxFind(s->cgroups, (unsigned char *)name, namelen, NULL)) return NULL;

    streamCG *cg = zmalloc(sizeof(*cg));
    cg->pel = raxNew();
    cg->consumers = raxNew();
    cg->last_id = *id;
    cg->entries_read = entries_read;
    raxInsert(s->cgroups, (unsigned char *)name, namelen, cg, NULL);
    return cg;
}

/* Used for generic free functions. */
static void streamFreeCGVoid(void *cg_) {
    streamCG *cg = (streamCG *)cg_;
    raxFreeWithCallback(cg->pel, zfree);
    raxFreeWithCallback(cg->consumers, streamFreeConsumerVoid);
    zfree(cg);
}

