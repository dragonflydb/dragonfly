/*
 * Copyright (c) 2017, Salvatore Sanfilippo <antirez at gmail dot com>
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
#include <string.h>

#include "endianconv.h"
#include "stream.h"
#include "redis_aux.h"
#include "zmalloc.h"


/* Every stream item inside the listpack, has a flags field that is used to
 * mark the entry as deleted, or having the same field as the "master"
 * entry at the start of the listpack> */
#define STREAM_ITEM_FLAG_NONE 0             /* No special flags. */
#define STREAM_ITEM_FLAG_DELETED (1<<0)     /* Entry is deleted. Skip it. */
#define STREAM_ITEM_FLAG_SAMEFIELDS (1<<1)  /* Same fields as master entry. */

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
#define STREAM_LISTPACK_MAX_SIZE (1<<30)

void streamFreeCG(streamCG *cg);
void streamFreeNACK(streamNACK *na);

/* -----------------------------------------------------------------------
 * Low level stream encoding: a radix tree of listpacks.
 * ----------------------------------------------------------------------- */

/* Create a new stream data structure. */
stream *streamNew(void) {
    stream *s = zmalloc(sizeof(*s));
    s->rax_tree = raxNew();
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

/* Free a stream, including the listpacks stored inside the radix tree. */
void freeStream(stream *s) {
    raxFreeWithCallback(s->rax_tree,(void(*)(void*))lpFree);
    if (s->cgroups)
        raxFreeWithCallback(s->cgroups,(void(*)(void*))streamFreeCG);
    zfree(s);
}

/* Set 'id' to be its successor stream ID.
 * If 'id' is the maximal possible id, it is wrapped around to 0-0 and a
 * C_ERR is returned. */
int streamIncrID(streamID *id) {
    int ret = C_OK;
    if (id->seq == UINT64_MAX) {
        if (id->ms == UINT64_MAX) {
            /* Special case where 'id' is the last possible streamID... */
            id->ms = id->seq = 0;
            ret = C_ERR;
        } else {
            id->ms++;
            id->seq = 0;
        }
    } else {
        id->seq++;
    }
    return ret;
}

/* Set 'id' to be its predecessor stream ID.
 * If 'id' is the minimal possible id, it remains 0-0 and a C_ERR is
 * returned. */
int streamDecrID(streamID *id) {
    int ret = C_OK;
    if (id->seq == 0) {
        if (id->ms == 0) {
            /* Special case where 'id' is the first possible streamID... */
            id->ms = id->seq = UINT64_MAX;
            ret = C_ERR;
        } else {
            id->ms--;
            id->seq = UINT64_MAX;
        }
    } else {
        id->seq--;
    }
    return ret;
}

/* This is a wrapper function for lpGet() to directly get an integer value
 * from the listpack (that may store numbers as a string), converting
 * the string if needed.
 * The 'valid" argument is an optional output parameter to get an indication
 * if the record was valid, when this parameter is NULL, the function will
 * fail with an assertion. */
static inline int64_t lpGetIntegerIfValid(unsigned char *ele, int *valid) {
    int64_t v;
    unsigned char *e = lpGet(ele,&v,NULL);
    if (e == NULL) {
        if (valid)
            *valid = 1;
        return v;
    }
    /* The following code path should never be used for how listpacks work:
     * they should always be able to store an int64_t value in integer
     * encoded form. However the implementation may change. */
    long long ll;
    int ret = string2ll((char*)e,v,&ll);
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
int lpGetEdgeStreamID(unsigned char *lp, int first, streamID *master_id, streamID *edge_id)
{
   if (lp == NULL)
       return 0;

   unsigned char *lp_ele;

   /* We need to seek either the first or the last entry depending
    * on the direction of the iteration. */
   if (first) {
       /* Get the master fields count. */
       lp_ele = lpFirst(lp);        /* Seek items count */
       lp_ele = lpNext(lp, lp_ele); /* Seek deleted count. */
       lp_ele = lpNext(lp, lp_ele); /* Seek num fields. */
       int64_t master_fields_count = lpGetInteger(lp_ele);
       lp_ele = lpNext(lp, lp_ele); /* Seek first field. */

       /* If we are iterating in normal order, skip the master fields
        * to seek the first actual entry. */
       for (int64_t i = 0; i < master_fields_count; i++)
           lp_ele = lpNext(lp, lp_ele);

       /* If we are going forward, skip the previous entry's
        * lp-count field (or in case of the master entry, the zero
        * term field) */
       lp_ele = lpNext(lp, lp_ele);
       if (lp_ele == NULL)
           return 0;
   } else {
       /* If we are iterating in reverse direction, just seek the
        * last part of the last entry in the listpack (that is, the
        * fields count). */
       lp_ele = lpLast(lp);

       /* If we are going backward, read the number of elements this
        * entry is composed of, and jump backward N times to seek
        * its start. */
       int64_t lp_count = lpGetInteger(lp_ele);
       if (lp_count == 0) /* We reached the master entry. */
           return 0;

       while (lp_count--)
           lp_ele = lpPrev(lp, lp_ele);
   }

   lp_ele = lpNext(lp, lp_ele); /* Seek ID (lp_ele currently points to 'flags'). */

   /* Get the ID: it is encoded as difference between the master
    * ID and this entry ID. */
   streamID id = *master_id;
   id.ms += lpGetInteger(lp_ele);
   lp_ele = lpNext(lp, lp_ele);
   id.seq += lpGetInteger(lp_ele);
   *edge_id = id;
   return 1;
}

/* Debugging function to log the full content of a listpack. Useful
 * for development and debugging. */
void streamLogListpackContent(unsigned char *lp) {
    unsigned char *p = lpFirst(lp);
    while(p) {
        unsigned char buf[LP_INTBUF_SIZE];
        int64_t v;
        unsigned char *ele = lpGet(p,&v,buf);
        serverLog(LL_WARNING,"- [%d] '%.*s'", (int)v, (int)v, ele);
        p = lpNext(lp,p);
    }
}

/* Convert the specified stream entry ID as a 128 bit big endian number, so
 * that the IDs can be sorted lexicographically. */
void streamEncodeID(void *buf, streamID *id) {
    uint64_t e[2];
    e[0] = htonu64(id->ms);
    e[1] = htonu64(id->seq);
    memcpy(buf,e,sizeof(e));
}

/* This is the reverse of streamEncodeID(): the decoded ID will be stored
 * in the 'id' structure passed by reference. The buffer 'buf' must point
 * to a 128 bit big-endian encoded ID. */
void streamDecodeID(void *buf, streamID *id) {
    uint64_t e[2];
    memcpy(e,buf,sizeof(e));
    id->ms = ntohu64(e[0]);
    id->seq = ntohu64(e[1]);
}

/* Compare two stream IDs. Return -1 if a < b, 0 if a == b, 1 if a > b. */
int streamCompareID(streamID *a, streamID *b) {
    if (a->ms > b->ms) return 1;
    else if (a->ms < b->ms) return -1;
    /* The ms part is the same. Check the sequence part. */
    else if (a->seq > b->seq) return 1;
    else if (a->seq < b->seq) return -1;
    /* Everything is the same: IDs are equal. */
    return 0;
}

/* Retrieves the ID of the stream edge entry. An edge is either the first or
 * the last ID in the stream, and may be a tombstone. To filter out tombstones,
 * set the'skip_tombstones' argument to 1. */
void streamGetEdgeID(stream *s, int first, int skip_tombstones, streamID *edge_id)
{
    streamIterator si;
    int64_t numfields;
    streamIteratorStart(&si,s,NULL,NULL,!first);
    si.skip_tombstones = skip_tombstones;
    int found = streamIteratorGetID(&si,edge_id,&numfields);
    if (!found) {
        streamID min_id = {0, 0}, max_id = {UINT64_MAX, UINT64_MAX};
        *edge_id = first ? max_id : min_id;
    }
    streamIteratorStop(&si);
}

/* Trim the stream 's' according to args->trim_strategy, and return the
 * number of elements removed from the stream. The 'approx' option, if non-zero,
 * specifies that the trimming must be performed in a approximated way in
 * order to maximize performances. This means that the stream may contain
 * entries with IDs < 'id' in case of MINID (or more elements than 'maxlen'
 * in case of MAXLEN), and elements are only removed if we can remove
 * a *whole* node of the radix tree. The elements are removed from the head
 * of the stream (older elements).
 *
 * The function may return zero if:
 *
 * 1) The minimal entry ID of the stream is already < 'id' (MINID); or
 * 2) The stream is already shorter or equal to the specified max length (MAXLEN); or
 * 3) The 'approx' option is true and the head node did not have enough elements
 *    to be deleted.
 *
 * args->limit is the maximum number of entries to delete. The purpose is to
 * prevent this function from taking to long.
 * If 'limit' is 0 then we do not limit the number of deleted entries.
 * Much like the 'approx', if 'limit' is smaller than the number of entries
 * that should be trimmed, there is a chance we will still have entries with
 * IDs < 'id' (or number of elements >= maxlen in case of MAXLEN).
 */
int64_t streamTrim(stream *s, streamAddTrimArgs *args) {
    size_t maxlen = args->maxlen;
    streamID *id = &args->minid;
    int approx = args->approx_trim;
    int64_t limit = args->limit;
    int trim_strategy = args->trim_strategy;

    if (trim_strategy == TRIM_STRATEGY_NONE)
        return 0;

    raxIterator ri;
    raxStart(&ri,s->rax_tree);
    raxSeek(&ri,"^",NULL,0);

    int64_t deleted = 0;
    while (raxNext(&ri)) {
        if (trim_strategy == TRIM_STRATEGY_MAXLEN && s->length <= maxlen)
            break;

        unsigned char *lp = ri.data, *p = lpFirst(lp);
        int64_t entries = lpGetInteger(p);

        /* Check if we exceeded the amount of work we could do */
        if (limit && (deleted + entries) > limit)
            break;

        /* Check if we can remove the whole node. */
        int remove_node;
        streamID master_id = {0}; /* For MINID */
        if (trim_strategy == TRIM_STRATEGY_MAXLEN) {
            remove_node = s->length - entries >= maxlen;
        } else {
            /* Read the master ID from the radix tree key. */
            streamDecodeID(ri.key, &master_id);

            /* Read last ID. */
            streamID last_id = {0, 0};
            lpGetEdgeStreamID(lp, 0, &master_id, &last_id);

            /* We can remove the entire node id its last ID < 'id' */
            remove_node = streamCompareID(&last_id, id) < 0;
        }

        if (remove_node) {
            lpFree(lp);
            raxRemove(s->rax_tree,ri.key,ri.key_len,NULL);
            raxSeek(&ri,">=",ri.key,ri.key_len);
            s->length -= entries;
            deleted += entries;
            continue;
        }

        /* If we cannot remove a whole element, and approx is true,
         * stop here. */
        if (approx) break;

        /* Now we have to trim entries from within 'lp' */
        int64_t deleted_from_lp = 0;

        p = lpNext(lp, p); /* Skip deleted field. */
        p = lpNext(lp, p); /* Skip num-of-fields in the master entry. */

        /* Skip all the master fields. */
        int64_t master_fields_count = lpGetInteger(p);
        p = lpNext(lp,p); /* Skip the first field. */
        for (int64_t j = 0; j < master_fields_count; j++)
            p = lpNext(lp,p); /* Skip all master fields. */
        p = lpNext(lp,p); /* Skip the zero master entry terminator. */

        /* 'p' is now pointing to the first entry inside the listpack.
         * We have to run entry after entry, marking entries as deleted
         * if they are already not deleted. */
        while (p) {
            /* We keep a copy of p (which point to flags part) in order to
             * update it after (and if) we actually remove the entry */
            unsigned char *pcopy = p;

            int64_t flags = lpGetInteger(p);
            p = lpNext(lp, p); /* Skip flags. */
            int64_t to_skip;

            int64_t ms_delta = lpGetInteger(p);
            p = lpNext(lp, p); /* Skip ID ms delta */
            int64_t seq_delta = lpGetInteger(p);
            p = lpNext(lp, p); /* Skip ID seq delta */

            streamID currid = {0}; /* For MINID */
            if (trim_strategy == TRIM_STRATEGY_MINID) {
                currid.ms = master_id.ms + ms_delta;
                currid.seq = master_id.seq + seq_delta;
            }

            int stop;
            if (trim_strategy == TRIM_STRATEGY_MAXLEN) {
                stop = s->length <= maxlen;
            } else {
                /* Following IDs will definitely be greater because the rax
                 * tree is sorted, no point of continuing. */
                stop = streamCompareID(&currid, id) >= 0;
            }
            if (stop)
                break;

            if (flags & STREAM_ITEM_FLAG_SAMEFIELDS) {
                to_skip = master_fields_count;
            } else {
                to_skip = lpGetInteger(p); /* Get num-fields. */
                p = lpNext(lp,p); /* Skip num-fields. */
                to_skip *= 2; /* Fields and values. */
            }

            while(to_skip--) p = lpNext(lp,p); /* Skip the whole entry. */
            p = lpNext(lp,p); /* Skip the final lp-count field. */

            /* Mark the entry as deleted. */
            if (!(flags & STREAM_ITEM_FLAG_DELETED)) {
                intptr_t delta = p - lp;
                flags |= STREAM_ITEM_FLAG_DELETED;
                lp = lpReplaceInteger(lp, &pcopy, flags);
                deleted_from_lp++;
                s->length--;
                p = lp + delta;
            }
        }
        deleted += deleted_from_lp;

        /* Now we update the entries/deleted counters. */
        p = lpFirst(lp);
        lp = lpReplaceInteger(lp,&p,entries-deleted_from_lp);
        p = lpNext(lp,p); /* Skip deleted field. */
        int64_t marked_deleted = lpGetInteger(p);
        lp = lpReplaceInteger(lp,&p,marked_deleted+deleted_from_lp);
        p = lpNext(lp,p); /* Skip num-of-fields in the master entry. */

        /* Here we should perform garbage collection in case at this point
         * there are too many entries deleted inside the listpack. */
        entries -= deleted_from_lp;
        marked_deleted += deleted_from_lp;
        if (entries + marked_deleted > 10 && marked_deleted > entries/2) {
            /* TODO: perform a garbage collection. */
        }

        /* Update the listpack with the new pointer. */
        raxInsert(s->rax_tree,ri.key,ri.key_len,lp,NULL);

        break; /* If we are here, there was enough to delete in the current
                  node, so no need to go to the next node. */
    }
    raxStop(&ri);

    /* Update the stream's first ID after the trimming. */
    if (s->length == 0) {
        s->first_id.ms = 0;
        s->first_id.seq = 0;
    } else if (deleted) {
        streamGetEdgeID(s,1,1,&s->first_id);
    }

    return deleted;
}

/* Trims a stream by length. Returns the number of deleted items. */
int64_t streamTrimByLength(stream *s, long long maxlen, int approx) {
    streamAddTrimArgs args = {
        .trim_strategy = TRIM_STRATEGY_MAXLEN,
        .approx_trim = approx,
        .limit = approx ? 100 * server.stream_node_max_entries : 0,
        .maxlen = maxlen
    };
    return streamTrim(s, &args);
}

/* Trims a stream by minimum ID. Returns the number of deleted items. */
int64_t streamTrimByID(stream *s, streamID minid, int approx) {
    streamAddTrimArgs args = {
        .trim_strategy = TRIM_STRATEGY_MINID,
        .approx_trim = approx,
        .limit = approx ? 100 * server.stream_node_max_entries : 0,
        .minid = minid
    };
    return streamTrim(s, &args);
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
        streamEncodeID(si->start_key,start);
    } else {
        si->start_key[0] = 0;
        si->start_key[1] = 0;
    }

    if (end) {
        streamEncodeID(si->end_key,end);
    } else {
        si->end_key[0] = UINT64_MAX;
        si->end_key[1] = UINT64_MAX;
    }

    /* Seek the correct node in the radix tree. */
    raxStart(&si->ri,s->rax_tree);
    if (!rev) {
        if (start && (start->ms || start->seq)) {
            raxSeek(&si->ri,"<=",(unsigned char*)si->start_key,
                    sizeof(si->start_key));
            if (raxEOF(&si->ri)) raxSeek(&si->ri,"^",NULL,0);
        } else {
            raxSeek(&si->ri,"^",NULL,0);
        }
    } else {
        if (end && (end->ms || end->seq)) {
            raxSeek(&si->ri,"<=",(unsigned char*)si->end_key,
                    sizeof(si->end_key));
            if (raxEOF(&si->ri)) raxSeek(&si->ri,"$",NULL,0);
        } else {
            raxSeek(&si->ri,"$",NULL,0);
        }
    }
    si->stream = s;
    si->lp = NULL;     /* There is no current listpack right now. */
    si->lp_ele = NULL; /* Current listpack cursor. */
    si->rev = rev;     /* Direction, if non-zero reversed, from end to start. */
    si->skip_tombstones = 1;    /* By default tombstones aren't emitted. */
}

/* Return 1 and store the current item ID at 'id' if there are still
 * elements within the iteration range, otherwise return 0 in order to
 * signal the iteration terminated. */
int streamIteratorGetID(streamIterator *si, streamID *id, int64_t *numfields) {
    while(1) { /* Will stop when element > stop_key or end of radix tree. */
        /* If the current listpack is set to NULL, this is the start of the
         * iteration or the previous listpack was completely iterated.
         * Go to the next node. */
        if (si->lp == NULL || si->lp_ele == NULL) {
            if (!si->rev && !raxNext(&si->ri)) return 0;
            else if (si->rev && !raxPrev(&si->ri)) return 0;
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
            while(lp_count--) si->lp_ele = lpPrev(si->lp,si->lp_ele);
            /* Seek lp-count of prev entry. */
            si->lp_ele = lpPrev(si->lp,si->lp_ele);
        }

        /* For every radix tree node, iterate the corresponding listpack,
         * returning elements when they are within range. */
        while(1) {
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
            si->lp_ele = lpNext(si->lp,si->lp_ele);
            id->seq += lpGetInteger(si->lp_ele);
            si->lp_ele = lpNext(si->lp,si->lp_ele);
            unsigned char buf[sizeof(streamID)];
            streamEncodeID(buf,id);

            /* The number of entries is here or not depending on the
             * flags. */
            if (flags & STREAM_ITEM_FLAG_SAMEFIELDS) {
                *numfields = si->master_fields_count;
            } else {
                *numfields = lpGetInteger(si->lp_ele);
                si->lp_ele = lpNext(si->lp,si->lp_ele);
            }
            serverAssert(*numfields>=0);

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
                if (memcmp(buf,si->end_key,sizeof(streamID)) <= 0 &&
                    (!si->skip_tombstones || !(flags & STREAM_ITEM_FLAG_DELETED)))
                {
                    if (memcmp(buf,si->start_key,sizeof(streamID)) < 0)
                        return 0; /* We are already out of range. */
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
                int64_t to_discard = (flags & STREAM_ITEM_FLAG_SAMEFIELDS) ?
                                      *numfields : *numfields*2;
                for (int64_t i = 0; i < to_discard; i++)
                    si->lp_ele = lpNext(si->lp,si->lp_ele);
            } else {
                int64_t prev_times = 4; /* flag + id ms + id seq + one more to
                                           go back to the previous entry "count"
                                           field. */
                /* If the entry was not flagged SAMEFIELD we also read the
                 * number of fields, so go back one more. */
                if (!(flags & STREAM_ITEM_FLAG_SAMEFIELDS)) prev_times++;
                while(prev_times--) si->lp_ele = lpPrev(si->lp,si->lp_ele);
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
        *fieldptr = lpGet(si->lp_ele,fieldlen,si->field_buf);
        si->lp_ele = lpNext(si->lp,si->lp_ele);
    }
    *valueptr = lpGet(si->lp_ele,valuelen,si->value_buf);
    si->lp_ele = lpNext(si->lp,si->lp_ele);
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
void streamIteratorRemoveEntry(streamIterator *si, streamID *current) {
    unsigned char *lp = si->lp;
    int64_t aux;

    /* We do not really delete the entry here. Instead we mark it as
     * deleted by flagging it, and also incrementing the count of the
     * deleted entries in the listpack header.
     *
     * We start flagging: */
    int64_t flags = lpGetInteger(si->lp_flags);
    flags |= STREAM_ITEM_FLAG_DELETED;
    lp = lpReplaceInteger(lp,&si->lp_flags,flags);

    /* Change the valid/deleted entries count in the master entry. */
    unsigned char *p = lpFirst(lp);
    aux = lpGetInteger(p);

    if (aux == 1) {
        /* If this is the last element in the listpack, we can remove the whole
         * node. */
        lpFree(lp);
        raxRemove(si->stream->rax_tree,si->ri.key,si->ri.key_len,NULL);
    } else {
        /* In the base case we alter the counters of valid/deleted entries. */
        lp = lpReplaceInteger(lp,&p,aux-1);
        p = lpNext(lp,p); /* Seek deleted field. */
        aux = lpGetInteger(p);
        lp = lpReplaceInteger(lp,&p,aux+1);

        /* Update the listpack with the new pointer. */
        if (si->lp != lp)
            raxInsert(si->stream->rax_tree,si->ri.key,si->ri.key_len,lp,NULL);
    }

    /* Update the number of entries counter. */
    si->stream->length--;

    /* Re-seek the iterator to fix the now messed up state. */
    streamID start, end;
    if (si->rev) {
        streamDecodeID(si->start_key,&start);
        end = *current;
    } else {
        start = *current;
        streamDecodeID(si->end_key,&end);
    }
    streamIteratorStop(si);
    streamIteratorStart(si,si->stream,&start,&end,si->rev);

    /* TODO: perform a garbage collection here if the ratio between
     * deleted and valid goes over a certain limit. */
}

/* Stop the stream iterator. The only cleanup we need is to free the rax
 * iterator, since the stream iterator itself is supposed to be stack
 * allocated. */
void streamIteratorStop(streamIterator *si) {
    raxStop(&si->ri);
}

/* Return 1 if `id` exists in `s` (and not marked as deleted) */
int streamEntryExists(stream *s, streamID *id) {
    streamIterator si;
    streamIteratorStart(&si,s,id,id,0);
    streamID myid;
    int64_t numfields;
    int found = streamIteratorGetID(&si,&myid,&numfields);
    streamIteratorStop(&si);
    if (!found)
        return 0;
    serverAssert(streamCompareID(id,&myid) == 0);
    return 1;
}

/* Delete the specified item ID from the stream, returning 1 if the item
 * was deleted 0 otherwise (if it does not exist). */
int streamDeleteItem(stream *s, streamID *id) {
    int deleted = 0;
    streamIterator si;
    streamIteratorStart(&si,s,id,id,0);
    streamID myid;
    int64_t numfields;
    if (streamIteratorGetID(&si,&myid,&numfields)) {
        streamIteratorRemoveEntry(&si,&myid);
        deleted = 1;
    }
    streamIteratorStop(&si);
    return deleted;
}

/* Get the last valid (non-tombstone) streamID of 's'. */
void streamLastValidID(stream *s, streamID *maxid)
{
    streamIterator si;
    streamIteratorStart(&si,s,NULL,NULL,1);
    int64_t numfields;
    if (!streamIteratorGetID(&si,maxid,&numfields) && s->length)
        serverPanic("Corrupt stream, length is %llu, but no max id", (unsigned long long)s->length);
    streamIteratorStop(&si);
}


/* Returns non-zero if the ID is 0-0. */
int streamIDEqZero(streamID *id) {
    return !(id->ms || id->seq);
}

/* A helper that returns non-zero if the range from 'start' to `end`
 * contains a tombstone.
 *
 * NOTE: this assumes that the caller had verified that 'start' is less than
 * 's->last_id'. */
int streamRangeHasTombstones(stream *s, streamID *start, streamID *end) {
    streamID start_id, end_id;

    if (!s->length || streamIDEqZero(&s->max_deleted_entry_id)) {
        /* The stream is empty or has no tombstones. */
        return 0;
    }

    if (streamCompareID(&s->first_id,&s->max_deleted_entry_id) > 0) {
        /* The latest tombstone is before the first entry. */
        return 0;
    }

    if (start) {
        start_id = *start;
    } else {
        start_id.ms = 0;
        start_id.seq = 0;
    }

    if (end) {
        end_id = *end;
    } else {
        end_id.ms = UINT64_MAX;
        end_id.seq = UINT64_MAX;
    }

    if (streamCompareID(&start_id,&s->max_deleted_entry_id) <= 0 &&
        streamCompareID(&s->max_deleted_entry_id,&end_id) <= 0)
    {
        /* start_id <= max_deleted_entry_id <= end_id: The range does include a tombstone. */
        return 1;
    }

    /* The range doesn't includes a tombstone. */
    return 0;
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
    if (!s->length && streamCompareID(id,&s->last_id) < 1) {
        return s->entries_added;
    }

    int cmp_last = streamCompareID(id,&s->last_id);
    if (cmp_last == 0) {
        /* Return the exact counter of the last entry in the stream. */
        return s->entries_added;
    } else if (cmp_last > 0) {
        /* The counter of a future ID is unknown. */
        return SCG_INVALID_ENTRIES_READ;
    }

    int cmp_id_first = streamCompareID(id,&s->first_id);
    int cmp_xdel_first = streamCompareID(&s->max_deleted_entry_id,&s->first_id);
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

long long streamCGLag(stream *s, streamCG *cg) {
    int valid = 0;
    long long lag = 0;

    if (!s->entries_added) {
        /* The lag of a newly-initialized stream is 0. */
        lag = 0;
        valid = 1;
    } else if (cg->entries_read != SCG_INVALID_ENTRIES_READ && !streamRangeHasTombstones(s,&cg->last_id,NULL)) {
        /* No fragmentation ahead means that the group's logical reads counter
         * is valid for performing the lag calculation. */
        lag = (long long)s->entries_added - cg->entries_read;
        valid = 1;
    } else {
        /* Attempt to retrieve the group's last ID logical read counter. */
        long long entries_read = streamEstimateDistanceFromFirstEverEntry(s,&cg->last_id);
        if (entries_read != SCG_INVALID_ENTRIES_READ) {
            /* A valid counter was obtained. */
            lag = (long long)s->entries_added - entries_read;
            valid = 1;
        }
    }

    if (valid) {
        return lag;
    }
    return SCG_INVALID_LAG;
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
 * and slaves, in the case a consumer group was passed: we need to generate
 * XCLAIM commands to create the pending list into AOF/slaves in that case.
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
#define STREAM_RWR_NOACK (1<<0)         /* Do not create entries in the PEL. */
#define STREAM_RWR_RAWENTRIES (1<<1)    /* Do not emit protocol for array
                                           boundaries, just the entries. */
#define STREAM_RWR_HISTORY (1<<2)       /* Only serve consumer local PEL. */


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
void streamFreeConsumer(streamConsumer *sc) {
    raxFree(sc->pel); /* No value free callback: the PEL entries are shared
                         between the consumer and the main stream PEL. */
    sdsfree(sc->name);
    zfree(sc);
}

/* Create a new consumer group in the context of the stream 's', having the
 * specified name, last server ID and reads counter. If a consumer group with
 * the same name already exists NULL is returned, otherwise the pointer to the
 * consumer group is returned. */
streamCG *streamCreateCG(stream *s, const char *name, size_t namelen, streamID *id, long long entries_read) {
    if (s->cgroups == NULL) s->cgroups = raxNew();
    if (raxFind(s->cgroups,(unsigned char*)name,namelen) != raxNotFound)
        return NULL;

    streamCG *cg = zmalloc(sizeof(*cg));
    cg->pel = raxNew();
    cg->consumers = raxNew();
    cg->last_id = *id;
    cg->entries_read = entries_read;
    raxInsert(s->cgroups,(unsigned char*)name,namelen,cg,NULL);
    return cg;
}

/* Free a consumer group and all its associated data. */
void streamFreeCG(streamCG *cg) {
    raxFreeWithCallback(cg->pel,(void(*)(void*))streamFreeNACK);
    raxFreeWithCallback(cg->consumers,(void(*)(void*))streamFreeConsumer);
    zfree(cg);
}

/* Lookup the consumer group in the specified stream and returns its
 * pointer, otherwise if there is no such group, NULL is returned. */
streamCG *streamLookupCG(stream *s, sds groupname) {
    if (s->cgroups == NULL) return NULL;
    streamCG *cg = raxFind(s->cgroups,(unsigned char*)groupname,
                           sdslen(groupname));
    return (cg == raxNotFound) ? NULL : cg;
}

/* Lookup the consumer with the specified name in the group 'cg' */
streamConsumer *streamLookupConsumer(streamCG *cg, sds name) {
    if (cg == NULL) return NULL;
    streamConsumer *consumer = raxFind(cg->consumers,(unsigned char*)name,
                                       sdslen(name));
    if (consumer == raxNotFound) return NULL;
    return consumer;
}

/* Delete the consumer specified in the consumer group 'cg'. */
void streamDelConsumer(streamCG *cg, streamConsumer *consumer) {
    /* Iterate all the consumer pending messages, deleting every corresponding
     * entry from the global entry. */
    raxIterator ri;
    raxStart(&ri,consumer->pel);
    raxSeek(&ri,"^",NULL,0);
    while(raxNext(&ri)) {
        streamNACK *nack = ri.data;
        raxRemove(cg->pel,ri.key,ri.key_len,NULL);
        streamFreeNACK(nack);
    }
    raxStop(&ri);

    /* Deallocate the consumer. */
    raxRemove(cg->consumers,(unsigned char*)consumer->name,
              sdslen(consumer->name),NULL);
    streamFreeConsumer(consumer);
}


/* Validate the integrity stream listpack entries structure. Both in term of a
 * valid listpack, but also that the structure of the entries matches a valid
 * stream. return 1 if valid 0 if not valid. */
int streamValidateListpackIntegrity(unsigned char *lp, size_t size, int deep) {
    int valid_record;
    unsigned char *p, *next;

    /* Since we don't want to run validation of all records twice, we'll
     * run the listpack validation of just the header and do the rest here. */
    if (!lpValidateIntegrity(lp, size, 0, NULL, NULL))
        return 0;

    /* In non-deep mode we just validated the listpack header (encoded size) */
    if (!deep) return 1;

    next = p = lpValidateFirst(lp);
    if (!lpValidateNext(lp, &next, size)) return 0;
    if (!p) return 0;

    /* entry count */
    int64_t entry_count = lpGetIntegerIfValid(p, &valid_record);
    if (!valid_record) return 0;
    p = next; if (!lpValidateNext(lp, &next, size)) return 0;

    /* deleted */
    int64_t deleted_count = lpGetIntegerIfValid(p, &valid_record);
    if (!valid_record) return 0;
    p = next; if (!lpValidateNext(lp, &next, size)) return 0;

    /* num-of-fields */
    int64_t master_fields = lpGetIntegerIfValid(p, &valid_record);
    if (!valid_record) return 0;
    p = next; if (!lpValidateNext(lp, &next, size)) return 0;

    /* the field names */
    for (int64_t j = 0; j < master_fields; j++) {
        p = next; if (!lpValidateNext(lp, &next, size)) return 0;
    }

    /* the zero master entry terminator. */
    int64_t zero = lpGetIntegerIfValid(p, &valid_record);
    if (!valid_record || zero != 0) return 0;
    p = next; if (!lpValidateNext(lp, &next, size)) return 0;

    entry_count += deleted_count;
    while (entry_count--) {
        if (!p) return 0;
        int64_t fields = master_fields, extra_fields = 3;
        int64_t flags = lpGetIntegerIfValid(p, &valid_record);
        if (!valid_record) return 0;
        p = next; if (!lpValidateNext(lp, &next, size)) return 0;

        /* entry id */
        lpGetIntegerIfValid(p, &valid_record);
        if (!valid_record) return 0;
        p = next; if (!lpValidateNext(lp, &next, size)) return 0;
        lpGetIntegerIfValid(p, &valid_record);
        if (!valid_record) return 0;
        p = next; if (!lpValidateNext(lp, &next, size)) return 0;

        if (!(flags & STREAM_ITEM_FLAG_SAMEFIELDS)) {
            /* num-of-fields */
            fields = lpGetIntegerIfValid(p, &valid_record);
            if (!valid_record) return 0;
            p = next; if (!lpValidateNext(lp, &next, size)) return 0;

            /* the field names */
            for (int64_t j = 0; j < fields; j++) {
                p = next; if (!lpValidateNext(lp, &next, size)) return 0;
            }

            extra_fields += fields + 1;
        }

        /* the values */
        for (int64_t j = 0; j < fields; j++) {
            p = next; if (!lpValidateNext(lp, &next, size)) return 0;
        }

        /* lp-count */
        int64_t lp_count = lpGetIntegerIfValid(p, &valid_record);
        if (!valid_record) return 0;
        if (lp_count != fields + extra_fields) return 0;
        p = next; if (!lpValidateNext(lp, &next, size)) return 0;
    }

    if (next)
        return 0;

    return 1;
}

