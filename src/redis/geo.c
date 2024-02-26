/*
 * Copyright (c) 2014, Matt Stancliff <matt@genges.com>.
 * Copyright (c) 2015-2016, Salvatore Sanfilippo <antirez@gmail.com>.
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

#include <string.h>

#include "geo.h"
#include "geohash_helper.h"
#include "listpack.h"
#include "util.h"
#include "zmalloc.h"
#include "sds.h"


// D - noop
#define D(...) while (0)

/* ====================================================================
 * This file implements the following commands:
 *
 *   - geoadd - add coordinates for value to geoset
 *   - georadius - search radius by coordinates in geoset
 *   - georadiusbymember - search radius based on geoset member position
 * ==================================================================== */

/* ====================================================================
 * geoArray implementation
 * ==================================================================== */

/* Create a new array of geoPoints. */
geoArray *geoArrayCreate(void) {
    geoArray *ga = zmalloc(sizeof(*ga));
    /* It gets allocated on first geoArrayAppend() call. */
    ga->array = NULL;
    ga->buckets = 0;
    ga->used = 0;
    return ga;
}

/* Add and populate with data a new entry to the geoArray. */
geoPoint *geoArrayAppend(geoArray *ga, double *xy, double dist,
                         double score, char *member)
{
    if (ga->used == ga->buckets) {
        ga->buckets = (ga->buckets == 0) ? 8 : ga->buckets*2;
        ga->array = zrealloc(ga->array,sizeof(geoPoint)*ga->buckets);
    }
    geoPoint *gp = ga->array+ga->used;
    gp->longitude = xy[0];
    gp->latitude = xy[1];
    gp->dist = dist;
    gp->member = member;
    gp->score = score;
    ga->used++;
    return gp;
}

/* Destroy a geoArray created with geoArrayCreate(). */
void geoArrayFree(geoArray *ga) {
    size_t i;
    for (i = 0; i < ga->used; i++) sdsfree(ga->array[i].member);
    zfree(ga->array);
    zfree(ga);
}

/* ====================================================================
 * Helpers
 * ==================================================================== */
int decodeGeohash(double bits, double *xy) {
    GeoHashBits hash = { .bits = (uint64_t)bits, .step = GEO_STEP_MAX };
    return geohashDecodeToLongLatWGS84(hash, xy);
}


/* Helper function for geoGetPointsInRange(): given a sorted set score
 * representing a point, and a GeoShape, checks if the point is within the search area.
 *
 * shape: the rectangle
 * score: the encoded version of lat,long
 * xy: output variable, the decoded lat,long
 * distance: output variable, the distance between the center of the shape and the point
 *
 * Return values:
 *
 * The return value is C_OK if the point is within search area, or C_ERR if it is outside.
 * "*xy" is populated with the decoded lat,long.
 * "*distance" is populated with the distance between the center of the shape and the point.
 */
int geoWithinShape(GeoShape *shape, double score, double *xy, double *distance) {
    if (!decodeGeohash(score,xy)) return C_ERR; /* Can't decode. */
    /* Note that geohashGetDistanceIfInRadiusWGS84() takes arguments in
     * reverse order: longitude first, latitude later. */
    if (shape->type == CIRCULAR_TYPE) {
        if (!geohashGetDistanceIfInRadiusWGS84(shape->xy[0], shape->xy[1], xy[0], xy[1],
                                               shape->t.radius*shape->conversion, distance))
            return C_ERR;
    } else if (shape->type == RECTANGLE_TYPE) {
        if (!geohashGetDistanceIfInRectangle(shape->t.r.width * shape->conversion,
                                             shape->t.r.height * shape->conversion,
                                             shape->xy[0], shape->xy[1], xy[0], xy[1], distance))
            return C_ERR;
    }
    return C_OK;
}

/* Compute the sorted set scores min (inclusive), max (exclusive) we should
 * query in order to retrieve all the elements inside the specified area
 * 'hash'. The two scores are returned by reference in *min and *max. */
void scoresOfGeoHashBox(GeoHashBits hash, GeoHashFix52Bits *min, GeoHashFix52Bits *max) {
    /* We want to compute the sorted set scores that will include all the
     * elements inside the specified Geohash 'hash', which has as many
     * bits as specified by hash.step * 2.
     *
     * So if step is, for example, 3, and the hash value in binary
     * is 101010, since our score is 52 bits we want every element which
     * is in binary: 101010?????????????????????????????????????????????
     * Where ? can be 0 or 1.
     *
     * To get the min score we just use the initial hash value left
     * shifted enough to get the 52 bit value. Later we increment the
     * 6 bit prefix (see the hash.bits++ statement), and get the new
     * prefix: 101011, which we align again to 52 bits to get the maximum
     * value (which is excluded from the search). So we get everything
     * between the two following scores (represented in binary):
     *
     * 1010100000000000000000000000000000000000000000000000 (included)
     * and
     * 1010110000000000000000000000000000000000000000000000 (excluded).
     */
    *min = geohashAlign52Bits(hash);
    hash.bits++;
    *max = geohashAlign52Bits(hash);
}