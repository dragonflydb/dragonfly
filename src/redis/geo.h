#ifndef __GEO_H__
#define __GEO_H__

#include <stddef.h> /* for size_t */
#include "geohash_helper.h"

/* Structures used inside geo.c in order to represent points and array of
 * points on the earth. */
typedef struct geoPoint {
    double longitude;
    double latitude;
    double dist;
    double score;
    char *member;
} geoPoint;

typedef struct geoArray {
    struct geoPoint *array;
    size_t buckets;
    size_t used;
} geoArray;

int geoWithinShape(GeoShape *shape, double score, double *xy, double *distance);
void scoresOfGeoHashBox(GeoHashBits hash, GeoHashFix52Bits *min, GeoHashFix52Bits *max);

#endif
