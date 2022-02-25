#ifndef __REDIS_ZSET_H
#define __REDIS_ZSET_H

#include "redis/sds.h"

/* Sorted sets data type */

/* Sorted sets data type */

/* Input flags. */
#define ZADD_IN_NONE 0
#define ZADD_IN_INCR (1 << 0) /* Increment the score instead of setting it. */
#define ZADD_IN_NX (1 << 1)   /* Don't touch elements not already existing. */
#define ZADD_IN_XX (1 << 2)   /* Only touch elements already existing. */
#define ZADD_IN_GT (1 << 3)   /* Only update existing when new scores are higher. */
#define ZADD_IN_LT (1 << 4)   /* Only update existing when new scores are lower. */

/* Output flags. */
#define ZADD_OUT_NOP (1 << 0)     /* Operation not performed because of conditionals.*/
#define ZADD_OUT_NAN (1 << 1)     /* Only touch elements already existing. */
#define ZADD_OUT_ADDED (1 << 2)   /* The element was new and was added. */
#define ZADD_OUT_UPDATED (1 << 3) /* The element already existed, score updated. */

/* ZSETs use a specialized version of Skiplists */
typedef struct zskiplistNode {
  sds ele;
  double score;
  struct zskiplistNode* backward;
  struct zskiplistLevel {
    struct zskiplistNode* forward;
    unsigned long span;
  } level[];
} zskiplistNode;

typedef struct zskiplist {
  struct zskiplistNode *header, *tail;
  unsigned long length;
  int level;
} zskiplist;

struct dict;

typedef struct zset {
  struct dict* dict;
  zskiplist* zsl;
} zset;

/* Struct to hold an inclusive/exclusive range spec by score comparison. */
typedef struct {
  double min, max;
  int minex, maxex; /* are min or max exclusive? */
} zrangespec;

/* Struct to hold an inclusive/exclusive range spec by lexicographic comparison. */
typedef struct {
  sds min, max;     /* May be set to shared.(minstring|maxstring) */
  int minex, maxex; /* are min or max exclusive? */
} zlexrangespec;

typedef struct redisObject robj;

zskiplist* zslCreate(void);
void zslFree(zskiplist* zsl);
zskiplistNode* zslInsert(zskiplist* zsl, double score, sds ele);
unsigned char* zzlInsert(unsigned char* zl, sds ele, double score);
// int zslDelete(zskiplist *zsl, double score, sds ele, zskiplistNode **node);
zskiplistNode* zslFirstInRange(zskiplist* zsl, const zrangespec* range);
zskiplistNode* zslLastInRange(zskiplist* zsl, const zrangespec* range);
// double zzlGetScore(unsigned char *sptr);
// void zzlNext(unsigned char *zl, unsigned char **eptr, unsigned char **sptr);
// void zzlPrev(unsigned char *zl, unsigned char **eptr, unsigned char **sptr);
unsigned char* zzlFirstInRange(unsigned char* zl, const zrangespec* range);
unsigned char* zzlLastInRange(unsigned char* zl, const zrangespec* range);
unsigned long zsetLength(const robj* zobj);
void zsetConvert(robj* zobj, int encoding);
void zsetConvertToZiplistIfNeeded(robj* zobj, size_t maxelelen);
int zsetScore(robj* zobj, sds member, double* score);
// unsigned long zslGetRank(zskiplist *zsl, double score, sds o);
int zsetAdd(robj* zobj, double score, sds ele, int in_flags, int* out_flags, double* newscore);
long zsetRank(robj* zobj, sds ele, int reverse);
int zsetDel(robj* zobj, sds ele);

void zzlPrev(unsigned char* zl, unsigned char** eptr, unsigned char** sptr);
void zzlNext(unsigned char* zl, unsigned char** eptr, unsigned char** sptr);
double zzlGetScore(unsigned char* sptr);
int zslValueGteMin(double value, const zrangespec* spec);
int zslValueLteMax(double value, const zrangespec* spec);
void zslFreeLexRange(zlexrangespec* spec);
int zslParseLexRange(robj* min, robj* max, zlexrangespec* spec);
unsigned char* zzlFirstInLexRange(unsigned char* zl, zlexrangespec* range);
unsigned char* zzlLastInLexRange(unsigned char* zl, zlexrangespec* range);
zskiplistNode* zslFirstInLexRange(zskiplist* zsl, zlexrangespec* range);
zskiplistNode* zslLastInLexRange(zskiplist* zsl, zlexrangespec* range);
int zzlLexValueGteMin(unsigned char* p, const zlexrangespec* spec);
int zzlLexValueLteMax(unsigned char* p, const zlexrangespec* spec);
int zslLexValueGteMin(sds value, const zlexrangespec* spec);
int zslLexValueLteMax(sds value, const zlexrangespec* spec);
int zsetZiplistValidateIntegrity(unsigned char* zl, size_t size, int deep);

extern size_t zset_max_ziplist_entries;
extern size_t zset_max_ziplist_value;

#endif
