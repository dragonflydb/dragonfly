#ifndef __REDIS_ZSET_H
#define __REDIS_ZSET_H

#include "redis/sds.h"

/* Sorted sets data type */

/* Sorted sets data type */

/* Input flags. */
#define ZADD_IN_NONE 0
#define ZADD_IN_INCR (1 << 0) /* Increment the score instead of setting it. */
#define ZADD_IN_NX (1 << 1)   /* Don't touch elements already existing. */
#define ZADD_IN_XX (1 << 2)   /* Only touch elements already existing. */
#define ZADD_IN_GT (1 << 3)   /* Only update existing when new scores are higher. */
#define ZADD_IN_LT (1 << 4)   /* Only update existing when new scores are lower. */

/* Output flags. */
#define ZADD_OUT_NOP (1 << 0)     /* Operation not performed because of conditionals.*/
#define ZADD_OUT_NAN (1 << 1)     /* Only touch elements already existing. */
#define ZADD_OUT_ADDED (1 << 2)   /* The element was new and was added. */
#define ZADD_OUT_UPDATED (1 << 3) /* The element already existed, score updated. */

typedef struct dict dict;

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
int zslDelete(zskiplist *zsl, double score, sds ele, zskiplistNode **node);
zskiplistNode* zslFirstInRange(zskiplist* zsl, const zrangespec* range);
zskiplistNode* zslLastInRange(zskiplist* zsl, const zrangespec* range);
zskiplistNode *zslUpdateScore(zskiplist *zsl, double curscore, sds ele, double newscore);
unsigned char *zzlFind(unsigned char *lp, sds ele, double *score);
unsigned char* zzlFirstInRange(unsigned char* zl, const zrangespec* range);
unsigned char* zzlLastInRange(unsigned char* zl, const zrangespec* range);
unsigned long zslGetRank(zskiplist *zsl, double score, sds ele);

void zzlPrev(unsigned char* zl, unsigned char** eptr, unsigned char** sptr);
void zzlNext(unsigned char* zl, unsigned char** eptr, unsigned char** sptr);
double zzlGetScore(unsigned char* sptr);
int zslValueGteMin(double value, const zrangespec* spec);
int zslValueLteMax(double value, const zrangespec* spec);
void zslFreeLexRange(zlexrangespec* spec);
int zslParseLexRange(robj* min, robj* max, zlexrangespec* spec);
unsigned char* zzlFirstInLexRange(unsigned char* zl, const zlexrangespec* range);
unsigned char* zzlLastInLexRange(unsigned char* zl, const zlexrangespec* range);
zskiplistNode* zslFirstInLexRange(zskiplist* zsl, const zlexrangespec* range);
zskiplistNode* zslLastInLexRange(zskiplist* zsl, const zlexrangespec* range);
int zzlLexValueGteMin(unsigned char* p, const zlexrangespec* spec);
int zzlLexValueLteMax(unsigned char* p, const zlexrangespec* spec);
int zslLexValueGteMin(sds value, const zlexrangespec* spec);
int zslLexValueLteMax(sds value, const zlexrangespec* spec);
zskiplistNode* zslGetElementByRank(zskiplist *zsl, unsigned long rank);
unsigned long zslDeleteRangeByRank(zskiplist *zsl, unsigned int start, unsigned int end,
                                   dict *dict);
unsigned long zslDeleteRangeByScore(zskiplist *zsl, const zrangespec *range, dict *dict);
unsigned char *zzlDeleteRangeByScore(unsigned char *zl, const zrangespec *range, unsigned long *deleted);

unsigned long zslDeleteRangeByLex(zskiplist *zsl, const zlexrangespec *range, dict *dict);

unsigned char *zzlDeleteRangeByLex(unsigned char *zl, const zlexrangespec *range, unsigned long *deleted);

extern sds cmaxstring;
extern sds cminstring;

#define ZSKIPLIST_MAXLEVEL 32 /* Should be enough for 2^64 elements */
#define ZSKIPLIST_P 0.25      /* Skiplist P = 1/4 */

#endif
