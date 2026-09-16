#include "read_alloc.h"

#include <stdlib.h>
#include <string.h>

typedef struct {
  size_t len;   /* used bytes, excluding the header and trailing NUL */
  size_t alloc; /* allocated bytes, excluding the header and trailing NUL */
} rsds_hdr;

#define RSDS_HDR(s) ((rsds_hdr *)(s) - 1)
#define RSDS_MAX_PREALLOC (1024 * 1024)

static rsds rsdsnewlen(const void *init, size_t initlen) {
  rsds_hdr *sh = malloc(sizeof(rsds_hdr) + initlen + 1);
  if (sh == NULL)
    return NULL;

  sh->len = initlen;
  sh->alloc = initlen;

  rsds s = (rsds)(sh + 1);
  if (initlen && init)
    memcpy(s, init, initlen);
  s[initlen] = '\0';
  return s;
}

rsds rsdsempty(void) {
  return rsdsnewlen("", 0);
}

void rsdsfree(rsds s) {
  if (s == NULL)
    return;
  free(RSDS_HDR(s));
}

size_t rsdslen(const rsds s) {
  return RSDS_HDR(s)->len;
}

size_t rsdsavail(const rsds s) {
  rsds_hdr *sh = RSDS_HDR(s);
  return sh->alloc - sh->len;
}

static rsds rsdsMakeRoomFor(rsds s, size_t addlen) {
  rsds_hdr *sh = RSDS_HDR(s);
  if (sh->alloc - sh->len >= addlen)
    return s;

  size_t newlen = sh->len + addlen;
  if (newlen < RSDS_MAX_PREALLOC)
    newlen *= 2;
  else
    newlen += RSDS_MAX_PREALLOC;

  rsds_hdr *newsh = realloc(sh, sizeof(rsds_hdr) + newlen + 1);
  if (newsh == NULL)
    return NULL;
  newsh->alloc = newlen;
  return (rsds)(newsh + 1);
}

rsds rsdscatlen(rsds s, const void *t, size_t len) {
  size_t curlen = rsdslen(s);
  s = rsdsMakeRoomFor(s, len);
  if (s == NULL)
    return NULL;
  memcpy(s + curlen, t, len);
  rsds_hdr *sh = RSDS_HDR(s);
  sh->len = curlen + len;
  s[sh->len] = '\0';
  return s;
}

int rsdsrange(rsds s, long start, long end) {
  size_t len = rsdslen(s);
  if (len == 0)
    return 0;

  if (start < 0) {
    start = (long)len + start;
    if (start < 0)
      start = 0;
  }
  if (end < 0) {
    end = (long)len + end;
    if (end < 0)
      end = 0;
  }

  size_t newlen = (start > end) ? 0 : (size_t)(end - start) + 1;
  if (newlen != 0) {
    if (start >= (long)len) {
      newlen = 0;
    } else if (end >= (long)len) {
      end = (long)len - 1;
      newlen = (start > end) ? 0 : (size_t)(end - start) + 1;
    }
  } else {
    start = 0;
  }

  if (start && newlen)
    memmove(s, s + start, newlen);
  s[newlen] = '\0';
  RSDS_HDR(s)->len = newlen;
  return 0;
}
