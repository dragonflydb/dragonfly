/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
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

#ifndef __REDIS_UTIL_H
#define __REDIS_UTIL_H

#include <stdint.h>
#include <time.h>
#include <unistd.h>


/* The maximum number of characters needed to represent a long double
 * as a string (long double has a huge range).
 * This should be the size of the buffer given to ld2string */
#define MAX_LONG_DOUBLE_CHARS 5*1024

/* long double to string conversion options */
typedef enum {
    LD_STR_AUTO,     /* %.17Lg */
    LD_STR_HUMAN,    /* %.17Lf + Trimming of trailing zeros */
    LD_STR_HEX       /* %La */
} ld2string_mode;

int stringmatchlen(const char *p, int plen, const char *s, int slen, int nocase);
int stringmatch(const char *p, const char *s, int nocase);
int stringmatchlen_fuzz_test(void);
long long memtoll(const char *p, int *err);
const char *mempbrk(const char *s, size_t len, const char *chars, size_t charslen);
char *memmapchars(char *s, size_t len, const char *from, const char *to, size_t setlen);
uint32_t digits10(uint64_t v);
uint32_t sdigits10(int64_t v);
int ll2string(char *s, size_t len, long long value);
int string2ll(const char *s, size_t slen, long long *value);
int string2ull(const char *s, unsigned long long *value);
int string2l(const char *s, size_t slen, long *value);
int string2ld(const char *s, size_t slen, long double *dp);
int d2string(char *buf, size_t len, double value);
int ld2string(char *buf, size_t len, long double value, ld2string_mode mode);

long getTimeZone(void);
int pathIsBaseName(char *path);

#define LOG_MAX_LEN    1024 /* Default maximum length of syslog messages.*/

/* Log levels */
#define LL_DEBUG 0
#define LL_VERBOSE 1
#define LL_NOTICE 2
#define LL_WARNING 3
#define LL_RAW (1<<10) /* Modifier to log without timestamp */


#define LRU_BITS 24
#define LRU_CLOCK_MAX ((1<<LRU_BITS)-1) /* Max value of obj->lru */
#define LRU_CLOCK_RESOLUTION 1000 /* LRU clock resolution in ms */

void serverLog(int level, const char *fmt, ...);
void _serverPanic(const char *file, int line, const char *msg, ...);
void _serverAssert(const char *estr, const char *file, int line);
void serverLogHexDump(int level, char *descr, void *value, size_t len);

#define serverPanic(...) _serverPanic(__FILE__,__LINE__,__VA_ARGS__),_exit(1)
#define serverAssert(_e) ((_e)?(void)0 : (_serverAssert(#_e,__FILE__,__LINE__),_exit(1)))

extern int verbosity;

typedef long long mstime_t; /* millisecond time type. */
unsigned int LRU_CLOCK(void);
void debugDelay(int usec);
long long ustime(void);

/* Return the current time in minutes, just taking the least significant
 * 16 bits. The returned time is suitable to be stored as LDT (last decrement
 * time) for the LFU implementation. */
static inline unsigned long LFUGetTimeInMinutesT(size_t sec) {
    return (sec / 60) & 65535;
}

static inline unsigned long LFUGetTimeInMinutes() {
    return LFUGetTimeInMinutesT(time(NULL));
}

/* Given an object last access time, compute the minimum number of minutes
 * that elapsed since the last access. Handle overflow (ldt greater than
 * the current 16 bits minutes time) considering the time as wrapping
 * exactly once. */
static inline unsigned long LFUTimeElapsed(time_t sec, unsigned long ldt) {
    unsigned long now = LFUGetTimeInMinutesT(sec);
    if (now >= ldt) return now-ldt;
    return 65535-ldt+now;
}


/* Return the UNIX time in milliseconds */
static inline mstime_t mstime(void) {
  return ustime()/1000;
}

/* Return the LRU clock, based on the clock resolution. This is a time
 * in a reduced-bits format that can be used to set and check the
 * object->lru field of redisObject structures. */
static inline unsigned int getLRUClock(void) {
    int64_t t = time(NULL);
    return t & LRU_CLOCK_MAX;
}

#ifdef REDIS_TEST
int utilTest(int argc, char **argv, int accurate);
#endif

#endif
