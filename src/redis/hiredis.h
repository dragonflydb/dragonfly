/*
 * Copyright (c) 2009-2011, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2010-2014, Pieter Noordhuis <pcnoordhuis at gmail dot com>
 * Copyright (c) 2015, Matt Stancliff <matt at genges dot com>,
 *                     Jan-Erik Rediger <janerik at fnordig dot com>
 *
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

#ifndef __HIREDIS_H
#define __HIREDIS_H
#include "read.h"
#include <stdarg.h> /* for va_list */
#ifndef _MSC_VER
#include <sys/time.h> /* for struct timeval */
#else
struct timeval; /* forward declaration */
typedef long long ssize_t;
#endif
#include <stdint.h> /* uintXX_t, etc */
#include "sds.h" /* for sds */
#include "sdsalloc.h" /* for allocation wrappers */

#define HIREDIS_MAJOR 1
#define HIREDIS_MINOR 3
#define HIREDIS_PATCH 0
#define HIREDIS_SONAME 1.3.0

/* Connection type can be blocking or non-blocking and is set in the
 * least significant bit of the flags field in redisContext. */
#define REDIS_BLOCK 0x1

/* Connection may be disconnected before being free'd. The second bit
 * in the flags field is set when the context is connected. */
#define REDIS_CONNECTED 0x2

/* The async API might try to disconnect cleanly and flush the output
 * buffer and read all subsequent replies before disconnecting.
 * This flag means no new commands can come in and the connection
 * should be terminated once all replies have been read. */
#define REDIS_DISCONNECTING 0x4

/* Flag specific to the async API which means that the context should be clean
 * up as soon as possible. */
#define REDIS_FREEING 0x8

/* Flag that is set when an async callback is executed. */
#define REDIS_IN_CALLBACK 0x10

/* Flag that is set when the async context has one or more subscriptions. */
#define REDIS_SUBSCRIBED 0x20

/* Flag that is set when monitor mode is active */
#define REDIS_MONITORING 0x40

/* Flag that is set when we should set SO_REUSEADDR before calling bind() */
#define REDIS_REUSEADDR 0x80

/* Flag that is set when the async connection supports push replies. */
#define REDIS_SUPPORTS_PUSH 0x100

/**
 * Flag that indicates the user does not want the context to
 * be automatically freed upon error
 */
#define REDIS_NO_AUTO_FREE 0x200

/* Flag that indicates the user does not want replies to be automatically freed */
#define REDIS_NO_AUTO_FREE_REPLIES 0x400

/* Flags to prefer IPv6 or IPv4 when doing DNS lookup. (If both are set,
 * AF_UNSPEC is used.) */
#define REDIS_PREFER_IPV4 0x800
#define REDIS_PREFER_IPV6 0x1000

#define REDIS_KEEPALIVE_INTERVAL 15 /* seconds */

/* number of times we retry to connect in the case of EADDRNOTAVAIL and
 * SO_REUSEADDR is being used. */
#define REDIS_CONNECT_RETRIES  10

/* Forward declarations for structs defined elsewhere */
struct redisAsyncContext;
struct redisContext;

/* RESP3 push helpers and callback prototypes */
#define redisIsPushReply(r) (((redisReply*)(r))->type == REDIS_REPLY_PUSH)
typedef void (redisPushFn)(void *, void *);
typedef void (redisAsyncPushFn)(struct redisAsyncContext *, void *);

#ifdef __cplusplus
extern "C" {
#endif

/* This is the reply object returned by redisCommand() */
typedef struct redisReply {
    int type; /* REDIS_REPLY_* */
    long long integer; /* The integer when type is REDIS_REPLY_INTEGER */
    double dval; /* The double when type is REDIS_REPLY_DOUBLE */
    size_t len; /* Length of string */
    char *str; /* Used for REDIS_REPLY_ERROR, REDIS_REPLY_STRING
                  REDIS_REPLY_VERB, REDIS_REPLY_DOUBLE (in additional to dval),
                  and REDIS_REPLY_BIGNUM. */
    char vtype[4]; /* Used for REDIS_REPLY_VERB, contains the null
                      terminated 3 character content type, such as "txt". */
    size_t elements; /* number of elements, for REDIS_REPLY_ARRAY */
    struct redisReply **element; /* elements vector for REDIS_REPLY_ARRAY */
} redisReply;

redisReader *redisReaderCreate(void);

/* Function to free the reply objects hiredis returns by default. */
void freeReplyObject(void *reply);

/* Functions to format a command according to the protocol. */
int redisvFormatCommand(char **target, const char *format, va_list ap);
int redisFormatCommand(char **target, const char *format, ...);
long long redisFormatCommandArgv(char **target, int argc, const char **argv, const size_t *argvlen);
long long redisFormatSdsCommandArgv(sds *target, int argc, const char ** argv, const size_t *argvlen);
void redisFreeCommand(char *cmd);
void redisFreeSdsCommand(sds cmd);



#ifdef __cplusplus
}
#endif

#endif
