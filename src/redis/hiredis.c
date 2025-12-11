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

#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <errno.h>
#include <ctype.h>

#include "hiredis.h"
#include "sds.h"


static redisReply *createReplyObject(int type);
static void *createStringObject(const redisReadTask *task, char *str, size_t len);
static void *createArrayObject(const redisReadTask *task, size_t elements);
static void *createIntegerObject(const redisReadTask *task, long long value);
static void *createDoubleObject(const redisReadTask *task, double value, char *str, size_t len);
static void *createNilObject(const redisReadTask *task);
static void *createBoolObject(const redisReadTask *task, int bval);

/* Default set of functions to build the reply. Keep in mind that such a
 * function returning NULL is interpreted as OOM. */
static redisReplyObjectFunctions defaultFunctions = {
    createStringObject,
    createArrayObject,
    createIntegerObject,
    createDoubleObject,
    createNilObject,
    createBoolObject,
    freeReplyObject
};

/* Create a reply object */
static redisReply *createReplyObject(int type) {
    redisReply *r = s_calloc(sizeof(*r));

    if (r == NULL)
        return NULL;

    r->type = type;
    return r;
}

/* Free a reply object */
void freeReplyObject(void *reply) {
    redisReply *r = reply;
    size_t j;

    if (r == NULL)
        return;

    switch(r->type) {
    case REDIS_REPLY_INTEGER:
    case REDIS_REPLY_NIL:
    case REDIS_REPLY_BOOL:
        break; /* Nothing to free */
    case REDIS_REPLY_ARRAY:
    case REDIS_REPLY_MAP:
    case REDIS_REPLY_ATTR:
    case REDIS_REPLY_SET:
    case REDIS_REPLY_PUSH:
        if (r->element != NULL) {
            for (j = 0; j < r->elements; j++)
                freeReplyObject(r->element[j]);
            s_free(r->element);
        }
        break;
    case REDIS_REPLY_ERROR:
    case REDIS_REPLY_STATUS:
    case REDIS_REPLY_STRING:
    case REDIS_REPLY_DOUBLE:
    case REDIS_REPLY_VERB:
    case REDIS_REPLY_BIGNUM:
        s_free(r->str);
        break;
    }
    s_free(r);
}

static void *createStringObject(const redisReadTask *task, char *str, size_t len) {
    redisReply *r, *parent;
    char *buf;

    r = createReplyObject(task->type);
    if (r == NULL)
        return NULL;

    assert(task->type == REDIS_REPLY_ERROR  ||
           task->type == REDIS_REPLY_STATUS ||
           task->type == REDIS_REPLY_STRING ||
           task->type == REDIS_REPLY_VERB   ||
           task->type == REDIS_REPLY_BIGNUM);

    /* Copy string value */
    if (task->type == REDIS_REPLY_VERB) {
        buf = s_malloc(len-4+1); /* Skip 4 bytes of verbatim type header. */
        if (buf == NULL) goto oom;

        memcpy(r->vtype,str,3);
        r->vtype[3] = '\0';
        memcpy(buf,str+4,len-4);
        buf[len-4] = '\0';
        r->len = len - 4;
    } else {
        buf = s_malloc(len+1);
        if (buf == NULL) goto oom;

        memcpy(buf,str,len);
        buf[len] = '\0';
        r->len = len;
    }
    r->str = buf;

    if (task->parent) {
        parent = task->parent->obj;
        assert(parent->type == REDIS_REPLY_ARRAY ||
               parent->type == REDIS_REPLY_MAP ||
               parent->type == REDIS_REPLY_ATTR ||
               parent->type == REDIS_REPLY_SET ||
               parent->type == REDIS_REPLY_PUSH);
        parent->element[task->idx] = r;
    }
    return r;

oom:
    freeReplyObject(r);
    return NULL;
}

static void *createArrayObject(const redisReadTask *task, size_t elements) {
    redisReply *r, *parent;

    r = createReplyObject(task->type);
    if (r == NULL)
        return NULL;

    if (elements > 0) {
        r->element = s_calloc(elements * sizeof(redisReply*));
        if (r->element == NULL) {
            freeReplyObject(r);
            return NULL;
        }
    }

    r->elements = elements;

    if (task->parent) {
        parent = task->parent->obj;
        assert(parent->type == REDIS_REPLY_ARRAY ||
               parent->type == REDIS_REPLY_MAP ||
               parent->type == REDIS_REPLY_ATTR ||
               parent->type == REDIS_REPLY_SET ||
               parent->type == REDIS_REPLY_PUSH);
        parent->element[task->idx] = r;
    }
    return r;
}

static void *createIntegerObject(const redisReadTask *task, long long value) {
    redisReply *r, *parent;

    r = createReplyObject(REDIS_REPLY_INTEGER);
    if (r == NULL)
        return NULL;

    r->integer = value;

    if (task->parent) {
        parent = task->parent->obj;
        assert(parent->type == REDIS_REPLY_ARRAY ||
               parent->type == REDIS_REPLY_MAP ||
               parent->type == REDIS_REPLY_ATTR ||
               parent->type == REDIS_REPLY_SET ||
               parent->type == REDIS_REPLY_PUSH);
        parent->element[task->idx] = r;
    }
    return r;
}

static void *createDoubleObject(const redisReadTask *task, double value, char *str, size_t len) {
    redisReply *r, *parent;

    if (len == SIZE_MAX) // Prevents s_malloc(0) if len equals to SIZE_MAX
        return NULL;

    r = createReplyObject(REDIS_REPLY_DOUBLE);
    if (r == NULL)
        return NULL;

    r->dval = value;
    r->str = s_malloc(len+1);
    if (r->str == NULL) {
        freeReplyObject(r);
        return NULL;
    }

    /* The double reply also has the original protocol string representing a
     * double as a null terminated string. This way the caller does not need
     * to format back for string conversion, especially since Redis does efforts
     * to make the string more human readable avoiding the calssical double
     * decimal string conversion artifacts. */
    memcpy(r->str, str, len);
    r->str[len] = '\0';
    r->len = len;

    if (task->parent) {
        parent = task->parent->obj;
        assert(parent->type == REDIS_REPLY_ARRAY ||
               parent->type == REDIS_REPLY_MAP ||
               parent->type == REDIS_REPLY_ATTR ||
               parent->type == REDIS_REPLY_SET ||
               parent->type == REDIS_REPLY_PUSH);
        parent->element[task->idx] = r;
    }
    return r;
}

static void *createNilObject(const redisReadTask *task) {
    redisReply *r, *parent;

    r = createReplyObject(REDIS_REPLY_NIL);
    if (r == NULL)
        return NULL;

    if (task->parent) {
        parent = task->parent->obj;
        assert(parent->type == REDIS_REPLY_ARRAY ||
               parent->type == REDIS_REPLY_MAP ||
               parent->type == REDIS_REPLY_ATTR ||
               parent->type == REDIS_REPLY_SET ||
               parent->type == REDIS_REPLY_PUSH);
        parent->element[task->idx] = r;
    }
    return r;
}

static void *createBoolObject(const redisReadTask *task, int bval) {
    redisReply *r, *parent;

    r = createReplyObject(REDIS_REPLY_BOOL);
    if (r == NULL)
        return NULL;

    r->integer = bval != 0;

    if (task->parent) {
        parent = task->parent->obj;
        assert(parent->type == REDIS_REPLY_ARRAY ||
               parent->type == REDIS_REPLY_MAP ||
               parent->type == REDIS_REPLY_ATTR ||
               parent->type == REDIS_REPLY_SET ||
               parent->type == REDIS_REPLY_PUSH);
        parent->element[task->idx] = r;
    }
    return r;
}

/* Return the number of digits of 'v' when converted to string in radix 10.
 * Implementation borrowed from link in redis/src/util.c:string2ll(). */
static uint32_t countDigits(uint64_t v) {
  uint32_t result = 1;
  for (;;) {
    if (v < 10) return result;
    if (v < 100) return result + 1;
    if (v < 1000) return result + 2;
    if (v < 10000) return result + 3;
    v /= 10000U;
    result += 4;
  }
}

/* Helper that calculates the bulk length given a certain string length. */
static size_t bulklen(size_t len) {
    return 1+countDigits(len)+2+len+2;
}

int redisvFormatCommand(char **target, const char *format, va_list ap) {
    const char *c = format;
    char *cmd = NULL; /* final command */
    int pos; /* position in final command */
    sds curarg, newarg; /* current argument */
    int touched = 0; /* was the current argument touched? */
    char **curargv = NULL, **newargv = NULL;
    int argc = 0;
    int totlen = 0;
    int error_type = 0; /* 0 = no error; -1 = memory error; -2 = format error */
    int j;

    /* Abort if there is not target to set */
    if (target == NULL)
        return -1;

    /* Build the command string accordingly to protocol */
    curarg = sdsempty();
    if (curarg == NULL)
        return -1;

    while(*c != '\0') {
        if (*c != '%' || c[1] == '\0') {
            if (*c == ' ') {
                if (touched) {
                    newargv = s_realloc(curargv,sizeof(char*)*(argc+1));
                    if (newargv == NULL) goto memory_err;
                    curargv = newargv;
                    curargv[argc++] = curarg;
                    totlen += bulklen(sdslen(curarg));

                    /* curarg is put in argv so it can be overwritten. */
                    curarg = sdsempty();
                    if (curarg == NULL) goto memory_err;
                    touched = 0;
                }
            } else {
                newarg = sdscatlen(curarg,c,1);
                if (newarg == NULL) goto memory_err;
                curarg = newarg;
                touched = 1;
            }
        } else {
            char *arg;
            size_t size;

            /* Set newarg so it can be checked even if it is not touched. */
            newarg = curarg;

            switch(c[1]) {
            case 's':
                arg = va_arg(ap,char*);
                size = strlen(arg);
                if (size > 0)
                    newarg = sdscatlen(curarg,arg,size);
                break;
            case 'b':
                arg = va_arg(ap,char*);
                size = va_arg(ap,size_t);
                if (size > 0)
                    newarg = sdscatlen(curarg,arg,size);
                break;
            case '%':
                newarg = sdscat(curarg,"%");
                break;
            default:
                /* Try to detect printf format */
                {
                    static const char intfmts[] = "diouxX";
                    static const char flags[] = "#0-+ ";
                    char _format[16];
                    const char *_p = c+1;
                    size_t _l = 0;
                    va_list _cpy;

                    /* Flags */
                    while (*_p != '\0' && strchr(flags,*_p) != NULL) _p++;

                    /* Field width */
                    while (*_p != '\0' && isdigit((int) *_p)) _p++;

                    /* Precision */
                    if (*_p == '.') {
                        _p++;
                        while (*_p != '\0' && isdigit((int) *_p)) _p++;
                    }

                    /* Copy va_list before consuming with va_arg */
                    va_copy(_cpy,ap);

                    /* Make sure we have more characters otherwise strchr() accepts
                     * '\0' as an integer specifier. This is checked after above
                     * va_copy() to avoid UB in fmt_invalid's call to va_end(). */
                    if (*_p == '\0') goto fmt_invalid;

                    /* Integer conversion (without modifiers) */
                    if (strchr(intfmts,*_p) != NULL) {
                        va_arg(ap,int);
                        goto fmt_valid;
                    }

                    /* Double conversion (without modifiers) */
                    if (strchr("eEfFgGaA",*_p) != NULL) {
                        va_arg(ap,double);
                        goto fmt_valid;
                    }

                    /* Size: char */
                    if (_p[0] == 'h' && _p[1] == 'h') {
                        _p += 2;
                        if (*_p != '\0' && strchr(intfmts,*_p) != NULL) {
                            va_arg(ap,int); /* char gets promoted to int */
                            goto fmt_valid;
                        }
                        goto fmt_invalid;
                    }

                    /* Size: short */
                    if (_p[0] == 'h') {
                        _p += 1;
                        if (*_p != '\0' && strchr(intfmts,*_p) != NULL) {
                            va_arg(ap,int); /* short gets promoted to int */
                            goto fmt_valid;
                        }
                        goto fmt_invalid;
                    }

                    /* Size: long long */
                    if (_p[0] == 'l' && _p[1] == 'l') {
                        _p += 2;
                        if (*_p != '\0' && strchr(intfmts,*_p) != NULL) {
                            va_arg(ap,long long);
                            goto fmt_valid;
                        }
                        goto fmt_invalid;
                    }

                    /* Size: long */
                    if (_p[0] == 'l') {
                        _p += 1;
                        if (*_p != '\0' && strchr(intfmts,*_p) != NULL) {
                            va_arg(ap,long);
                            goto fmt_valid;
                        }
                        goto fmt_invalid;
                    }

                fmt_invalid:
                    va_end(_cpy);
                    goto format_err;

                fmt_valid:
                    _l = (_p+1)-c;
                    if (_l < sizeof(_format)-2) {
                        memcpy(_format,c,_l);
                        _format[_l] = '\0';
                        newarg = sdscatvprintf(curarg,_format,_cpy);

                        /* Update current position (note: outer blocks
                         * increment c twice so compensate here) */
                        c = _p-1;
                    }

                    va_end(_cpy);
                    break;
                }
            }

            if (newarg == NULL) goto memory_err;
            curarg = newarg;

            touched = 1;
            c++;
            if (*c == '\0')
                break;
        }
        c++;
    }

    /* Add the last argument if needed */
    if (touched) {
        newargv = s_realloc(curargv,sizeof(char*)*(argc+1));
        if (newargv == NULL) goto memory_err;
        curargv = newargv;
        curargv[argc++] = curarg;
        totlen += bulklen(sdslen(curarg));
    } else {
        sdsfree(curarg);
    }

    /* Clear curarg because it was put in curargv or was free'd. */
    curarg = NULL;

    /* Add bytes needed to hold multi bulk count */
    totlen += 1+countDigits(argc)+2;

    /* Build the command at protocol level */
    cmd = s_malloc(totlen+1);
    if (cmd == NULL) goto memory_err;

    pos = sprintf(cmd,"*%d\r\n",argc);
    for (j = 0; j < argc; j++) {
        pos += sprintf(cmd+pos,"$%zu\r\n",sdslen(curargv[j]));
        memcpy(cmd+pos,curargv[j],sdslen(curargv[j]));
        pos += sdslen(curargv[j]);
        sdsfree(curargv[j]);
        cmd[pos++] = '\r';
        cmd[pos++] = '\n';
    }
    assert(pos == totlen);
    cmd[pos] = '\0';

    s_free(curargv);
    *target = cmd;
    return totlen;

format_err:
    error_type = -2;
    goto cleanup;

memory_err:
    error_type = -1;
    goto cleanup;

cleanup:
    if (curargv) {
        while(argc--)
            sdsfree(curargv[argc]);
        s_free(curargv);
    }

    sdsfree(curarg);
    s_free(cmd);

    return error_type;
}

/* Format a command according to the Redis protocol. This function
 * takes a format similar to printf:
 *
 * %s represents a C null terminated string you want to interpolate
 * %b represents a binary safe string
 *
 * When using %b you need to provide both the pointer to the string
 * and the length in bytes as a size_t. Examples:
 *
 * len = redisFormatCommand(target, "GET %s", mykey);
 * len = redisFormatCommand(target, "SET %s %b", mykey, myval, myvallen);
 */
int redisFormatCommand(char **target, const char *format, ...) {
    va_list ap;
    int len;
    va_start(ap,format);
    len = redisvFormatCommand(target,format,ap);
    va_end(ap);

    /* The API says "-1" means bad result, but we now also return "-2" in some
     * cases.  Force the return value to always be -1. */
    if (len < 0)
        len = -1;

    return len;
}

/* Format a command according to the Redis protocol using an sds string and
 * sdscatfmt for the processing of arguments. This function takes the
 * number of arguments, an array with arguments and an array with their
 * lengths. If the latter is set to NULL, strlen will be used to compute the
 * argument lengths.
 */
long long redisFormatSdsCommandArgv(sds *target, int argc, const char **argv,
                                    const size_t *argvlen)
{
    sds cmd, aux;
    unsigned long long totlen, len;
    int j;

    /* Abort on a NULL target */
    if (target == NULL)
        return -1;

    /* Calculate our total size */
    totlen = 1+countDigits(argc)+2;
    for (j = 0; j < argc; j++) {
        len = argvlen ? argvlen[j] : strlen(argv[j]);
        totlen += bulklen(len);
    }

    /* Use an SDS string for command construction */
    cmd = sdsempty();
    if (cmd == NULL)
        return -1;

    /* We already know how much storage we need */
    aux = sdsMakeRoomFor(cmd, totlen);
    if (aux == NULL) {
        sdsfree(cmd);
        return -1;
    }

    cmd = aux;

    /* Construct command */
    cmd = sdscatfmt(cmd, "*%i\r\n", argc);
    for (j=0; j < argc; j++) {
        len = argvlen ? argvlen[j] : strlen(argv[j]);
        cmd = sdscatfmt(cmd, "$%U\r\n", len);
        cmd = sdscatlen(cmd, argv[j], len);
        cmd = sdscatlen(cmd, "\r\n", sizeof("\r\n")-1);
    }

    assert(sdslen(cmd)==totlen);

    *target = cmd;
    return totlen;
}

void redisFreeSdsCommand(sds cmd) {
    sdsfree(cmd);
}

/* Format a command according to the Redis protocol. This function takes the
 * number of arguments, an array with arguments and an array with their
 * lengths. If the latter is set to NULL, strlen will be used to compute the
 * argument lengths.
 */
long long redisFormatCommandArgv(char **target, int argc, const char **argv, const size_t *argvlen) {
    char *cmd = NULL; /* final command */
    size_t pos; /* position in final command */
    size_t len, totlen;
    int j;

    /* Abort on a NULL target */
    if (target == NULL)
        return -1;

    /* Calculate number of bytes needed for the command */
    totlen = 1+countDigits(argc)+2;
    for (j = 0; j < argc; j++) {
        len = argvlen ? argvlen[j] : strlen(argv[j]);
        totlen += bulklen(len);
    }

    /* Build the command at protocol level */
    cmd = s_malloc(totlen+1);
    if (cmd == NULL)
        return -1;

    pos = sprintf(cmd,"*%d\r\n",argc);
    for (j = 0; j < argc; j++) {
        len = argvlen ? argvlen[j] : strlen(argv[j]);
        pos += sprintf(cmd+pos,"$%zu\r\n",len);
        memcpy(cmd+pos,argv[j],len);
        pos += len;
        cmd[pos++] = '\r';
        cmd[pos++] = '\n';
    }
    assert(pos == totlen);
    cmd[pos] = '\0';

    *target = cmd;
    return totlen;
}

void redisFreeCommand(char *cmd) {
    s_free(cmd);
}

redisReader *redisReaderCreate(void) {
    return redisReaderCreateWithFunctions(&defaultFunctions);
}


