/*
 * Copyright (c) 2009-2020, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2020, Redis Labs, Inc
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


#include <stdarg.h>
#include <stdio.h>
#include <syslog.h>
#include <assert.h>

#include "util.h"

int verbosity = LL_NOTICE;

void serverLog(int level, const char *fmt, ...) {
    va_list ap;
    char msg[LOG_MAX_LEN];

    if ((level&0xff) < verbosity) return;

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    fprintf(stdout, "%s\n",msg);
}

void _serverPanic(const char *file, int line, const char *msg, ...) {
    va_list ap;
    va_start(ap,msg);
    char fmtmsg[256];
    vsnprintf(fmtmsg,sizeof(fmtmsg),msg,ap);
    va_end(ap);

    serverLog(LL_WARNING, "------------------------------------------------");
    serverLog(LL_WARNING, "!!! Software Failure. Press left mouse button to continue");
    serverLog(LL_WARNING, "Guru Meditation: %s #%s:%d", fmtmsg,file,line);
#ifndef NDEBUG
#if defined(__APPLE__)
    __assert_rtn(msg, file, line, "");
#elif defined(__FreeBSD__)
    __assert("", file, line, msg);
#else      
    __assert_fail(msg, file, line, "");
#endif    
#endif 
}

void _serverAssert(const char *estr, const char *file, int line) {
    serverLog(LL_WARNING,"=== ASSERTION FAILED ===");
    serverLog(LL_WARNING,"==> %s:%d '%s' is not true",file,line,estr);
}
