/* zmalloc - total amount of allocated memory aware version of malloc()
 *
 * Copyright (c) 2009-2010, Salvatore Sanfilippo <antirez at gmail dot com>
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

#ifndef __ZMALLOC_H
#define __ZMALLOC_H

#include <stdint.h>

/* Double expansion needed for stringification of macro values. */
#define __xstr(s) __zm_str(s)
#define __zm_str(s) #s

#if defined(USE_JEMALLOC)
#define ZMALLOC_LIB ("jemalloc-" __xstr(JEMALLOC_VERSION_MAJOR) "." __xstr(JEMALLOC_VERSION_MINOR) "." __xstr(JEMALLOC_VERSION_BUGFIX))
#include <jemalloc/jemalloc.h>
#if (JEMALLOC_VERSION_MAJOR == 2 && JEMALLOC_VERSION_MINOR >= 1) || (JEMALLOC_VERSION_MAJOR > 2)
#define HAVE_MALLOC_SIZE 1
#define zmalloc_size(p) je_malloc_usable_size(p)
#else
#error "Newer version of jemalloc required"
#endif

#elif defined(__APPLE__)
#include <malloc/malloc.h>
#define HAVE_MALLOC_SIZE 1
#ifdef USE_ZMALLOC_MI
#define zmalloc_size(p) zmalloc_usable_size(p)
#else
#define zmalloc_size(p) malloc_size(p)
#endif
#define ZMALLOC_LIB "macos"
#endif

/* On native libc implementations, we should still do our best to provide a
 * HAVE_MALLOC_SIZE capability. This can be set explicitly as well:
 *
 * NO_MALLOC_USABLE_SIZE disables it on all platforms, even if they are
 *      known to support it.
 * USE_MALLOC_USABLE_SIZE forces use of malloc_usable_size() regardless
 *      of platform.
 */
#ifndef ZMALLOC_LIB
#define ZMALLOC_LIB "libc"

#include <malloc.h>

#define HAVE_MALLOC_SIZE 1
#ifdef USE_ZMALLOC_MI
#define zmalloc_size(p) zmalloc_usable_size(p)
#else
#define zmalloc_size(p) malloc_usable_size(p)
#endif

#endif  // ZMALLOC_LIB

/* We can enable the Redis defrag capabilities only if we are using Jemalloc
 * and the version used is our special version modified for Redis having
 * the ability to return per-allocation fragmentation hints. */
#if defined(USE_JEMALLOC) && defined(JEMALLOC_FRAG_HINT)
#define HAVE_DEFRAG
#endif

void *zmalloc(size_t size);
void *zcalloc(size_t size);
void *zrealloc(void *ptr, size_t size);
void *ztrymalloc(size_t size);
void *ztrycalloc(size_t size);
void *ztryrealloc(void *ptr, size_t size);
void zfree(void *ptr);

size_t znallocx(size_t size); // Equivalent to nallocx for jemalloc or mi_good_size for mimalloc.
void zfree_size(void* ptr, size_t size);  // equivalent to sdallocx or mi_free_size

void *zmalloc_usable(size_t size, size_t *usable);
void *zcalloc_usable(size_t size, size_t *usable);
void *zrealloc_usable(void *ptr, size_t size, size_t *usable);
void *ztrymalloc_usable(size_t size, size_t *usable);
void *ztrycalloc_usable(size_t size, size_t *usable);
void *ztryrealloc_usable(void *ptr, size_t size, size_t *usable);

// size_t zmalloc_used_memory(void);
void zmalloc_set_oom_handler(void (*oom_handler)(size_t));
size_t zmalloc_get_rss(void);
int zmalloc_get_allocator_info(size_t *allocated, size_t *active, size_t *resident);
void set_jemalloc_bg_thread(int enable);
int jemalloc_purge();
size_t zmalloc_get_private_dirty(long pid);
size_t zmalloc_get_smap_bytes_by_field(char *field, long pid);
size_t zmalloc_get_memory_size(void);
size_t zmalloc_usable_size(const void* p);

/* get the memory usage + the number of wasted locations of memory
Based on a given threshold (ratio < 1).
Note that if a block is not used, it would not counted as wasted
*/
int zmalloc_get_allocator_wasted_blocks(float ratio, size_t* allocated, size_t* commited,
                                        size_t* wasted);

/*
 * checks whether a page that the pointer ptr located at is underutilized.
 * This uses the current local thread heap.
 * return 0 if not, 1 if underutilized
 */
int zmalloc_page_is_underutilized(void *ptr, float ratio);
char *zstrdup(const char *s);

void init_zmalloc_threadlocal(void* heap);
extern __thread ssize_t zmalloc_used_memory_tl;

#undef __zm_str
#undef __xstr

#endif /* __ZMALLOC_H */
