/**
 * Adaptive histogram based on something like streaming k-means crossed with Q-digest.
 * The implementation is a direct descendent of MergingDigest
 * https://github.com/tdunning/t-digest/
 *
 * Copyright (c) 2021 Redis, All rights reserved.
 *
 * Allocator selection.
 *
 * This file is used in order to change the t-digest allocator at compile time.
 * Just define the following defines to what you want to use. Also add
 * the include of your alternate allocator if needed (not needed in order
 * to use the default libc allocator). */
#ifndef TD_ALLOC_H
#define TD_ALLOC_H

#include "zmalloc.h"

#define __td_malloc zmalloc
#define __td_calloc zcalloc
#define __td_realloc zrealloc
#define __td_free zfree

#endif
