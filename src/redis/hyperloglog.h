#ifndef __REDIS_HYPERLOGLOG_H
#define __REDIS_HYPERLOGLOG_H

#include <stddef.h>
#include <stdint.h>

#include "redis/sds.h"

/* This version of hyperloglog, forked from Redis, only supports using the dense format of HLL.
 * The reason is that it is of a fixed size, which makes it easier to integrate into Dragonfly.
 * We do support converting of existing sprase-encoded HLL into dense-encoded, which can be useful
 * for replication, serialization, etc. */

enum HllValidness {
  HLL_INVALID,
  HLL_VALID_SPARSE,
  HLL_VALID_DENSE,
};

/* Convenience struct for pointing to an Hll buffer along with its size */
struct HllBufferPtr {
  unsigned char* hll;
  size_t size;
};

extern size_t HLL_SPARSE_MAX_BYTES;

enum HllValidness isValidHLL(struct HllBufferPtr hll_ptr);

size_t getDenseHllSize();
size_t getSparseHllInitSize();


int initSparseHll(struct HllBufferPtr hll_ptr);
/* Writes into `hll_ptr` an empty dense-encoded HLL.
 * Returns 0 upon success, or a negative number when `hll_ptr.size` is different from
 * getDenseHllSize() */
int createDenseHll(struct HllBufferPtr hll_ptr);

/* Converts an existing sparse-encoded HLL pointed by `in_hll`, and writes the converted result into
 * `out_hll`.
 * Returns 0 upon success, otherwise a negative number.
 * Failures can occur when `out_hll.size` is different from getDenseHllSize() or when input is not a
 * valid sparse-encoded HLL. */
int convertSparseToDenseHll(struct HllBufferPtr in_hll, struct HllBufferPtr out_hll);

/* Adds `value` of size `size`, to `hll_ptr`.
 * If `obj` does not have an underlying type of HLL a negative number is returned. */
int pfadd_sparse(sds* hll_ptr, unsigned char* value, size_t size, int* promoted);
int pfadd_dense(struct HllBufferPtr hll_ptr, unsigned char* value, size_t size);

/* Returns the estimated count of elements for `hll_ptr`.
 * If `hll_ptr` is not a valid dense-encoded HLL, a negative number is returned. */
int64_t pfcountSingle(struct HllBufferPtr hll_ptr);

/* Returns the estimated count for all HLLs in `hlls` array of size `hlls_count`.
 * All `hlls` elements must be valid, dense-encoded HLLs. */
int64_t pfcountMulti(struct HllBufferPtr* hlls, size_t hlls_count);

/* Merges array of HLLs pointed to be `in_hlls` of size `in_hlls_count` into `out_hll`.
 * Returns 0 upon success, otherwise a negative number.
 * Failure can occur when any of `in_hlls` or `out_hll` is not a dense-encoded HLL.
 * `out_hll` *can* be one of the elements in `in_hlls`. */
int pfmerge(struct HllBufferPtr* in_hlls, size_t in_hlls_count, struct HllBufferPtr out_hll);

#endif
