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
int pfadd_sparse(sds* hll_ptr, const unsigned char* value, size_t size, int* promoted);
int pfadd_dense(struct HllBufferPtr hll_ptr, const unsigned char* value, size_t size);

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


/* PFDEBUG helpers. */
/* Reads all HLL_REGISTERS (16384) register values from a dense HLL into regs_out.
 * Returns 0 on success, -1 if not a valid dense HLL. */
int pfDebugGetReg(struct HllBufferPtr hll_ptr, int* regs_out);

/* Decodes a sparse HLL into human-readable format written to out_buf.
 * Format: "z:N" for zero runs, "Z:N" for extended zero runs, "v:val,len" for value runs.
 * Returns 0 on success, -1 if not sparse, -2 if buffer too small. */
int pfDebugDecode(struct HllBufferPtr hll_ptr, char* out_buf, size_t out_buf_size);

/* Returns the encoding of the HLL: 0 for dense, 1 for sparse, -1 for invalid. */
int pfDebugGetEncoding(struct HllBufferPtr hll_ptr);

#endif
