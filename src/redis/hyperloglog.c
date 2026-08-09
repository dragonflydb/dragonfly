/* hyperloglog.c - HyperLogLog probabilistic cardinality approximation.
 * This file implements the algorithm and the exported commands.
 *
 * Copyright (c) 2014, Redis Ltd.
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
/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 * Last synced with valkey 06d30f293
 */

#include "redis/hyperloglog.h"

#include <stdint.h>
#include <math.h>
#include <string.h>

#include "redis/config.h"
#include "redis/redis_aux.h"
#include "redis/util.h"

#define min(a, b) ((a) < (b) ? (a) : (b))

#if HAVE_X86_SIMD
/* Define __MM_MALLOC_H to prevent importing the memory aligned
 * allocation functions, which we don't use. */
#define __MM_MALLOC_H
#include <immintrin.h>
#endif

#if HAVE_ARM_NEON
#include <arm_neon.h>
#endif

/* The HyperLogLog implementation is based on the following ideas:
 *
 * * The use of a 64 bit hash function as proposed in [1], in order to estimate
 *   cardinalities larger than 10^9, at the cost of just 1 additional bit per
 *   register.
 * * The use of 16384 6-bit registers for a great level of accuracy, using
 *   a total of 12k per key.
 * * The use of the string data type. No new type is introduced.
 * * No attempt is made to compress the data structure as in [1]. Also the
 *   algorithm used is the original HyperLogLog Algorithm as in [2], with
 *   the only difference that a 64 bit hash function is used, so no correction
 *   is performed for values near 2^32 as in [1].
 *
 * [1] Heule, Nunkesser, Hall: HyperLogLog in Practice: Algorithmic
 *     Engineering of a State of The Art Cardinality Estimation Algorithm.
 *
 * [2] P. Flajolet, Éric Fusy, O. Gandouet, and F. Meunier. Hyperloglog: The
 *     analysis of a near-optimal cardinality estimation algorithm.
 *
 * We use two representations:
 *
 * 1) A "dense" representation where every entry is represented by
 *    a 6-bit integer.
 * 2) A "sparse" representation using run length compression suitable
 *    for representing HyperLogLogs with many registers set to 0 in
 *    a memory efficient way.
 *
 *
 * HLL header
 * ===
 *
 * Both the dense and sparse representation have a 16 byte header as follows:
 *
 * +------+---+-----+----------+
 * | HYLL | E | N/U | Cardin.  |
 * +------+---+-----+----------+
 *
 * The first 4 bytes are a magic string set to the bytes "HYLL".
 * "E" is one byte encoding, currently set to HLL_DENSE or
 * HLL_SPARSE. N/U are three not used bytes.
 *
 * The "Cardin." field is a 64 bit integer stored in little endian format
 * with the latest cardinality computed that can be reused if the data
 * structure was not modified since the last computation (this is useful
 * because there are high probabilities that HLLADD operations don't
 * modify the actual data structure and hence the approximated cardinality).
 *
 * When the most significant bit in the most significant byte of the cached
 * cardinality is set, it means that the data structure was modified and
 * we can't reuse the cached value that must be recomputed.
 *
 * Dense representation
 * ===
 *
 * The dense representation is the following:
 *
 * +--------+--------+--------+------//      //--+
 * |11000000|22221111|33333322|55444444 ....     |
 * +--------+--------+--------+------//      //--+
 *
 * The 6 bits counters are encoded one after the other starting from the
 * LSB to the MSB, and using the next bytes as needed.
 *
 * Sparse representation
 * ===
 *
 * The sparse representation encodes registers using a run length
 * encoding composed of three opcodes, two using one byte, and one using
 * of two bytes. The opcodes are called ZERO, XZERO and VAL.
 *
 * ZERO opcode is represented as 00xxxxxx. The 6-bit integer represented
 * by the six bits 'xxxxxx', plus 1, means that there are N registers set
 * to 0. This opcode can represent from 1 to 64 contiguous registers set
 * to the value of 0.
 *
 * XZERO opcode is represented by two bytes 01xxxxxx yyyyyyyy. The 14-bit
 * integer represented by the bits 'xxxxxx' as most significant bits and
 * 'yyyyyyyy' as least significant bits, plus 1, means that there are N
 * registers set to 0. This opcode can represent from 0 to 16384 contiguous
 * registers set to the value of 0.
 *
 * VAL opcode is represented as 1vvvvvxx. It contains a 5-bit integer
 * representing the value of a register, and a 2-bit integer representing
 * the number of contiguous registers set to that value 'vvvvv'.
 * To obtain the value and run length, the integers vvvvv and xx must be
 * incremented by one. This opcode can represent values from 1 to 32,
 * repeated from 1 to 4 times.
 *
 * The sparse representation can't represent registers with a value greater
 * than 32, however it is very unlikely that we find such a register in an
 * HLL with a cardinality where the sparse representation is still more
 * memory efficient than the dense representation. When this happens the
 * HLL is converted to the dense representation.
 *
 * The sparse representation is purely positional. For example a sparse
 * representation of an empty HLL is just: XZERO:16384.
 *
 * An HLL having only 3 non-zero registers at position 1000, 1020, 1021
 * respectively set to 2, 3, 3, is represented by the following three
 * opcodes:
 *
 * XZERO:1000 (Registers 0-999 are set to 0)
 * VAL:2,1    (1 register set to value 2, that is register 1000)
 * ZERO:19    (Registers 1001-1019 set to 0)
 * VAL:3,2    (2 registers set to value 3, that is registers 1020,1021)
 * XZERO:15362 (Registers 1022-16383 set to 0)
 *
 * In the example the sparse representation used just 7 bytes instead
 * of 12k in order to represent the HLL registers. In general for low
 * cardinality there is a big win in terms of space efficiency, traded
 * with CPU time since the sparse representation is slower to access.
 *
 * The following table shows average cardinality vs bytes used, 100
 * samples per cardinality (when the set was not representable because
 * of registers with too big value, the dense representation size was used
 * as a sample).
 *
 * 100 267
 * 200 485
 * 300 678
 * 400 859
 * 500 1033
 * 600 1205
 * 700 1375
 * 800 1544
 * 900 1713
 * 1000 1882
 * 2000 3480
 * 3000 4879
 * 4000 6089
 * 5000 7138
 * 6000 8042
 * 7000 8823
 * 8000 9500
 * 9000 10088
 * 10000 10591
 *
 * The dense representation uses 12288 bytes, so there is a big win up to
 * a cardinality of ~2000-3000. For bigger cardinalities the constant times
 * involved in updating the sparse representation is not justified by the
 * memory savings. The exact maximum length of the sparse representation
 * when this implementation switches to the dense representation is
 * configured via the define HLL_SPARSE_MAX_BYTES.
 */

#define HLL_SPARSE_MAX_BYTES 3000

struct hllhdr {
    char magic[4];       /* "HYLL" */
    uint8_t encoding;    /* HLL_DENSE or HLL_SPARSE. */
    uint8_t notused[3];  /* Reserved for future use, must be zero. */
    uint8_t card[8];     /* Cached cardinality, little endian. */
    uint8_t registers[]; /* Data bytes. */
};

/* The cached cardinality MSB is used to signal validity of the cached value. */
#define HLL_INVALIDATE_CACHE(hdr) (hdr)->card[7] |= (1 << 7)
#define HLL_VALID_CACHE(hdr) (((hdr)->card[7] & (1 << 7)) == 0)

#define HLL_P 14                       /* The greater is P, the smaller the error. */
#define HLL_Q (64 - HLL_P)             /* The number of bits of the hash value used for \
                                          determining the number of leading zeros. */
#define HLL_REGISTERS (1 << HLL_P)     /* With P=14, 16384 registers. */
#define HLL_P_MASK (HLL_REGISTERS - 1) /* Mask to index register. */
#define HLL_BITS 6                     /* Enough to count up to 63 leading zeroes. */
#define HLL_REGISTER_MAX ((1 << HLL_BITS) - 1)
#define HLL_HDR_SIZE sizeof(struct hllhdr)
#define HLL_DENSE_SIZE (HLL_HDR_SIZE + ((HLL_REGISTERS * HLL_BITS + 7) / 8))
#define HLL_DENSE 0  /* Dense encoding. */
#define HLL_SPARSE 1 /* Sparse encoding. */
#define HLL_RAW 255  /* Only used internally, never exposed. */
#define HLL_MAX_ENCODING 1

#if HAVE_X86_SIMD || HAVE_ARM_NEON
#define SIMD_SUPPORTED 1
static int simd_enabled = 1;
#else
#define SIMD_SUPPORTED 0
#endif

#if HAVE_X86_SIMD
#define HLL_USE_AVX2 (simd_enabled && __builtin_cpu_supports("avx2"))
#else
#define HLL_USE_AVX2 0
#endif

#if defined(__aarch64__) && HAVE_ARM_NEON
#define HLL_USE_NEON (simd_enabled)
#else
#define HLL_USE_NEON 0
#endif


/* =========================== Low level bit macros ========================= */

/* Macros to access the dense representation.
 *
 * We need to get and set 6 bit counters in an array of 8 bit bytes.
 * We use macros to make sure the code is inlined since speed is critical
 * especially in order to compute the approximated cardinality in
 * HLLCOUNT where we need to access all the registers at once.
 * For the same reason we also want to avoid conditionals in this code path.
 *
 * +--------+--------+--------+------//
 * |11000000|22221111|33333322|55444444
 * +--------+--------+--------+------//
 *
 * Note: in the above representation the most significant bit (MSB)
 * of every byte is on the left. We start using bits from the LSB to MSB,
 * and so forth passing to the next byte.
 *
 * Example, we want to access to counter at pos = 1 ("111111" in the
 * illustration above).
 *
 * The index of the first byte b0 containing our data is:
 *
 *  b0 = 6 * pos / 8 = 0
 *
 *   +--------+
 *   |11000000|  <- Our byte at b0
 *   +--------+
 *
 * The position of the first bit (counting from the LSB = 0) in the byte
 * is given by:
 *
 *  fb = 6 * pos % 8 -> 6
 *
 * Right shift b0 of 'fb' bits.
 *
 *   +--------+
 *   |11000000|  <- Initial value of b0
 *   |00000011|  <- After right shift of 6 pos.
 *   +--------+
 *
 * Left shift b1 of bits 8-fb bits (2 bits)
 *
 *   +--------+
 *   |22221111|  <- Initial value of b1
 *   |22111100|  <- After left shift of 2 bits.
 *   +--------+
 *
 * OR the two bits, and finally AND with 111111 (63 in decimal) to
 * clean the higher order bits we are not interested in:
 *
 *   +--------+
 *   |00000011|  <- b0 right shifted
 *   |22111100|  <- b1 left shifted
 *   |22111111|  <- b0 OR b1
 *   |  111111|  <- (b0 OR b1) AND 63, our value.
 *   +--------+
 *
 * We can try with a different example, like pos = 0. In this case
 * the 6-bit counter is actually contained in a single byte.
 *
 *  b0 = 6 * pos / 8 = 0
 *
 *   +--------+
 *   |11000000|  <- Our byte at b0
 *   +--------+
 *
 *  fb = 6 * pos % 8 = 0
 *
 *  So we right shift of 0 bits (no shift in practice) and
 *  left shift the next byte of 8 bits, even if we don't use it,
 *  but this has the effect of clearing the bits so the result
 *  will not be affected after the OR.
 *
 * -------------------------------------------------------------------------
 *
 * Setting the register is a bit more complex, let's assume that 'val'
 * is the value we want to set, already in the right range.
 *
 * We need two steps, in one we need to clear the bits, and in the other
 * we need to bitwise-OR the new bits.
 *
 * Let's try with 'pos' = 1, so our first byte at 'b' is 0,
 *
 * "fb" is 6 in this case.
 *
 *   +--------+
 *   |11000000|  <- Our byte at b0
 *   +--------+
 *
 * To create an AND-mask to clear the bits about this position, we just
 * initialize the mask with the value 63, left shift it of "fs" bits,
 * and finally invert the result.
 *
 *   +--------+
 *   |00111111|  <- "mask" starts at 63
 *   |11000000|  <- "mask" after left shift of "ls" bits.
 *   |00111111|  <- "mask" after invert.
 *   +--------+
 *
 * Now we can bitwise-AND the byte at "b" with the mask, and bitwise-OR
 * it with "val" left-shifted of "ls" bits to set the new bits.
 *
 * Now let's focus on the next byte b1:
 *
 *   +--------+
 *   |22221111|  <- Initial value of b1
 *   +--------+
 *
 * To build the AND mask we start again with the 63 value, right shift
 * it by 8-fb bits, and invert it.
 *
 *   +--------+
 *   |00111111|  <- "mask" set at 2&6-1
 *   |00001111|  <- "mask" after the right shift by 8-fb = 2 bits
 *   |11110000|  <- "mask" after bitwise not.
 *   +--------+
 *
 * Now we can mask it with b+1 to clear the old bits, and bitwise-OR
 * with "val" left-shifted by "rs" bits to set the new value.
 */

/* Note: if we access the last counter, we will also access the b+1 byte
 * that is out of the array, but sds strings always have an implicit null
 * term, so the byte exists, and we can skip the conditional (or the need
 * to allocate 1 byte more explicitly). */

/* Store the value of the register at position 'regnum' into variable 'target'.
 * 'p' is an array of unsigned bytes. */
#define HLL_DENSE_GET_REGISTER(target, p, regnum)                 \
    do {                                                          \
        uint8_t *_p = (uint8_t *)p;                               \
        unsigned long _byte = regnum * HLL_BITS / 8;              \
        unsigned long _fb = regnum * HLL_BITS & 7;                \
        unsigned long _fb8 = 8 - _fb;                             \
        unsigned long b0 = _p[_byte];                             \
        unsigned long b1 = _p[_byte + 1];                         \
        target = ((b0 >> _fb) | (b1 << _fb8)) & HLL_REGISTER_MAX; \
    } while (0)

/* Set the value of the register at position 'regnum' to 'val'.
 * 'p' is an array of unsigned bytes. */
#define HLL_DENSE_SET_REGISTER(p, regnum, val)         \
    do {                                               \
        uint8_t *_p = (uint8_t *)p;                    \
        unsigned long _byte = (regnum) * HLL_BITS / 8; \
        unsigned long _fb = (regnum) * HLL_BITS & 7;   \
        unsigned long _fb8 = 8 - _fb;                  \
        unsigned long _v = (val);                      \
        _p[_byte] &= ~(HLL_REGISTER_MAX << _fb);       \
        _p[_byte] |= _v << _fb;                        \
        _p[_byte + 1] &= ~(HLL_REGISTER_MAX >> _fb8);  \
        _p[_byte + 1] |= _v >> _fb8;                   \
    } while (0)

/* Macros to access the sparse representation.
 * The macros parameter is expected to be an uint8_t pointer. */
#define HLL_SPARSE_XZERO_BIT 0x40                    /* 01xxxxxx */
#define HLL_SPARSE_VAL_BIT 0x80                      /* 1vvvvvxx */
#define HLL_SPARSE_IS_ZERO(p) (((*(p)) & 0xc0) == 0) /* 00xxxxxx */
#define HLL_SPARSE_IS_XZERO(p) (((*(p)) & 0xc0) == HLL_SPARSE_XZERO_BIT)
#define HLL_SPARSE_IS_VAL(p) ((*(p)) & HLL_SPARSE_VAL_BIT)
#define HLL_SPARSE_ZERO_LEN(p) (((*(p)) & 0x3f) + 1)
#define HLL_SPARSE_XZERO_LEN(p) (((((*(p)) & 0x3f) << 8) | (*((p) + 1))) + 1)
#define HLL_SPARSE_VAL_VALUE(p) ((((*(p)) >> 2) & 0x1f) + 1)
#define HLL_SPARSE_VAL_LEN(p) (((*(p)) & 0x3) + 1)
#define HLL_SPARSE_VAL_MAX_VALUE 32
#define HLL_SPARSE_VAL_MAX_LEN 4
#define HLL_SPARSE_ZERO_MAX_LEN 64
#define HLL_SPARSE_XZERO_MAX_LEN 16384
#define HLL_SPARSE_VAL_SET(p, val, len)                               \
    do {                                                              \
        *(p) = (((val) - 1) << 2 | ((len) - 1)) | HLL_SPARSE_VAL_BIT; \
    } while (0)
#define HLL_SPARSE_ZERO_SET(p, len) \
    do {                            \
        *(p) = (len) - 1;           \
    } while (0)
#define HLL_SPARSE_XZERO_SET(p, len)             \
    do {                                         \
        int _l = (len) - 1;                      \
        *(p) = (_l >> 8) | HLL_SPARSE_XZERO_BIT; \
        *((p) + 1) = (_l & 0xff);                \
    } while (0)
#define HLL_ALPHA_INF 0.721347520444481703680 /* constant for 0.5/ln(2) */

/* ========================= HyperLogLog algorithm  ========================= */

/* Our hash function is MurmurHash2, 64 bit version.
 * It was modified in order to provide the same result in
 * big and little endian archs (endian neutral). */
VALKEY_NO_SANITIZE("alignment")
uint64_t MurmurHash64A(const void *key, int len, unsigned int seed) {
    const uint64_t m = 0xc6a4a7935bd1e995;
    const int r = 47;
    uint64_t h = seed ^ (len * m);
    const uint8_t *data = (const uint8_t *)key;
    const uint8_t *end = data + (len - (len & 7));

    while (data != end) {
        uint64_t k;

#if (BYTE_ORDER == LITTLE_ENDIAN)
#ifdef USE_ALIGNED_ACCESS
        memcpy(&k, data, sizeof(uint64_t));
#else
        k = *((uint64_t *)data);
#endif
#else
        k = (uint64_t)data[0];
        k |= (uint64_t)data[1] << 8;
        k |= (uint64_t)data[2] << 16;
        k |= (uint64_t)data[3] << 24;
        k |= (uint64_t)data[4] << 32;
        k |= (uint64_t)data[5] << 40;
        k |= (uint64_t)data[6] << 48;
        k |= (uint64_t)data[7] << 56;
#endif

        k *= m;
        k ^= k >> r;
        k *= m;
        h ^= k;
        h *= m;
        data += 8;
    }

    switch (len & 7) {
    case 7: h ^= (uint64_t)data[6] << 48;   /* fall-thru */
    case 6: h ^= (uint64_t)data[5] << 40;   /* fall-thru */
    case 5: h ^= (uint64_t)data[4] << 32;   /* fall-thru */
    case 4: h ^= (uint64_t)data[3] << 24;   /* fall-thru */
    case 3: h ^= (uint64_t)data[2] << 16;   /* fall-thru */
    case 2: h ^= (uint64_t)data[1] << 8;    /* fall-thru */
    case 1: h ^= (uint64_t)data[0]; h *= m; /* fall-thru */
    };

    h ^= h >> r;
    h *= m;
    h ^= h >> r;
    return h;
}

/* Given a string element to add to the HyperLogLog, returns the length
 * of the pattern 000..1 of the element hash. As a side effect 'regp' is
 * set to the register index this element hashes to. */
int hllPatLen(unsigned char *ele, size_t elesize, long *regp) {
    uint64_t hash, index;
    int count;

    /* Count the number of zeroes starting from bit HLL_REGISTERS
     * (that is a power of two corresponding to the first bit we don't use
     * as index). The max run can be 64-P+1 = Q+1 bits.
     *
     * Note that the final "1" ending the sequence of zeroes must be
     * included in the count, so if we find "001" the count is 3, and
     * the smallest count possible is no zeroes at all, just a 1 bit
     * at the first position, that is a count of 1. */
    hash = MurmurHash64A(ele, elesize, 0xadc83b19ULL);
    index = hash & HLL_P_MASK;      /* Register index. */
    hash >>= HLL_P;                 /* Remove bits used to address the register. */
    hash |= ((uint64_t)1 << HLL_Q); /* Make sure count will be <= Q+1. */
    count = 1;                      /* Initialized to 1 since we count the "00000...1" pattern. */
    count += __builtin_ctzll(hash);
    *regp = (int)index;
    return count;
}

/* ================== Dense representation implementation  ================== */

/* Low level function to set the dense HLL register at 'index' to the
 * specified value if the current value is smaller than 'count'.
 *
 * 'registers' is expected to have room for HLL_REGISTERS plus an
 * additional byte on the right. This requirement is met by sds strings
 * automatically since they are implicitly null terminated.
 *
 * The function always succeed, however if as a result of the operation
 * the approximated cardinality changed, 1 is returned. Otherwise 0
 * is returned. */
int hllDenseSet(uint8_t *registers, long index, uint8_t count) {
    uint8_t oldcount;

    HLL_DENSE_GET_REGISTER(oldcount, registers, index);
    if (count > oldcount) {
        HLL_DENSE_SET_REGISTER(registers, index, count);
        return 1;
    } else {
        return 0;
    }
}

/* "Add" the element in the dense hyperloglog data structure.
 * Actually nothing is added, but the max 0 pattern counter of the subset
 * the element belongs to is incremented if needed.
 *
 * This is just a wrapper to hllDenseSet(), performing the hashing of the
 * element in order to retrieve the index and zero-run count. */
static int hllDenseAdd(uint8_t *registers, unsigned char *ele, size_t elesize) {
    long index;
    uint8_t count = hllPatLen(ele, elesize, &index);
    /* Update the register if this element produced a longer run of zeroes. */
    return hllDenseSet(registers, index, count);
}

/* Compute the register histogram in the dense representation. */
void hllDenseRegHisto(uint8_t *registers, int *reghisto) {
    int j;

    /* Default is to use 16384 registers 6 bits each. The code works
     * with other values by modifying the defines, but for our target value
     * we take a faster path with unrolled loops. */
    if (HLL_REGISTERS == 16384 && HLL_BITS == 6) {
        uint8_t *r = registers;
        unsigned long r0, r1, r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15;
        for (j = 0; j < 1024; j++) {
            /* Handle 16 registers per iteration. */
            r0 = r[0] & 63;
            r1 = (r[0] >> 6 | r[1] << 2) & 63;
            r2 = (r[1] >> 4 | r[2] << 4) & 63;
            r3 = (r[2] >> 2) & 63;
            r4 = r[3] & 63;
            r5 = (r[3] >> 6 | r[4] << 2) & 63;
            r6 = (r[4] >> 4 | r[5] << 4) & 63;
            r7 = (r[5] >> 2) & 63;
            r8 = r[6] & 63;
            r9 = (r[6] >> 6 | r[7] << 2) & 63;
            r10 = (r[7] >> 4 | r[8] << 4) & 63;
            r11 = (r[8] >> 2) & 63;
            r12 = r[9] & 63;
            r13 = (r[9] >> 6 | r[10] << 2) & 63;
            r14 = (r[10] >> 4 | r[11] << 4) & 63;
            r15 = (r[11] >> 2) & 63;

            reghisto[r0]++;
            reghisto[r1]++;
            reghisto[r2]++;
            reghisto[r3]++;
            reghisto[r4]++;
            reghisto[r5]++;
            reghisto[r6]++;
            reghisto[r7]++;
            reghisto[r8]++;
            reghisto[r9]++;
            reghisto[r10]++;
            reghisto[r11]++;
            reghisto[r12]++;
            reghisto[r13]++;
            reghisto[r14]++;
            reghisto[r15]++;

            r += 12;
        }
    } else {
        for (j = 0; j < HLL_REGISTERS; j++) {
            unsigned long reg;
            HLL_DENSE_GET_REGISTER(reg, registers, j);
            reghisto[reg]++;
        }
    }
}

/* ================== Sparse representation implementation  ================= */

/* Convert the HLL with sparse representation given as input in its dense
 * representation. Both representations are represented by SDS strings, and
 * the input representation is freed as a side effect.
 *
 * The function returns C_OK if the sparse representation was valid,
 * otherwise C_ERR is returned if the representation was corrupted. */
int hllSparseToDense(sds *hll_ptr) {
    sds sparse = *hll_ptr, dense;
    struct hllhdr *hdr, *oldhdr = (struct hllhdr *)sparse;
    int idx = 0, runlen, regval;
    uint8_t *p = (uint8_t *)sparse, *end = p + sdslen(sparse);
    int valid = 1;

    /* If the representation is already the right one return ASAP. */
    hdr = (struct hllhdr *)sparse;
    if (hdr->encoding == HLL_DENSE) return C_OK;

    /* Create a string of the right size filled with zero bytes.
     * Note that the cached cardinality is set to 0 as a side effect
     * that is exactly the cardinality of an empty HLL. */
    dense = sdsnewlen(NULL, HLL_DENSE_SIZE);
    hdr = (struct hllhdr *)dense;
    *hdr = *oldhdr; /* This will copy the magic and cached cardinality. */
    hdr->encoding = HLL_DENSE;

    /* Now read the sparse representation and set non-zero registers
     * accordingly. */
    p += HLL_HDR_SIZE;
    while (p < end) {
        if (HLL_SPARSE_IS_ZERO(p)) {
            runlen = HLL_SPARSE_ZERO_LEN(p);
            if ((runlen + idx) > HLL_REGISTERS) { /* Overflow. */
                valid = 0;
                break;
            }
            idx += runlen;
            p++;
        } else if (HLL_SPARSE_IS_XZERO(p)) {
            runlen = HLL_SPARSE_XZERO_LEN(p);
            if ((runlen + idx) > HLL_REGISTERS) { /* Overflow. */
                valid = 0;
                break;
            }
            idx += runlen;
            p += 2;
        } else {
            runlen = HLL_SPARSE_VAL_LEN(p);
            regval = HLL_SPARSE_VAL_VALUE(p);
            if ((runlen + idx) > HLL_REGISTERS) { /* Overflow. */
                valid = 0;
                break;
            }
            while (runlen--) {
                HLL_DENSE_SET_REGISTER(hdr->registers, idx, regval);
                idx++;
            }
            p++;
        }
    }

    /* If the sparse representation was valid, we expect to find idx
     * set to HLL_REGISTERS. */
    if (!valid || idx != HLL_REGISTERS) {
        sdsfree(dense);
        return C_ERR;
    }

    /* Free the old representation and set the new one. */
    sdsfree(*hll_ptr);
    *hll_ptr = dense;
    return C_OK;
}

/* Low level function to set the sparse HLL register at 'index' to the
 * specified value if the current value is smaller than 'count'.
 *
 * The object 'hll_ptr' is the sds string holding the HLL. The function requires
 * a reference to the object in order to be able to enlarge the string if
 * needed.
 *
 * On success, the function returns 1 if the cardinality changed, or 0
 * if the register for this element was not updated.
 * On error (if the representation is invalid) -1 is returned.
 *
 * As a side effect the function may promote the HLL representation from
 * sparse to dense: this happens when a register requires to be set to a value
 * not representable with the sparse representation, or when the resulting
 * size would be greater than HLL_SPARSE_MAX_BYTES. '*promoted' is set to 1 when
 * that happens, so that the caller can observe the encoding change. */
static int hllSparseSet(sds *hll_ptr, long index, uint8_t count, int *promoted) {
    struct hllhdr *hdr;
    sds hll = *hll_ptr;
    uint8_t oldcount, *sparse, *end, *p, *prev, *next;
    long first, span;
    long is_zero = 0, is_xzero = 0, is_val = 0, runlen = 0;

    /* If the count is too big to be representable by the sparse representation
     * switch to dense representation. */
    if (count > HLL_SPARSE_VAL_MAX_VALUE) goto promote;

    /* When updating a sparse representation, sometimes we may need to enlarge the
     * buffer for up to 3 bytes in the worst case (XZERO split into XZERO-VAL-XZERO),
     * and the following code does the enlarge job.
     * Actually, we use a greedy strategy, enlarge more than 3 bytes to avoid the need
     * for future reallocates on incremental growth. But we do not allocate more than
     * 'HLL_SPARSE_MAX_BYTES' bytes for the sparse representation.
     * If the available size of hyperloglog sds string is not enough for the increment
     * we need, we promote the hyperloglog to dense representation in 'step 3'.
     */
    if (sdsalloc(hll) < HLL_SPARSE_MAX_BYTES && sdsavail(hll) < 3) {
        size_t newlen = sdslen(hll) + 3;
        newlen +=
            min(newlen,
                300); /* Greediness: double 'newlen' if it is smaller than 300, or add 300 to it when it exceeds 300 */
        if (newlen > HLL_SPARSE_MAX_BYTES) newlen = HLL_SPARSE_MAX_BYTES;
        hll = *hll_ptr = sdsResize(hll, newlen);
    }

    /* Step 1: we need to locate the opcode we need to modify to check
     * if a value update is actually needed. */
    sparse = p = ((uint8_t *)hll) + HLL_HDR_SIZE;
    end = p + sdslen(hll) - HLL_HDR_SIZE;

    first = 0;
    prev = NULL; /* Points to previous opcode at the end of the loop. */
    next = NULL; /* Points to the next opcode at the end of the loop. */
    span = 0;
    while (p < end) {
        long oplen;

        /* Set span to the number of registers covered by this opcode.
         *
         * This is the most performance critical loop of the sparse
         * representation. Sorting the conditionals from the most to the
         * least frequent opcode in many-bytes sparse HLLs is faster. */
        oplen = 1;
        if (HLL_SPARSE_IS_ZERO(p)) {
            span = HLL_SPARSE_ZERO_LEN(p);
        } else if (HLL_SPARSE_IS_VAL(p)) {
            span = HLL_SPARSE_VAL_LEN(p);
        } else { /* XZERO. */
            span = HLL_SPARSE_XZERO_LEN(p);
            oplen = 2;
        }
        /* Break if this opcode covers the register as 'index'. */
        if (index <= first + span - 1) break;
        prev = p;
        p += oplen;
        first += span;
    }
    if (span == 0 || p >= end) return -1; /* Invalid format. */

    next = HLL_SPARSE_IS_XZERO(p) ? p + 2 : p + 1;
    if (next >= end) next = NULL;

    /* Cache current opcode type to avoid using the macro again and
     * again for something that will not change.
     * Also cache the run-length of the opcode. */
    if (HLL_SPARSE_IS_ZERO(p)) {
        is_zero = 1;
        runlen = HLL_SPARSE_ZERO_LEN(p);
    } else if (HLL_SPARSE_IS_XZERO(p)) {
        is_xzero = 1;
        runlen = HLL_SPARSE_XZERO_LEN(p);
    } else {
        is_val = 1;
        runlen = HLL_SPARSE_VAL_LEN(p);
    }

    /* Step 2: After the loop:
     *
     * 'first' stores to the index of the first register covered
     *  by the current opcode, which is pointed by 'p'.
     *
     * 'next' ad 'prev' store respectively the next and previous opcode,
     *  or NULL if the opcode at 'p' is respectively the last or first.
     *
     * 'span' is set to the number of registers covered by the current
     *  opcode.
     *
     * There are different cases in order to update the data structure
     * in place without generating it from scratch:
     *
     * A) If it is a VAL opcode already set to a value >= our 'count'
     *    no update is needed, regardless of the VAL run-length field.
     *    In this case PFADD returns 0 since no changes are performed.
     *
     * B) If it is a VAL opcode with len = 1 (representing only our
     *    register) and the value is less than 'count', we just update it
     *    since this is a trivial case. */
    if (is_val) {
        oldcount = HLL_SPARSE_VAL_VALUE(p);
        /* Case A. */
        if (oldcount >= count) return 0;

        /* Case B. */
        if (runlen == 1) {
            HLL_SPARSE_VAL_SET(p, count, 1);
            goto updated;
        }
    }

    /* C) Another trivial to handle case is a ZERO opcode with a len of 1.
     * We can just replace it with a VAL opcode with our value and len of 1. */
    if (is_zero && runlen == 1) {
        HLL_SPARSE_VAL_SET(p, count, 1);
        goto updated;
    }

    /* D) General case.
     *
     * The other cases are more complex: our register requires to be updated
     * and is either currently represented by a VAL opcode with len > 1,
     * by a ZERO opcode with len > 1, or by an XZERO opcode.
     *
     * In those cases the original opcode must be split into multiple
     * opcodes. The worst case is an XZERO split in the middle resulting into
     * XZERO - VAL - XZERO, so the resulting sequence max length is
     * 5 bytes.
     *
     * We perform the split writing the new sequence into the 'new' buffer
     * with 'newlen' as length. Later the new sequence is inserted in place
     * of the old one, possibly moving what is on the right a few bytes
     * if the new sequence is longer than the older one. */
    uint8_t seq[5], *n = seq;
    int last = first + span - 1; /* Last register covered by the sequence. */
    int len;

    if (is_zero || is_xzero) {
        /* Handle splitting of ZERO / XZERO. */
        if (index != first) {
            len = index - first;
            if (len > HLL_SPARSE_ZERO_MAX_LEN) {
                HLL_SPARSE_XZERO_SET(n, len);
                n += 2;
            } else {
                HLL_SPARSE_ZERO_SET(n, len);
                n++;
            }
        }
        HLL_SPARSE_VAL_SET(n, count, 1);
        n++;
        if (index != last) {
            len = last - index;
            if (len > HLL_SPARSE_ZERO_MAX_LEN) {
                HLL_SPARSE_XZERO_SET(n, len);
                n += 2;
            } else {
                HLL_SPARSE_ZERO_SET(n, len);
                n++;
            }
        }
    } else {
        /* Handle splitting of VAL. */
        int curval = HLL_SPARSE_VAL_VALUE(p);

        if (index != first) {
            len = index - first;
            HLL_SPARSE_VAL_SET(n, curval, len);
            n++;
        }
        HLL_SPARSE_VAL_SET(n, count, 1);
        n++;
        if (index != last) {
            len = last - index;
            HLL_SPARSE_VAL_SET(n, curval, len);
            n++;
        }
    }

    /* Step 3: substitute the new sequence with the old one.
     *
     * Note that we already allocated space on the sds string
     * calling sdsResize(). */
    int seqlen = n - seq;
    int oldlen = is_xzero ? 2 : 1;
    int deltalen = seqlen - oldlen;

    if (deltalen > 0 && sdslen(hll) + deltalen > HLL_SPARSE_MAX_BYTES) goto promote;
    serverAssert(sdslen(hll) + deltalen <= sdsalloc(hll));
    if (deltalen && next) memmove(next + deltalen, next, end - next);
    sdsIncrLen(hll, deltalen);
    memcpy(p, seq, seqlen);
    end += deltalen;

updated:
    /* Step 4: Merge adjacent values if possible.
     *
     * The representation was updated, however the resulting representation
     * may not be optimal: adjacent VAL opcodes can sometimes be merged into
     * a single one. */
    p = prev ? prev : sparse;
    int scanlen = 5; /* Scan up to 5 upcodes starting from prev. */
    while (p < end && scanlen--) {
        if (HLL_SPARSE_IS_XZERO(p)) {
            p += 2;
            continue;
        } else if (HLL_SPARSE_IS_ZERO(p)) {
            p++;
            continue;
        }
        /* We need two adjacent VAL opcodes to try a merge, having
         * the same value, and a len that fits the VAL opcode max len. */
        if (p + 1 < end && HLL_SPARSE_IS_VAL(p + 1)) {
            int v1 = HLL_SPARSE_VAL_VALUE(p);
            int v2 = HLL_SPARSE_VAL_VALUE(p + 1);
            if (v1 == v2) {
                int len = HLL_SPARSE_VAL_LEN(p) + HLL_SPARSE_VAL_LEN(p + 1);
                if (len <= HLL_SPARSE_VAL_MAX_LEN) {
                    HLL_SPARSE_VAL_SET(p + 1, v1, len);
                    memmove(p, p + 1, end - p);
                    sdsIncrLen(hll, -1);
                    end--;
                    /* After a merge we reiterate without incrementing 'p'
                     * in order to try to merge the just merged value with
                     * a value on its right. */
                    continue;
                }
            }
        }
        p++;
    }

    /* Invalidate the cached cardinality. */
    hdr = (struct hllhdr *)hll;
    HLL_INVALIDATE_CACHE(hdr);
    return 1;

promote:                                         /* Promote to dense representation. */
    if (hllSparseToDense(&hll) == C_ERR) return -1; /* Corrupted HLL. */
    *hll_ptr = hll;
    hdr = (struct hllhdr *)hll;

    /* We need to call hllDenseAdd() to perform the operation after the
     * conversion. However the result must be 1, since if we need to
     * convert from sparse to dense a register requires to be updated.
     *
     * Note that this in turn means that PFADD will make sure the command
     * is propagated to replicas / AOF, so if there is a sparse -> dense
     * conversion, it will be performed in all the replicas as well. */
    int dense_retval = hllDenseSet(hdr->registers, index, count);
    serverAssert(dense_retval == 1);
    *promoted = 1;
    return dense_retval;
}

/* "Add" the element in the sparse hyperloglog data structure.
 * Actually nothing is added, but the max 0 pattern counter of the subset
 * the element belongs to is incremented if needed.
 *
 * This function is actually a wrapper for hllSparseSet(), it only performs
 * the hashing of the element to obtain the index and zeros run length. */
static int hllSparseAdd(sds *hll_ptr, unsigned char *ele, size_t elesize, int *promoted) {
    long index;
    uint8_t count = hllPatLen(ele, elesize, &index);
    /* Update the register if this element produced a longer run of zeroes. */
    return hllSparseSet(hll_ptr, index, count, promoted);
}

/* Compute the register histogram in the sparse representation. */
void hllSparseRegHisto(uint8_t *sparse, int sparselen, int *invalid, int *reghisto) {
    int idx = 0, runlen, regval;
    uint8_t *end = sparse + sparselen, *p = sparse;
    int valid = 1;

    while (p < end) {
        if (HLL_SPARSE_IS_ZERO(p)) {
            runlen = HLL_SPARSE_ZERO_LEN(p);
            if ((runlen + idx) > HLL_REGISTERS) { /* Overflow. */
                valid = 0;
                break;
            }
            idx += runlen;
            reghisto[0] += runlen;
            p++;
        } else if (HLL_SPARSE_IS_XZERO(p)) {
            runlen = HLL_SPARSE_XZERO_LEN(p);
            if ((runlen + idx) > HLL_REGISTERS) { /* Overflow. */
                valid = 0;
                break;
            }
            idx += runlen;
            reghisto[0] += runlen;
            p += 2;
        } else {
            runlen = HLL_SPARSE_VAL_LEN(p);
            regval = HLL_SPARSE_VAL_VALUE(p);
            if ((runlen + idx) > HLL_REGISTERS) { /* Overflow. */
                valid = 0;
                break;
            }
            idx += runlen;
            reghisto[regval] += runlen;
            p++;
        }
    }
    if ((!valid || idx != HLL_REGISTERS) && invalid) *invalid = 1;
}

/* ========================= HyperLogLog Count ==============================
 * This is the core of the algorithm where the approximated count is computed.
 * The function uses the lower level hllDenseRegHisto() and hllSparseRegHisto()
 * functions as helpers to compute histogram of register values part of the
 * computation, which is representation-specific, while all the rest is common. */

/* Implements the register histogram calculation for uint8_t data type
 * which is only used internally as speedup for PFCOUNT with multiple keys. */
void hllRawRegHisto(uint8_t *registers, int *reghisto) {
    uint64_t *word = (uint64_t *)registers;
    uint8_t *bytes;
    int j;

    for (j = 0; j < HLL_REGISTERS / 8; j++) {
        if (*word == 0) {
            reghisto[0] += 8;
        } else {
            bytes = (uint8_t *)word;
            reghisto[bytes[0]]++;
            reghisto[bytes[1]]++;
            reghisto[bytes[2]]++;
            reghisto[bytes[3]]++;
            reghisto[bytes[4]]++;
            reghisto[bytes[5]]++;
            reghisto[bytes[6]]++;
            reghisto[bytes[7]]++;
        }
        word++;
    }
}

/* Helper function sigma as defined in
 * "New cardinality estimation algorithms for HyperLogLog sketches"
 * Otmar Ertl, arXiv:1702.01284 */
double hllSigma(double x) {
    if (x == 1.) return INFINITY;
    double zPrime;
    double y = 1;
    double z = x;
    do {
        x *= x;
        zPrime = z;
        z += x * y;
        y += y;
    } while (zPrime != z);
    return z;
}

/* Helper function tau as defined in
 * "New cardinality estimation algorithms for HyperLogLog sketches"
 * Otmar Ertl, arXiv:1702.01284 */
double hllTau(double x) {
    if (x == 0. || x == 1.) return 0.;
    double zPrime;
    double y = 1.0;
    double z = 1 - x;
    do {
        x = sqrt(x);
        zPrime = z;
        y *= 0.5;
        z -= pow(1 - x, 2) * y;
    } while (zPrime != z);
    return z / 3;
}

/* Return the approximated cardinality of the set based on the harmonic
 * mean of the registers values. 'hdr' points to the start of the SDS
 * representing the String object holding the HLL representation.
 *
 * If the sparse representation of the HLL object is not valid, the integer
 * pointed by 'invalid' is set to non-zero, otherwise it is left untouched.
 *
 * hllCount() supports a special internal-only encoding of HLL_RAW, that
 * is, hdr->registers will point to an uint8_t array of HLL_REGISTERS element.
 * This is useful in order to speedup PFCOUNT when called against multiple
 * keys (no need to work with 6-bit integers encoding). */
uint64_t hllCount(struct hllhdr *hdr, int *invalid) {
    double m = HLL_REGISTERS;
    double E;
    int j;
    /* Note that reghisto size could be just HLL_Q+2, because HLL_Q+1 is
     * the maximum frequency of the "000...1" sequence the hash function is
     * able to return. However it is slow to check for sanity of the
     * input: instead we history array at a safe size: overflows will
     * just write data to wrong, but correctly allocated, places. */
    int reghisto[64] = {0};

    /* Compute register histogram */
    if (hdr->encoding == HLL_DENSE) {
        hllDenseRegHisto(hdr->registers, reghisto);
    } else if (hdr->encoding == HLL_SPARSE) {
        hllSparseRegHisto(hdr->registers, sdslen((sds)hdr) - HLL_HDR_SIZE, invalid, reghisto);
    } else if (hdr->encoding == HLL_RAW) {
        hllRawRegHisto(hdr->registers, reghisto);
    } else {
        serverPanic("Unknown HyperLogLog encoding in hllCount()");
    }

    /* Estimate cardinality from register histogram. See:
     * "New cardinality estimation algorithms for HyperLogLog sketches"
     * Otmar Ertl, arXiv:1702.01284 */
    double z = m * hllTau((m - reghisto[HLL_Q + 1]) / (double)m);
    for (j = HLL_Q; j >= 1; --j) {
        z += reghisto[j];
        z *= 0.5;
    }
    z += m * hllSigma(reghisto[0] / (double)m);
    E = llroundl(HLL_ALPHA_INF * m * m / z);

    return (uint64_t)E;
}

#if 0
/* Call hllDenseAdd() or hllSparseAdd() according to the HLL encoding. */
static int hllAdd(robj *o, unsigned char *ele, size_t elesize) {
    struct hllhdr *hdr = objectGetVal(o);
    switch (hdr->encoding) {
    case HLL_DENSE: return hllDenseAdd(hdr->registers, ele, elesize);
    case HLL_SPARSE: return hllSparseAdd(o, ele, elesize);
    default: return -1; /* Invalid representation. */
    }
}
#endif

#if HAVE_X86_SIMD
/* A specialized version of hllMergeDense, optimized for default configurations.
 *
 * Requirements:
 * 1) HLL_REGISTERS == 16384 && HLL_BITS == 6
 * 2) The CPU supports AVX2 (checked at runtime in hllMergeDense)
 *
 * reg_raw: pointer to the raw representation array (16384 bytes, one byte per register)
 * reg_dense: pointer to the dense representation array (12288 bytes, 6 bits per register)
 */
ATTRIBUTE_TARGET_AVX2
void hllMergeDenseAVX2(uint8_t *reg_raw, const uint8_t *reg_dense) {
    /* Shuffle indices for unpacking bytes of dense registers
     * From: {XXXX|AAAB|BBCC|CDDD|EEEF|FFGG|GHHH|XXXX}
     * To:   {AAA0|BBB0|CCC0|DDD0|EEE0|FFF0|GGG0|HHH0}
     */
    const __m256i shuffle = _mm256_setr_epi8( //
        4, 5, 6, -1,                          //
        7, 8, 9, -1,                          //
        10, 11, 12, -1,                       //
        13, 14, 15, -1,                       //
        0, 1, 2, -1,                          //
        3, 4, 5, -1,                          //
        6, 7, 8, -1,                          //
        9, 10, 11, -1                         //
    );

    /* Merge the first 8 registers (6 bytes) normally
     * as the AVX2 algorithm needs 4 padding bytes at the start */
    uint8_t val;
    for (int i = 0; i < 8; i++) {
        HLL_DENSE_GET_REGISTER(val, reg_dense, i);
        if (val > reg_raw[i]) {
            reg_raw[i] = val;
        }
    }

    /* Dense to Raw:
     *
     * 4 registers in 3 bytes:
     * {bbaaaaaa|ccccbbbb|ddddddcc}
     *
     * LOAD 32 bytes (32 registers) per iteration:
     * 4(padding) + 12(16 registers) + 12(16 registers) + 4(padding)
     * {XXXX|AAAB|BBCC|CDDD|EEEF|FFGG|GHHH|XXXX}
     *
     * SHUFFLE to:
     * {AAA0|BBB0|CCC0|DDD0|EEE0|FFF0|GGG0|HHH0}
     * {bbaaaaaa|ccccbbbb|ddddddcc|00000000} x8
     *
     * AVX2 is little endian, each of the 8 groups is a little-endian int32.
     * A group (int32) contains 3 valid bytes (4 registers) and a zero byte.
     *
     * extract registers in each group with AND and SHIFT:
     * {00aaaaaa|00000000|00000000|00000000} x8 (<<0)
     * {00000000|00bbbbbb|00000000|00000000} x8 (<<2)
     * {00000000|00000000|00cccccc|00000000} x8 (<<4)
     * {00000000|00000000|00000000|00dddddd} x8 (<<6)
     *
     * merge the extracted registers with OR:
     * {00aaaaaa|00bbbbbb|00cccccc|00dddddd} x8
     *
     * Finally, compute MAX(reg_raw, merged) and STORE it back to reg_raw
     */

    /* Skip 8 registers (6 bytes) */
    const uint8_t *r = reg_dense + 6 - 4;
    uint8_t *t = reg_raw + 8;

    for (int i = 0; i < HLL_REGISTERS / 32 - 1; ++i) {
        __m256i x0, x;
        x0 = _mm256_loadu_si256((__m256i *)r);
        x = _mm256_shuffle_epi8(x0, shuffle);

        __m256i a1, a2, a3, a4;
        a1 = _mm256_and_si256(x, _mm256_set1_epi32(0x0000003f));
        a2 = _mm256_and_si256(x, _mm256_set1_epi32(0x00000fc0));
        a3 = _mm256_and_si256(x, _mm256_set1_epi32(0x0003f000));
        a4 = _mm256_and_si256(x, _mm256_set1_epi32(0x00fc0000));

        a2 = _mm256_slli_epi32(a2, 2);
        a3 = _mm256_slli_epi32(a3, 4);
        a4 = _mm256_slli_epi32(a4, 6);

        __m256i y1, y2, y;
        y1 = _mm256_or_si256(a1, a2);
        y2 = _mm256_or_si256(a3, a4);
        y = _mm256_or_si256(y1, y2);

        __m256i z = _mm256_loadu_si256((__m256i *)t);

        z = _mm256_max_epu8(z, y);

        _mm256_storeu_si256((__m256i *)t, z);

        r += 24;
        t += 32;
    }

    /* Merge the last 24 registers normally
     * as the AVX2 algorithm needs 4 padding bytes at the end */
    for (int i = HLL_REGISTERS - 24; i < HLL_REGISTERS; i++) {
        HLL_DENSE_GET_REGISTER(val, reg_dense, i);
        if (val > reg_raw[i]) {
            reg_raw[i] = val;
        }
    }
}
#endif

#if HAVE_ARM_NEON
/*
 * hllMergeDenseNEON is an ARM optimized version of hllMergeDense using NEON
 *
 * This function merges HyperLogLog (HLL) dense registers using ARM NEON SIMD instructions.
 * It extracts 6 bits registers from a dense format, and stores them in raw format
 *
 * Parameters:
 * - reg_raw: Pointer to the raw register array
 * - reg_dense: Pointer to the dense register array
 */
void hllMergeDenseNEON(uint8_t *reg_raw, const uint8_t *reg_dense) {
    uint8_t *dense_ptr = (uint8_t *)reg_dense;
    uint8_t *raw_ptr = (uint8_t *)reg_raw;

    uint8x16_t idx = {0, 1, 2, 0xFF,
                      3, 4, 5, 0xFF,
                      6, 7, 8, 0xFF,
                      9, 10, 11, 0xFF};

    // Bit masks for extracting specific bit ranges
    uint8x16_t mask1 = vreinterpretq_u8_u32(vdupq_n_u32(0x0000003f)); // Bits 0-5
    uint8x16_t mask2 = vreinterpretq_u8_u32(vdupq_n_u32(0x00000fc0)); // Bits 6-11
    uint8x16_t mask3 = vreinterpretq_u8_u32(vdupq_n_u32(0x0003f000)); // Bits 12-17
    uint8x16_t mask4 = vreinterpretq_u8_u32(vdupq_n_u32(0x00fc0000)); // Bits 18-23

    for (int i = 0; i < HLL_REGISTERS / 16 - 1; ++i) {
        /* Load 16 bytes from dense registers but only the first 12 bytes are processed because they contain
         * 16 registers, which is copied into 16 bytes raw registers.
         * The last 4 bytes are ignored because (1) they do not form a complete number of registers, and do not fit
         * in the 16 bytes. The unprocessed 4 bytes are processed in the next iteration.
         */
        uint8x16_t r = vld1q_u8(dense_ptr);

        /* Reorder bytes based on index mapping
         * Lookup indices
         *From: {AAAB|BBCC|CDDD}
         *To:   {AAA0|BBB0|CCC0|DDD0}
         */
        uint8x16_t x = vqtbl1q_u8(r, idx);

        // Extract and isolate registers
        uint8x16_t a1 = vandq_u8(x, mask1);
        uint8x16_t a2 = vandq_u8(x, mask2);
        uint8x16_t a3 = vandq_u8(x, mask3);
        uint8x16_t a4 = vandq_u8(x, mask4);

        // Align extracted values by shifting left
        uint32x4_t a2_32 = vreinterpretq_u32_u8(a2);
        a2_32 = vshlq_n_u32(a2_32, 2);
        a2 = vreinterpretq_u8_u32(a2_32);

        uint32x4_t a3_32 = vreinterpretq_u32_u8(a3);
        a3_32 = vshlq_n_u32(a3_32, 4);
        a3 = vreinterpretq_u8_u32(a3_32);

        uint32x4_t a4_32 = vreinterpretq_u32_u8(a4);
        a4_32 = vshlq_n_u32(a4_32, 6);
        a4 = vreinterpretq_u8_u32(a4_32);

        // Combine extracted values
        uint8x16_t y1 = vorrq_u8(a1, a2);
        uint8x16_t y2 = vorrq_u8(a3, a4);
        uint8x16_t y = vorrq_u8(y1, y2);

        // Load current raw register values
        uint8x16_t z = vld1q_u8(raw_ptr);

        // Update raw registers with max values
        z = vmaxq_u8(z, y);

        // Store updated values
        vst1q_u8(raw_ptr, z);

        raw_ptr += 16;
        dense_ptr += 12;
    }

    /* Process remaining registers, we do this manually because we don't want to over-read 4 bytes */
    uint8_t val;
    for (int i = HLL_REGISTERS - 16; i < HLL_REGISTERS; i++) {
        HLL_DENSE_GET_REGISTER(val, reg_dense, i);
        if (val > reg_raw[i]) {
            reg_raw[i] = val; // Update raw register if new value is greater
        }
    }
}
#endif /* HAVE_ARM_NEON */

/* Merge dense-encoded registers to raw registers array. */
void hllMergeDense(uint8_t *reg_raw, const uint8_t *reg_dense) {
#if HAVE_X86_SIMD
    if (HLL_REGISTERS == 16384 && HLL_BITS == 6) {
        if (HLL_USE_AVX2) {
            hllMergeDenseAVX2(reg_raw, reg_dense);
            return;
        }
    }
#endif
#if defined(__aarch64__) && HAVE_ARM_NEON && HLL_REGISTERS == 16384 && HLL_BITS == 6
    if (HLL_USE_NEON) {
        hllMergeDenseNEON(reg_raw, reg_dense);
        return;
    }
#endif

    uint8_t val;
    for (int i = 0; i < HLL_REGISTERS; i++) {
        HLL_DENSE_GET_REGISTER(val, reg_dense, i);
        if (val > reg_raw[i]) {
            reg_raw[i] = val;
        }
    }
}

#if 0
/* Merge by computing MAX(registers[i],hll[i]) the HyperLogLog 'hll'
 * with an array of uint8_t HLL_REGISTERS registers pointed by 'max'.
 *
 * The hll object must be already validated via isHLLObjectOrReply()
 * or in some other way.
 *
 * If the HyperLogLog is sparse and is found to be invalid, C_ERR
 * is returned, otherwise the function always succeeds. */
int hllMerge(uint8_t *max, robj *hll) {
    struct hllhdr *hdr = objectGetVal(hll);
    int i;

    if (hdr->encoding == HLL_DENSE) {
        hllMergeDense(max, hdr->registers);
    } else {
        uint8_t *p = objectGetVal(hll), *end = p + sdslen(objectGetVal(hll));
        long runlen, regval;
        int valid = 1;

        p += HLL_HDR_SIZE;
        i = 0;
        while (p < end) {
            if (HLL_SPARSE_IS_ZERO(p)) {
                runlen = HLL_SPARSE_ZERO_LEN(p);
                if ((runlen + i) > HLL_REGISTERS) { /* Overflow. */
                    valid = 0;
                    break;
                }
                i += runlen;
                p++;
            } else if (HLL_SPARSE_IS_XZERO(p)) {
                runlen = HLL_SPARSE_XZERO_LEN(p);
                if ((runlen + i) > HLL_REGISTERS) { /* Overflow. */
                    valid = 0;
                    break;
                }
                i += runlen;
                p += 2;
            } else {
                runlen = HLL_SPARSE_VAL_LEN(p);
                regval = HLL_SPARSE_VAL_VALUE(p);
                if ((runlen + i) > HLL_REGISTERS) { /* Overflow. */
                    valid = 0;
                    break;
                }
                while (runlen--) {
                    if (regval > max[i]) max[i] = regval;
                    i++;
                }
                p++;
            }
        }
        if (!valid || i != HLL_REGISTERS) return C_ERR;
    }
    return C_OK;
}
#endif

#if HAVE_X86_SIMD
/* A specialized version of hllDenseCompress, optimized for default configurations.
 *
 * Requirements:
 * 1) HLL_REGISTERS == 16384 && HLL_BITS == 6
 * 2) The CPU supports AVX2 (checked at runtime in hllDenseCompress)
 *
 * reg_dense: pointer to the dense representation array (12288 bytes, 6 bits per register)
 * reg_raw: pointer to the raw representation array (16384 bytes, one byte per register)
 */
ATTRIBUTE_TARGET_AVX2
void hllDenseCompressAVX2(uint8_t *reg_dense, const uint8_t *reg_raw) {
    /* Shuffle indices for packing bytes of dense registers
     * From: {AAA0|BBB0|CCC0|DDD0|EEE0|FFF0|GGG0|HHH0}
     * To:   {AAAB|BBCC|CDDD|0000|EEEF|FFGG|GHHH|0000}
     */
    const __m256i shuffle = _mm256_setr_epi8( //
        0, 1, 2,                              //
        4, 5, 6,                              //
        8, 9, 10,                             //
        12, 13, 14,                           //
        -1, -1, -1, -1,                       //
        0, 1, 2,                              //
        4, 5, 6,                              //
        8, 9, 10,                             //
        12, 13, 14,                           //
        -1, -1, -1, -1                        //
    );

    /* Raw to Dense:
     *
     * LOAD 32 bytes (32 registers) per iteration:
     * {00aaaaaa|00bbbbbb|00cccccc|00dddddd} x8
     *
     * AVX2 is little endian, each of the 8 groups is a little-endian int32.
     * A group (int32) contains 4 registers.
     *
     * move the registers to correct positions with AND and SHIFT:
     * {00aaaaaa|00000000|00000000|00000000} x8 (>>0)
     * {bb000000|0000bbbb|00000000|00000000} x8 (>>2)
     * {00000000|cccc0000|000000cc|00000000} x8 (>>4)
     * {00000000|00000000|dddddd00|00000000} x8 (>>6)
     *
     * merge the registers with OR:
     * {bbaaaaaa|ccccbbbb|ddddddcc|00000000} x8
     * {AAA0|BBB0|CCC0|DDD0|EEE0|FFF0|GGG0|HHH0}
     *
     * SHUFFLE to:
     * {AAAB|BBCC|CDDD|0000|EEEF|FFGG|GHHH|0000}
     *
     * STORE the lower half and higher half respectively:
     * AAABBBCCCDDD0000
     *             EEEFFFGGGHHH0000
     * AAABBBCCCDDDEEEFFFGGGHHH0000
     *
     * Note that the last 4 bytes are padding bytes.
     */

    const uint8_t *r = reg_raw;
    uint8_t *t = reg_dense;

    for (int i = 0; i < HLL_REGISTERS / 32 - 1; ++i) {
        __m256i x = _mm256_loadu_si256((__m256i *)r);

        __m256i a1, a2, a3, a4;
        a1 = _mm256_and_si256(x, _mm256_set1_epi32(0x0000003f));
        a2 = _mm256_and_si256(x, _mm256_set1_epi32(0x00003f00));
        a3 = _mm256_and_si256(x, _mm256_set1_epi32(0x003f0000));
        a4 = _mm256_and_si256(x, _mm256_set1_epi32(0x3f000000));

        a2 = _mm256_srli_epi32(a2, 2);
        a3 = _mm256_srli_epi32(a3, 4);
        a4 = _mm256_srli_epi32(a4, 6);

        __m256i y1, y2, y;
        y1 = _mm256_or_si256(a1, a2);
        y2 = _mm256_or_si256(a3, a4);
        y = _mm256_or_si256(y1, y2);
        y = _mm256_shuffle_epi8(y, shuffle);

        __m128i lower, higher;
        lower = _mm256_castsi256_si128(y);
        higher = _mm256_extracti128_si256(y, 1);

        _mm_storeu_si128((__m128i *)t, lower);
        _mm_storeu_si128((__m128i *)(t + 12), higher);

        r += 32;
        t += 24;
    }

    /* Merge the last 32 registers normally
     * as the AVX2 algorithm needs 4 padding bytes at the end */
    for (int i = HLL_REGISTERS - 32; i < HLL_REGISTERS; i++) {
        HLL_DENSE_SET_REGISTER(reg_dense, i, reg_raw[i]);
    }
}
#endif

#if HAVE_ARM_NEON
/*
 * hllDenseCompressNEON is ARM optimized version of hllDenseCompress using NEON.
 *
 * This function takes a raw register (`reg_raw`) and compresses it into a dense representation (`reg_dense`).
 * It uses NEON SIMD instructions to process multiple values at once.
 *
 * - The first loop processes most of the registers in 16-element blocks using NEON instructions.
 * - The second loop handles the remaining registers using a direct assignment macro.
 *
 */
void hllDenseCompressNEON(uint8_t *reg_dense, const uint8_t *reg_raw) {
    /* Shuffle indices for packing bytes of dense registers
     * From: {AAA0|BBB0|CCC0|DDD0}
     * To:   {AAAB|BBCC|CDDD|0000}
     */
    uint8x16_t idx = {
        0, 1, 2,               // Extract bytes from lane 0
        4, 5, 6,               // Extract bytes from lane 1
        8, 9, 10,              // Extract bytes from lane 2
        12, 13, 14,            // Extract bytes from lane 3
        0xFF, 0xFF, 0xFF, 0xFF // Zero out last 4 elements (padding)
    };

    // Bit masks for extracting first 6 bits from every byte within 32-bit lanes
    uint32x4_t mask1 = vdupq_n_u32(0x0000003F); // Extract bits 0-5
    uint32x4_t mask2 = vdupq_n_u32(0x00003F00); // Extract bits 8-13
    uint32x4_t mask3 = vdupq_n_u32(0x003F0000); // Extract bits 16-21
    uint32x4_t mask4 = vdupq_n_u32(0x3F000000); // Extract bits 24-29

    uint8_t *r = (uint8_t *)reg_raw;   // Input pointer
    uint8_t *t = (uint8_t *)reg_dense; // Output pointer

    // Process registers in blocks of 16 using NEON instructions
    // The last 16 registers are processed separately to avoid overwriting, as the final write is 12 bytes.
    for (int i = 0; i < HLL_REGISTERS / 16 - 1; i++) {
        // Load 16 bytes as 4x 32-bit values
        uint32x4_t x = vld1q_u32((uint32_t *)r);

        // Apply masks to extract a single register from every 4 registers, for every lane
        uint32x4_t a1 = vandq_u32(x, mask1);
        uint32x4_t a2 = vandq_u32(x, mask2);
        uint32x4_t a3 = vandq_u32(x, mask3);
        uint32x4_t a4 = vandq_u32(x, mask4);

        // Shift extracted bits to align them properly
        a2 = vshrq_n_u32(a2, 2);
        a3 = vshrq_n_u32(a3, 4);
        a4 = vshrq_n_u32(a4, 6);

        uint32x4_t y1 = vorrq_u32(a1, a2);
        uint32x4_t y2 = vorrq_u32(a3, a4);
        uint32x4_t y = vorrq_u32(y1, y2);

        // Perform a table lookup to shuffle extracted values and align them in 12 bytes
        vst1q_u8(t, vqtbl1q_u8(vreinterpretq_u8_u32(y), idx));

        t += 12;
        r += 16;
    }

    // Handle the remaining registers individually (12 bytes)
    for (int i = HLL_REGISTERS - 16; i < HLL_REGISTERS; i++) {
        HLL_DENSE_SET_REGISTER(reg_dense, i, reg_raw[i]);
    }
}
#endif /* HAVE_ARM_NEON */

/* Compress raw registers to dense representation. */
void hllDenseCompress(uint8_t *reg_dense, const uint8_t *reg_raw) {
#if HAVE_X86_SIMD && HLL_REGISTERS == 16384 && HLL_BITS == 6
    if (HLL_USE_AVX2) {
        hllDenseCompressAVX2(reg_dense, reg_raw);
        return;
    }

#endif

#if HAVE_ARM_NEON && HLL_REGISTERS == 16384 && HLL_BITS == 6
    if (HLL_USE_NEON) {
        hllDenseCompressNEON(reg_dense, reg_raw);
        return;
    }
#endif

    for (int i = 0; i < HLL_REGISTERS; i++) {
        HLL_DENSE_SET_REGISTER(reg_dense, i, reg_raw[i]);
    }
}

/* ========================== HyperLogLog commands ========================== */

#if 0
/* Create an HLL object. We always create the HLL using sparse encoding.
 * This will be upgraded to the dense representation as needed. */
robj *createHLLObject(void) {
    robj *o;
    struct hllhdr *hdr;
    sds s;
    uint8_t *p;
    int sparselen = HLL_HDR_SIZE + (((HLL_REGISTERS + (HLL_SPARSE_XZERO_MAX_LEN - 1)) / HLL_SPARSE_XZERO_MAX_LEN) * 2);
    int aux;

    /* Populate the sparse representation with as many XZERO opcodes as
     * needed to represent all the registers. */
    aux = HLL_REGISTERS;
    s = sdsnewlen(NULL, sparselen);
    p = (uint8_t *)s + HLL_HDR_SIZE;
    while (aux) {
        int xzero = HLL_SPARSE_XZERO_MAX_LEN;
        if (xzero > aux) xzero = aux;
        HLL_SPARSE_XZERO_SET(p, xzero);
        p += 2;
        aux -= xzero;
    }
    serverAssert((p - (uint8_t *)s) == sparselen);

    /* Create the actual object. */
    o = createObject(OBJ_STRING, s);
    hdr = objectGetVal(o);
    memcpy(hdr->magic, "HYLL", 4);
    hdr->encoding = HLL_SPARSE;
    return o;
}
#endif

/* ========================== Dragonfly custom functions ===================== */

enum HllValidness isValidHLL(struct HllBufferPtr hll_buffer) {
  struct hllhdr* hdr;

  if (hll_buffer.size < sizeof(*hdr)) {
    return HLL_INVALID;
  }

  hdr = (struct hllhdr*)hll_buffer.hll;

  /* Magic should be "HYLL". */
  if (hdr->magic[0] != 'H' || hdr->magic[1] != 'Y' || hdr->magic[2] != 'L' ||
      hdr->magic[3] != 'L') {
    return HLL_INVALID;
  }

  if (hdr->encoding > HLL_MAX_ENCODING) {
    return HLL_INVALID;
  }

  switch (hdr->encoding) {
    case HLL_DENSE:
      /* Dense representation string length should match exactly. */
      return (hll_buffer.size == HLL_DENSE_SIZE) ? HLL_VALID_DENSE : HLL_INVALID;
    case HLL_SPARSE:
      return HLL_VALID_SPARSE;
    default:
      return HLL_INVALID;
  }
}

size_t getDenseHllSize() {
  return HLL_DENSE_SIZE;
}

size_t getSparseHllInitSize() {
  return HLL_HDR_SIZE +
         (((HLL_REGISTERS + (HLL_SPARSE_XZERO_MAX_LEN - 1)) / HLL_SPARSE_XZERO_MAX_LEN) * 2);
}

int initSparseHll(struct HllBufferPtr hll_ptr) {
  if (hll_ptr.size != getSparseHllInitSize()) {
    return C_ERR;
  }

  memset(hll_ptr.hll, 0, hll_ptr.size);

  /* Populate the sparse representation with as many XZERO opcodes as
   * needed to represent all the registers. */
  int aux = HLL_REGISTERS;
  uint8_t* p = (uint8_t*)hll_ptr.hll + HLL_HDR_SIZE;
  while (aux) {
    int xzero = HLL_SPARSE_XZERO_MAX_LEN;
    if (xzero > aux)
      xzero = aux;
    HLL_SPARSE_XZERO_SET(p, xzero);
    p += 2;
    aux -= xzero;
  }

  struct hllhdr* hdr = (struct hllhdr*)hll_ptr.hll;

  memcpy(hdr->magic, "HYLL", 4);
  hdr->encoding = HLL_SPARSE;
  return C_OK;
}

int createDenseHll(struct HllBufferPtr hll_ptr) {
  if (hll_ptr.size != getDenseHllSize()) {
    return C_ERR;
  }

  memset(hll_ptr.hll, 0, hll_ptr.size);
  struct hllhdr* hdr = (struct hllhdr*)hll_ptr.hll;
  memcpy(hdr->magic, "HYLL", 4);
  hdr->encoding = HLL_DENSE;
  return C_OK;
}

/* This is a copied & modified version of hllSparseToDense() above that does not use sds.
 * Keep the run-length overflow checks in sync with it (CVE-2025-32023). */
int convertSparseToDenseHll(struct HllBufferPtr in_hll, struct HllBufferPtr out_hll) {
  struct hllhdr *hdr, *oldhdr = (struct hllhdr*)in_hll.hll;
  int idx = 0, runlen, regval;
  uint8_t *p = (uint8_t*)in_hll.hll, *end = p + in_hll.size;
  int valid = 1;

  if (oldhdr->encoding != HLL_SPARSE)
    return C_ERR;
  if (out_hll.size != getDenseHllSize())
    return C_ERR;

  /* Create a string of the right size filled with zero bytes.
   * Note that the cached cardinality is set to 0 as a side effect
   * that is exactly the cardinality of an empty HLL. */
  hdr = (struct hllhdr*)out_hll.hll;
  *hdr = *oldhdr; /* This will copy the magic and cached cardinality. */
  hdr->encoding = HLL_DENSE;

  /* Now read the sparse representation and set non-zero registers
   * accordingly. */
  p += HLL_HDR_SIZE;
  while (p < end) {
    if (HLL_SPARSE_IS_ZERO(p)) {
      runlen = HLL_SPARSE_ZERO_LEN(p);
      if ((runlen + idx) > HLL_REGISTERS) { /* Overflow. */
        valid = 0;
        break;
      }
      idx += runlen;
      p++;
    } else if (HLL_SPARSE_IS_XZERO(p)) {
      runlen = HLL_SPARSE_XZERO_LEN(p);
      if ((runlen + idx) > HLL_REGISTERS) { /* Overflow. */
        valid = 0;
        break;
      }
      idx += runlen;
      p += 2;
    } else {
      runlen = HLL_SPARSE_VAL_LEN(p);
      regval = HLL_SPARSE_VAL_VALUE(p);
      if ((runlen + idx) > HLL_REGISTERS) { /* Overflow. */
        valid = 0;
        break;
      }
      while (runlen--) {
        HLL_DENSE_SET_REGISTER(hdr->registers, idx, regval);
        idx++;
      }
      p++;
    }
  }

  /* If the sparse representation was valid, we expect to find idx
   * set to HLL_REGISTERS. */
  if (!valid || idx != HLL_REGISTERS) {
    return C_ERR;
  }

  return C_OK;
}

int pfadd_sparse(sds* hll_ptr, const unsigned char* value, size_t size, int* promoted) {
  int retval = hllSparseAdd(hll_ptr, (unsigned char*)value, size, promoted);
  switch (retval) {
    case 1: {
      /* Re-read the header: hllSparseAdd() may have reallocated the sds string,
       * or replaced it entirely when promoting to the dense encoding. */
      struct hllhdr* hdr = (struct hllhdr*)(*hll_ptr);
      HLL_INVALIDATE_CACHE(hdr);
      return 1;
    }
    default:
      return retval;
  }
}

int pfadd_dense(struct HllBufferPtr hll_ptr, const unsigned char* value, size_t size) {
  if (isValidHLL(hll_ptr) != HLL_VALID_DENSE)
    return C_ERR;

  struct hllhdr* hdr = (struct hllhdr*)hll_ptr.hll;

  /* Perform the low level ADD operation for every element. */
  int retval = hllDenseAdd(hdr->registers, (unsigned char*)value, size);
  switch (retval) {
    case 1:
      HLL_INVALIDATE_CACHE(hdr);
      return 1;
    default:
      return retval;
  }
}

int64_t pfcountSingle(struct HllBufferPtr hll_ptr) {
  uint64_t card;

  if (isValidHLL(hll_ptr) != HLL_VALID_DENSE)
    return C_ERR;

  /* Check if the cached cardinality is valid. */
  struct hllhdr* hdr = (struct hllhdr*)hll_ptr.hll;
  if (HLL_VALID_CACHE(hdr)) {
    /* Just return the cached value. */
    card = (uint64_t)hdr->card[0];
    card |= (uint64_t)hdr->card[1] << 8;
    card |= (uint64_t)hdr->card[2] << 16;
    card |= (uint64_t)hdr->card[3] << 24;
    card |= (uint64_t)hdr->card[4] << 32;
    card |= (uint64_t)hdr->card[5] << 40;
    card |= (uint64_t)hdr->card[6] << 48;
    card |= (uint64_t)hdr->card[7] << 56;
  } else {
    int invalid = 0;
    /* Recompute it and update the cached value. */
    card = hllCount(hdr, &invalid);
    if (invalid) {
      return -1;
    }
    hdr->card[0] = card & 0xff;
    hdr->card[1] = (card >> 8) & 0xff;
    hdr->card[2] = (card >> 16) & 0xff;
    hdr->card[3] = (card >> 24) & 0xff;
    hdr->card[4] = (card >> 32) & 0xff;
    hdr->card[5] = (card >> 40) & 0xff;
    hdr->card[6] = (card >> 48) & 0xff;
    hdr->card[7] = (card >> 56) & 0xff;
  }
  return card;
}

int64_t pfcountMulti(struct HllBufferPtr* hlls, size_t hlls_count) {
  struct hllhdr* hdr;
  uint8_t max[HLL_HDR_SIZE + HLL_REGISTERS];

  /* Compute an HLL with M[i] = MAX(M[i]_j). */
  memset(max, 0, sizeof(max));
  hdr = (struct hllhdr*)max;
  hdr->encoding = HLL_RAW; /* Special internal-only encoding. */

  /* hllCount() reads the raw registers from hdr->registers, so the merge must
   * target the same offset and not the start of the buffer. */
  uint8_t* registers = max + HLL_HDR_SIZE;
  for (size_t j = 0; j < hlls_count; j++) {
    /* Check type and size. */
    struct HllBufferPtr hll = hlls[j];
    if (isValidHLL(hll) != HLL_VALID_DENSE) {
      return C_ERR;
    }

    hllMergeDense(registers, ((struct hllhdr*)hll.hll)->registers);
  }

  /* Compute cardinality of the resulting set. */
  return hllCount(hdr, NULL);
}

int pfmerge(struct HllBufferPtr* in_hlls, size_t in_hlls_count, struct HllBufferPtr out_hll) {
  if (isValidHLL(out_hll) != HLL_VALID_DENSE) {
    return C_ERR;
  }

  uint8_t max[HLL_REGISTERS];

  /* Compute an HLL with M[i] = MAX(M[i]_j).
   * We store the maximum into the max array of registers. We'll write
   * it to the target variable later. */
  memset(max, 0, sizeof(max));

  for (size_t j = 0; j < in_hlls_count; j++) {
    struct HllBufferPtr hll = in_hlls[j];
    if (isValidHLL(hll) != HLL_VALID_DENSE) {
      return C_ERR;
    }

    hllMergeDense(max, ((struct hllhdr*)hll.hll)->registers);
  }

  struct hllhdr* hdr = (struct hllhdr*)out_hll.hll;
  hllDenseCompress(hdr->registers, max);
  HLL_INVALIDATE_CACHE(hdr);

  return C_OK;
}

/* Test-only knob mirroring valkey's `PFDEBUG SIMD (ON|OFF)`: lets unit tests
 * check that the AVX2/NEON fast paths agree with the scalar implementation.
 * Returns 1 if a SIMD fast path is in effect after the call. */
int hllEnableSimd(int enable) {
#if SIMD_SUPPORTED
  simd_enabled = enable;
#else
  (void)enable;
#endif
  return (HLL_USE_AVX2 || HLL_USE_NEON) ? 1 : 0;
}
