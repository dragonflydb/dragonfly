// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "core/sds_utils.h"

#include "base/endian.h"

extern "C" {
#include "redis/sds.h"
#include "redis/zmalloc.h"
}

namespace dfly {

namespace {

inline char SdsReqType(size_t string_size) {
  if (string_size < 1 << 5)
    return SDS_TYPE_5;
  if (string_size < 1 << 8)
    return SDS_TYPE_8;
  if (string_size < 1 << 16)
    return SDS_TYPE_16;
  if (string_size < 1ll << 32)
    return SDS_TYPE_32;
  return SDS_TYPE_64;
}

inline int SdsHdrSize(char type) {
  switch (type & SDS_TYPE_MASK) {
    case SDS_TYPE_5:
      return sizeof(struct sdshdr5);
    case SDS_TYPE_8:
      return sizeof(struct sdshdr8);
    case SDS_TYPE_16:
      return sizeof(struct sdshdr16);
    case SDS_TYPE_32:
      return sizeof(struct sdshdr32);
    case SDS_TYPE_64:
      return sizeof(struct sdshdr64);
  }
  return 0;
}

}  // namespace

void SdsUpdateExpireTime(const void* obj, uint32_t time_at, uint32_t offset) {
  sds str = (sds)obj;
  char* valptr = str + sdslen(str) + 1;
  absl::little_endian::Store32(valptr + offset, time_at);
}

char* AllocSdsWithSpace(uint32_t strlen, uint32_t space) {
  size_t usable;
  char type = SdsReqType(strlen);
  int hdrlen = SdsHdrSize(type);

  char* ptr = (char*)zmalloc_usable(hdrlen + strlen + 1 + space, &usable);
  char* s = ptr + hdrlen;
  char* fp = s - 1;

  switch (type) {
    case SDS_TYPE_5: {
      *fp = type | (strlen << SDS_TYPE_BITS);
      break;
    }

    case SDS_TYPE_8: {
      SDS_HDR_VAR(8, s);
      sh->len = strlen;
      sh->alloc = strlen;
      *fp = type;
      break;
    }

    case SDS_TYPE_16: {
      SDS_HDR_VAR(16, s);
      sh->len = strlen;
      sh->alloc = strlen;
      *fp = type;
      break;
    }

    case SDS_TYPE_32: {
      SDS_HDR_VAR(32, s);
      sh->len = strlen;
      sh->alloc = strlen;
      *fp = type;
      break;
    }
    case SDS_TYPE_64: {
      SDS_HDR_VAR(64, s);
      sh->len = strlen;
      sh->alloc = strlen;
      *fp = type;
      break;
    }
  }

  s[strlen] = '\0';
  return s;
}

}  // namespace dfly
