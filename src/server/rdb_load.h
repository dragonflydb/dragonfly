// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include <system_error>

extern "C" {
#include "redis/object.h"
}

#include "base/io_buf.h"
#include "io/io.h"
#include "server/common_types.h"

namespace dfly {

class RdbLoader {
 public:
  explicit RdbLoader();

  ~RdbLoader();

  std::error_code Load(::io::Source* src);

 private:
  using MutableBytes = ::io::MutableBytes;

  void ResizeDb(size_t key_num, size_t expire_num);
  std::error_code HandleAux();

  ::io::Result<uint8_t> FetchType() {
    return FetchInt<uint8_t>();
  }

  template <typename T> io::Result<T> FetchInt();

  io::Result<uint64_t> LoadLen(bool* is_encoded);
  std::error_code FetchBuf(size_t size, void* dest);

  // FetchGenericString may return various types. I basically copied the code
  // from rdb.c and tried not to shoot myself on the way.
  // flags are RDB_LOAD_XXX masks.
  using OpaqueBuf = std::pair<void*, size_t>;
  io::Result<OpaqueBuf> FetchGenericString(int flags);
  io::Result<OpaqueBuf> FetchIntegerObject(int enctype, int flags, size_t* lenptr);

  ::io::Result<sds> ReadKey();
  ::io::Result<robj*> ReadObj(int type);

  std::error_code EnsureRead(size_t min_sz) {
    if (mem_buf_.InputLen() >= min_sz)
      return std::error_code{};

    return EnsureReadInternal(min_sz);
  }

  std::error_code EnsureReadInternal(size_t min_sz);
  std::error_code LoadKeyValPair(int type);
  std::error_code VerifyChecksum();

  base::IoBuf mem_buf_;
  ::io::Source* src_ = nullptr;
  size_t bytes_read_ = 0;
  DbIndex cur_db_index_ = 0;
};

}  // namespace dfly
