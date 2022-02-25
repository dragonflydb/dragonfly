option(REDIS_ZMALLOC_MI "Implement zmalloc layer using mimalloc allocator" ON)

if (REDIS_ZMALLOC_MI)
  set(ZMALLOC_SRC "zmalloc_mi.c")
  set(ZMALLOC_DEPS "TRDP::mimalloc")
else()
  set(ZMALLOC_SRC "zmalloc.c")
  set(ZMALLOC_DEPS "")
endif()

add_library(redis_lib crc64.c crcspeed.c debug.c dict.c endianconv.c intset.c 
            listpack.c mt19937-64.c object.c lzf_c.c lzf_d.c sds.c sha256.c
            quicklist.c redis_aux.c siphash.c t_zset.c util.c ${ZMALLOC_SRC})

cxx_link(redis_lib  ${ZMALLOC_DEPS})

if (REDIS_ZMALLOC_MI)
  target_compile_definitions(redis_lib PUBLIC USE_ZMALLOC_MI)
endif()
