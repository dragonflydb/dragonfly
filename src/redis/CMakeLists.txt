option(REDIS_ZMALLOC_MI "Implement zmalloc layer using mimalloc allocator" ON)

if (REDIS_ZMALLOC_MI)
  set(ZMALLOC_SRC "zmalloc_mi.c")
  set(ZMALLOC_DEPS "TRDP::mimalloc")
else()
  set(ZMALLOC_SRC "zmalloc.c")
  set(ZMALLOC_DEPS "")
endif()

add_library(redis_lib crc16.c crc64.c crcspeed.c debug.c  intset.c geo.c 
            geohash.c geohash_helper.c t_zset.c
            listpack.c lzf_c.c lzf_d.c sds.c
            quicklist.c rax.c redis_aux.c t_stream.c 
            util.c ziplist.c hyperloglog.c ${ZMALLOC_SRC})

cxx_link(redis_lib  ${ZMALLOC_DEPS})

add_library(redis_test_lib dict.c siphash.c)
cxx_link(redis_test_lib redis_lib)

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  target_compile_options(redis_lib PRIVATE -Wno-maybe-uninitialized)
endif()

if (REDIS_ZMALLOC_MI)
  target_compile_definitions(redis_lib PUBLIC USE_ZMALLOC_MI)
endif()

add_subdirectory(lua)
