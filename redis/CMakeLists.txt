add_library(redis_lib crc64.c crcspeed.c debug.c dict.c endianconv.c intset.c 
            listpack.c mt19937-64.c object.c lzf_c.c lzf_d.c sds.c sha256.c
            quicklist.c redis_aux.c siphash.c t_zset.c util.c zmalloc.c)

cxx_link(redis_lib)
