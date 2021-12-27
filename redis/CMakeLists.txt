add_library(redis_lib endianconv.c intset.c listpack.c object.c 
            lzf_c.c lzf_d.c sds.c sha256.c
            quicklist.c util.c zmalloc.c)

cxx_link(redis_lib)
