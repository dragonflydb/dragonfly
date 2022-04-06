add_library(dfly_facade dragonfly_listener.cc dragonfly_connection.cc facade.cc
            memcache_parser.cc redis_parser.cc reply_builder.cc)
cxx_link(dfly_facade base uring_fiber_lib fibers_ext strings_lib http_server_lib 
         tls_lib TRDP::mimalloc TRDP::dconv)

add_library(facade_test facade_test.cc)
cxx_link(facade_test dfly_facade gtest_main_ext)

cxx_test(memcache_parser_test dfly_facade LABELS DFLY)
cxx_test(redis_parser_test facade_test LABELS DFLY)

add_executable(ok_backend ok_main.cc)
cxx_link(ok_backend dfly_facade)
