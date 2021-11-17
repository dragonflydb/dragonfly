add_executable(dragonfly dfly_main.cc)
cxx_link(dragonfly base dragonfly_lib)

add_library(dragonfly_lib db_slice.cc dragonfly_listener.cc dragonfly_connection.cc 
            main_service.cc engine_shard_set.cc
            redis_parser.cc resp_expr.cc)

cxx_link(dragonfly_lib uring_fiber_lib
         fibers_ext strings_lib http_server_lib)

add_library(dfly_test_lib test_utils.cc)
cxx_link(dfly_test_lib dragonfly_lib gtest_main_ext)

cxx_test(redis_parser_test dfly_test_lib LABELS DFLY)
