add_library(dfly_parser_lib redis_parser.cc resp_expr.cc )
cxx_link(dfly_parser_lib base strings_lib)

add_library(dfly_facade conn_context.cc dragonfly_listener.cc dragonfly_connection.cc facade.cc
            memcache_parser.cc reply_builder.cc op_status.cc service_interface.cc
            reply_capture.cc cmd_arg_parser.cc tls_error.cc)

if (DF_USE_SSL)
  set(TLS_LIB tls_lib)
  target_compile_definitions(dfly_facade PRIVATE DFLY_USE_SSL)
endif()

cxx_link(dfly_facade dfly_parser_lib http_server_lib fibers2
         ${TLS_LIB} TRDP::mimalloc TRDP::dconv)

add_library(facade_test facade_test.cc)
cxx_link(facade_test dfly_facade gtest_main_ext)

cxx_test(memcache_parser_test dfly_facade LABELS DFLY)
cxx_test(redis_parser_test facade_test LABELS DFLY)
cxx_test(reply_builder_test facade_test LABELS DFLY)
cxx_test(cmd_arg_parser_test facade_test LABELS DFLY)

add_executable(ok_backend ok_main.cc)
cxx_link(ok_backend dfly_facade)
