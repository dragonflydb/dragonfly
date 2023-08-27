add_executable(dragonfly dfly_main.cc)
cxx_link(dragonfly base dragonfly_lib)

if (CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" AND CMAKE_BUILD_TYPE STREQUAL "Release")
  # Add core2 only to this file, thus avoiding instructions in this object file that
  # can cause SIGILL.
  set_source_files_properties(dfly_main.cc PROPERTIES COMPILE_FLAGS -march=core2 COMPILE_DEFINITIONS
                              SOURCE_PATH_FROM_BUILD_ENV=${CMAKE_SOURCE_DIR})
endif()

add_library(dfly_transaction db_slice.cc malloc_stats.cc engine_shard_set.cc blocking_controller.cc
            common.cc
            io_mgr.cc journal/journal.cc journal/types.cc journal/journal_slice.cc server_state.cc table.cc
            tiered_storage.cc top_keys.cc transaction.cc)
cxx_link(dfly_transaction dfly_core strings_lib)

add_library(dragonfly_lib channel_store.cc command_registry.cc
            config_registry.cc conn_context.cc debugcmd.cc dflycmd.cc
            generic_family.cc hset_family.cc json_family.cc
            search/search_family.cc search/doc_index.cc search/doc_accessors.cc
            list_family.cc main_service.cc memory_cmd.cc rdb_load.cc rdb_save.cc replica.cc
            protocol_client.cc
            snapshot.cc script_mgr.cc server_family.cc malloc_stats.cc
            set_family.cc stream_family.cc string_family.cc
            zset_family.cc version.cc bitops_family.cc container_utils.cc io_utils.cc
            serializer_commons.cc journal/serializer.cc journal/executor.cc journal/streamer.cc
            top_keys.cc multi_command_squasher.cc hll_family.cc cluster/cluster_config.cc
            cluster/cluster_family.cc acl/user.cc acl/user_registry.cc acl/acl_family.cc)


cxx_link(dragonfly_lib dfly_transaction dfly_facade redis_lib aws_lib strings_lib html_lib
         http_client_lib absl::random_random TRDP::jsoncons zstd TRDP::lz4 TRDP::croncpp)

if (DF_USE_SSL)
  set(TLS_LIB tls_lib)
  target_compile_definitions(dragonfly_lib PRIVATE DFLY_USE_SSL)
endif()

add_library(dfly_test_lib test_utils.cc)
cxx_link(dfly_test_lib dragonfly_lib facade_test gtest_main_ext)

cxx_test(dragonfly_test dfly_test_lib LABELS DFLY)
cxx_test(multi_test dfly_test_lib LABELS DFLY)
cxx_test(generic_family_test dfly_test_lib LABELS DFLY)
cxx_test(hset_family_test dfly_test_lib LABELS DFLY)
cxx_test(list_family_test dfly_test_lib LABELS DFLY)
cxx_test(set_family_test dfly_test_lib LABELS DFLY)
cxx_test(stream_family_test dfly_test_lib LABELS DFLY)
cxx_test(string_family_test dfly_test_lib LABELS DFLY)
cxx_test(bitops_family_test dfly_test_lib LABELS DFLY)
cxx_test(rdb_test dfly_test_lib DATA testdata/empty.rdb testdata/redis6_small.rdb
         testdata/redis6_stream.rdb testdata/hll.rdb testdata/redis7_small.rdb LABELS DFLY)
cxx_test(zset_family_test dfly_test_lib LABELS DFLY)
cxx_test(blocking_controller_test dragonfly_lib LABELS DFLY)
cxx_test(snapshot_test dragonfly_lib LABELS DFLY)
cxx_test(json_family_test dfly_test_lib LABELS DFLY)
cxx_test(journal/journal_test dfly_test_lib LABELS DFLY)
cxx_test(tiered_storage_test dfly_test_lib LABELS DFLY)
cxx_test(top_keys_test dfly_test_lib LABELS DFLY)
cxx_test(hll_family_test dfly_test_lib LABELS DFLY)
cxx_test(search/search_family_test dfly_test_lib LABELS DFLY)
cxx_test(cluster/cluster_config_test dfly_test_lib LABELS DFLY)
cxx_test(cluster/cluster_family_test dfly_test_lib LABELS DFLY)
cxx_test(acl/user_registry_test dfly_test_lib LABELS DFLY)



add_custom_target(check_dfly WORKING_DIRECTORY .. COMMAND ctest -L DFLY)
add_dependencies(check_dfly dragonfly_test json_family_test list_family_test
                 generic_family_test memcache_parser_test rdb_test journal_test
                 redis_parser_test snapshot_test stream_family_test string_family_test
                 bitops_family_test set_family_test zset_family_test hll_family_test
                 cluster_config_test cluster_family_test user_registry_test)
