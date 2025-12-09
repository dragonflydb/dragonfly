option(DF_ENABLE_MEMORY_TRACKING "Adds memory tracking debugging via MEMORY TRACK command" ON)
option(PRINT_STACKTRACES_ON_SIGNAL "Enables DF to print all fiber stacktraces on SIGUSR1" OFF)

option(WITH_COLLECTION_CMDS "Compile SET/HASH/ZSET/STREAM commands" ON)
option(WITH_EXTENSION_CMDS "Compile BLOOM/BITOPS/GEO/HLL/JSON commands" ON)

option(WITH_TIERING "Compile for macos" ON)

if(APPLE)
    message(STATUS "Macos detected. Set WITH_TIERING=off")
    set(WITH_TIERING OFF CACHE BOOL "Compile for macos" FORCE)
endif()

add_executable(dragonfly dfly_main.cc version_monitor.cc)
add_custom_target(check_dfly WORKING_DIRECTORY .. COMMAND ctest -L DFLY)
cxx_link(dragonfly base dragonfly_lib)

if (CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64" AND CMAKE_BUILD_TYPE STREQUAL "Release")
  # Add core2 only to this file, thus avoiding instructions in this object file that
  # can cause SIGILL.
  set_source_files_properties(dfly_main.cc PROPERTIES COMPILE_FLAGS "-march=core2")
endif()

set_property(SOURCE dfly_main.cc APPEND PROPERTY COMPILE_DEFINITIONS
             SOURCE_PATH_FROM_BUILD_ENV=${CMAKE_SOURCE_DIR})

if (WITH_TIERING)
    SET(TX_LINUX_SRCS tiering/disk_storage.cc tiering/op_manager.cc
      tiering/small_bins.cc tiering/external_alloc.cc)

    add_executable(dfly_bench dfly_bench.cc)
    cxx_link(dfly_bench dfly_parser_lib fibers2 absl::random_random redis_lib)
    cxx_test(tiering/disk_storage_test dfly_test_lib LABELS DFLY)
    cxx_test(tiering/op_manager_test dfly_test_lib LABELS DFLY)
    cxx_test(tiering/small_bins_test dfly_test_lib LABELS DFLY)
    cxx_test(tiering/external_alloc_test dfly_test_lib LABELS DFLY)
    cxx_test(tiering/serialized_map_test dfly_test_lib LABELS DFLY)
endif()

# Optionally include command families as needed
add_subdirectory(search)
if (WITH_SEARCH)
  add_definitions(-DWITH_SEARCH)
endif()

# Optionally compile collection commands
if (WITH_COLLECTION_CMDS)
  set(DF_FAMILY_SRCS set_family.cc hset_family.cc zset_family.cc stream_family.cc)
  add_definitions(-DWITH_COLLECTION_CMDS)
else()
  set(DF_FAMILY_SRCS collection_family_fallback.cc)
endif()

# Optionally compile extension commands
if (WITH_EXTENSION_CMDS)
  list(APPEND DF_FAMILY_SRCS geo_family.cc hll_family.cc bitops_family.cc bloom_family.cc json_family.cc)
  add_definitions(-DWITH_EXTENSION_CMDS)
endif()

add_library(dfly_transaction db_slice.cc blocking_controller.cc
            command_registry.cc  cluster_support.cc
            journal/cmd_serializer.cc journal/tx_executor.cc namespaces.cc
            common.cc journal/journal.cc journal/types.cc journal/journal_slice.cc
            server_state.cc table.cc  transaction.cc tx_base.cc
            serializer_commons.cc journal/serializer.cc journal/executor.cc journal/streamer.cc
            tiering/serialized_map.cc tiering/decoders.cc
            ${TX_LINUX_SRCS} acl/acl_log.cc slowlog.cc channel_store.cc)


if (WITH_TIERING)
  SET(DF_LINUX_SRCS tiered_storage.cc)
  add_definitions(-DWITH_TIERING)
  cxx_test(tiered_storage_test dfly_test_lib LABELS DFLY)
endif()

set(FAMILY_SRCS "")


add_library(dragonfly_lib
            config_registry.cc conn_context.cc debugcmd.cc dflycmd.cc engine_shard.cc
            engine_shard_set.cc error.cc family_utils.cc
            server_family.cc string_family.cc list_family.cc generic_family.cc
            ${DF_FAMILY_SRCS}
            main_service.cc memory_cmd.cc rdb_load.cc rdb_save.cc replica.cc http_api.cc
            protocol_client.cc
            snapshot.cc script_mgr.cc
            detail/compressor.cc detail/decompress.cc detail/save_stages_controller.cc detail/snapshot_storage.cc
            version.cc container_utils.cc
            multi_command_squasher.cc
            ${DF_LINUX_SRCS}
            cluster/cluster_config.cc cluster/cluster_family.cc cluster/incoming_slot_migration.cc
            cluster/outgoing_slot_migration.cc cluster/cluster_defs.cc cluster/cluster_utility.cc
            acl/user.cc acl/user_registry.cc acl/acl_family.cc
            acl/validator.cc
            sharding.cc)

if (DF_ENABLE_MEMORY_TRACKING)
  target_compile_definitions(dragonfly_lib PRIVATE DFLY_ENABLE_MEMORY_TRACKING)
  target_compile_definitions(dragonfly PRIVATE DFLY_ENABLE_MEMORY_TRACKING)
endif()

if (PRINT_STACKTRACES_ON_SIGNAL)
  target_compile_definitions(dragonfly_lib PRIVATE PRINT_STACKTRACES_ON_SIGNAL)
endif()

if (WITH_AWS)
  SET(AWS_LIB awsv2_lib)
  add_definitions(-DWITH_AWS)
endif()

if (WITH_GCP)
  SET(GCP_LIB gcp_lib)
  add_definitions(-DWITH_GCP)
endif()

cxx_link(dfly_transaction dfly_core strings_lib TRDP::fast_float TRDP::hdr_histogram)
cxx_link(dragonfly_lib dfly_transaction dfly_facade dfly_search_server
         redis_lib ${AWS_LIB} ${GCP_LIB} azure_lib jsonpath
         strings_lib html_lib
         http_client_lib absl::random_random TRDP::jsoncons TRDP::zstd TRDP::lz4
         TRDP::croncpp TRDP::flatbuffers)

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
cxx_test(server_family_test dfly_test_lib LABELS DFLY)
cxx_test(set_family_test dfly_test_lib LABELS DFLY)
cxx_test(stream_family_test dfly_test_lib LABELS DFLY)
cxx_test(string_family_test dfly_test_lib LABELS DFLY)
cxx_test(bitops_family_test dfly_test_lib LABELS DFLY)
cxx_test(rdb_test dfly_test_lib DATA testdata/empty.rdb testdata/redis6_small.rdb
         testdata/redis6_stream.rdb testdata/hll.rdb testdata/redis7_small.rdb
         testdata/redis_json.rdb testdata/RDB_TYPE_STREAM_LISTPACKS_2.rdb
         testdata/RDB_TYPE_STREAM_LISTPACKS_3.rdb testdata/ignore_expiry.rdb LABELS DFLY)
cxx_test(zset_family_test dfly_test_lib LABELS DFLY)
cxx_test(geo_family_test dfly_test_lib LABELS DFLY)
cxx_test(blocking_controller_test dfly_test_lib LABELS DFLY)
cxx_test(json_family_test dfly_test_lib LABELS DFLY)
cxx_test(json_family_memory_test dfly_test_lib LABELS DFLY)
cxx_test(journal/journal_test dfly_test_lib LABELS DFLY)
cxx_test(hll_family_test dfly_test_lib LABELS DFLY)
cxx_test(bloom_family_test dfly_test_lib LABELS DFLY)
cxx_test(cluster/cluster_config_test dfly_test_lib LABELS DFLY)
cxx_test(cluster/cluster_family_test dfly_test_lib LABELS DFLY)
cxx_test(acl/acl_family_test dfly_test_lib LABELS DFLY)
cxx_test(engine_shard_set_test dfly_test_lib LABELS DFLY)

add_dependencies(check_dfly dragonfly_test json_family_test list_family_test
                 generic_family_test memcache_parser_test rdb_test journal_test
                 redis_parser_test stream_family_test string_family_test
                 bitops_family_test set_family_test zset_family_test geo_family_test
                 hll_family_test cluster_config_test cluster_family_test acl_family_test
                 json_family_memory_test)
