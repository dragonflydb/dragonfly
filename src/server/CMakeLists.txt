add_executable(dragonfly dfly_main.cc)
cxx_link(dragonfly base dragonfly_lib)

add_library(dragonfly_lib channel_slice.cc command_registry.cc common.cc config_flags.cc
            db_slice.cc debugcmd.cc 
            engine_shard_set.cc generic_family.cc hset_family.cc
            list_family.cc main_service.cc  rdb_load.cc rdb_save.cc replica.cc
            snapshot.cc  script_mgr.cc server_family.cc 
            set_family.cc
            string_family.cc transaction.cc zset_family.cc)

cxx_link(dragonfly_lib dfly_core dfly_facade redis_lib strings_lib)

add_library(dfly_test_lib test_utils.cc)
cxx_link(dfly_test_lib dragonfly_lib facade_test gtest_main_ext)

cxx_test(dragonfly_test dfly_test_lib LABELS DFLY)
cxx_test(generic_family_test dfly_test_lib LABELS DFLY)
cxx_test(hset_family_test dfly_test_lib LABELS DFLY)
cxx_test(list_family_test dfly_test_lib LABELS DFLY)
cxx_test(set_family_test dfly_test_lib LABELS DFLY)
cxx_test(string_family_test dfly_test_lib LABELS DFLY)
cxx_test(rdb_test dfly_test_lib DATA testdata/empty.rdb testdata/small.rdb LABELS DFLY)
cxx_test(zset_family_test dfly_test_lib LABELS DFLY)


add_custom_target(check_dfly WORKING_DIRECTORY .. COMMAND ctest -L DFLY)
add_dependencies(check_dfly dragonfly_test list_family_test
                 generic_family_test memcache_parser_test rdb_test 
                 redis_parser_test string_family_test)
