add_library(dfly_core compact_object.cc dragonfly_core.cc extent_tree.cc
    external_alloc.cc interpreter.cc json_object.cc mi_memory_resource.cc sds_utils.cc
    segment_allocator.cc simple_lru_counter.cc score_map.cc small_string.cc sorted_map.cc
    tx_queue.cc dense_set.cc
    string_set.cc string_map.cc detail/bitpacking.cc)

cxx_link(dfly_core base query_parser absl::flat_hash_map absl::str_format redis_lib TRDP::lua lua_modules
    fibers2 TRDP::jsoncons OpenSSL::Crypto)

add_executable(dash_bench dash_bench.cc)
cxx_link(dash_bench dfly_core)

cxx_test(dfly_core_test dfly_core LABELS DFLY)
cxx_test(compact_object_test dfly_core LABELS DFLY)
cxx_test(extent_tree_test dfly_core LABELS DFLY)
cxx_test(external_alloc_test dfly_core LABELS DFLY)
cxx_test(dash_test dfly_core file DATA testdata/ids.txt LABELS DFLY)
cxx_test(interpreter_test dfly_core LABELS DFLY)
cxx_test(json_test dfly_core TRDP::jsoncons LABELS DFLY)
cxx_test(simple_lru_counter_test dfly_core LABELS DFLY)
cxx_test(string_set_test dfly_core LABELS DFLY)
cxx_test(string_map_test dfly_core LABELS DFLY)
cxx_test(sorted_map_test dfly_core LABELS DFLY)
cxx_test(bptree_set_test dfly_core LABELS DFLY)
cxx_test(score_map_test dfly_core LABELS DFLY)

add_subdirectory(search)
