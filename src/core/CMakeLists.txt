add_library(dfly_core compact_object.cc dragonfly_core.cc extent_tree.cc
    external_alloc.cc interpreter.cc json_object.cc mi_memory_resource.cc
    segment_allocator.cc small_string.cc tx_queue.cc dense_set.cc string_set.cc
    detail/bitpacking.cc)

cxx_link(dfly_core base absl::flat_hash_map absl::str_format redis_lib TRDP::lua lua_modules
    Boost::fiber TRDP::jsoncons crypto)

add_executable(dash_bench dash_bench.cc)
cxx_link(dash_bench dfly_core)

cxx_test(dfly_core_test dfly_core LABELS DFLY)
cxx_test(compact_object_test dfly_core LABELS DFLY)
cxx_test(extent_tree_test dfly_core LABELS DFLY)
cxx_test(external_alloc_test dfly_core LABELS DFLY)
cxx_test(dash_test dfly_core file DATA testdata/ids.txt LABELS DFLY)
cxx_test(interpreter_test dfly_core LABELS DFLY)
cxx_test(json_test dfly_core TRDP::jsoncons LABELS DFLY)
cxx_test(string_set_test dfly_core LABELS DFLY)
