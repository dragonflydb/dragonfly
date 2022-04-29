add_library(dfly_core compact_object.cc dragonfly_core.cc extent_tree.cc 
            external_alloc.cc interpreter.cc 
            segment_allocator.cc small_string.cc tx_queue.cc)
cxx_link(dfly_core base absl::flat_hash_map absl::str_format redis_lib TRDP::lua 
         Boost::fiber crypto)

cxx_test(dfly_core_test dfly_core LABELS DFLY)
cxx_test(compact_object_test dfly_core LABELS DFLY)
cxx_test(extent_tree_test dfly_core LABELS DFLY)
cxx_test(external_alloc_test dfly_core LABELS DFLY)
cxx_test(dash_test dfly_core LABELS DFLY)
cxx_test(interpreter_test dfly_core LABELS DFLY)
