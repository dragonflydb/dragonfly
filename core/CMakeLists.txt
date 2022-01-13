add_library(dfly_core  compact_object.cc dragonfly_core.cc tx_queue.cc)
cxx_link(dfly_core base absl::flat_hash_map redis_lib)

cxx_test(dfly_core_test dfly_core)
cxx_test(compact_object_test dfly_core)