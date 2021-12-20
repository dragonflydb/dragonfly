add_library(dfly_core  tx_queue.cc  dragonfly_core.cc)
cxx_link(dfly_core base absl::flat_hash_map)

cxx_test(dfly_core_test dfly_core)