add_library(dfly_core  compact_object.cc dragonfly_core.cc interpreter.cc 
            tx_queue.cc)
cxx_link(dfly_core base absl::flat_hash_map redis_lib TRDP::lua crypto)

cxx_test(dfly_core_test dfly_core LABELS DFLY)
cxx_test(compact_object_test dfly_core LABELS DFLY)
cxx_test(dash_test dfly_core LABELS DFLY)
cxx_test(interpreter_test dfly_core LABELS DFLY)
