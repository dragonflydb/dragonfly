# Minimum set needed for successful compilation
if(NOT WITH_TIERING)
    add_library(dfly_tiering decoders.cc serialized_map.cc)
    target_link_libraries(dfly_tiering dfly_transaction dfly_facade redis_lib base io)
    return()
endif()

add_library(dfly_tiering
    decoders.cc disk_storage.cc external_alloc.cc
    op_manager.cc serialized_map.cc small_bins.cc)
target_link_libraries(dfly_tiering
    dfly_transaction dfly_facade redis_lib base io)


cxx_test(disk_storage_test dfly_test_lib LABELS DFLY)
cxx_test(external_alloc_test dfly_test_lib LABELS DFLY)
cxx_test(op_manager_test dfly_test_lib LABELS DFLY)
cxx_test(serialized_map_test dfly_test_lib LABELS DFLY)
cxx_test(small_bins_test dfly_test_lib LABELS DFLY)

add_dependencies(check_dfly disk_storage_test external_alloc_test op_manager_test serialized_map_test small_bins_test)
