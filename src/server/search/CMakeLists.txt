if (NOT WITH_SEARCH)
  add_library(dfly_search_server doc_index_fallback.cc)
  target_link_libraries(dfly_search_server dfly_transaction dfly_facade redis_lib jsonpath TRDP::jsoncons)
  return()
endif()

add_library(dfly_search_server aggregator.cc doc_accessors.cc doc_index.cc search_family.cc)
target_link_libraries(dfly_search_server dfly_transaction dragonfly_lib dfly_facade redis_lib jsonpath TRDP::jsoncons)


cxx_test(search_family_test dfly_test_lib LABELS DFLY)
cxx_test(aggregator_test dfly_test_lib LABELS DFLY)


add_dependencies(check_dfly search_family_test aggregator_test)
