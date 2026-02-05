if (NOT WITH_SEARCH)
  SET(DF_SEARCH_SRCS search/doc_index_fallback.cc PARENT_SCOPE)
else()
  SET(DF_SEARCH_SRCS
    search/aggregator.cc
    search/doc_accessors.cc
    search/doc_index.cc
    search/search_family.cc
    search/index_join.cc
    search/global_hnsw_index.cc
    search/index_builder.cc
    PARENT_SCOPE)
endif()
