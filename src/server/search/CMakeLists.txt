if (NOT WITH_SEARCH)
  SET(DF_SEARCH_SRCS search/doc_index_fallback.cc PARENT_SCOPE)
else()
  gen_flex(filter_lexer)
  gen_bison(filter_parser)

  cur_gen_dir(gen_dir)

  set_source_files_properties(${gen_dir}/filter_parser.cc PROPERTIES
                              COMPILE_FLAGS "-Wno-maybe-uninitialized")

  # Keeps gen_flex/gen_bison and add_library in the same CMakeLists scope
  # (same pattern as src/core/search/CMakeLists.txt with dfly_search_core).
  add_library(filter_parser_lib
    filter_driver.cc
    ${gen_dir}/filter_lexer.cc
    ${gen_dir}/filter_parser.cc)

  target_link_libraries(filter_parser_lib TRDP::reflex absl::strings base)

  SET(DF_FILTER_LIB filter_parser_lib PARENT_SCOPE)

  SET(DF_SEARCH_SRCS
    search/aggregator.cc
    search/filter_eval.cc
    search/filter_functions.cc
    search/doc_accessors.cc
    search/doc_index.cc
    search/search_family.cc
    search/index_join.cc
    search/global_hnsw_index.cc
    search/index_builder.cc
    PARENT_SCOPE)
endif()
