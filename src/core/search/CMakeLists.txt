gen_flex(lexer)
gen_bison(parser)

cur_gen_dir(gen_dir)

set_source_files_properties(${gen_dir}/parser.cc PROPERTIES
                            COMPILE_FLAGS "-Wno-maybe-uninitialized")
add_library(query_parser base.cc ast_expr.cc query_driver.cc search.cc indices.cc
            sort_indices.cc vector_utils.cc compressed_sorted_set.cc block_list.cc
            range_tree.cc synonyms.cc ${gen_dir}/parser.cc ${gen_dir}/lexer.cc)

target_link_libraries(query_parser base absl::strings TRDP::reflex TRDP::uni-algo TRDP::hnswlib redis_lib)

cxx_test(compressed_sorted_set_test query_parser LABELS DFLY)
cxx_test(block_list_test query_parser LABELS DFLY)
cxx_test(range_tree_test query_parser LABELS DFLY)
cxx_test(rax_tree_test redis_test_lib LABELS DFLY)
cxx_test(search_parser_test query_parser LABELS DFLY)
cxx_test(search_test redis_test_lib query_parser LABELS DFLY)

if(USE_SIMSIMD)
  target_link_libraries(search_test TRDP::simsimd)
  target_compile_definitions(search_test PRIVATE USE_SIMSIMD=1)
endif()
