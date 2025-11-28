gen_flex(lexer)
gen_bison(parser)

cur_gen_dir(gen_dir)

set_source_files_properties(${gen_dir}/parser.cc PROPERTIES
                            COMPILE_FLAGS "-Wno-maybe-uninitialized")
add_library(dfly_search_core ast_expr.cc base.cc hnsw_index.cc query_driver.cc search.cc
            indices.cc sort_indices.cc vector_utils.cc compressed_sorted_set.cc block_list.cc
            range_tree.cc synonyms.cc ${gen_dir}/parser.cc ${gen_dir}/lexer.cc)

target_link_libraries(dfly_search_core base redis_lib absl::strings
  TRDP::reflex TRDP::uni-algo TRDP::hnswlib Boost::headers)

if(WITH_SIMSIMD)
  target_link_libraries(dfly_search_core TRDP::simsimd)
  target_compile_definitions(dfly_search_core PRIVATE
    WITH_SIMSIMD=1
    SIMSIMD_DYNAMIC_DISPATCH=1
    SIMSIMD_NATIVE_F16=$<IF:$<BOOL:${SIMSIMD_NATIVE_F16}>,1,0>
    SIMSIMD_NATIVE_BF16=$<IF:$<BOOL:${SIMSIMD_NATIVE_F16}>,1,0>)
endif()

cxx_test(compressed_sorted_set_test dfly_search_core LABELS DFLY)
cxx_test(block_list_test dfly_search_core LABELS DFLY)
cxx_test(range_tree_test dfly_search_core LABELS DFLY)
cxx_test(rax_tree_test redis_test_lib LABELS DFLY)
cxx_test(search_parser_test dfly_search_core LABELS DFLY)
cxx_test(search_test redis_test_lib dfly_search_core LABELS DFLY)

if(WITH_SIMSIMD)
  target_link_libraries(search_test TRDP::simsimd)
  target_compile_definitions(search_test PRIVATE
    WITH_SIMSIMD=1
    SIMSIMD_DYNAMIC_DISPATCH=1
    SIMSIMD_NATIVE_F16=$<IF:$<BOOL:${SIMSIMD_NATIVE_F16}>,1,0>
    SIMSIMD_NATIVE_BF16=$<IF:$<BOOL:${SIMSIMD_NATIVE_F16}>,1,0>)
endif()
