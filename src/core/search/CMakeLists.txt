gen_flex(lexer)
gen_bison(parser)

cur_gen_dir(gen_dir)

add_library(query_parser base.cc ast_expr.cc query_driver.cc search.cc indices.cc
            sort_indices.cc vector_utils.cc compressed_sorted_set.cc block_list.cc
            ${gen_dir}/parser.cc ${gen_dir}/lexer.cc)

target_link_libraries(query_parser base absl::strings TRDP::reflex TRDP::uni-algo TRDP::hnswlib redis_lib)

cxx_test(compressed_sorted_set_test query_parser LABELS DFLY)
cxx_test(block_list_test query_parser LABELS DFLY)
cxx_test(rax_tree_test redis_test_lib LABELS DFLY)
cxx_test(search_parser_test query_parser LABELS DFLY)
cxx_test(search_test redis_test_lib query_parser LABELS DFLY)
