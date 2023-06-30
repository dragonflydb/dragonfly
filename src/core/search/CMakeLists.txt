gen_flex(lexer)
gen_bison(parser)

cur_gen_dir(gen_dir)

add_library(query_parser ast_expr.cc query_driver.cc search.cc indices.cc vector.cc compressed_sorted_set.cc
    ${gen_dir}/parser.cc ${gen_dir}/lexer.cc)
target_link_libraries(query_parser base absl::strings TRDP::reflex)
cxx_test(compressed_sorted_set_test query_parser LABELS DFLY)
cxx_test(search_parser_test query_parser LABELS DFLY)
cxx_test(search_test query_parser LABELS DFLY)
