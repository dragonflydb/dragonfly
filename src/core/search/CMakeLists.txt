gen_flex(lexer)
gen_bison(parser)

cur_gen_dir(gen_dir)

find_package(ICU REQUIRED COMPONENTS uc i18n)

add_library(query_parser ast_expr.cc query_driver.cc search.cc indices.cc vector_utils.cc compressed_sorted_set.cc
    ${gen_dir}/parser.cc ${gen_dir}/lexer.cc)

target_link_libraries(query_parser ICU::uc ICU::i18n)

target_link_libraries(query_parser base absl::strings TRDP::reflex)
cxx_test(compressed_sorted_set_test query_parser LABELS DFLY)
cxx_test(search_parser_test query_parser LABELS DFLY)
cxx_test(search_test query_parser LABELS DFLY)
