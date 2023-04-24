# apt install libfl-dev

gen_flex(lexer)
gen_bison(parser)

cur_gen_dir(gen_dir)

add_library(query_parser query_driver.cc ${gen_dir}/parser.cc ${gen_dir}/lexer.cc)
target_link_libraries(query_parser base absl::strings)
cxx_test(search_parser_test query_parser)
