gen_flex(jsonpath_lexer)
gen_bison(jsonpath_grammar)

cur_gen_dir(gen_dir)

add_library(jsonpath lexer_impl.cc driver.cc
            ${gen_dir}/jsonpath_lexer.cc ${gen_dir}/jsonpath_grammar.cc)
target_link_libraries(jsonpath base absl::strings TRDP::reflex)

cxx_test(jsonpath_test jsonpath LABELS DFLY)
