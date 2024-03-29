gen_flex(jsonpath_lexer)
gen_bison(jsonpath_grammar)

cur_gen_dir(gen_dir)

add_library(jsonpath lexer_impl.cc driver.cc path.cc
            ${gen_dir}/jsonpath_lexer.cc ${gen_dir}/jsonpath_grammar.cc json_object.cc
            detail/jsoncons_dfs.cc detail/flat_dfs.cc)
target_link_libraries(jsonpath base absl::strings TRDP::reflex TRDP::jsoncons TRDP::flatbuffers)

cxx_test(jsonpath_test jsonpath LABELS DFLY)
cxx_test(json_test jsonpath TRDP::jsoncons LABELS DFLY)
