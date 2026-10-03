add_library(dfly_common dtoa.cc)
cxx_link(dfly_common absl::strings TRDP::dragonbox)

helio_cxx_test(dtoa_test dfly_common LABELS DFLY)
