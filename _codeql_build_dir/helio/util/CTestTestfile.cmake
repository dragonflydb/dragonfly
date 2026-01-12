# CMake generated Testfile for 
# Source directory: /home/runner/work/dragonfly/dragonfly/helio/util
# Build directory: /home/runner/work/dragonfly/dragonfly/_codeql_build_dir/helio/util
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(accept_server_test "/home/runner/work/dragonfly/dragonfly/_codeql_build_dir/accept_server_test")
set_tests_properties(accept_server_test PROPERTIES  LABELS "CI" _BACKTRACE_TRIPLES "/home/runner/work/dragonfly/dragonfly/helio/cmake/internal.cmake;222;add_test;/home/runner/work/dragonfly/dragonfly/helio/util/CMakeLists.txt;1;helio_cxx_test;/home/runner/work/dragonfly/dragonfly/helio/util/CMakeLists.txt;0;")
subdirs("fibers")
subdirs("html")
subdirs("metrics")
subdirs("tls")
subdirs("http")
subdirs("cloud")
subdirs("aws")
