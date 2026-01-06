# CMake generated Testfile for 
# Source directory: /home/runner/work/dragonfly/dragonfly/helio/util/tls
# Build directory: /home/runner/work/dragonfly/dragonfly/_codeql_build_dir/helio/util/tls
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(tls_engine_test "/home/runner/work/dragonfly/dragonfly/_codeql_build_dir/tls_engine_test")
set_tests_properties(tls_engine_test PROPERTIES  LABELS "CI" _BACKTRACE_TRIPLES "/home/runner/work/dragonfly/dragonfly/helio/cmake/internal.cmake;222;add_test;/home/runner/work/dragonfly/dragonfly/helio/util/tls/CMakeLists.txt;6;helio_cxx_test;/home/runner/work/dragonfly/dragonfly/helio/util/tls/CMakeLists.txt;0;")
add_test(tls_socket_test "/home/runner/work/dragonfly/dragonfly/_codeql_build_dir/tls_socket_test")
set_tests_properties(tls_socket_test PROPERTIES  LABELS "CI" _BACKTRACE_TRIPLES "/home/runner/work/dragonfly/dragonfly/helio/cmake/internal.cmake;222;add_test;/home/runner/work/dragonfly/dragonfly/helio/util/tls/CMakeLists.txt;7;helio_cxx_test;/home/runner/work/dragonfly/dragonfly/helio/util/tls/CMakeLists.txt;0;")
