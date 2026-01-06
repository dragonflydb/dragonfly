# CMake generated Testfile for 
# Source directory: /home/runner/work/dragonfly/dragonfly/src/server/tiering
# Build directory: /home/runner/work/dragonfly/dragonfly/_codeql_build_dir/src/server/tiering
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(disk_storage_test "/home/runner/work/dragonfly/dragonfly/_codeql_build_dir/disk_storage_test")
set_tests_properties(disk_storage_test PROPERTIES  LABELS "DFLY" _BACKTRACE_TRIPLES "/home/runner/work/dragonfly/dragonfly/helio/cmake/internal.cmake;222;add_test;/home/runner/work/dragonfly/dragonfly/src/server/tiering/CMakeLists.txt;15;helio_cxx_test;/home/runner/work/dragonfly/dragonfly/src/server/tiering/CMakeLists.txt;0;")
add_test(external_alloc_test "/home/runner/work/dragonfly/dragonfly/_codeql_build_dir/external_alloc_test")
set_tests_properties(external_alloc_test PROPERTIES  LABELS "DFLY" _BACKTRACE_TRIPLES "/home/runner/work/dragonfly/dragonfly/helio/cmake/internal.cmake;222;add_test;/home/runner/work/dragonfly/dragonfly/src/server/tiering/CMakeLists.txt;16;helio_cxx_test;/home/runner/work/dragonfly/dragonfly/src/server/tiering/CMakeLists.txt;0;")
add_test(op_manager_test "/home/runner/work/dragonfly/dragonfly/_codeql_build_dir/op_manager_test")
set_tests_properties(op_manager_test PROPERTIES  LABELS "DFLY" _BACKTRACE_TRIPLES "/home/runner/work/dragonfly/dragonfly/helio/cmake/internal.cmake;222;add_test;/home/runner/work/dragonfly/dragonfly/src/server/tiering/CMakeLists.txt;17;helio_cxx_test;/home/runner/work/dragonfly/dragonfly/src/server/tiering/CMakeLists.txt;0;")
add_test(serialized_map_test "/home/runner/work/dragonfly/dragonfly/_codeql_build_dir/serialized_map_test")
set_tests_properties(serialized_map_test PROPERTIES  LABELS "DFLY" _BACKTRACE_TRIPLES "/home/runner/work/dragonfly/dragonfly/helio/cmake/internal.cmake;222;add_test;/home/runner/work/dragonfly/dragonfly/src/server/tiering/CMakeLists.txt;18;helio_cxx_test;/home/runner/work/dragonfly/dragonfly/src/server/tiering/CMakeLists.txt;0;")
add_test(small_bins_test "/home/runner/work/dragonfly/dragonfly/_codeql_build_dir/small_bins_test")
set_tests_properties(small_bins_test PROPERTIES  LABELS "DFLY" _BACKTRACE_TRIPLES "/home/runner/work/dragonfly/dragonfly/helio/cmake/internal.cmake;222;add_test;/home/runner/work/dragonfly/dragonfly/src/server/tiering/CMakeLists.txt;19;helio_cxx_test;/home/runner/work/dragonfly/dragonfly/src/server/tiering/CMakeLists.txt;0;")
