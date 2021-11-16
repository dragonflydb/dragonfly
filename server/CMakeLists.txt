add_executable(dragonfly dfly_main.cc)
cxx_link(dragonfly base dragonfly_lib)

add_library(dragonfly_lib dragonfly_listener.cc dragonfly_connection.cc main_service.cc)

cxx_link(dragonfly_lib uring_fiber_lib
         fibers_ext strings_lib http_server_lib)
