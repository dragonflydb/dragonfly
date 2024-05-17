project(DragonflyCPPWasmSDK)

cmake_minimum_required(VERSION 3.20)

add_library(df-cpp-wasm-sdk INTERFACE)
target_include_directories(df-cpp-wasm-sdk INTERFACE ${PROJECT_SOURCE_DIR}/include)

add_executable(example examples/example.cc)
target_link_libraries(example df-cpp-wasm-sdk)
