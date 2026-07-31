cmake_minimum_required(VERSION 3.16)
project(cpp_networking_runtime LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(NMR_ENABLE_SANITIZERS "Build tests with ASan/UBSan" OFF)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

add_compile_options(-Wall -Wextra -Wpedantic -Wno-unused-parameter)

if(NMR_ENABLE_SANITIZERS)
  add_compile_options(-O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address,undefined)
endif()

include_directories(${CMAKE_SOURCE_DIR}/include)

add_library(nmr STATIC
  src/memory_pool.cpp
  src/poller.cpp
  src/protocol.cpp
  src/broker.cpp
  src/server.cpp
)
target_include_directories(nmr PUBLIC ${CMAKE_SOURCE_DIR}/include)
find_package(Threads REQUIRED)
target_link_libraries(nmr PUBLIC Threads::Threads)

add_executable(nmr-broker src/main.cpp)
target_link_libraries(nmr-broker PRIVATE nmr)

add_executable(nmr-loadgen src/loadgen.cpp)
target_link_libraries(nmr-loadgen PRIVATE nmr)

add_executable(nmr-tests
  tests/test_main.cpp
  tests/test_pool.cpp
  tests/test_queue.cpp
  tests/test_protocol.cpp
)
target_include_directories(nmr-tests PRIVATE ${CMAKE_SOURCE_DIR}/tests)
target_link_libraries(nmr-tests PRIVATE nmr)

add_executable(nmr-bench
  bench/bench_queues.cpp
  bench/bench_pool.cpp
  bench/bench_e2e.cpp
)
target_link_libraries(nmr-bench PRIVATE nmr)

enable_testing()
add_test(NAME nmr-tests COMMAND nmr-tests)
