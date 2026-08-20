cmake_minimum_required(VERSION 3.20)
project(nmr VERSION 0.2.0 LANGUAGES CXX)

option(NMR_BUILD_TESTS "Build unit and integration tests" ON)
option(NMR_BUILD_BENCH "Build benchmarks" ON)
option(NMR_WERROR "Treat compiler warnings as errors" OFF)
set(NMR_SANITIZER "" CACHE STRING "Build with a sanitizer: address, thread, or empty")

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Threads REQUIRED)

# Warnings and sanitizer flags, applied to everything we build.
add_library(nmr_options INTERFACE)
target_compile_options(nmr_options INTERFACE
  -Wall -Wextra -Wpedantic -Wshadow -Wnon-virtual-dtor -Wimplicit-fallthrough)
if(NMR_WERROR)
  target_compile_options(nmr_options INTERFACE -Werror)
endif()

if(NMR_SANITIZER STREQUAL "address")
  set(_san -fsanitize=address,undefined -fno-sanitize-recover=all)
elseif(NMR_SANITIZER STREQUAL "thread")
  set(_san -fsanitize=thread)
elseif(NOT NMR_SANITIZER STREQUAL "")
  message(FATAL_ERROR "NMR_SANITIZER must be address, thread, or empty")
endif()
if(_san)
  target_compile_options(nmr_options INTERFACE ${_san} -fno-omit-frame-pointer -g)
  target_link_options(nmr_options INTERFACE ${_san})
endif()

add_library(nmr STATIC
  src/broker.cpp
  src/client.cpp
  src/connection.cpp
  src/cpu.cpp
  src/frame_pool.cpp
  src/poller.cpp
  src/protocol.cpp
  src/server.cpp
  src/socket.cpp
)
target_include_directories(nmr PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(nmr PUBLIC Threads::Threads PRIVATE nmr_options)

add_library(nmr_workload STATIC tools/workload.cpp)
target_include_directories(nmr_workload PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/tools)
target_link_libraries(nmr_workload PUBLIC nmr PRIVATE nmr_options)

add_executable(nmr-broker tools/broker_main.cpp)
target_link_libraries(nmr-broker PRIVATE nmr nmr_options)

add_executable(nmr-loadgen tools/loadgen_main.cpp)
target_link_libraries(nmr-loadgen PRIVATE nmr_workload nmr_options)

if(NMR_BUILD_TESTS)
  enable_testing()
  add_executable(nmr-tests
    tests/test_main.cpp
    tests/test_broker.cpp
    tests/test_frame_pool.cpp
    tests/test_histogram.cpp
    tests/test_protocol.cpp
    tests/test_server.cpp
    tests/test_spsc_queue.cpp
  )
  target_include_directories(nmr-tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
  target_link_libraries(nmr-tests PRIVATE nmr nmr_options)
  add_test(NAME nmr-tests COMMAND nmr-tests)
endif()

if(NMR_BUILD_BENCH)
  add_executable(nmr-bench
    bench/bench_main.cpp
    bench/bench_e2e.cpp
    bench/bench_pool.cpp
    bench/bench_queue.cpp
  )
  target_link_libraries(nmr-bench PRIVATE nmr_workload nmr_options)
endif()
