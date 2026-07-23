cmake_minimum_required(VERSION 3.16)
project(cpp_networking_runtime LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_compile_options(-Wall -Wextra)

include_directories(${CMAKE_SOURCE_DIR}/include)

find_package(Threads REQUIRED)

add_executable(nmr-tests
  tests/test_main.cpp
  tests/test_queue.cpp
)
target_include_directories(nmr-tests PRIVATE ${CMAKE_SOURCE_DIR}/tests)
target_link_libraries(nmr-tests PRIVATE Threads::Threads)
