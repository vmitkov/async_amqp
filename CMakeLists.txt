cmake_minimum_required (VERSION 3.12)

project(async_amqp VERSION "2.2.4")

add_subdirectory(external)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

SET(Boost_USE_STATIC_LIBS ON)
find_package (Boost REQUIRED COMPONENTS
  log
  log_setup
  json
  system
  thread
  unit_test_framework)
include_directories(${Boost_INCLUDE_DIR})

if (WIN32)
    add_definitions(-DNOMINMAX)
    add_definitions(-D_WIN32_WINNT=0x0601)
    add_definitions(-DBOOST_USE_WINAPI_VERSION=0x0601)
endif ()

include_directories(include)

add_subdirectory (examples)

enable_testing()
add_subdirectory (tests)
