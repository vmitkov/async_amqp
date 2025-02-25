cmake_minimum_required (VERSION 3.12)

project(async_amqp VERSION "2.2.6")

add_subdirectory(external)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(MSVC_CXX_FLAGS "/W4 /EHsc /w14242 /w14254 /w14263 /w14265 /w14287 /w14289 /w14296 /w14311 /w14545 /w14546 /w14547 /w14549 /w14555 /w14640 /w14826 /w14928 /WX")
set(GCC_CXX_FLAGS "-Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wsign-conversion -Wcast-align -Wcast-qual -Wnull-dereference -Woverloaded-virtual -Wformat=2 -Werror")

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
#    add_definitions(-D_WIN32_WINNT=0x0601)
#    add_definitions(-DBOOST_USE_WINAPI_VERSION=0x0601)
    add_definitions(-D_WIN32_WINNT=0x0A00)
    add_definitions(-DBOOST_USE_WINAPI_VERSION=0x0A00)

endif ()

add_library(${PROJECT_NAME} INTERFACE)

include(GNUInstallDirs)

target_include_directories(
    ${PROJECT_NAME}
    INTERFACE 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

install(
    TARGETS
        ${PROJECT_NAME}
    EXPORT
        ${PROJECT_NAME}-config
)

install(
    EXPORT
        ${PROJECT_NAME}-config
    NAMESPACE 
        ${PROJECT_NAME}::
    DESTINATION 
        ${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}
)

install(
    DIRECTORY 
        ${CMAKE_CURRENT_LIST_DIR}/include/${PROJECT_NAME}/
    DESTINATION 
        ${CMAKE_INSTALL_INCLUDEDIR}/${PROJECT_NAME}
)

add_subdirectory (examples)

enable_testing()
add_subdirectory (tests)
