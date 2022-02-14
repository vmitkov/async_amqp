FetchContent_Declare(
  amqp-cpp
  GIT_REPOSITORY https://github.com/CopernicaMarketingSoftware/AMQP-CPP.git
  GIT_TAG master
  GIT_SHALLOW TRUE
  GIT_PROGRESS TRUE
  USES_TERMINAL_DOWNLOAD TRUE
)

#FetchContent_MakeAvailable(amqp-cpp)

FetchContent_GetProperties(amqp-cpp)
if(NOT amqp-cpp)
  FetchContent_Populate(amqp-cpp)
  set(AMQP-CPP_LINUX_TCP OFF CACHE INTERNAL "Build Linux-only TCP module")
  add_subdirectory(${amqp-cpp_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR}/amqp-cpp-build)
endif()

add_library(AmqpCpp::AmqpCpp ALIAS amqpcpp)
include_directories(${amqp-cpp_SOURCE_DIR}/include)