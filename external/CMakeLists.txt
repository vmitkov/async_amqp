include(FetchContent)

set(FETCHCONTENT_QUIET off)

get_filename_component(fc_base "../external"
                       REALPATH BASE_DIR "${CMAKE_BINARY_DIR}")
set(FETCHCONTENT_BASE_DIR ${fc_base})

include(amqp-cpp.cmake)
