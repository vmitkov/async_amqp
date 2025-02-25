add_executable(async_amqp_example main.cpp)

target_link_libraries(async_amqp_example
	async_amqp
	Boost::thread
	Boost::system
	Boost::json
	AmqpCpp::AmqpCpp
	Threads::Threads)

add_executable(async_amqp_simple simple.cpp)

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    set_target_properties(async_amqp_simple PROPERTIES COMPILE_FLAGS ${GCC_CXX_FLAGS})
elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
    set_target_properties(async_amqp_simple PROPERTIES COMPILE_FLAGS ${MSVC_CXX_FLAGS})
endif()

target_link_libraries(async_amqp_simple
	async_amqp
	Boost::thread
	Boost::system
	Boost::json
	AmqpCpp::AmqpCpp
	Threads::Threads)
