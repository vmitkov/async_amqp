set(BINARY async_amqp_example)
set(SOURCES main.cpp)

add_executable(${BINARY} ${SOURCES})

target_link_libraries(${BINARY}
	Boost::thread
	Boost::system
	Boost::json
	AmqpCpp::AmqpCpp
	Threads::Threads)
