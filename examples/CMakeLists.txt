add_executable(async_amqp_example main.cpp)

target_link_libraries(async_amqp_example
	Boost::thread
	Boost::system
	Boost::json
	AmqpCpp::AmqpCpp
	Threads::Threads)

add_executable(async_amqp_simple simple.cpp)

target_link_libraries(async_amqp_simple
	Boost::thread
	Boost::system
	Boost::json
	AmqpCpp::AmqpCpp
	Threads::Threads)
