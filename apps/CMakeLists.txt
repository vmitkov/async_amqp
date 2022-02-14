# Add source to this project's executable.
#set(BINARY ${PROJECT_NAME}_${PROJECT_VERSION_MAJOR}_${PROJECT_VERSION_MINOR}_${PROJECT_VERSION_PATCH})
set(BINARY amqp_client)
set(SOURCES main.cpp)

add_executable(${BINARY} ${SOURCES})

target_link_libraries(${BINARY}
	Boost::thread
	Boost::system
	Boost::json
	Boost::log
	Boost::log_setup
	Boost::program_options
	AmqpCpp::AmqpCpp
	Threads::Threads)
