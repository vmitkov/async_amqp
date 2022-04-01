include(${PROJECT_SOURCE_DIR}/cmake/BoostTestHelpers.cmake)

add_boost_test("amqp_test.cpp" Boost::json Boost::log Boost::log_setup AmqpCpp::AmqpCpp Threads::Threads)
