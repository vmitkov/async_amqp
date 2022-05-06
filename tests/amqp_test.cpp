#define BOOST_TEST_MODULE async_amqp_test

#include "async_amqp/channels.hpp"

#include <boost/test/unit_test.hpp>
#include <boost/asio.hpp>
#include <boost/json.hpp>
#include <boost/log/trivial.hpp>

#include <string>
#include <chrono>
#include <functional>

using async_amqp::severity_level_t;
using async_amqp::channels_t;

namespace json = boost::json;
namespace io = boost::asio;
using io::steady_timer;

using namespace std::literals;
using namespace std::chrono_literals;

void periodic_publish(
	steady_timer& timer,
	channels_t& channels,
	json::object const& obj,
	int& counter)
{
	if (counter == 20) { return; }
	timer.expires_after(io::chrono::milliseconds(20));
	timer.async_wait([&](boost::system::error_code const& ec)
		{
			if (!ec)
			{
				channels.publish(json::object{ obj });
				periodic_publish(timer, channels, obj, ++counter);
			}
		});
}

BOOST_AUTO_TEST_CASE(async_amqp_test)
{
	struct options_t
	{
		std::string amqp_url{ "amqp://127.0.0.1:5672/"s };
		std::string amqp_exchange{ "test_exchange"s };
		std::string amqp_queue{ "test_queue"s };
		std::string amqp_route{ "test_queue"s };
	};

	const json::object json_msg_sample{ {
		{"key"s, "value"s}
	} };

	options_t options;

	io::io_context io_context;

	auto log = [](severity_level_t severity_level, std::string const& message)
	{
		switch (severity_level)
		{
		case severity_level_t::trace:
			BOOST_LOG_TRIVIAL(trace) << message;
			break;
		case severity_level_t::debug:
			BOOST_LOG_TRIVIAL(debug) << message;
			break;
		case severity_level_t::info:
			BOOST_LOG_TRIVIAL(info) << message;
			break;
		case severity_level_t::warning:
			BOOST_LOG_TRIVIAL(warning) << message;
			break;
		case severity_level_t::error:
			BOOST_LOG_TRIVIAL(error) << message;
			break;
		case severity_level_t::fatal:
			BOOST_LOG_TRIVIAL(fatal) << message;
			break;
		default:
			break;
		}
	};

	channels_t channels(
		io_context,
		options.amqp_url,
		options.amqp_exchange,
		options.amqp_queue,
		options.amqp_route,
		std::move(log));

	int msg_receiver_counter{ 0 };
	int msg_sender_counter{ 0 };
	steady_timer publish_timer(io_context);

	channels.on_received(
		[&](channels_t&, json::object&& object)
		{
			if (msg_sender_counter > 0)
			{
				++msg_receiver_counter;
				BOOST_CHECK(object == json_msg_sample);
				if (msg_receiver_counter == 20)
				{
					io::post(io_context, [&]() { io_context.stop(); });
				}
			}
		});

	channels.on_ready(
		[&](channels_t& channels)
		{
			periodic_publish(publish_timer, channels, json_msg_sample, msg_sender_counter);
		});

	channels.open();

	steady_timer stop_timer(io_context);
	stop_timer.expires_after(io::chrono::seconds(5));
	stop_timer.async_wait(
		[&](const boost::system::error_code&) {	io_context.stop(); });

	io_context.run();

	BOOST_LOG_TRIVIAL(debug) << "msg_sender_counter: " << msg_sender_counter;
	BOOST_LOG_TRIVIAL(debug) << "msg_receiver_counter: " << msg_receiver_counter;

	BOOST_CHECK(msg_receiver_counter == 20 && msg_sender_counter == 20);
}
