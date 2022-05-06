#include "async_amqp/channels.hpp"

#include <boost/asio.hpp>
#include <boost/json.hpp>

#include <optional>
#include <string>
#include <functional>
#include <chrono>

using async_amqp::channels_t;
using async_amqp::severity_level_t;

namespace io = boost::asio;

using namespace std::literals;
using namespace std::chrono_literals;

void periodic_publish(io::steady_timer& timer, async_amqp::channels_t& channels)
{
	timer.expires_after(1s);
	timer.async_wait([&](boost::system::error_code const& ec)
		{
			if (!ec)
			{
				channels.publish(boost::json::object{ {"key"s, "value"s} });
				periodic_publish(timer, channels);
			}
		});
}

int main()
{
	std::string const url{ "amqp://127.0.0.1:5672/"s };
	std::string const exchange{ "test_exchange"s };
	std::string const queue{ "test_queue"s };
	std::string const route{ "test_queue"s };

	io::io_context io_context;

	channels_t channels(io_context, url, exchange, queue, route,
		[](severity_level_t severity_level, std::string const& message)
		{
			std::clog << severity_level << "\t" << message << std::endl;
		});

	channels.on_received(
		[](channels_t& channels, boost::json::object&& obj)
		{
			channels.log(severity_level_t::debug, 
				"receive message: "s + boost::json::serialize(obj));
		});

	channels.open();

	io::signal_set signals(io_context, SIGINT, SIGTERM);
	signals.async_wait([&](auto, auto)
		{
			std::clog << "Signal received" << std::endl;
			io_context.stop();
		});

	io::steady_timer timer(io_context);
	periodic_publish(timer, channels);

	io_context.run();

	return 0;
}
