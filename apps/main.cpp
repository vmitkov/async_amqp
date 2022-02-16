#include "kd/amqp/connection.hpp"

#include <boost/asio.hpp>

namespace io = boost::asio;

int main()
{

	io::io_context io_context;

	kd::amqp::connection_t connection(io_context, "amqp://127.0.0.1:5672/");

	io_context.run();

	io::signal_set signals(io_context, SIGINT, SIGTERM);
	signals.async_wait([&](auto, auto) { io_context.stop(); });

	return 0;
}
