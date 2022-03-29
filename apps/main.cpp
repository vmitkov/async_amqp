#include "async_amqp/connection.hpp"

#include <boost/asio.hpp>
#include <boost/json.hpp>

#include <optional>
#include <string>
#include <functional>
#include <chrono>

using async_amqp::connection_t;

namespace io = boost::asio;

using namespace std::literals;
using namespace std::chrono_literals;

template<typename Options>
class amqp_t
{
public:

	using options_t = Options;
	using received_handler_t = std::function<void(amqp_t&, boost::json::object&&)>;

	amqp_t(io::io_context& io_context, options_t const& options)
		: io_context_(io_context), options_(options), timer_(io_context_)
	{
	}

	inline void open()
	{
		try
		{
			finish = false;
			connection_o_.emplace(io_context_, options_.amqp_url);
			connection_o_->on_log(std::bind(&amqp_t::on_log_, this, std::placeholders::_1, std::placeholders::_2));
			connection_o_->on_ready(std::bind(&amqp_t::on_connection_ready_, this, std::placeholders::_1));
			connection_o_->on_error(std::bind(&amqp_t::on_connection_error_, this, std::placeholders::_1, std::placeholders::_2));
			connection_o_->on_closed(std::bind(&amqp_t::on_connection_closed_, this, std::placeholders::_1));
			connection_o_->open();
		}
		catch (...)
		{
			std::clog << "amqp_t::open: Exception" << std::endl;
		}
	}

	inline void close()
	{
		finish = true;
		close_connection_();
	}

	void publish(boost::json::object&& obj)
	{
		boost::asio::post(
			io_context_,
			/*on_publish_*/[this, obj = std::move(obj)]() mutable noexcept
		{
			try
			{
				if (!calls_channel_o_) { throw std::runtime_error("channel is closed"); }

				auto const buffer{ boost::json::serialize(obj) };
				AMQP::Envelope envelope(buffer.data(), buffer.size());
				envelope.setContentType("application/json");
				envelope.setContentEncoding("utf-8");
				calls_channel_o_->publish(
					options_.amqp_exchange, options_.amqp_route, envelope);

				std::clog << options_.amqp_exchange << ":" << options_.amqp_route
					<< " <- " << buffer << std::endl;
			}
			catch (...)
			{
				std::clog << "amqp_t::on_publish_: Exception" << std::endl;
			}
		});
	}

	inline void on_received(received_handler_t handler) { received_handler_ = handler; }

private:

	void on_log_(async_amqp::severity_level_t severity_level, std::string message)
	{
		std::clog << severity_level << "\t" << message << std::endl;
	}

	void open_calls_channel_()
	{
		try
		{
			if (!connection_o_) { throw std::runtime_error("connection is closed"); }

			auto connection_p{ &connection_o_->amqp_connection() };

			AMQP::Table arguments;
			arguments["x-queue-mode"] = "lazy";

			calls_channel_o_.emplace(connection_p);
			calls_channel_o_->onError(std::bind(&amqp_t::on_channel_error_, this, std::placeholders::_1));

			calls_channel_o_->declareExchange(options_.amqp_exchange, AMQP::direct);

			calls_channel_o_->declareQueue(options_.amqp_route, AMQP::durable, arguments);
			calls_channel_o_->bindQueue(options_.amqp_exchange, options_.amqp_route, options_.amqp_route);
		}
		catch (...)
		{
			std::clog << "amqp_t::open_calls_channel_: Exception" << std::endl;
			throw;
		}
	}

	void open_streams_channel_()
	{
		try
		{
			if (!connection_o_) { throw std::runtime_error("connection is closed"); }

			auto connection_p{ &connection_o_->amqp_connection() };

			AMQP::Table arguments;
			arguments["x-queue-mode"] = "lazy";

			streams_channel_o_.emplace(connection_p);
			streams_channel_o_->onError(std::bind(&amqp_t::on_channel_error_, this, std::placeholders::_1));

			streams_channel_o_->declareExchange(options_.amqp_exchange, AMQP::direct);

			streams_channel_o_->declareQueue(options_.amqp_queue, AMQP::durable, arguments);
			streams_channel_o_->bindQueue(options_.amqp_exchange, options_.amqp_queue, "");

			streams_channel_o_->consume(options_.amqp_queue).onReceived(std::bind(
				&amqp_t::on_received_,
				this,
				std::placeholders::_1,
				std::placeholders::_2,
				std::placeholders::_3));
		}
		catch (...)
		{
			std::clog << "amqp_t::open_streams_channel_: Exception" << std::endl;
			throw;
		}
	}

	void on_connection_ready_(connection_t&) noexcept
	{
		try
		{
			open_calls_channel_();
			open_streams_channel_();
		}
		catch (...)
		{
			std::clog << "amqp_t::on_ready_: Exception" << std::endl;
			close_connection_();
		}
	}

	void on_connection_error_(connection_t&, std::string message)
	{
		std::clog << "amqp_t::on_connection_error_: Error:" << "\t" << message << std::endl;
		close_connection_();
	}

	void on_connection_closed_(connection_t&)
	{
		std::clog << "amqp_t::on_connection_closed_: Connection closed" << std::endl;
		if (!finish)
		{
			timer_.expires_after(1s);
			timer_.async_wait(
				[&](const boost::system::error_code& ec)
				{
					if (!ec) { open(); }
				});
		}
	}

	void on_channel_error_(char const* message)
	{
		std::clog << "amqp_t::on_channel_error_: Channel error" << std::endl;
		if (connection_o_) { on_connection_error_(*connection_o_, message); }
	}

	void on_received_(
		const AMQP::Message& message, uint64_t delivery_tag, bool redelivered) noexcept
	{
		try
		{
			streams_channel_o_->ack(delivery_tag);
			std::string message_str(message.body(), message.bodySize());

			std::clog << options_.amqp_exchange << ":" << options_.amqp_queue
				<< " -> " << message_str << std::endl;

			boost::json::object obj{ boost::json::parse(message_str).as_object() };

			if (received_handler_ != nullptr)
			{
				received_handler_(*this, std::move(obj));
			}
		}
		catch (...)
		{
			std::clog << "amqp_t::on_received_: Exception" << std::endl;
		}
	}

	inline void close_connection_() { if (connection_o_) { connection_o_->close(); } }

private:
	io::io_context& io_context_;
	options_t const& options_;
	boost::asio::steady_timer timer_;

	std::optional<connection_t> connection_o_;
	std::optional<AMQP::Channel> calls_channel_o_;
	std::optional<AMQP::Channel> streams_channel_o_;

	received_handler_t received_handler_{ nullptr };
	std::atomic<bool> finish{ false };
};

template<typename Amqp>
void periodic_publish(boost::asio::steady_timer& timer, Amqp& amqp)
{
	timer.expires_after(1s);
	timer.async_wait([&](boost::system::error_code const& ec)
		{
			if (!ec)
			{
				amqp.publish(boost::json::object{ {"key"s, "value"s} });
				periodic_publish(timer, amqp);
			}

		});
}


int main()
{
	struct options_t
	{
		std::string amqp_url{ "amqp://127.0.0.1:5672/"s };
		std::string amqp_exchange{ "X3"s };
		std::string amqp_queue{ "streams"s };
		std::string amqp_route{ "calls"s };
	};

	options_t options;
	io::io_context io_context;

	amqp_t amqp(io_context, options);
	amqp.open();

	io::signal_set signals(io_context, SIGINT, SIGTERM);
	signals.async_wait([&](auto, auto)
		{
			std::clog << "Signal received" << std::endl;
			amqp.close();
		});

	boost::asio::steady_timer timer(io_context);
	periodic_publish(timer, amqp);

	io_context.run();

	return 0;
}
