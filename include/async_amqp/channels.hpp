#pragma once

#include "connection.hpp"

#include <amqpcpp.h>
#include <boost/asio.hpp>
#include <boost/json.hpp>

#include <string>
#include <optional>
#include <cstddef>
#include <cassert>
#include <atomic>

namespace async_amqp
{

namespace io = boost::asio;

using namespace std::literals;

class channels_t
{
public:

	using log_handler_t = std::function<void(severity_level_t severity_level, std::string const& message)>;
	using received_handler_t = std::function<void(channels_t&, boost::json::object&&)>;

	channels_t(
		io::io_context& io_context,
		std::string const& url,
		std::string const& exchange,
		std::string const& queue,
		std::string const& route,
		log_handler_t log_handler = nullptr)
		: io_context_(io_context),
		url_(url),
		exchange_(exchange),
		queue_(queue),
		route_(route),
		log_handler_(log_handler),
		reconnection_timer_(io_context_)
	{
		log(severity_level_t::debug, "async_amqp::channels_t::channels_t");
		do_wait_for_reconnection_();
	}

	~channels_t()
	{
		log(severity_level_t::debug, "async_amqp::channels_t::~channels_t");
	}

	inline void log(severity_level_t severity_level, std::string const& message)
	{
		if (log_handler_ != nullptr) { log_handler_(severity_level, message); }
	}

	inline void open() noexcept { finish_ = false; }

	inline void close()
	{
		finish_ = true;
		close_connection_();
	}

	inline void publish(boost::json::object&& obj)
	{
		io::post(io_context_,
			[this, obj = std::move(obj)]() mutable noexcept { on_publish_(std::move(obj)); });
	}

	inline void on_received(received_handler_t handler) { received_handler_ = handler; }

private:

	void open_connection_() noexcept
	{
		using namespace std::placeholders;
		try
		{
			connection_o_.emplace(io_context_, url_, log_handler_);

			connection_o_->on_ready(std::bind(&channels_t::on_connection_ready_, this, _1));
			connection_o_->on_error(std::bind(&channels_t::on_connection_error_, this, _1, _2));
			connection_o_->on_closed(std::bind(&channels_t::on_connection_closed_, this, _1));
			connection_o_->open();
		}
		catch (...)
		{
			log(severity_level_t::error, "async_amqp::channels_t::open: Exception"s);
		}
	}

	inline void close_connection_() 
	{
		if (out_reliable_o_) { out_reliable_o_->close(); }
		if (out_channel_o_) { out_channel_o_->close(); }
		if (in_channel_o_) { in_channel_o_->close(); }
		if (connection_o_) { connection_o_->close(); }
	}

	void do_wait_for_reconnection_()
	{
		reconnection_timer_.expires_after(5s);
		reconnection_timer_.async_wait([&](boost::system::error_code const& ec)
			{
				if (!ec)
				{
					if (!connection_o_ && !finish_) { open_connection_(); }
					do_wait_for_reconnection_();
				}
			});
	}

	void open_out_channel_()
	{
		using namespace std::placeholders;
		try
		{
			if (!connection_o_) { throw std::runtime_error("Connection is closed"); }

			auto connection_p{ connection_o_->amqp_connection() };
			if (connection_p == nullptr) { throw std::runtime_error("AMQP::Connection is closed"); }

			AMQP::Table arguments;
			arguments["x-queue-mode"] = "lazy";

			out_channel_o_.emplace(connection_p);
			out_channel_o_->onError(std::bind(&channels_t::on_channel_error_, this, _1));

			out_channel_o_->declareExchange(exchange_, AMQP::direct);

			out_channel_o_->declareQueue(route_, AMQP::durable, arguments);
			out_channel_o_->bindQueue(exchange_, route_, route_);

			out_reliable_o_.emplace(*out_channel_o_);
		}
		catch (...)
		{
			std::throw_with_nested(std::runtime_error("async_amqp::channels_t::open_out_channel_"));
		}
	}

	void open_in_channel_()
	{
		using namespace std::placeholders;
		try
		{
			if (!connection_o_) { throw std::runtime_error("Connection is closed"); }

			auto connection_p{ connection_o_->amqp_connection() };
			if (connection_p == nullptr) { throw std::runtime_error("AMQP::Connection is closed"); }

			AMQP::Table arguments;
			arguments["x-queue-mode"] = "lazy";

			in_channel_o_.emplace(connection_p);
			in_channel_o_->onError(std::bind(&channels_t::on_channel_error_, this, _1));

			in_channel_o_->declareExchange(exchange_, AMQP::direct);

			in_channel_o_->declareQueue(queue_, AMQP::durable, arguments);
			in_channel_o_->bindQueue(exchange_, queue_, "");

			in_channel_o_->consume(queue_).onReceived(std::bind(
				&channels_t::on_received_, this, _1, _2, _3));
		}
		catch (...)
		{
			std::throw_with_nested(std::runtime_error("async_amqp::channels_t::open_in_channel_"));
		}
	}

	void on_connection_ready_(connection_t& connection) noexcept
	{
		try
		{
			open_out_channel_();
			open_in_channel_();
		}
		catch (...)
		{
			connection.error("async_amqp::channels_t::on_ready_: Exception"s);
		}
	}

	void on_connection_error_(connection_t& connection, std::string message)
	{
		connection.log(severity_level_t::error,
			"async_amqp::channels_t::on_connection_error_: "s + message);
		close_connection_();
	}

	void on_connection_closed_(connection_t& connection)
	{
		connection.log(severity_level_t::info,
			"async_amqp::channels_t::on_connection_closed_: Connection closed"s);
		connection_o_.reset();
	}

	void on_channel_error_(char const* message)
	{
		log(severity_level_t::error, 
			"async_amqp::channels_t::on_channel_error_: "s + message);
		close_connection_();
	}

	void on_publish_ack_(std::string buffer)
	{
		log(severity_level_t::debug,
			"async_amqp::channels_t::on_publish_ack_: "s
			+ exchange_ + ":"s + route_ + " <- "s + buffer);
	}

	void on_publish_lost_(std::string buffer)
	{
		log(severity_level_t::error,
			"async_amqp::channels_t::on_publish_lost_: "s
			+ exchange_ + ":"s + route_ + " <- "s + buffer);
	}

	void on_publish_error_(std::string buffer, char const* message)
	{
		log(severity_level_t::error,
			"async_amqp::channels_t::on_publish_error_: "s
			+ message + ": "s
			+ exchange_ + ":"s + route_ + " <- "s + buffer);
		close_connection_();
	}

	void on_publish_(boost::json::object&& obj) noexcept
	{
		using namespace std::placeholders;
		try
		{
			if (!out_channel_o_ || !out_reliable_o_ || !out_channel_o_->usable())
			{
				throw std::runtime_error("Channel is closed");
			}

			std::string buffer{ boost::json::serialize(obj) };
			AMQP::Envelope envelope(buffer.data(), buffer.size());
			envelope.setContentType("application/json");
			envelope.setContentEncoding("utf-8");

			out_reliable_o_->publish(exchange_, route_, envelope)
				.onAck(std::bind(&channels_t::on_publish_ack_, this, buffer))
				.onLost(std::bind(&channels_t::on_publish_lost_, this, buffer))
				.onError(std::bind(&channels_t::on_publish_error_, this, buffer, _1));
		}
		catch (...)
		{
			log(severity_level_t::error, "async_amqp::channels_t::on_publish_: Exception"s);
		}
	}

	void on_received_(
		const AMQP::Message& message, uint64_t delivery_tag, bool redelivered) noexcept
	{
		try
		{
			in_channel_o_->ack(delivery_tag);
			std::string message_str(message.body(), message.bodySize());

			log(severity_level_t::debug,
				"async_amqp::channels_t::on_received_: "
				+ exchange_ + ":"s + queue_ + " -> "s + message_str);

			if (received_handler_ != nullptr)
			{
				received_handler_(*this,
					boost::json::object{ boost::json::parse(message_str).as_object() });
			}
		}
		catch (...)
		{
			log(severity_level_t::error, "async_amqp::channels_t::on_received_: Exception"s);
		}
	}

private:
	io::io_context& io_context_;
	std::string const url_;
	std::string const exchange_;
	std::string const queue_;
	std::string const route_;
	log_handler_t log_handler_{ nullptr };
	received_handler_t received_handler_{ nullptr };

	io::steady_timer reconnection_timer_;

	std::optional<connection_t> connection_o_;
	std::optional<AMQP::Channel> out_channel_o_;
	std::optional<AMQP::Reliable<>> out_reliable_o_;
	std::optional<AMQP::Channel> in_channel_o_;

	std::atomic<bool> finish_{ false };
};

} //namespace async_amqp
