#pragma once

#include <amqpcpp.h>

#include <boost/asio.hpp>
#include <boost/bind.hpp>

#include <iostream> 
#include <algorithm>
#include <deque>
#include <vector>
#include <cassert>
#include <cstddef>

namespace async_amqp
{

namespace sys = boost::system;
namespace io = boost::asio;
namespace ip = io::ip;
using tcp = ip::tcp;
using error_code = boost::system::error_code;
using work_guard_t = io::executor_work_guard<io::io_context::executor_type>;

enum class severity_level_t
{
	trace,
	debug,
	info,
	warning,
	error,
	fatal
};

inline std::ostream& operator<<(std::ostream& os, severity_level_t severity_level)
{
	switch (severity_level)
	{
	case severity_level_t::trace:
		return os << "TRACE";
	case severity_level_t::debug:
		return os << "DEBUG";
	case severity_level_t::info:
		return os << "INFO";
	case severity_level_t::warning:
		return os << "WARNING";
	case severity_level_t::error:
		return os << "ERROR";
	case severity_level_t::fatal:
		return os << "FATAL";
	default:
		return os;
	}
}

class connection_t : private AMQP::ConnectionHandler
{
public:
	using log_handler_t = std::function<void(severity_level_t severity_level, std::string const& message)>;
	using ready_handler_t = std::function<void(connection_t& self)>;
	using error_handler_t = std::function<void(connection_t& self, std::string const& message)>;
	using closed_handler_t = std::function<void(connection_t& self)>;

public:
	connection_t(io::io_context& io_context, AMQP::Address&& address) :
		connection_(this, address.login(), address.vhost()),
		//work_guard_(io_context.get_executor()),
		io_context_(io_context),
		address_(std::move(address)),
		resolver_(io_context),
		socket_(io_context)
	{
		input_buffer_.prepare(connection_.maxFrame());
		log_(severity_level_t::debug, "connection_t::connection_t");
	}

	virtual ~connection_t() noexcept
	{
		log_(severity_level_t::debug, "connection_t::~connection_t");
	}

	inline void open()
	{
		boost::asio::post(io_context_, [this]() { do_resolve_(); });
	}

	inline void close() 
	{
		boost::asio::post(io_context_, [this]() { connection_.close(); });
	}

	AMQP::Connection& amqp_connection() { return connection_; }

	inline void on_log(log_handler_t handler) { log_handler_ = handler; }
	inline void on_ready(ready_handler_t handler) { ready_handler_ = handler; }
	inline void on_error(error_handler_t handler) { error_handler_ = handler; }
	inline void on_closed(closed_handler_t handler) { closed_handler_ = handler; }

private:
	inline void log_(severity_level_t severity_level, std::string const& message)
	{
		if (log_handler_ != nullptr) { log_handler_(severity_level, message); }
	}

	void do_resolve_()
	{
		assert(!socket_.is_open());

		resolver_.async_resolve(
			address_.hostname(),
			std::to_string(address_.port()),
			boost::bind(&connection_t::on_resolve_, this,
				io::placeholders::error,
				io::placeholders::results));
	}

	void on_resolve_(
		sys::error_code const& ec,
		tcp::resolver::results_type const& endpoints)
	{
		if (!ec)
		{
			do_connect_(endpoints);
		}
		else
		{
			log_(
				severity_level_t::error,
				"async_amqp::connection_t::on_resolve_: " + ec.message());
		}
	}

	void do_connect_(tcp::resolver::results_type const& endpoints)
	{
		assert(!socket_.is_open());

		// Attempt a connection to each endpoint in the list until we
		// successfully establish a connection.
		io::async_connect(
			socket_,
			endpoints,
			boost::bind(
				&connection_t::on_connect_,
				this,
				io::placeholders::error));
	}

	void on_connect_(sys::error_code const& ec)
	{
		if (!ec)
		{
			// The connection was successful. Send the request.
			do_read_();
			if (!output_buffers_.empty())
			{
				do_write_();
			}
		}
		else
		{
			log_(
				severity_level_t::error,
				"async_amqp::connection_t::on_connect_: " + ec.message());
		}
	}

	void do_write_()
	{
		if (!socket_.is_open()) { return; }

		io::async_write(socket_,
			io::buffer(output_buffers_.front()),
			boost::bind(
				&connection_t::on_write_,
				this,
				boost::asio::placeholders::error));
	}


	void on_write_(boost::system::error_code ec)
	{
		if (!ec)
		{
			output_buffers_.pop_front();
			if (!output_buffers_.empty())
			{
				do_write_();
			}
		}
		else
		{
			log_(
				severity_level_t::error,
				"async_amqp::connection_t::on_write_: " + ec.message());
			if (socket_.is_open()) { do_close_(); }
		}
	}

	void do_read_()
	{
		if (!socket_.is_open()) { return; }

		io::async_read(
			socket_,
			input_buffer_,
			boost::asio::transfer_at_least(
				connection_.expected() - parse_buffer_.size()),
			boost::bind(
				&connection_t::on_read_,
				this,
				boost::asio::placeholders::error));
	}

	void on_read_(sys::error_code const& ec)
	{
		if (!ec)
		{
			parse_buffer_.insert(
				parse_buffer_.end(),
				io::buffers_begin(input_buffer_.data()),
				io::buffers_end(input_buffer_.data()));
			input_buffer_.consume(input_buffer_.size());

			while (parse_buffer_.size() >= connection_.expected())
			{
				auto const parsed{ connection_.parse(
					parse_buffer_.data(), parse_buffer_.size()) };
				if (parsed > 0)
				{
					parse_buffer_.erase(
						parse_buffer_.begin(),
						parse_buffer_.begin() + parsed);
				}
				else
				{
					break;
				}
			}
			do_read_();
		}
		else
		{
			log_(
				severity_level_t::error,
				"async_amqp::connection_t::on_read_: " + ec.message());

			if (socket_.is_open()) { do_close_(); }
		}
	}

	void do_close_()
	{
		error_code error;
		socket_.close(error);
	}

	/**
	 *  Method that is called by the AMQP library every time it has data
	 *  available that should be sent to RabbitMQ.
	 *  @param  connection  pointer to the main connection object
	 *  @param  data        memory buffer with the data that should be sent to RabbitMQ
	 *  @param  size        size of the buffer
	 */
	virtual void onData(AMQP::Connection*, const char* data_p, size_t data_size)
	{
		// @todo
		//  Add your own implementation, for example by doing a call to the
		//  send() system call. But be aware that the send() call may not
		//  send all data at once, so you also need to take care of buffering
		//  the bytes that could not immediately be sent, and try to send
		//  them again when the socket becomes writable again

		std::vector<char> data(data_p, data_p + data_size);

		boost::asio::post(
			io_context_,
			[this, data = std::move(data)]() mutable
		{
			bool write_in_progress = !output_buffers_.empty();
			output_buffers_.emplace_back(std::move(data));
			if (!write_in_progress && socket_.is_open())
			{
				do_write_();
			}
		});
	}

	/**
	 *  Method that is called by the AMQP library when the login attempt
	 *  succeeded. After this method has been called, the connection is ready
	 *  to use.
	 *  @param  connection      The connection that can now be used
	 */
	virtual void onReady(AMQP::Connection* connection_p)
	{
		if (ready_handler_ != nullptr) { ready_handler_(*this); }
	}

	/**
	 *  Method that is called by the AMQP library when a fatal error occurs
	 *  on the connection, for example because data received from RabbitMQ
	 *  could not be recognized.
	 *  @param  connection      The connection on which the error occurred
	 *  @param  message         A human readable error message
	 */
	virtual void onError(AMQP::Connection* connection_p, const char* message)
	{
		// @todo
		//  add your own implementation, for example by reporting the error
		//  to the user of your program, log the error, and destruct the
		//  connection object because it is no longer in a usable state
		if (error_handler_ != nullptr) { error_handler_(*this, message); }
	}

	/**
	 *  Method that is called when the connection was closed. This is the
	 *  counter part of a call to Connection::close() and it confirms that the
	 *  AMQP connection was correctly closed.
	 *
	 *  @param  connection      The connection that was closed and that is now unusable
	 */
	virtual void onClosed(AMQP::Connection* connection_p)
	{
		// @todo
		//  add your own implementation, for example by closing down the
		//  underlying TCP connection too
		do_close_();
		if (closed_handler_ != nullptr) { closed_handler_(*this); }
	}

private:
	io::io_context& io_context_;
	//work_guard_t work_guard_;
	AMQP::Address address_;
	tcp::resolver resolver_;
	tcp::socket socket_;
	AMQP::Connection connection_;
	io::streambuf input_buffer_;
	std::vector<char> parse_buffer_;
	std::deque<std::vector<char>> output_buffers_;

	log_handler_t log_handler_{ nullptr };
	ready_handler_t ready_handler_{ nullptr };
	error_handler_t error_handler_{ nullptr };
	closed_handler_t closed_handler_{ nullptr };

};

} //namespace async_amqp