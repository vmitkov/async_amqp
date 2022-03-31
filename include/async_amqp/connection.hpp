#pragma once

#include <amqpcpp.h>
#include <boost/asio.hpp>

#include <string>
#include <iostream> 
#include <algorithm>
#include <deque>
#include <vector>
#include <cassert>
#include <cstddef>
#include <optional>
#include <bitset>
#include <memory>

namespace async_amqp
{

namespace sys = boost::system;
namespace io = boost::asio;
namespace ip = io::ip;
using tcp = ip::tcp;
using resolver = tcp::resolver;
using error_code = boost::system::error_code;
using work_guard_t = io::executor_work_guard<io::io_context::executor_type>;

using namespace std::literals;

template<class F> auto scope_guard(F&& f) {
	return std::unique_ptr<void, F>{(void*)1, std::forward<F>(f)};
}

// auto unique = scope_guard([&](void*) {/* cleanup here */});


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

class connection_t : public AMQP::ConnectionHandler
{
public:
	using log_handler_t = std::function<void(severity_level_t severity_level, std::string const& message)>;
	using ready_handler_t = std::function<void(connection_t& self)>;
	using error_handler_t = std::function<void(connection_t& self, std::string const& message)>;
	using closed_handler_t = std::function<void(connection_t& self)>;

public:
	connection_t(io::io_context& io_context, AMQP::Address&& address, log_handler_t log_handler) :
		//work_guard_(io_context.get_executor()),
		io_context_(io_context),
		address_(std::move(address)),
		resolver_(io_context),
		socket_(io_context),
		log_handler_(log_handler)
	{
		log(severity_level_t::debug, "async_amqp::connection_t::connection_t");
	}

	virtual ~connection_t() noexcept
	{
		log(severity_level_t::debug, "async_amqp::connection_t::~connection_t");
	}

	inline void log(severity_level_t severity_level, std::string const& message) const noexcept
	{
		try
		{
			if (log_handler_ != nullptr) { log_handler_(severity_level, message); }
		}
		catch (...)
		{
		}
	}

	inline void log_exception(int level = 0) const noexcept
	{
		try
		{
			throw;
		}
		catch (const std::system_error& e)
		{
			try {
				log(severity_level_t::error, std::string(level, ' ')
					+ "system_error: "s + e.what() + ", message: "s + e.code().message());
				std::rethrow_if_nested(e);
			}
			catch (...) {
				log_exception(++level);
			}
		}
		catch (const boost::system::system_error& e)
		{
			try {
				log(severity_level_t::error, std::string(level, ' ')
					+ "system_error: "s + e.what() + ", message: "s + e.code().message());
				std::rethrow_if_nested(e);
			}
			catch (...) {
				log_exception(++level);
			}
		}
		catch (const std::exception& e)
		{
			try {
				log(severity_level_t::error, std::string(level, ' ')
					+ "exception: "s + e.what());
				std::rethrow_if_nested(e);
			}
			catch (...) {
				log_exception(++level);
			}
		}
		catch (...)
		{
			log(severity_level_t::error, std::string(level, ' ') + "unknown exception"s);
		}
	}

	inline void log_exception(std::string const& message) const noexcept
	{
		assert(!message.empty());
		log(severity_level_t::error, message);
		log_exception(1);
	}

	inline void error(std::string const& message)
	{
		try
		{
			log(severity_level_t::error, message);
			if (error_handler_ != nullptr) { error_handler_(*this, message); }
		}
		catch (...)
		{
			std::throw_with_nested(std::runtime_error("async_amqp::connection_t::error"s));
		}
	}

	inline void open()
	{
		try
		{
			io::post(io_context_,
				/*on_open_*/[this]() noexcept
				{
					try
					{
						if (state_.none()) { do_resolve_(); }
					}
					catch (...)
					{
						log_exception("async_amqp::connection_t::on_open_"s);
					}
				});
		}
		catch (...)
		{
			std::throw_with_nested(std::runtime_error("async_amqp::connection_t::open"s));
		}
	}

	inline void close()
	{
		try
		{
			io::post(io_context_,
				/*on_close_*/[this]() noexcept
				{
					try
					{
						if (state_[state_t::closing]) { return; }

						state_.set(state_t::closing);

						if (connection_o_) { connection_o_.reset(); }
						do_close_();
					}
					catch (...)
					{
						log_exception("async_amqp::connection_t::on_close_"s);
					}
				});
		}
		catch (...)
		{
			std::throw_with_nested(std::runtime_error("async_amqp::connection_t::close"s));
		}
	}

	AMQP::Connection* amqp_connection() noexcept { return connection_o_ ? &(*connection_o_) : nullptr; }

	inline void on_ready(ready_handler_t handler) noexcept { ready_handler_ = handler; }
	inline void on_error(error_handler_t handler) noexcept { error_handler_ = handler; }
	inline void on_closed(closed_handler_t handler) noexcept { closed_handler_ = handler; }

private:
	void do_resolve_()
	{
		using namespace std::placeholders;
		try
		{
			assert(!socket_.is_open() && state_.none());

			state_.set(state_t::resolving);

			resolver_.async_resolve(
				address_.hostname(),
				std::to_string(address_.port()),
				std::bind(&connection_t::on_resolve_, this, _1, _2));
		}
		catch (...)
		{
			std::throw_with_nested(std::runtime_error("async_amqp::connection_t::do_resolve_"s));
		}
	}

	void on_resolve_(sys::error_code const& ec, resolver::results_type const& endpoints) noexcept
	{
		try
		{
			auto guard{ scope_guard([&](void*) { state_.reset(state_t::resolving); }) };

			if (!ec)
			{
				do_connect_(endpoints);
			}
			else
			{
				error("async_amqp::connection_t::on_resolve_: " + ec.message());
			}
		}
		catch (...)
		{
			log_exception("async_amqp::connection_t::on_resolve_"s);
		}
	}

	void do_connect_(tcp::resolver::results_type const& endpoints)
	{
		using namespace std::placeholders;

		try
		{
			assert(!socket_.is_open() && state_[state_t::resolving]);

			state_.set(state_t::connecting);

			// Attempt a connection to each endpoint in the list until we
			// successfully establish a connection.
			io::async_connect(socket_, endpoints, std::bind(&connection_t::on_connect_, this, _1));
		}
		catch (...)
		{
			std::throw_with_nested(std::runtime_error("async_amqp::connection_t::do_connect_"s));
		}
	}

	void on_connect_(sys::error_code const& ec) noexcept
	{
		try
		{
			auto guard{ scope_guard([&](void*) { state_.reset(state_t::connecting); }) };

			if (!ec)
			{
				// The connection was successful. Send the request.
				connection_o_.emplace(this, address_.login(), address_.vhost());
				input_buffer_.prepare(connection_o_->maxFrame());

				do_read_();
				if (!output_buffers_.empty())
				{
					do_write_();
				}
			}
			else
			{
				error("async_amqp::connection_t::on_connect_: " + ec.message());
			}
		}
		catch (...)
		{
			log_exception("async_amqp::connection_t::on_connect_"s);
		}
	}

	void do_write_()
	{
		using namespace std::placeholders;

		try
		{
			if (!socket_.is_open()) { return; }

			state_.set(state_t::writing);

			io::async_write(socket_,
				io::buffer(output_buffers_.front()),
				std::bind(&connection_t::on_write_, this, _1));
		}
		catch (...)
		{
			std::throw_with_nested(std::runtime_error("async_amqp::connection_t::do_write_"s));
		}
	}

	void on_write_(boost::system::error_code ec) noexcept
	{
		try
		{
			auto guard{ scope_guard([&](void*) { state_.reset(state_t::writing); }) };

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
				error("async_amqp::connection_t::on_write_: " + ec.message());
			}
		}
		catch (...)
		{
			log_exception("async_amqp::connection_t::on_write_"s);
		}
	}

	void do_read_()
	{
		using namespace std::placeholders;

		try
		{
			if (!socket_.is_open() || !connection_o_) { return; }

			state_.set(state_t::reading);

			io::async_read(
				socket_,
				input_buffer_,
				io::transfer_at_least(
					connection_o_->expected() - parse_buffer_.size()),
				std::bind(&connection_t::on_read_, this, _1));
		}
		catch (...)
		{
			std::throw_with_nested(std::runtime_error("async_amqp::connection_t::do_read_"s));
		}
	}

	void on_read_(sys::error_code const& ec) noexcept
	{
		try
		{
			if (!connection_o_) { return; }

			auto guard{ scope_guard([&](void*) { state_.reset(state_t::reading); }) };

			if (!ec)
			{
				parse_buffer_.insert(
					parse_buffer_.end(),
					io::buffers_begin(input_buffer_.data()),
					io::buffers_end(input_buffer_.data()));
				input_buffer_.consume(input_buffer_.size());

				while (parse_buffer_.size() >= connection_o_->expected())
				{
					auto const parsed{ connection_o_->parse(
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
				error("async_amqp::connection_t::on_read_: " + ec.message());
			}
		}
		catch (...)
		{
			log_exception("async_amqp::connection_t::on_read_"s);
		}
	}

	void do_close_()
	{
		try
		{
			if (socket_.is_open())
			{
				error_code error;
				socket_.close(error);
				do_wait_for_closed_();
			}
		}
		catch (...)
		{
			std::throw_with_nested(std::runtime_error("async_amqp::connection_t::do_close_"s));
		}
	}

	void do_wait_for_closed_()
	{
		try
		{
			if (!state_[state_t::closing]) { return; }

			if (state_[state_t::resolving]
				|| state_[state_t::connecting]
				|| state_[state_t::reading]
				|| state_[state_t::writing])
			{
				io::post(io_context_,
					/*on_wait_for_closed_*/[this]() noexcept
					{
						try
						{
							do_wait_for_closed_();
						}
						catch (...)
						{
							log_exception("async_amqp::connection_t::on_wait_for_closed_"s);
						}
					});
			}
			else
			{
				state_.reset();
				if (closed_handler_ != nullptr) { closed_handler_(*this); }
			}
		}
		catch (...)
		{
			std::throw_with_nested(std::runtime_error("async_amqp::connection_t::do_wait_for_closed_"s));
		}
	}

	/**
	 *  Method that is called by the AMQP library every time it has data
	 *  available that should be sent to RabbitMQ.
	 *  @param  connection  pointer to the main connection object
	 *  @param  data        memory buffer with the data that should be sent to RabbitMQ
	 *  @param  size        size of the buffer
	 */
	virtual void onData(AMQP::Connection*, const char* data_p, std::size_t data_size) noexcept
	{
		// @todo
		//  Add your own implementation, for example by doing a call to the
		//  send() system call. But be aware that the send() call may not
		//  send all data at once, so you also need to take care of buffering
		//  the bytes that could not immediately be sent, and try to send
		//  them again when the socket becomes writable again
		try
		{
			std::vector<char> data(data_p, data_p + data_size);

			io::post(
				io_context_,
				/*on_data_*/[this, data = std::move(data)]() mutable noexcept
			{
				try
				{
					bool write_in_progress = !output_buffers_.empty();
					output_buffers_.emplace_back(std::move(data));
					if (!write_in_progress && socket_.is_open())
					{
						do_write_();
					}
				}
				catch (...)
				{
					log_exception("async_amqp::connection_t::on_data_"s);
				}
			});
		}
		catch (...)
		{
			log_exception("async_amqp::connection_t::onData"s);
		}
	}

	/**
	 *  Method that is called by the AMQP library when the login attempt
	 *  succeeded. After this method has been called, the connection is ready
	 *  to use.
	 *  @param  connection      The connection that can now be used
	 */
	virtual void onReady(AMQP::Connection* connection_p) noexcept
	{
		try
		{
			if (ready_handler_ != nullptr) { ready_handler_(*this); }
		}
		catch (...)
		{
			log_exception("async_amqp::connection_t::onReady"s);
		}
	}

	/**
	 *  Method that is called by the AMQP library when a fatal error occurs
	 *  on the connection, for example because data received from RabbitMQ
	 *  could not be recognized.
	 *  @param  connection      The connection on which the error occurred
	 *  @param  message         A human readable error message
	 */
	virtual void onError(AMQP::Connection* connection_p, const char* message) noexcept
	{
		// @todo
		//  add your own implementation, for example by reporting the error
		//  to the user of your program, log the error, and destruct the
		//  connection object because it is no longer in a usable state
		try
		{
			if (error_handler_ != nullptr) { error_handler_(*this, message); }
		}
		catch (...)
		{
			log_exception("async_amqp::connection_t::onError"s);
		}
	}

	/**
	 *  Method that is called when the connection was closed. This is the
	 *  counter part of a call to Connection::close() and it confirms that the
	 *  AMQP connection was correctly closed.
	 *
	 *  @param  connection      The connection that was closed and that is now unusable
	 */
	virtual void onClosed(AMQP::Connection* connection_p) noexcept
	{
		// @todo
		//  add your own implementation, for example by closing down the
		//  underlying TCP connection too
		try
		{
			connection_o_.reset();
			do_close_();
		}
		catch (...)
		{
			log_exception("async_amqp::connection_t::onClosed"s);
		}
	}

private:
	enum state_t
	{
		resolving,
		connecting,
		reading,
		writing,
		closing,
		size
	};

	io::io_context& io_context_;
	AMQP::Address address_;
	tcp::resolver resolver_;
	tcp::socket socket_;
	std::optional<AMQP::Connection> connection_o_;
	io::streambuf input_buffer_;
	std::vector<char> parse_buffer_;
	std::deque<std::vector<char>> output_buffers_;

	log_handler_t log_handler_{ nullptr };
	ready_handler_t ready_handler_{ nullptr };
	error_handler_t error_handler_{ nullptr };
	closed_handler_t closed_handler_{ nullptr };

	std::bitset<state_t::size> state_;
};

} //namespace async_amqp
