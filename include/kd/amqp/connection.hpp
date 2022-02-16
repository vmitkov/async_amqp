#pragma once

#include <amqpcpp.h>

#include <boost/asio.hpp>
#include <boost/bind.hpp>

#include <iostream> 
#include <algorithm>
#include <deque>
#include <vector>

namespace kd::amqp
{

namespace sys = boost::system;
namespace io = boost::asio;
namespace ip = io::ip;
using tcp = ip::tcp;
using error_code = boost::system::error_code;
using work_guard_t = io::executor_work_guard<io::io_context::executor_type>;

class connection_t : private AMQP::ConnectionHandler
{
public:
	connection_t(io::io_context& io_context, AMQP::Address&& address)
		: connection_(this, address.login(), address.vhost()),
		work_guard_(io_context.get_executor()),
		io_context_(io_context),
		address_(std::move(address)),
		resolver_(io_context),
		socket_(io_context)
	{
		input_buffer_.prepare(connection_.maxFrame());
		do_resolve_();
	}

private:

	void do_resolve_()
	{
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
			std::cout << "Error: " << ec.message() << "\n";
		}
	}

	void do_connect_(tcp::resolver::results_type const& endpoints)
	{
		// Attempt a connection to each endpoint in the list until we
		// successfully establish a connection.
		io::async_connect(socket_, endpoints,
			boost::bind(&connection_t::on_connect_, this,
				io::placeholders::error));
	}

	void on_connect_(sys::error_code const& ec)
	{
		if (!ec)
		{
			// The connection was successful. Send the request.
			std::cout << "Connected\n";
			do_read_();
			if (!output_buffers_.empty())
			{
				do_write_();
			}
		}
		else
		{
			std::cout << "Error: " << ec.message() << "\n";
		}
	}

	void do_write_()
	{
		io::async_write(socket_,
			io::buffer(output_buffers_.front()),
			boost::bind(&connection_t::on_write_, this,
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
			std::cout << "Error: " << ec.message() << "\n";
			socket_.close();
		}
	}

	void do_read_()
	{
		io::async_read(socket_, input_buffer_,
			boost::asio::transfer_at_least(
				connection_.expected() - parse_buffer_.size()),
			boost::bind(&connection_t::on_read_, this,
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

		}
		else if (ec != boost::asio::error::eof)
		{
			std::cout << "Error: " << ec << "\n";
			return;
		}

		do_read_();
	}

	/**
	 *  Method that is called by the AMQP library every time it has data
	 *  available that should be sent to RabbitMQ.
	 *  @param  connection  pointer to the main connection object
	 *  @param  data        memory buffer with the data that should be sent to RabbitMQ
	 *  @param  size        size of the buffer
	 */
	virtual void onData(AMQP::Connection* connection, const char* data_p, size_t data_size)
	{
		// @todo
		//  Add your own implementation, for example by doing a call to the
		//  send() system call. But be aware that the send() call may not
		//  send all data at once, so you also need to take care of buffering
		//  the bytes that could not immediately be sent, and try to send
		//  them again when the socket becomes writable again

		std::vector<char> data(data_p, data_p + data_size);

		boost::asio::post(io_context_,
			[this, data = std::move(data)]()
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
		// @todo
		//  add your own implementation, for example by creating a channel
		//  instance, and start publishing or consuming

		// and create a channel
		AMQP::Channel channel(connection_p);

		channel.declareExchange("X3", AMQP::direct);

		AMQP::Table arguments;
		arguments["x-queue-mode"] = "lazy";

		channel.declareQueue("calls", AMQP::durable, arguments);
		channel.bindQueue("X3", "calls", "calls");
	}

	/**
	 *  Method that is called by the AMQP library when a fatal error occurs
	 *  on the connection, for example because data received from RabbitMQ
	 *  could not be recognized.
	 *  @param  connection      The connection on which the error occurred
	 *  @param  message         A human readable error message
	 */
	virtual void onError(AMQP::Connection* connection, const char* message)
	{
		// @todo
		//  add your own implementation, for example by reporting the error
		//  to the user of your program, log the error, and destruct the
		//  connection object because it is no longer in a usable state
	}

	/**
	 *  Method that is called when the connection was closed. This is the
	 *  counter part of a call to Connection::close() and it confirms that the
	 *  AMQP connection was correctly closed.
	 *
	 *  @param  connection      The connection that was closed and that is now unusable
	 */
	virtual void onClosed(AMQP::Connection* connection)
	{
		// @todo
		//  add your own implementation, for example by closing down the
		//  underlying TCP connection too
	}

private:
	io::io_context& io_context_;
	work_guard_t work_guard_;
	AMQP::Address address_;
	tcp::resolver resolver_;
	tcp::socket socket_;
	AMQP::Connection connection_;
	io::streambuf input_buffer_;
	std::vector<char> parse_buffer_;
	std::deque<std::vector<char>> output_buffers_;
};

} //namespace kd::amqp
