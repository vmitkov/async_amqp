/**
 *  @author Viktor Mitkov <vmitkov@mail.ru>
 */

#pragma once

#include <amqpcpp.h>
#include <boost/asio.hpp>

#include <algorithm>
#include <bitset>
#include <cassert>
#include <cstddef>
#include <deque>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace async_amqp
{

namespace sys = boost::system;
namespace io = boost::asio;
namespace ip = io::ip;
using tcp = ip::tcp;
using resolver = tcp::resolver;

using namespace std::literals;

template <class F>
auto scope_guard(F&& f)
{
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

class log_t
{
public:
    using log_handler_t = std::function<void(severity_level_t severity_level, std::string const& message)>;

    template <typename LogHandler>
    log_t(LogHandler log_handler) : log_handler_(std::move(log_handler))
    {
    }

    inline void log(severity_level_t severity_level, std::string const& message) const noexcept
    {
        try
        {
            if (log_handler_ != nullptr) log_handler_(severity_level, message);
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
            try
            {
                log(severity_level_t::error, std::string(level, ' ') + "system_error: "s + e.what() + ", message: "s + e.code().message());
                std::rethrow_if_nested(e);
            }
            catch (...)
            {
                log_exception(++level);
            }
        }
        catch (const sys::system_error& e)
        {
            try
            {
                log(severity_level_t::error, std::string(level, ' ') + "system_error: "s + e.what() + ", message: "s + e.code().message());
                std::rethrow_if_nested(e);
            }
            catch (...)
            {
                log_exception(++level);
            }
        }
        catch (const std::exception& e)
        {
            try
            {
                log(severity_level_t::error, std::string(level, ' ') + "exception: "s + e.what());
                std::rethrow_if_nested(e);
            }
            catch (...)
            {
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

private:
    log_handler_t log_handler_{nullptr};
};

class connection_t : public AMQP::ConnectionHandler, public log_t
{
public:
    using ready_handler_t = std::function<void(connection_t& self)>;
    using error_handler_t = std::function<void(connection_t& self, std::string const& message)>;
    using closed_handler_t = std::function<void(connection_t& self)>;

public:
    template <typename LogHandler>
    connection_t(
        io::io_context& io_context,
        AMQP::Address address,
        LogHandler log_handler)
        : log_t(std::move(log_handler)),
          io_context_(io_context),
          address_(std::move(address)),
          resolver_(io_context),
          socket_(io_context),
          heartbeat_timer_(io_context),
          wait_for_closed_timer_(io_context)
    {
        log(severity_level_t::debug, "async_amqp::connection_t::connection_t");
    }

    virtual ~connection_t() noexcept
    {
        log(severity_level_t::debug, "async_amqp::connection_t::~connection_t");
    }

    // This method should be called before calling the open method
    template <typename ReadyHandler>
    inline connection_t& on_ready(ReadyHandler handler) noexcept
    {
        ready_handler_ = std::move(handler);
        return *this;
    }

    // This method should be called before calling the open method
    template <typename ErrorHandler>
    inline connection_t& on_error(ErrorHandler handler) noexcept
    {
        error_handler_ = std::move(handler);
        return *this;
    }

    // This method should be called before calling the open method
    template <typename ClosedHandler>
    inline connection_t& on_closed(ClosedHandler handler) noexcept
    {
        closed_handler_ = std::move(handler);
        return *this;
    }

    inline void error(std::string const& message)
    {
        try
        {
            log(severity_level_t::error, message);
            if (error_handler_ != nullptr) error_handler_(*this, message);
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
                /*on_open_*/
                [this]() noexcept
                {
                    try
                    {
                        if (is_closed()) do_resolve_();
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
                /*on_close_*/
                [this]() noexcept
                {
                    try
                    {
                        if (state_[state_t::closing]) return;
                        state_.set(state_t::closing);
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

    void do_heartbeat(uint16_t interval)
    {
        try
        {
            io::post(
                io_context_,
                /*on_heartbeat_*/
                [this, interval]() noexcept
                {
                    try
                    {
                        interval_ = interval;
                        if (interval != 0)
                        {
                            do_timeout_check_();
                        }
                    }
                    catch (...)
                    {
                        log_exception("async_amqp::connection_t::on_heartbeat"s);
                    }
                });
        }
        catch (...)
        {
            std::throw_with_nested(std::runtime_error("async_amqp::connection_t::do_heartbeat"s));
        }
    }

    io::io_context& io_context() { return io_context_; }

    // Next public methods should be called only at the event processing loop of io_context_
    AMQP::Connection* amqp_connection() noexcept { return connection_o_ ? &(*connection_o_) : nullptr; }
    bool is_closed() const { return !socket_.is_open() && state_.none() && !connection_o_; }

private:
    void do_resolve_()
    {
        using namespace std::placeholders;
        try
        {
            assert(is_closed());

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
            auto guard{scope_guard([&](void*)
                { state_.reset(state_t::resolving); })};

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
            auto guard{scope_guard([&](void*)
                { state_.reset(state_t::connecting); })};

            if (!ec)
            {
                // The connection was successful. Send the request.
                connection_o_.emplace(this, address_.login(), address_.vhost());
                input_buffer_.prepare(connection_o_->maxFrame());

                do_read_();
                if (!output_buffers_.empty()) do_write_();
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
            if (!socket_.is_open()) return;

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

    void on_write_(sys::error_code ec) noexcept
    {
        try
        {
            auto guard{scope_guard([&](void*)
                { state_.reset(state_t::writing); })};

            if (!ec)
            {
                output_buffers_.pop_front();
                if (!output_buffers_.empty()) do_write_();
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
            if (!socket_.is_open() || !connection_o_) return;

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
            auto guard{scope_guard([&](void*)
                { state_.reset(state_t::reading); })};

            if (!connection_o_) return;

            if (!ec)
            {
                parse_buffer_.insert(
                    parse_buffer_.end(),
                    io::buffers_begin(input_buffer_.data()),
                    io::buffers_end(input_buffer_.data()));

                while (parse_buffer_.size() >= connection_o_->expected())
                {
                    auto const parsed{connection_o_->parse(
                        parse_buffer_.data(), parse_buffer_.size())};
                    if (parsed > 0 && parse_buffer_.size() >= parsed)
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
                input_buffer_.consume(input_buffer_.size());
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
            if (connection_o_ && connection_o_->usable())
            {
                connection_o_->close();
            }
            else if (socket_.is_open())
            {
                sys::error_code error;
                socket_.close(error);
            }
            do_wait_for_closed_();
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
            if (!state_[state_t::closing]) return;

            if (state_[state_t::resolving]
                || state_[state_t::connecting]
                || state_[state_t::reading]
                || state_[state_t::writing]
                || socket_.is_open())
            {
                wait_for_closed_timer_.expires_after(io::chrono::seconds(1));
                wait_for_closed_timer_.async_wait(
                    /*on_wait_for_closed_*/
                    [this](sys::error_code const& ec) noexcept
                    {
                        try
                        {
                            if (ec != io::error::operation_aborted)
                            {
                                if (ec) throw std::system_error(ec);

                                if (socket_.is_open() && ++wait_counter_ >= 5)
                                {
                                    sys::error_code error;
                                    socket_.close(error);
                                }
                                do_wait_for_closed_();
                            }
                        }
                        catch (...)
                        {
                            log_exception("async_amqp::connection_t::on_wait_for_closed_"s);
                        }
                    });
            }
            else
            {
                connection_o_.reset();
                state_.reset();
                input_buffer_.consume(input_buffer_.size());
                parse_buffer_.clear();
                output_buffers_.clear();
                wait_counter_ = 0;
                if (closed_handler_ != nullptr) closed_handler_(*this);
            }
        }
        catch (...)
        {
            std::throw_with_nested(std::runtime_error("async_amqp::connection_t::do_wait_for_closed_"s));
        }
    }

    void on_timeout_check_(sys::error_code const& ec) noexcept
    {
        try
        {
            if (ec != io::error::operation_aborted)
            {
                if (ec) throw std::system_error(ec);
                if (connection_o_) connection_o_->heartbeat();
                do_timeout_check_();
            }
        }
        catch (...)
        {
            log_exception("async_amqp::connection_t::on_timeout_check_"s);
        }
    }

    void do_timeout_check_()
    {
        using namespace std::placeholders;
        try
        {
            heartbeat_timer_.expires_after(io::chrono::seconds(interval_));
            heartbeat_timer_.async_wait(std::bind(&connection_t::on_timeout_check_, this, _1));
        }
        catch (...)
        {
            std::throw_with_nested(std::runtime_error("async_amqp::connection_t::do_timeout_check_"));
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
                /*on_data_*/
                [this, data = std::move(data)]() mutable noexcept
                {
                    try
                    {
                        bool write_in_progress{!output_buffers_.empty()};
                        output_buffers_.emplace_back(std::move(data));
                        if (!write_in_progress && socket_.is_open()) do_write_();
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
            if (ready_handler_ != nullptr) ready_handler_(*this);
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
            if (error_handler_ != nullptr) error_handler_(*this, message);
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
            if (socket_.is_open())
            {
                sys::error_code error;
                socket_.close(error);
            }
        }
        catch (...)
        {
            log_exception("async_amqp::connection_t::onClosed"s);
        }
    }

    /**
     *  Method that is called when the server tries to negotiate a heartbeat
     *  interval, and that is overridden to get rid of the default implementation
     *  (which vetoes the suggested heartbeat interval), and accept the interval
     *  instead.
     *  @param  connection      The connection on which the error occurred
     *  @param  interval        The suggested interval in seconds
     */
    virtual uint16_t onNegotiate(AMQP::Connection* connection_p, uint16_t interval)
    {
        // @todo
        //  set a timer in your event loop, and make sure that you call
        //  connection->heartbeat() every _interval_ seconds if no other
        //  instruction was sent in that period.

        // return the interval that we want to use
        return interval_;
    }

    /**
     *  Method that is called when the AMQP-CPP library received a heartbeat
     *  frame that was sent by the server to the client.
     *
     *  You do not have to do anything here, the client sends back a heartbeat
     *  frame automatically, but if you like, you can implement/override this
     *  method if you want to be notified of such heartbeats
     *
     *  @param  connection      The connection over which the heartbeat was received
     */
    virtual void onHeartbeat(AMQP::Connection* connection_p)
    {
#ifndef NDEBUG
        log(severity_level_t::trace, "async_amqp::connection_t::onHeartbeat"s);
#endif
    }

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

    ready_handler_t ready_handler_{nullptr};
    error_handler_t error_handler_{nullptr};
    closed_handler_t closed_handler_{nullptr};

    std::bitset<state_t::size> state_;
    uint16_t interval_{0};
    io::steady_timer heartbeat_timer_;
    io::steady_timer wait_for_closed_timer_;
    int wait_counter_{0};
};

} // namespace async_amqp
