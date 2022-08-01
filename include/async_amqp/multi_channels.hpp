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
#include <functional>
#include <map>

namespace async_amqp
{

namespace io = boost::asio;

using namespace std::literals;

template<typename Parent>
class in_channel_t
{
public:
    using received_handler_t = std::function<void(Parent&, boost::json::object&&)>;

    template<typename ReceivedHandler>
    in_channel_t(
        Parent& parent,
        std::string const& exchange,
        std::string const& queue,
        ReceivedHandler&& handler)
        : parent_(parent),
        exchange_(exchange),
        queue_(queue),
        received_handler_(std::move(handler))
    {
        try
        {
            parent_.log(severity_level_t::debug, "async_amqp::in_channel_t::in_channel_t");
        }
        catch (...)
        {
            std::throw_with_nested(std::runtime_error("async_amqp::in_channel_t::in_channel_t"));
        }
    }

    ~in_channel_t() noexcept
    {
        parent_.log(severity_level_t::debug, "async_amqp::in_channel_t::~in_channel_t");
    }

    void open(AMQP::Connection* connection_p)
    {
        using namespace std::placeholders;
        try
        {
            assert(connection_p != nullptr);

            AMQP::Table arguments;
            arguments["x-queue-mode"] = "lazy";

            channel_o_.emplace(connection_p);
            channel_o_->onError(std::bind(&in_channel_t::on_channel_error_, this, _1));

            channel_o_->declareExchange(exchange_, AMQP::direct);

            channel_o_->declareQueue(queue_, AMQP::durable, arguments);
            channel_o_->bindQueue(exchange_, queue_, "");

            channel_o_->consume(queue_).onReceived(std::bind(
                &in_channel_t::on_received_, this, _1, _2, _3));
        }
        catch (...)
        {
            std::throw_with_nested(std::runtime_error("async_amqp::in_channel_t::open"));
        }
    }

    void close()
    {
        try
        {
            if (channel_o_) { channel_o_->close(); channel_o_.reset(); }
        }
        catch (...)
        {
            std::throw_with_nested(std::runtime_error("async_amqp::in_channel_t::close_connection_"s));
        }
    }

private:
    void on_channel_error_(char const* message) noexcept
    {
        try
        {
            parent_.log(severity_level_t::error,
                "async_amqp::in_channel_t::on_channel_error_: "s + message);
            parent_.close_connection_();
        }
        catch (...)
        {
            parent_.log_exception("async_amqp::in_channel_t::on_channel_error_"s);
        }
    }

    void on_received_(
        const AMQP::Message& message, uint64_t delivery_tag, bool redelivered) noexcept
    {
        try
        {
            assert(channel_o_);

            channel_o_->ack(delivery_tag);
            std::string message_str(message.body(), message.bodySize());

            parent_.log(severity_level_t::trace,
                "async_amqp::in_channel_t::on_received_: "s
                + exchange_ + ":"s + queue_ + " -> "s + message_str);

            if (received_handler_ != nullptr)
            {
                received_handler_(parent_,
                    boost::json::object{boost::json::parse(message_str).as_object()});
            }
        }
        catch (...)
        {
            parent_.log_exception("async_amqp::in_channel_t::on_received_"s);
        }
    }

private:
    Parent& parent_;
    std::string const exchange_;
    std::string const queue_;
    received_handler_t received_handler_{nullptr};
    std::optional<AMQP::Channel> channel_o_;
};

template<typename Parent>
class out_channel_t
{
public:
    using ready_handler_t = std::function<void(Parent&)>;

    template<typename ReadyHandler>
    out_channel_t(
        Parent& parent,
        std::string const& exchange,
        std::string const& route,
        ReadyHandler&& handler)
        : parent_(parent),
        exchange_(exchange),
        route_(route),
        ready_handler_(std::move(handler))
    {
        try
        {
            parent_.log(severity_level_t::debug, "async_amqp::out_channel_t::out_channel_t");
        }
        catch (...)
        {
            std::throw_with_nested(std::runtime_error("async_amqp::out_channel_t::out_channel_t"));
        }
    }

    ~out_channel_t() noexcept
    {
        parent_.log(severity_level_t::debug, "async_amqp::out_channel_t::~out_channel_t");
    }

    void open(AMQP::Connection* connection_p)
    {
        using namespace std::placeholders;
        try
        {
            assert(connection_p != nullptr);

            AMQP::Table arguments;
            arguments["x-queue-mode"] = "lazy";

            channel_o_.emplace(connection_p);
            channel_o_->onError(std::bind(&out_channel_t::on_channel_error_, this, _1));
            channel_o_->onReady(std::bind(&out_channel_t::on_channel_ready_, this));

            channel_o_->declareExchange(exchange_, AMQP::direct);

            channel_o_->declareQueue(route_, AMQP::durable, arguments);
            channel_o_->bindQueue(exchange_, route_, route_);

            reliable_o_.emplace(*channel_o_);
            reliable_o_->onError(std::bind(&out_channel_t::on_channel_error_, this, _1));
        }
        catch (...)
        {
            std::throw_with_nested(std::runtime_error("async_amqp::out_channel_t::open"));
        }
    }

    void close()
    {
        try
        {
            if (reliable_o_) { reliable_o_->close(); reliable_o_.reset(); }
            if (channel_o_) { channel_o_->close(); channel_o_.reset(); }
        }
        catch (...)
        {
            std::throw_with_nested(std::runtime_error("async_amqp::out_channel_t::close_connection_"s));
        }
    }

    inline void publish(boost::json::object&& obj)
    {
        try
        {
            io::post(parent_.io_context_,
                [this, obj = std::move(obj)]() mutable noexcept { on_publish_(std::move(obj)); });
        }
        catch (...)
        {
            std::throw_with_nested(std::runtime_error("async_amqp::out_channel_t::publish"));
        }
    }

private:
    void on_channel_error_(char const* message) noexcept
    {
        try
        {
            parent_.log(severity_level_t::error,
                "async_amqp::out_channel_t::on_channel_error_: "s + message);
            parent_.close_connection_();
        }
        catch (...)
        {
            parent_.log_exception("async_amqp::out_channel_t::on_channel_error_"s);
        }
    }

    void on_channel_ready_() noexcept
    {
        try
        {
            if (ready_handler_ != nullptr) { ready_handler_(parent_); }
        }
        catch (...)
        {
            parent_.log_exception("async_amqp::out_channel_t::on_channel_ready_"s);
        }
    }

    void on_publish_ack_(std::string buffer) noexcept
    {
        try
        {
            parent_.log(severity_level_t::trace,
                "async_amqp::out_channel_t::on_publish_ack_: "s
                + exchange_ + ":"s + route_ + " <- "s + buffer);
        }
        catch (...)
        {
            parent_.log_exception("async_amqp::out_channel_t::on_publish_ack_"s);
        }
    }

    void on_publish_lost_(std::string buffer) noexcept
    {
        try
        {
            parent_.log(severity_level_t::error,
                "async_amqp::out_channel_t::on_publish_lost_: "s
                + exchange_ + ":"s + route_ + " <- "s + buffer);
        }
        catch (...)
        {
            parent_.log_exception("async_amqp::out_channel_t::on_publish_lost_"s);
        }
    }

    void on_publish_error_(std::string buffer, char const* message) noexcept
    {
        try
        {
            parent_.log(severity_level_t::error,
                "async_amqp::out_channel_t::on_publish_error_: "s + message
                + ", "s + exchange_ + ":"s + route_ + " <- "s + buffer);
            parent_.close_connection_();
        }
        catch (...)
        {
            parent_.log_exception("async_amqp::out_channel_t::on_publish_error_"s);
        }
    }

    void on_publish_(boost::json::object&& obj) noexcept
    {
        using namespace std::placeholders;
        try
        {
            if (!channel_o_ || !reliable_o_ || !channel_o_->usable())
            {
                throw std::runtime_error("Channel is closed");
            }

            std::string buffer{boost::json::serialize(obj)};
            AMQP::Envelope envelope(buffer.data(), buffer.size());
            envelope.setContentType("application/json");
            envelope.setContentEncoding("utf-8");

            reliable_o_->publish(exchange_, route_, envelope)
                .onAck(std::bind(&out_channel_t::on_publish_ack_, this, buffer))
                .onLost(std::bind(&out_channel_t::on_publish_lost_, this, buffer))
                .onError(std::bind(&out_channel_t::on_publish_error_, this, buffer, _1));
        }
        catch (...)
        {
            parent_.log_exception("async_amqp::out_channel_t::on_publish_"s);
        }
    }

    Parent& parent_;
    std::string const exchange_;
    std::string const route_;
    ready_handler_t ready_handler_{nullptr};
    std::optional<AMQP::Channel> channel_o_;
    std::optional<AMQP::Reliable<>> reliable_o_;
};

class channels_t : public log_t
{
public:

    using received_handler_t = std::function<void(channels_t&, boost::json::object&&)>;
    using ready_handler_t = std::function<void(channels_t&)>;

    template<typename LogHandler>
    channels_t(
        io::io_context& io_context,
        std::string const& url,
        LogHandler&& log_handler)
        : io_context_(io_context),
        url_(url),
        log_t(std::move(log_handler)),
        reconnection_timer_(io_context_)
    {
        try
        {
            log(severity_level_t::debug, "async_amqp::channels_t::channels_t");
            do_wait_for_reconnection_();
        }
        catch (...)
        {
            std::throw_with_nested(std::runtime_error("async_amqp::channels_t::channels_t"));
        }
    }

    ~channels_t() noexcept
    {
        log(severity_level_t::debug, "async_amqp::channels_t::~channels_t");
    }

    inline void open()
    {
        try
        {
            finish_ = false;
            open_connection_();
        }
        catch (...)
        {
            std::throw_with_nested(std::runtime_error("async_amqp::channels_t::open"));
        }
    }

    inline void close()
    {
        try
        {
            finish_ = true;
            close_connection_();
        }
        catch (...)
        {
            std::throw_with_nested(std::runtime_error("async_amqp::channels_t::close"));
        }
    }

    inline void publish(std::string const& name, boost::json::object&& obj)
    {
        try
        {
            if (auto it = out_channels_.find(name); it != end(out_channels_))
            {
                it->second.publish(std::move(obj));
            }
            else
            {
                log(severity_level_t::error,
                    "async_amqp::channels_t::publish: channel "s + name + " not found"s);
            }
        }
        catch (...)
        {
            std::throw_with_nested(std::runtime_error("async_amqp::channels_t::publish"));
        }
    }

    template<typename Name, typename ReceivedHandler>
    auto add_in_channel(
        Name&& name,
        std::string const& exchange,
        std::string const& queue,
        ReceivedHandler&& handler)
    {
        return in_channels_.try_emplace(
            std::forward<Name>(name), *this, exchange, queue, std::move(handler));
    }

    template<typename Name, typename ReadyHandler>
    auto add_out_channel(
        Name&& name,
        std::string const& exchange,
        std::string const& route,
        ReadyHandler&& handler)
    {
        return out_channels_.try_emplace(
            std::forward<Name>(name), *this, exchange, route, std::move(handler));
    }

private:

    void open_connection_()
    {
        using namespace std::placeholders;
        try
        {
            connection_o_.emplace(
                io_context_, url_, std::bind(&channels_t::log, this, _1, _2));

            connection_o_->on_ready(std::bind(&channels_t::on_connection_ready_, this, _1));
            connection_o_->on_error(std::bind(&channels_t::on_connection_error_, this, _1, _2));
            connection_o_->on_closed(std::bind(&channels_t::on_connection_closed_, this, _1));
            connection_o_->open();
        }
        catch (...)
        {
            std::throw_with_nested(std::runtime_error("async_amqp::channels_t::open_connection_"s));
        }
    }

    inline void close_connection_()
    {
        try
        {
            for (auto& channel : in_channels_) { channel.second.close(); }
            for (auto& channel : out_channels_) { channel.second.close(); }
            if (connection_o_) { connection_o_->close(); }
        }
        catch (...)
        {
            std::throw_with_nested(std::runtime_error("async_amqp::channels_t::close_connection_"s));
        }
    }

    void do_wait_for_reconnection_()
    {
        try
        {
            reconnection_timer_.expires_after(io::chrono::seconds(5));
            reconnection_timer_.async_wait(
                /*on_wait_for_reconnection_*/[this](boost::system::error_code const& ec) noexcept
                {
                    try
                    {
                        if (ec) { throw std::system_error(ec); }
                        if (!connection_o_ && !finish_) { open_connection_(); }
                        do_wait_for_reconnection_();
                    }
                    catch (...)
                    {
                        log_exception("async_amqp::channels_t::on_wait_for_reconnection_"s);
                    }
                });
        }
        catch (...)
        {
            std::throw_with_nested(std::runtime_error("async_amqp::channels_t::do_wait_for_reconnection_"s));
        }
    }

    void on_connection_ready_(connection_t& connection) noexcept
    {
        try
        {
            if (!connection_o_) { throw std::runtime_error("Connection is closed"); }

            auto connection_p{connection_o_->amqp_connection()};
            if (connection_p == nullptr) { throw std::runtime_error("AMQP::Connection is closed"); }

            for (auto& channel : in_channels_) { channel.second.open(connection_p); }
            for (auto& channel : out_channels_) { channel.second.open(connection_p); }
        }
        catch (...)
        {
            log_exception("async_amqp::channels_t::on_ready_"s);
        }
    }

    void on_connection_error_(connection_t& connection, std::string message) noexcept
    {
        try
        {
            connection.log(severity_level_t::error,
                "async_amqp::channels_t::on_connection_error_: "s + message);
            close_connection_();
        }
        catch (...)
        {
            log_exception("async_amqp::channels_t::on_connection_error_"s);
        }
    }

    void on_connection_closed_(connection_t& connection) noexcept
    {
        try
        {
            connection.log(severity_level_t::info, "async_amqp::channels_t::on_connection_closed_: Connection closed"s);
            connection_o_.reset();
        }
        catch (...)
        {
            log_exception("async_amqp::channels_t::on_connection_closed_"s);
        }
    }

    template<typename Parent>
    friend class in_channel_t;

    template<typename Parent>
    friend class out_channel_t;

private:
    io::io_context& io_context_;
    std::string const url_;

    io::steady_timer reconnection_timer_;

    std::optional<connection_t> connection_o_;
    std::map<std::string, in_channel_t<channels_t>> in_channels_;
    std::map<std::string, out_channel_t<channels_t>> out_channels_;

    std::atomic<bool> finish_{false};
};

} //namespace async_amqp
