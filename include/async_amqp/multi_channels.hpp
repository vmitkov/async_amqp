/**
 *  @author Viktor Mitkov <vmitkov@mail.ru>
 */

#pragma once

#include "connection.hpp"

#include <amqpcpp.h>
#include <boost/asio.hpp>
#include <boost/json.hpp>

#include <atomic>
#include <cassert>
#include <cstddef>
#include <functional>
#include <map>
#include <optional>
#include <string>

namespace async_amqp
{

namespace io = boost::asio;
namespace json = boost::json;

using namespace std::literals;
using namespace std::placeholders;

namespace detail
{

template <typename Parent>
class channel_t_
{
public:
    channel_t_(
        Parent& parent,
        std::string const& exchange,
        std::string const& queue,
        std::string const& routing_key = ""s)
        : parent_(parent),
          exchange_(exchange),
          queue_(queue),
          routing_key_(routing_key),
          channel_name_(exchange_ + ':' + queue_)
    {
        parent_.log(severity_level_t::debug,
            "async_amqp::detail::channel_t_::channel_t_: " + channel_name_);
    }

    ~channel_t_() noexcept
    {
        parent_.log(severity_level_t::debug,
            "async_amqp::detail::channel_t_::~channel_t_: " + channel_name_);
    }

    channel_t_& exchange_type(AMQP::ExchangeType type)
    {
        exchange_type_ = type;
        return *this;
    }

    channel_t_& exchange_flags(int flags)
    {
        exchange_flags_ = flags;
        return *this;
    }

    channel_t_& exchange_arguments(AMQP::Table const& arguments)
    {
        exchange_arguments_ = arguments;
        return *this;
    }

    channel_t_& queue_flags(int flags)
    {
        queue_flags_ = flags;
        return *this;
    }

    channel_t_& queue_arguments(AMQP::Table const& arguments)
    {
        queue_arguments_ = arguments;
        return *this;
    }

    channel_t_& bind_arguments(AMQP::Table const& arguments)
    {
        bind_arguments_ = arguments;
        return *this;
    }

    void open(AMQP::Connection* connection_p)
    {
        try
        {
            assert(connection_p != nullptr);

            channel_o_.emplace(connection_p);
            channel_o_->onError(std::bind(&channel_t_::on_channel_error_, this, _1));

            channel_o_->declareExchange(
                exchange_,
                exchange_type_,
                exchange_flags_,
                exchange_arguments_);

            channel_o_->declareQueue(
                queue_,
                queue_flags_,
                queue_arguments_);

            channel_o_->bindQueue(
                exchange_,
                queue_,
                routing_key_,
                bind_arguments_);
        }
        catch (...)
        {
            std::throw_with_nested(std::runtime_error(
                "async_amqp::detail::channel_t_::open: " + channel_name_));
        }
    }

    void close()
    {
        try
        {
            if (channel_o_)
            {
                channel_o_->close();
                channel_o_.reset();
            }
        }
        catch (...)
        {
            std::throw_with_nested(std::runtime_error(
                "async_amqp::detail::channel_t_::close: "s + channel_name_));
        }
    }

protected:
    void on_channel_error_(char const* message) noexcept
    {
        try
        {
            parent_.log(severity_level_t::error,
                "async_amqp::detail::channel_t_::on_channel_error_: "s
                    + channel_name_ + ", "s + message);
            parent_.close_connection_();
        }
        catch (...)
        {
            parent_.log_exception(
                "async_amqp::detail::channel_t_::on_channel_error_, "s + channel_name_);
        }
    }

    Parent& parent_;

    std::string const exchange_;
    AMQP::ExchangeType exchange_type_ = AMQP::ExchangeType::fanout;
    int exchange_flags_ = 0;
    AMQP::Table exchange_arguments_;

    std::string const queue_;
    int queue_flags_ = 0;
    AMQP::Table queue_arguments_;

    std::string const routing_key_;
    AMQP::Table bind_arguments_;

    std::string const channel_name_;
    std::optional<AMQP::Channel> channel_o_;
};

template <typename Parent>
class in_channel_t_ : public channel_t_<Parent>
{
    using base_t_ = channel_t_<Parent>;

public:
    using received_handler_t = std::function<void(in_channel_t_&, json::value)>;

    template <typename ReceivedHandler>
    in_channel_t_(
        Parent& parent,
        std::string const& exchange_name,
        std::string const& queue_name,
        ReceivedHandler handler)
        : base_t_(parent, exchange_name, queue_name),
          received_handler_(std::move(handler))
    {
    }

    void open(AMQP::Connection* connection_p)
    {
        try
        {
            assert(connection_p != nullptr);

            base_t_::open(connection_p);
            base_t_::channel_o_->consume(base_t_::queue_).onReceived(std::bind(&in_channel_t_::on_received_, this, _1, _2, _3));
        }
        catch (...)
        {
            std::throw_with_nested(std::runtime_error(
                "async_amqp::detail::in_channel_t_::open: " + base_t_::channel_name_));
        }
    }

private:
    void on_received_(
        const AMQP::Message& message, uint64_t delivery_tag, bool redelivered) noexcept
    {
        try
        {
            assert(base_t_::channel_o_);

            base_t_::channel_o_->ack(delivery_tag);
            std::string message_str(message.body(), message.bodySize());

            base_t_::parent_.log(severity_level_t::trace,
                "async_amqp::detail::in_channel_t_::on_received_: "s
                    + base_t_::channel_name_ + ", "s + message_str);

            if (received_handler_ != nullptr)
            {
                received_handler_(*this, json::parse(message_str));
            }
        }
        catch (...)
        {
            base_t_::parent_.log_exception(
                "async_amqp::detail::in_channel_t_::on_received_: "s + base_t_::channel_name_);
        }
    }

private:
    received_handler_t received_handler_{nullptr};
};

template <typename Parent>
class out_channel_t_ : public channel_t_<Parent>
{
    using base_t_ = channel_t_<Parent>;

public:
    using ready_handler_t = std::function<void(out_channel_t_&)>;
    using publish_ack_handler_t = std::function<void(out_channel_t_&, std::string const& buffer)>;
    using publish_lost_handler_t = std::function<void(out_channel_t_&, std::string const& buffer)>;
    using publish_error_handler_t = std::function<void(out_channel_t_&, std::string const& buffer, char const* message)>;

    template <typename ReadyHandler>
    out_channel_t_(
        Parent& parent,
        std::string const& exchange_name,
        std::string const& queue_name,
        std::string const& route_name,
        ReadyHandler handler)
        : base_t_(parent, exchange_name, queue_name, route_name),
          ready_handler_(std::move(handler))
    {
    }

    template <typename Handler>
    out_channel_t_& on_publish_ack(Handler&& handler)
    {
        publish_ack_handler_ = std::forward<Handler>(handler);
        return *this;
    }

    template <typename Handler>
    out_channel_t_& on_publish_lost(Handler&& handler)
    {
        publish_lost_handler_ = std::forward<Handler>(handler);
        return *this;
    }

    template <typename Handler>
    out_channel_t_& on_publish_error(Handler&& handler)
    {
        publish_error_handler_ = std::forward<Handler>(handler);
        return *this;
    }

    void open(AMQP::Connection* connection_p)
    {
        try
        {
            assert(connection_p != nullptr);

            base_t_::open(connection_p);
            base_t_::channel_o_->onReady(std::bind(&out_channel_t_::on_channel_ready_, this));

            reliable_o_.emplace(*base_t_::channel_o_);
            reliable_o_->onError(std::bind(&out_channel_t_::on_channel_error_, this, _1));
        }
        catch (...)
        {
            std::throw_with_nested(std::runtime_error(
                "async_amqp::detail::out_channel_t_::open: " + base_t_::channel_name_));
        }
    }

    void close()
    {
        try
        {
            if (reliable_o_)
            {
                reliable_o_->close();
                reliable_o_.reset();
            }
            base_t_::close();
        }
        catch (...)
        {
            std::throw_with_nested(std::runtime_error(
                "async_amqp::detail::out_channel_t_::close_connection_: "s + base_t_::channel_name_));
        }
    }

    inline void publish(json::value value)
    {
        try
        {
            io::post(base_t_::parent_.io_context_,
                [this, value = std::move(value)]() mutable noexcept
                { on_publish_(std::move(value)); });
        }
        catch (...)
        {
            std::throw_with_nested(std::runtime_error(
                "async_amqp::detail::out_channel_t_::publish: " + base_t_::channel_name_));
        }
    }

private:
    void on_channel_error_(char const* message) noexcept
    {
        base_t_::on_channel_error_(message);
    }

    void on_channel_ready_() noexcept
    {
        try
        {
            if (ready_handler_ != nullptr)
            {
                ready_handler_(*this);
            }
        }
        catch (...)
        {
            base_t_::parent_.log_exception(
                "async_amqp::detail::out_channel_t_::on_channel_ready_: "s + base_t_::channel_name_);
        }
    }

    void on_publish_ack_(std::string buffer) noexcept
    {
        try
        {
            if (publish_ack_handler_ != nullptr)
            {
                publish_ack_handler_(*this, buffer);
            }

            base_t_::parent_.log(severity_level_t::trace,
                "async_amqp::detail::out_channel_t_::on_publish_ack_: "s
                    + base_t_::channel_name_ + ", "s + buffer);
        }
        catch (...)
        {
            base_t_::parent_.log_exception(
                "async_amqp::detail::out_channel_t_::on_publish_ack_: "s + base_t_::channel_name_);
        }
    }

    void on_publish_lost_(std::string buffer) noexcept
    {
        try
        {
            if (publish_lost_handler_ != nullptr)
            {
                publish_lost_handler_(*this, buffer);
            }

            base_t_::parent_.log(severity_level_t::error,
                "async_amqp::detail::out_channel_t_::on_publish_lost_: "s
                    + base_t_::channel_name_ + ", "s + buffer);
        }
        catch (...)
        {
            base_t_::parent_.log_exception(
                "async_amqp::detail::out_channel_t_::on_publish_lost_: "s + base_t_::channel_name_);
        }
    }

    void on_publish_error_(std::string buffer, char const* message) noexcept
    {
        try
        {
            if (publish_error_handler_ != nullptr)
            {
                publish_error_handler_(*this, buffer, message);
            }

            base_t_::parent_.log(severity_level_t::error,
                "async_amqp::detail::out_channel_t_::on_publish_error_: "s + message
                    + ", "s + base_t_::channel_name_ + ", "s + buffer);
            base_t_::parent_.close_connection_();
        }
        catch (...)
        {
            base_t_::parent_.log_exception(
                "async_amqp::detail::out_channel_t_::on_publish_error_: "s + base_t_::channel_name_);
        }
    }

    void on_publish_(json::value value) noexcept
    {
        try
        {
            if (!base_t_::channel_o_ || !reliable_o_ || !base_t_::channel_o_->usable())
            {
                throw std::runtime_error("Channel is closed");
            }

            std::string buffer{json::serialize(value.as_object())};
            AMQP::Envelope envelope(buffer.data(), buffer.size());
            envelope.setContentType("application/json");
            envelope.setContentEncoding("utf-8");

            reliable_o_->publish(base_t_::exchange_, base_t_::routing_key_, envelope)
                .onAck(std::bind(&out_channel_t_::on_publish_ack_, this, buffer))
                .onLost(std::bind(&out_channel_t_::on_publish_lost_, this, buffer))
                .onError(std::bind(&out_channel_t_::on_publish_error_, this, buffer, _1));
        }
        catch (...)
        {
            base_t_::parent_.log_exception(
                "async_amqp::detail::out_channel_t_::on_publish_: "s + base_t_::channel_name_);
        }
    }

    ready_handler_t ready_handler_{nullptr};
    publish_ack_handler_t publish_ack_handler_{nullptr};
    publish_lost_handler_t publish_lost_handler_{nullptr};
    publish_error_handler_t publish_error_handler_{nullptr};
    std::optional<AMQP::Reliable<>> reliable_o_;
};

} // namespace detail

class channels_t : public log_t
{
public:
    using received_handler_t = std::function<void(detail::in_channel_t_<channels_t>&, json::value)>;
    using ready_handler_t = std::function<void(detail::out_channel_t_<channels_t>&)>;
    using in_channels_t = std::map<std::string, detail::in_channel_t_<channels_t>>;
    using out_channels_t = std::map<std::string, detail::out_channel_t_<channels_t>>;

    template <typename LogHandler>
    channels_t(
        io::io_context& io_context,
        std::string const& url,
        LogHandler log_handler)
        : log_t(std::move(log_handler)),
          io_context_(io_context),
          reconnection_timer_(io_context),
          connection_(io_context, url, std::bind(&channels_t::log, this, _1, _2))
    {
        try
        {
            log(severity_level_t::debug, "async_amqp::channels_t::channels_t");

            connection_
                .on_ready(std::bind(&channels_t::on_connection_ready_, this, _1))
                .on_error(std::bind(&channels_t::on_connection_error_, this, _1, _2))
                .on_closed(std::bind(&channels_t::on_connection_closed_, this, _1));
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
            do_wait_for_reconnection_();
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

    inline void publish(std::string const& name, json::value value)
    {
        try
        {
            if (auto it = out_channels_.find(name); it != end(out_channels_))
            {
                it->second.publish(std::move(value));
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

    template <typename Name, typename ReceivedHandler>
    auto add_in_channel(
        Name&& name,
        std::string const& exchange,
        std::string const& queue,
        ReceivedHandler handler)
    {
        return in_channels_.try_emplace(
            std::forward<Name>(name), *this, exchange, queue, std::move(handler));
    }

    template <typename Name, typename ReadyHandler>
    auto add_out_channel(
        Name&& name,
        std::string const& exchange,
        std::string const& queue,
        std::string const& routing_key,
        ReadyHandler handler)
    {
        return out_channels_.try_emplace(
            std::forward<Name>(name), *this, exchange, queue, routing_key, std::move(handler));
    }

    void heartbeat_interval(uint16_t interval)
    {
        connection_.do_heartbeat(interval);
    }

    template <typename Parent>
    friend class detail::channel_t_;

    template <typename Parent>
    friend class detail::in_channel_t_;

    template <typename Parent>
    friend class detail::out_channel_t_;

private:
    inline void close_connection_()
    {
        try
        {
            for (auto& channel : in_channels_)
            {
                channel.second.close();
            }
            for (auto& channel : out_channels_)
            {
                channel.second.close();
            }
            connection_.close();
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
            if (!finish_) connection_.open();
            reconnection_timer_.expires_after(io::chrono::seconds(5));
            reconnection_timer_.async_wait(
                /*on_wait_for_reconnection_*/
                [this](boost::system::error_code const& ec) noexcept
                {
                    try
                    {
                        if (ec) throw std::system_error(ec);
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
            auto connection_p{connection.amqp_connection()};
            if (connection_p == nullptr)
            {
                throw std::runtime_error("AMQP::Connection is closed");
            }

            for (auto& channel : in_channels_)
            {
                channel.second.open(connection_p);
            }
            for (auto& channel : out_channels_)
            {
                channel.second.open(connection_p);
            }
        }
        catch (...)
        {
            connection.log_exception("async_amqp::channels_t::on_ready_"s);
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
            connection.log_exception("async_amqp::channels_t::on_connection_error_"s);
        }
    }

    void on_connection_closed_(connection_t& connection) noexcept
    {
        try
        {
            connection.log(severity_level_t::info, "async_amqp::channels_t::on_connection_closed_: Connection closed"s);
        }
        catch (...)
        {
            connection.log_exception("async_amqp::channels_t::on_connection_closed_"s);
        }
    }

    io::io_context& io_context_;
    std::atomic<bool> finish_{false};
    io::steady_timer reconnection_timer_;

    connection_t connection_;
    in_channels_t in_channels_;
    out_channels_t out_channels_;
};

using in_channel_t = detail::in_channel_t_<channels_t>;
using out_channel_t = detail::out_channel_t_<channels_t>;

} // namespace async_amqp
