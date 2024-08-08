#include "async_amqp/multi_channels.hpp"

#include <boost/asio.hpp>
#include <boost/json.hpp>

#include <cassert>
#include <chrono>
#include <functional>
#include <optional>
#include <string>

using async_amqp::channels_t;
using async_amqp::in_channel_t;
using async_amqp::out_channel_t;
using async_amqp::severity_level_t;

namespace sys = boost::system;
namespace io = boost::asio;
namespace json = boost::json;

using namespace std::literals;
using namespace std::chrono_literals;

void periodic_publish(
    io::steady_timer& timer,
    std::string const& channel_name,
    channels_t& channels)
{
    timer.expires_after(1s);
    timer.async_wait(
        [&](sys::error_code const& ec)
        {
            if (!ec)
            {
                channels.publish(channel_name, json::object{{"key"s, "value"s}});
                periodic_publish(timer, channel_name, channels);
            }
        });
}

int main()
{
    std::string const exchange{"test_exchange"s};
    std::string const queue{"test_queue"s};
    std::string const route{"test_queue"s};
    std::string const in_channel_name{"in_test"s};
    std::string const out_channel_name{"out_test"s};

    io::io_context io_context;
    io::steady_timer timer(io_context);

    channels_t channels(
        io_context,
        AMQP::Address("127.0.0.1", 5672, AMQP::Login("guest", "guest"), "/"),
        [](severity_level_t severity_level, std::string const& message)
        {
            std::clog << severity_level << "\t" << message << std::endl;
        });

    AMQP::Table arguments;
    arguments["x-queue-mode"] = "lazy";
    {
        auto [it, ok] = channels.add_in_channel(
            in_channel_name,
            exchange,
            queue,
            [&](in_channel_t&, json::value v)
            {
                channels.log(severity_level_t::debug,
                    "received message: "s + json::serialize(v));
            });

        assert(ok);
        it->second
            .exchange_type(AMQP::direct)
            .queue_flags(AMQP::durable)
            .queue_arguments(arguments);
    }

    {
        auto [it, ok] = channels.add_out_channel(
            out_channel_name,
            exchange,
            route,
            route,
            [&](out_channel_t&)
            {
                periodic_publish(
                    timer,
                    out_channel_name,
                    channels);
            });

        assert(ok);
        
        it->second
            .exchange_type(AMQP::direct)
            .queue_flags(AMQP::durable)
            .queue_arguments(arguments);
    }

    channels.heartbeat_interval(60);
    channels.open();

    io::signal_set signals(io_context, SIGINT, SIGTERM);
    signals.async_wait([&](auto, auto)
        {
            std::clog << "Signal received" << std::endl;
            io_context.stop();
        });

    io_context.run();

    return 0;
}
