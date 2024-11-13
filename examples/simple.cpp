#include "async_amqp/multi_channels.hpp"

#include <boost/asio.hpp>
#include <boost/json.hpp>

#include <array>
#include <cassert>
#include <chrono>
#include <functional>
#include <optional>
#include <string>

using async_amqp::channels_t;
using async_amqp::out_channel_t;
using async_amqp::severity_level_t;

namespace sys = boost::system;
namespace io = boost::asio;
namespace json = boost::json;

using namespace std::literals;
using namespace std::chrono_literals;

class strings_t
{
public:
    std::vector<std::string> values{
        "one"s,
        "two"s,
        "tree"s,
        "four"s,
        "five"s,
        "six"s,
        "seven"s,
        "eight"s,
        "nine"s,
        "ten"s,
    };
    unsigned sent_counter{0};
    unsigned ack_counter{0};
};

void publish_strings(
    io::io_context& io_context,
    std::string const& channel_name,
    channels_t& channels,
    auto& strings)
{
    if (strings.sent_counter == strings.values.size())
    {
        if (strings.ack_counter == strings.values.size())
        {
            io::post(io_context, [&]
                { io_context.stop(); });
            return;
        }
    }
    else
    {
        channels.publish(channel_name, json::object{{std::to_string(strings.sent_counter + 1), strings.values[strings.sent_counter]}});
        ++strings.sent_counter;
    }

    io::post(io_context, [&]
        { publish_strings(io_context, channel_name, channels, strings); });
}

int main()
{
    std::string const url{"amqp://127.0.0.1:5672/"s};
    std::string const exchange{"test_exchange"s};
    std::string const route{"simple_test_queue"s};
    std::string const out_channel_name{"out_simple_test"s};

    io::io_context io_context;

    strings_t strings;

    channels_t channels(
        io_context,
        url,
        [](severity_level_t severity_level, std::string const& message)
        {
            std::clog << severity_level << "\t" << message << std::endl;
        });

    AMQP::Table arguments;
    arguments["x-queue-mode"] = "lazy";

    auto [it, ok] = channels.add_out_channel(
        out_channel_name,
        exchange,
        route,
        route,
        [&](out_channel_t&)
        {
            publish_strings(io_context, out_channel_name, channels, strings);
        });

    assert(ok);

    it->second
        .on_publish_ack([&](out_channel_t&, std::string const& buffer)
            { ++strings.ack_counter; })
        .exchange_type(AMQP::direct)
        .queue_flags(AMQP::durable)
        .queue_arguments(arguments);

    channels.heartbeat_interval(60);
    channels.open();

    io::signal_set signals(io_context, SIGINT, SIGTERM);
    signals.async_wait([&](auto, auto)
        {
            std::clog << "Signal received" << std::endl;
            io_context.stop(); });

    io_context.run();

    return 0;
}
