#define BOOST_TEST_MODULE async_amqp_test

#include "async_amqp/multi_channels.hpp"

#include <boost/test/unit_test.hpp>
#include <boost/asio.hpp>
#include <boost/json.hpp>
#include <boost/log/trivial.hpp>

#include <string>
#include <chrono>
#include <functional>

using async_amqp::severity_level_t;
using async_amqp::channels_t;

namespace json = boost::json;
namespace io = boost::asio;
using io::steady_timer;

using namespace std::literals;
using namespace std::chrono_literals;

void periodic_publish(
    steady_timer& timer,
    channels_t& channels,
    json::object const& obj,
    int& counter)
{
    if (counter == 10) { return; }
    timer.expires_after(io::chrono::milliseconds(30));
    timer.async_wait([&](boost::system::error_code const& ec)
        {
            if (!ec)
            {
                channels.publish("test", json::object{obj});
                periodic_publish(timer, channels, obj, ++counter);
            }
        });
}

void periodic_publish_2(
    steady_timer& timer,
    channels_t& channels,
    json::object const& obj,
    int& counter)
{
    if (counter == 20) { return; }
    timer.expires_after(io::chrono::milliseconds(20));
    timer.async_wait([&](boost::system::error_code const& ec)
        {
            if (!ec)
            {
                channels.publish("test_2", json::object{obj});
                periodic_publish_2(timer, channels, obj, ++counter);
            }
        });
}

BOOST_AUTO_TEST_CASE(async_amqp_test)
{
    struct options_t
    {
        std::string amqp_url{"amqp://127.0.0.1:5672/"s};
        std::string amqp_exchange{"test_exchange"s};
        std::string amqp_queue{"test_queue"s};
        std::string amqp_route{"test_queue"s};
        std::string amqp_exchange_2{"test_exchange_2"s};
        std::string amqp_queue_2{"test_queue_2"s};
        std::string amqp_route_2{"test_queue_2"s};
    };

    const json::object json_msg_sample{{
        {"key"s, "value"s}
        }};

    const json::object json_msg_sample_2{{
        {"key_2"s, "value_2"s}
        }};

    options_t options;

    io::io_context io_context;

    auto log = [](severity_level_t severity_level, std::string const& message)
    {
        switch (severity_level)
        {
        case severity_level_t::trace:
            BOOST_LOG_TRIVIAL(trace) << message;
            break;
        case severity_level_t::debug:
            BOOST_LOG_TRIVIAL(debug) << message;
            break;
        case severity_level_t::info:
            BOOST_LOG_TRIVIAL(info) << message;
            break;
        case severity_level_t::warning:
            BOOST_LOG_TRIVIAL(warning) << message;
            break;
        case severity_level_t::error:
            BOOST_LOG_TRIVIAL(error) << message;
            break;
        case severity_level_t::fatal:
            BOOST_LOG_TRIVIAL(fatal) << message;
            break;
        default:
            break;
        }
    };

    channels_t channels(io_context, options.amqp_url, std::move(log));

    int msg_receiver_counter{0};
    int msg_sender_counter{0};
    steady_timer publish_timer(io_context);

    int msg_receiver_counter_2{0};
    int msg_sender_counter_2{0};
    steady_timer publish_timer_2(io_context);

    channels.add_in_channel(
        "test",
        options.amqp_exchange,
        options.amqp_queue,
        [&](channels_t&, json::object&& object)
        {
            if (msg_sender_counter > 0)
            {
                ++msg_receiver_counter;
                BOOST_CHECK(object == json_msg_sample);
                if (msg_receiver_counter == 10 && msg_receiver_counter_2 == 20)
                {
                    io::post(io_context, [&]() { io_context.stop(); });
                }
            }
        });

    channels.add_in_channel(
        "test_2",
        options.amqp_exchange_2,
        options.amqp_queue_2,
        [&](channels_t&, json::object&& object)
        {
            if (msg_sender_counter_2 > 0)
            {
                ++msg_receiver_counter_2;
                BOOST_CHECK(object == json_msg_sample_2);
                if (msg_receiver_counter == 10 && msg_receiver_counter_2 == 20)
                {
                    io::post(io_context, [&]() { io_context.stop(); });
                }
            }
        });

    channels.add_out_channel(
        "test",
        options.amqp_exchange,
        options.amqp_route,
        [&](channels_t& channels)
        {
            periodic_publish(publish_timer, channels, json_msg_sample, msg_sender_counter);
        });

    channels.add_out_channel(
        "test_2",
        options.amqp_exchange_2,
        options.amqp_route_2,
        [&](channels_t& channels)
        {
            periodic_publish_2(publish_timer_2, channels, json_msg_sample_2, msg_sender_counter_2);
        });

    channels.open();

    steady_timer stop_timer(io_context);
    stop_timer.expires_after(io::chrono::seconds(5000));
    stop_timer.async_wait(
        [&](const boost::system::error_code&) {	io_context.stop(); });

    io_context.run();

    BOOST_LOG_TRIVIAL(debug) << "msg_sender_counter: " << msg_sender_counter;
    BOOST_LOG_TRIVIAL(debug) << "msg_receiver_counter: " << msg_receiver_counter;

    BOOST_CHECK(msg_receiver_counter == 10 && msg_sender_counter == 10);

    BOOST_LOG_TRIVIAL(debug) << "msg_sender_counter_2: " << msg_sender_counter_2;
    BOOST_LOG_TRIVIAL(debug) << "msg_receiver_counter_2: " << msg_receiver_counter_2;

    BOOST_CHECK(msg_receiver_counter_2 == 20 && msg_sender_counter_2 == 20);
}
