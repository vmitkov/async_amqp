#define BOOST_TEST_MODULE async_amqp_test

#include "async_amqp/multi_channels.hpp"

#include <boost/asio.hpp>
#include <boost/json.hpp>
#include <boost/log/trivial.hpp>
#include <boost/test/unit_test.hpp>

#include <chrono>
#include <functional>
#include <string>

using async_amqp::channels_t;
using async_amqp::in_channel_t;
using async_amqp::out_channel_t;
using async_amqp::severity_level_t;

namespace json = boost::json;
namespace io = boost::asio;
using io::steady_timer;

using namespace std::literals;
using namespace std::chrono_literals;

class log_t
{
public:
    void operator()(severity_level_t severity_level, std::string const& message)
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
    }
};

void periodic_publish(
    steady_timer& timer,
    channels_t& channels,
    json::value value,
    int& counter)
{
    if (counter == 10)
    {
        return;
    }
    timer.expires_after(io::chrono::milliseconds(30));
    timer.async_wait(
        [&, value = std::move(value)](boost::system::error_code const& ec) mutable
        {
            if (!ec)
            {
                channels.publish("test", json::object(value.as_object()));
                periodic_publish(timer, channels, std::move(value), ++counter);
            }
        });
}

void periodic_publish_2(
    steady_timer& timer,
    channels_t& channels,
    json::value value,
    int& counter)
{
    if (counter == 20)
    {
        return;
    }
    timer.expires_after(io::chrono::milliseconds(20));
    timer.async_wait(
        [&, value = std::move(value)](boost::system::error_code const& ec) mutable
        {
            if (!ec)
            {
                channels.publish("test_2", json::object(value.as_object()));
                periodic_publish_2(timer, channels, std::move(value), ++counter);
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

    const json::object json_msg_sample({{"key"s, "value"s}});

    const json::object json_msg_sample_2({{"key_2"s, "value_2"s}});

    options_t options;

    io::io_context io_context;

    channels_t channels(io_context, options.amqp_url, log_t());

    int msg_receiver_counter{0};
    int msg_sender_counter{0};
    steady_timer publish_timer(io_context);

    int msg_receiver_counter_2{0};
    int msg_sender_counter_2{0};
    steady_timer publish_timer_2(io_context);

    AMQP::Table arguments;
    arguments["x-queue-mode"] = "lazy";

    {
        auto [it, ok] = channels.add_in_channel(
            "test",
            options.amqp_exchange,
            options.amqp_queue,
            [&](in_channel_t&, json::value value)
            {
                if (msg_sender_counter > 0)
                {
                    ++msg_receiver_counter;
                    BOOST_CHECK(value == json_msg_sample);
                    if (msg_receiver_counter == 10 && msg_receiver_counter_2 == 20)
                    {
                        io::post(io_context, [&]()
                            { io_context.stop(); });
                    }
                }
            });

        BOOST_CHECK(ok);
        it->second
            .exchange_type(AMQP::direct)
            .queue_flags(AMQP::durable)
            .queue_arguments(arguments);
    }

    {
        auto [it, ok] = channels.add_in_channel(
            "test_2",
            options.amqp_exchange_2,
            options.amqp_queue_2,
            [&](in_channel_t&, json::value value)
            {
                if (msg_sender_counter_2 > 0)
                {
                    ++msg_receiver_counter_2;
                    BOOST_CHECK(value == json_msg_sample_2);
                    if (msg_receiver_counter == 10 && msg_receiver_counter_2 == 20)
                    {
                        io::post(io_context, [&]()
                            { io_context.stop(); });
                    }
                }
            });
        BOOST_CHECK(ok);
        it->second
            .exchange_type(AMQP::direct)
            .queue_flags(AMQP::durable)
            .queue_arguments(arguments);
    }

    {
        auto [it, ok] = channels.add_out_channel(
            "test",
            options.amqp_exchange,
            options.amqp_route,
            options.amqp_route,
            [&](out_channel_t&)
            {
                periodic_publish(
                    publish_timer,
                    channels,
                    json::object(json_msg_sample),
                    msg_sender_counter);
            });
        BOOST_CHECK(ok);
        it->second
            .on_publish_ack([&](out_channel_t&, std::string const& buffer)
                { BOOST_CHECK(json::parse(buffer).as_object() == json_msg_sample); })
            .on_publish_lost([](out_channel_t&, std::string const&)
                { BOOST_CHECK(false); })
            .on_publish_error([](out_channel_t&, std::string const&, char const*)
                { BOOST_CHECK(false); })
            .exchange_type(AMQP::direct)
            .queue_flags(AMQP::durable)
            .queue_arguments(arguments);
    }

    {
        auto [it, ok] = channels.add_out_channel(
            "test_2",
            options.amqp_exchange_2,
            options.amqp_route_2,
            options.amqp_route_2,
            [&](out_channel_t& /*channel*/)
            {
                periodic_publish_2(
                    publish_timer_2,
                    channels,
                    json::object(json_msg_sample_2),
                    msg_sender_counter_2);
            });
        BOOST_CHECK(ok);
        it->second
            .on_publish_ack([&](out_channel_t&, std::string const& buffer)
                { BOOST_CHECK(json::parse(buffer).as_object() == json_msg_sample_2); })
            .on_publish_lost([](out_channel_t&, std::string const&)
                { BOOST_CHECK(false); })
            .on_publish_error([](out_channel_t&, std::string const&, char const*)
                { BOOST_CHECK(false); })
            .exchange_type(AMQP::direct)
            .queue_flags(AMQP::durable)
            .queue_arguments(arguments);
    }

    channels.heartbeat_interval(60);
    channels.open();

    steady_timer stop_timer(io_context);
    stop_timer.expires_after(io::chrono::seconds(5));
    stop_timer.async_wait(
        [&](const boost::system::error_code&)
        { io_context.stop(); });

    io_context.run();

    BOOST_LOG_TRIVIAL(debug) << "msg_sender_counter: " << msg_sender_counter;
    BOOST_LOG_TRIVIAL(debug) << "msg_receiver_counter: " << msg_receiver_counter;

    BOOST_CHECK(msg_receiver_counter == 10 && msg_sender_counter == 10);

    BOOST_LOG_TRIVIAL(debug) << "msg_sender_counter_2: " << msg_sender_counter_2;
    BOOST_LOG_TRIVIAL(debug) << "msg_receiver_counter_2: " << msg_receiver_counter_2;

    BOOST_CHECK(msg_receiver_counter_2 == 20 && msg_sender_counter_2 == 20);
}

BOOST_AUTO_TEST_CASE(async_amqp_resolve_test)
{
    io::io_context io_context;
    size_t connection_count{};

    steady_timer stop_timer(io_context);
    stop_timer.expires_after(io::chrono::seconds(10));
    stop_timer.async_wait(
        [&](const boost::system::error_code&)
        { io_context.stop(); });

    channels_t channels(
        io_context,
        "amqp://rabbitmq/"s,
        [&](severity_level_t severity_level, std::string const& message)
        {
            if (message.find("channels_t::on_connection_closed_"s) != std::string::npos)
            {
                if (++connection_count > 1) io_context.stop();
            }
            log_t().operator()(severity_level, message);
        });
    channels.open();

    io_context.run();

    BOOST_CHECK(connection_count > 1);
}

BOOST_AUTO_TEST_CASE(async_amqp_open_close_test)
{
    io::io_context io_context;
    size_t open_connection_count{};
    size_t close_connection_count{};

    channels_t channels(
        io_context,
        "amqp://127.0.0.1:5672/"s,
        [&](severity_level_t severity_level, std::string const& message)
        {
            if (message.find("channels_t::on_connection_closed_"s) != std::string::npos)
            {
                ++close_connection_count;
            }
            if (message.find("channels_t::on_connection_ready_"s) != std::string::npos)
            {
                ++open_connection_count;
            }
            log_t().operator()(severity_level, message);
        });

    channels.open();

    steady_timer stop_timer(io_context);
    stop_timer.expires_after(io::chrono::seconds(10));
    stop_timer.async_wait(
        [&](const boost::system::error_code&)
        { io_context.stop(); });

    steady_timer open_close_timer(io_context);
    auto open_channels{
        [&](const boost::system::error_code&)
        {
            channels.open();
            open_close_timer.expires_after(io::chrono::seconds(2));
            open_close_timer.async_wait(
                [&](const boost::system::error_code&)
                { channels.close(); });
        }};

    auto close_channels{
        [&](const boost::system::error_code&)
        {
            channels.close();
            open_close_timer.expires_after(io::chrono::seconds(2));
            open_close_timer.async_wait(open_channels);
        }};

    open_close_timer.expires_after(io::chrono::seconds(2));
    open_close_timer.async_wait(close_channels);

    io_context.run();

    BOOST_CHECK(open_connection_count == 2);
    BOOST_CHECK(close_connection_count == 2);
}