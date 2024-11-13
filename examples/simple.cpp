#include "async_amqp/multi_channels.hpp"

#include <boost/asio.hpp>
#include <boost/json.hpp>

#include <list>
#include <string>

using async_amqp::channels_t;
using async_amqp::out_channel_t;
using async_amqp::severity_level_t;

namespace sys = boost::system;
namespace io = boost::asio;
namespace json = boost::json;

using namespace std::literals;
using namespace std::chrono_literals;

class rabbit_mq_t
{
public:
    rabbit_mq_t(io::io_context& io_context)
        : io_context_(io_context),
          channels_(
              io_context_,
              url_,
              [](severity_level_t severity_level, std::string const& message)
              {
                  std::clog << severity_level << "\t" << message << std::endl;
              })
    {
        AMQP::Table arguments;
        arguments["x-queue-mode"] = "lazy";

        auto [it, ok] = channels_.add_out_channel(
            out_channel_name_,
            exchange_,
            route_,
            route_,
            [&](out_channel_t&)
            {
                ready_ = true;
            });

        if (!ok)
        {
            throw std::runtime_error("channel hasn't been added");
        }

        it->second
            .on_publish_ack([&](out_channel_t&, std::string const& buffer)
                { ++ack_counter_; })
            .exchange_type(AMQP::direct)
            .queue_flags(AMQP::durable)
            .queue_arguments(arguments);

        channels_.heartbeat_interval(60);
        channels_.open();
    }

    void publish(json::value msg)
    {
        try
        {
            io::post(
                io_context_,
                [this, msg = std::move(msg)]() mutable noexcept
                {
                    do_publish_(std::move(msg));
                });
        }
        catch (...)
        {
            std::throw_with_nested(std::runtime_error("rabbit_mq_t::publish"));
        }
    }

    void log_exception() { channels_.log_exception(); }

    void close()
    {
        try
        {
            io::post(io_context_,
                [this]
                {
                    if (queue_.empty() && sent_counter_ == ack_counter_)
                    {
                        //channels_.close();
                        io_context_.stop();
                    }
                    else
                    {
                        io::post(io_context_,
                            [this]
                            { close(); });
                    }
                });
        }
        catch (...)
        {
            std::throw_with_nested(std::runtime_error("rabbit_mq_t::stop"));
        }
    }

private:
    void do_publish_(json::value msg)
    {
        try
        {
            bool process_in_progress{!queue_.empty()};
            queue_.push(std::move(msg));

            if (!process_in_progress) process_queue_();
        }
        catch (...)
        {
            log_exception();
        }
    }

    void process_queue_()
    {
        try
        {
            io::post(io_context_,
                [this]
                { do_process_queue_(); });
        }
        catch (...)
        {
            std::throw_with_nested(std::runtime_error("rabbit_mq_t::process_queue_"));
        }
    }

    void do_process_queue_()
    {
        try
        {
            if (ready_)
            {
                channels_.publish(out_channel_name_, queue_.front());
                queue_.pop();
                ++sent_counter_;
            }
            if (!queue_.empty()) process_queue_();
        }
        catch (...)
        {
            log_exception();
        }
    }

    std::string const url_{"amqp://127.0.0.1:5672/"s};
    std::string const exchange_{"test_exchange"s};
    std::string const route_{"simple_test_queue"s};
    std::string const out_channel_name_{"out_simple_test"s};

    io::io_context& io_context_;
    channels_t channels_;
    std::queue<json::value> queue_;
    bool ready_{false};
    size_t sent_counter_{};
    size_t ack_counter_{};
};

class job_t
{
public:
    job_t(io::io_context& io_context, rabbit_mq_t& rabbit_mq, auto& list)
        : io_context_(io_context),
          rabbit_mq_(rabbit_mq)
    {
        process_list_(list);
    }

private:
    void process_list_(auto& list)
    {
        try
        {
            io::post(
                io_context_,
                [&]() mutable noexcept
                { do_process_list_(list); });
        }
        catch (...)
        {
            std::throw_with_nested(std::runtime_error("job_t::process_list_"));
        }
    }

    void do_process_list_(auto& list)
    {
        try
        {
            if (!list.empty())
            {
                rabbit_mq_.publish(std::move(list.front()));
                list.pop_front();
                process_list_(list);
            }
            else
            {
                rabbit_mq_.close();
            }
        }
        catch (...)
        {
            rabbit_mq_.log_exception();
        }
    }

    io::io_context& io_context_;
    rabbit_mq_t& rabbit_mq_;
};

int main()
{
    std::list<json::value> list{
        {{"1", "one"}},
        {{"2", "two"}},
        {{"3", "tree"}},
        {{"4", "four"}},
        {{"5", "five"}},
        {{"6", "six"}},
        {{"7", "seven"}},
        {{"8", "eight"}},
        {{"9", "nine"}},
        {{"10", "ten"}},
    };

    io::io_context io_context;

    rabbit_mq_t rabbit_mq(io_context);
    job_t(io_context, rabbit_mq, list);

    io::signal_set signals(io_context, SIGINT, SIGTERM);
    signals.async_wait([&](auto, auto)
        {
            std::clog << "Signal received" << std::endl;
            io_context.stop(); });

    io_context.run();

    return 0;
}
