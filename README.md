ASYNC-AMQP
==========

This lightweight header-only library realises the network layer for the AMQP-CPP library
(https://github.com/CopernicaMarketingSoftware/AMQP-CPP.git).

The ASYNC-AMQP makes a connection to RabbitMQ, performs all the nessasary IO operations and the event loop
using Boost::Asio library. It makes possible to use the AMQP-CPP library on both Linux or Windows.

The header file "include/async_amqp/connection.hpp" contains async_amqp::connection_t class, which extends
AMQP::ConnectionHandler class and does all the IO operations.

The header file "include/async_amqp/multi_channels.hpp" contains async_amqp::channels_t class, which manages
the input and output channels for AMQP connection.

The ASYNC-AMQP library also uses Boost::Json library to compose and parse messages from RabbitMQ.

You can find an example of using the ASYNC-AMQP library in examples/main.cpp file. 