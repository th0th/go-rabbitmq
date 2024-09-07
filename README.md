This project provides a thin wrapper around the [RabbitMQ's default go client library](https://github.com/rabbitmq/amqp091-go), primarily focusing on automatic handling of closed connections and channels. It aims to enhance the reliability of RabbitMQ usage in your applications while maintaining a lightweight approach.

## Overview

This wrapper offers a straightforward interface to interact with RabbitMQ, automatically managing connection and channel issues to ensure consistent messaging capabilities.

## Key features

- Automatic handling of closed connections and channels
- Reconnection and recovery mechanisms
- Simplified connection management
- Easy-to-use methods for publishing and consuming messages
- Support for basic RabbitMQ concepts like exchanges, queues, and routing keys

## Usage

To use this wrapper, import it into your project and utilize the provided methods to interact with RabbitMQ. The wrapper handles connection and channel management, allowing you to focus on your application logic without worrying about connection issues.

## Benefits

- Increased reliability and fault tolerance
- Reduces boilerplate code for connection management
- Easier to implement robust messaging systems
- Minimizes application downtime due to connection issues

## How it works

The wrapper continuously monitors the state of the RabbitMQ connection and channels. If a disconnection is detected, it automatically attempts to re-establish the connection and recreate any necessary channels, queues, and bindings.

## Note

This wrapper is designed to be a thin layer over the RabbitMQ client, focusing primarily on automatic reconnection. It may not cover all advanced scenarios or provide direct access to all features of the underlying RabbitMQ client library. For complex use cases, you might need to consider alternative solutions or extensions to this wrapper.


