# GoBroker v2

GoBroker is a unified messaging broker library for Go that provides a consistent API for working with multiple message broker systems, including RabbitMQ, Redis pub/sub, and Amazon MQ.

[![Go Reference](https://pkg.go.dev/badge/github.com/defensestation/gobroker.svg)](https://pkg.go.dev/github.com/defensestation/gobroker)

## Features

- **Unified API**: Consistent interface for publishing and subscribing across different broker implementations
- **Multiple Backends**: Support for RabbitMQ, Redis pub/sub, and Amazon MQ
- **Connection Management**: Automatic connection establishment, error handling, and reconnection
- **Channel Pooling**: Efficient channel management with pooling and reuse
- **Error Handling**: Comprehensive error handling and recovery
- **Auto-Reconnect**: Automatic reconnection on connection failures

## Installation

```bash
go install github.com/defensestation/gobroker/v2
```

## Dependencies

- [amqp091-go](https://github.com/rabbitmq/amqp091-go): RabbitMQ client
- [redis/go-redis/v9](https://github.com/redis/go-redis/v9): Redis client
- [go-stomp/stomp/v3](https://github.com/go-stomp/stomp): STOMP client for Amazon MQ


## Quick Start

### Creating a Broker

```go
// RabbitMQ broker
rabbitBroker := gobroker.NewBroker("localhost", gobroker.BrokerTypeRabbitMQ, &gobroker.EndpointOptions{
    Username: "guest",
    Password: "guest",
    Port:     "5672",
})
defer rabbitBroker.Close()

// Redis broker
redisBroker := gobroker.NewBroker("localhost", gobroker.BrokerTypeRedis, &gobroker.EndpointOptions{
    Password: "",
    Port:     "6379",
    DB:       0,
})
defer redisBroker.Close()

// Amazon MQ broker
amazonMQBroker := gobroker.NewBroker("admin:password@mq-broker.example.com:61613", gobroker.BrokerTypeAmazonMQ)
defer amazonMQBroker.Close()
```

### Publishing Messages

```go
// Create a message
message := map[string]interface{}{
    "id":        123,
    "content":   "Hello world!",
    "timestamp": time.Now().Unix(),
}

// Publish using the unified API
err := rabbitBroker.Publish("my-exchange.user.created", message)
err = redisBroker.Publish("user:created", message)
err = amazonMQBroker.Publish("/queue/user-events", message)
```

### Subscribing to Messages

```go
// Subscribe using the unified API
err := rabbitBroker.Subscribe("my-exchange.user.created", func(data []byte) {
    fmt.Printf("Received RabbitMQ message: %s\n", string(data))
}, "my-queue")

err = redisBroker.Subscribe("user:created", func(data []byte) {
    fmt.Printf("Received Redis message: %s\n", string(data))
})

err = amazonMQBroker.Subscribe("/queue/user-events", func(data []byte) {
    fmt.Printf("Received Amazon MQ message: %s\n", string(data))
})
```

## Broker-Specific Usage

### RabbitMQ

RabbitMQ implementation supports topics in the format `exchange.routekey`.

```go
// Creating an exchange
exchange, err := rabbitBroker.BuildExchange("user-events", &gobroker.ExchangeOptions{
    Type:       "topic",
    Durable:    true,
    AutoDelete: false,
})

// Publishing directly to an exchange
err = exchange.Publish("user.created", message)

// Queue declare and bind
queueName, err := rabbitBroker.QueueDeclareAndBind("user-events", "user.created", "user-created-queue")
```

### Redis

Redis implementation uses channel names as topics.

```go
// Multiple channel subscription
err = redisBroker.RunRedisConsumer([]string{"user:created", "user:updated"}, func(data []byte) {
    fmt.Printf("Received Redis message: %s\n", string(data))
})
```

### Amazon MQ

Amazon MQ implementation (using STOMP) uses destinations as topics.

```go
// Using queues
err = amazonMQBroker.PublishToAmazonMQQueue("/queue/user-events", message)

// Using topics
err = amazonMQBroker.PublishToAmazonMQQueue("/topic/user-events", message)
```

## Advanced Usage

### Connection Management

```go
// Get specific connection
conn, err := rabbitBroker.GetConnection(gobroker.PublishConnection)

// Get specific channel
ch, err := conn.GetChannel()

// Get specific channel by ID
ch, err := conn.GetChannel(5)
```

### Error Handling

```go
// All operations return errors that should be checked
if err := broker.Publish("topic", message); err != nil {
    log.Printf("Failed to publish message: %v", err)
}
```

### Connection Options

```go
// RabbitMQ with TLS
rabbitBroker := gobroker.NewBroker("secure-rabbit.example.com", gobroker.BrokerTypeRabbitMQ, &gobroker.EndpointOptions{
    Protocol: "amqps", // Use AMQPS protocol for TLS
    Username: "user",
    Password: "pass",
    Port:     "5671",  // Secure port
})

// Redis with authentication and database selection
redisBroker := gobroker.NewBroker("redis.example.com", gobroker.BrokerTypeRedis, &gobroker.EndpointOptions{
    Password: "secret",
    Port:     "6379",
    DB:       3, // Use database 3
})
```

## Design Philosophy

GoBroker was designed with the following principles in mind:

1. **Unified Interface**: Consistent API regardless of the underlying broker implementation
2. **Simplicity**: Easy to use but powerful enough for complex scenarios
3. **Resilience**: Robust error handling and automatic reconnection
4. **Performance**: Efficient connection and channel management
5. **Extensibility**: Easy to add support for additional broker types

## WARNING
```
AmazonMQ has not been tested yet.
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License
GoBroker is licensed under the [MIT License](LICENSE.md).