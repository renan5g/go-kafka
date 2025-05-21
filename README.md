# Kafka Wrapper Package

This Go package provides a high-level wrapper around the `github.com/segmentio/kafka-go` library, simplifying the process of producing and consuming messages with Apache Kafka. It offers a clean API for creating Kafka consumers and producers with flexible configuration options, robust error handling, and support for secure connections (TLS and SASL).

## Features

- **Consumer Interface**: Supports single message consumption, handler-based processing, and worker pool-based concurrent processing.
- **Producer Interface**: Allows synchronous or asynchronous message production with configurable batching and compression.
- **Security Support**: Configurable TLS and SASL mechanisms (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, AWS MSK IAM v2).
- **Flexible Configuration**: Uses the functional options pattern for both consumers and producers.
- **Logging**: Customizable logging with support for standard and no-op loggers.
- **Graceful Shutdown**: Ensures proper resource cleanup with `Close()` methods.

## Installation

To use this package, ensure you have Go installed, then include it in your project:

```bash
go get github.com/renan5g/go-kafka
```

This package depends on `github.com/segmentio/kafka-go` and, for AWS MSK IAM v2 authentication, `github.com/aws/aws-sdk-go-v2`. Install dependencies using:

```bash
go mod tidy
```

## Usage

### Creating a Consumer

The `Consumer` interface provides methods to consume messages from Kafka topics, either individually or with concurrent processing using handlers or worker pools.

```go
package main

import (
	"context"
	"fmt"

	"github.com/renan5g/go-kafka"
)

func main() {
	// Configure the consumer
	consumer := kafka.NewConsumer(
		kafka.WithConsumerBrokers("localhost:9092"),
		kafka.WithConsumerTopics("my-topic"),
		kafka.WithConsumerGroup("my-group"),
		kafka.WithConsumerLogger(kafka.NewStdLogger("consumer: ")),
	)

	// Ensure the consumer is closed on program exit
	defer consumer.Close()

	// Consume messages
	ctx := context.Background()
	for {
		msg, err := consumer.Consume(ctx)
		if err != nil {
			fmt.Printf("Error consuming message: %v\n", err)
			break
		}
		fmt.Printf("Received message: %s\n", string(msg.Value))
	}
}
```

### Consuming with a Handler

You can process messages concurrently using a handler function:

```go
handler := func(msg kafka.Message) error {
	fmt.Printf("Processing message: %s\n", string(msg.Value))
	return nil
}

consumer.ConsumeWithHandler(ctx, handler, kafka.WithConcurrency(4))
```

### Creating a Producer

The `Producer` interface allows sending messages to Kafka topics, with support for batching and asynchronous writes.

```go
package main

import (
	"context"

	"github.com/renan5g/go-kafka"
)

func main() {
	// Configure the producer
	producer := kafka.NewProducer(
		kafka.WithProducerBrokers("localhost:9092"),
		kafka.WithProducerTopic("my-topic"),
		kafka.WithProducerLogger(kafka.NewStdLogger("producer: ")),
	)

	// Ensure the producer is closed on program exit
	defer producer.Close()

	// Produce a message
	ctx := context.Background()
	err := producer.ProduceMessages(ctx, kafka.Message{
		Value: []byte("Hello, Kafka!"),
	})
	if err != nil {
		fmt.Printf("Error producing message: %v\n", err)
	}
}
```

### Security Configuration

Both consumers and producers support secure connections using TLS and SASL.

```go
securityConfig := &kafka.SecurityConfig{
	EnableTLS: true,
	TLSConfig: &tls.Config{...}, // Configure TLS as needed
	EnableSASL: true,
	SASL: &kafka.SASLConfig{
		Type:     kafka.PLAIN,
		Username: "user",
		Password: "pass",
	},
}

consumer := kafka.NewConsumer(
	kafka.WithConsumerBrokers("localhost:9092"),
	kafka.WithConsumerTopics("my-topic"),
	kafka.WithConsumerSecurity(securityConfig),
)
```

## Configuration Options

### Consumer Options

- `WithConsumerBrokers(brokers ...string)`: Sets the Kafka broker addresses.
- `WithConsumerTopics(topics ...string)`: Specifies the topic(s) to consume from.
- `WithConsumerGroup(group string)`: Sets the consumer group ID.
- `WithStartOffset(offset int64)`: Sets the starting offset (e.g., `kafka.FirstOffset` or `kafka.LastOffset`).
- `WithConsumerLogger(logger Logger)`: Sets a custom logger.
- `WithConsumerSecurity(config *SecurityConfig)`: Configures TLS/SASL security.
- And more (see `consumer_options.go` for full list).

### Producer Options

- `WithProducerBrokers(brokers ...string)`: Sets the Kafka broker addresses.
- `WithProducerTopic(topic string)`: Sets the target topic.
- `WithProducerBalancer(balancer kafka.Balancer)`: Sets the partition balancer.
- `WithProducerAsync(async bool)`: Enables asynchronous writes.
- `WithProducerCompression(compression kafka.Compression)`: Sets the compression codec.
- `WithProducerLogger(logger Logger)`: Sets a custom logger.
- `WithProducerSecurity(config *SecurityConfig)`: Configures TLS/SASL security.
- And more (see `producer_options.go` for full list).

## Important Notes

- **Graceful Shutdown**: Always call `Close()` on consumers and producers to ensure proper disconnection from Kafka. Use a `signal.Notify` handler to catch SIGINT/SIGTERM for graceful shutdown.
- **Offset Management**: When using consumer groups, `Consume` automatically commits offsets. For fine-grained control, use `FetchMessage` and `CommitMessages`.
- **Concurrency**: Use `ConsumeWithHandler` or `ConsumeWithWorkerPool` for concurrent message processing, specifying the concurrency level via `WithConcurrency`.
- **Error Handling**: The package logs errors using the configured logger. Ensure your application handles errors returned by methods like `Consume` and `ProduceMessages`.

## Dependencies

- `github.com/segmentio/kafka-go`
- `github.com/aws/aws-sdk-go-v2` (for AWS MSK IAM v2 authentication)

## License

This package is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
