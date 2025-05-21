package kafka

import (
	"time"

	"github.com/segmentio/kafka-go"
)

// Option represents a functional option for configuring a Producer
type Option func(*ProducerConfig)

// ProducerConfig holds all the configurable parameters for the Producer
type ProducerConfig struct {
	Brokers        []string
	Topic          string
	Balancer       kafka.Balancer
	Dialer         *kafka.Dialer
	WriteTimeout   time.Duration
	ReadTimeout    time.Duration
	MaxAttempts    int
	BatchSize      int
	BatchTimeout   time.Duration
	Async          bool
	Compression    kafka.CompressionCodec
	RequiredAcks   kafka.RequiredAcks
	Logger         Logger
	SecurityConfig *SecurityConfig
}

// DefaultConfig returns a producerOptions with default values
func DefaultConfig() ProducerConfig {
	return ProducerConfig{
		MaxAttempts:  3,
		BatchSize:    100,
		BatchTimeout: 1 * time.Second,
		Dialer: &kafka.Dialer{
			Timeout:   10 * time.Second,
			DualStack: true,
		},
		Logger: NewNoOpLogger(),
	}
}

// WithProducerBrokers sets the Kafka brokers
func WithProducerBrokers(brokers ...string) Option {
	return func(po *ProducerConfig) {
		po.Brokers = brokers
	}
}

// WithProducerTopic sets the Kafka topic
func WithProducerTopic(topic string) Option {
	return func(po *ProducerConfig) {
		po.Topic = topic
	}
}

// WithProducerBalancer sets the balancer to use
func WithProducerBalancer(balancer kafka.Balancer) Option {
	return func(po *ProducerConfig) {
		po.Balancer = balancer
	}
}

// WithWriteTimeout sets the write timeout
func WithWriteTimeout(timeout time.Duration) Option {
	return func(po *ProducerConfig) {
		po.WriteTimeout = timeout
	}
}

// WithReadTimeout sets the read timeout
func WithReadTimeout(timeout time.Duration) Option {
	return func(po *ProducerConfig) {
		po.ReadTimeout = timeout
	}
}

// WithMaxAttempts sets the maximum number of attempts
func WithMaxAttempts(attempts int) Option {
	return func(po *ProducerConfig) {
		po.MaxAttempts = attempts
	}
}

// WithBatchSize sets the batch size
func WithBatchSize(size int) Option {
	return func(po *ProducerConfig) {
		po.BatchSize = size
	}
}

// WithBatchTimeout sets the batch timeout
func WithBatchTimeout(timeout time.Duration) Option {
	return func(po *ProducerConfig) {
		po.BatchTimeout = timeout
	}
}

// WithProducerAsync sets the async mode
func WithProducerAsync(async bool) Option {
	return func(po *ProducerConfig) {
		po.Async = async
	}
}

// WithProducerCompression sets the compression codec
func WithProducerCompression(compression kafka.Compression) Option {
	return func(po *ProducerConfig) {
		po.Compression = compression.Codec()
	}
}

// WithRequiredAcks sets the required acknowledgments
func WithRequiredAcks(acks kafka.RequiredAcks) Option {
	return func(po *ProducerConfig) {
		po.RequiredAcks = acks
	}
}

// WithProducerLogger sets the logger
func WithProducerLogger(logger Logger) Option {
	return func(po *ProducerConfig) {
		po.Logger = logger
	}
}

// WithProducerSecurity sets the security configuration
func WithProducerSecurity(securityConfig *SecurityConfig) Option {
	return func(po *ProducerConfig) {
		po.SecurityConfig = securityConfig
	}
}
