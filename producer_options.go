package kafka

import (
	"time"

	"github.com/segmentio/kafka-go"
)

// ProducerOption represents a functional option for configuring a Producer
type ProducerOption func(*ProducerConfig)

// ProducerConfig holds all the configurable parameters for the Producer
type ProducerConfig struct {
	// The list of brokers used to discover the partitions available on the
	// kafka cluster.
	//
	// This field is required, attempting to create a writer with an empty list
	// of brokers will panic.
	Brokers []string

	// The topic that the writer will produce messages to.
	//
	// If provided, this will be used to set the topic for all produced messages.
	// If not provided, each Message must specify a topic for itself. This must be
	// mutually exclusive, otherwise the Writer will return an error.
	Topic string

	// The dialer used by the writer to establish connections to the kafka
	// cluster.
	//
	// If nil, the default dialer is used instead.
	Dialer *kafka.Dialer

	// The balancer used to distribute messages across partitions.
	//
	// The default is to use a round-robin distribution.
	Balancer kafka.Balancer

	// Limit on how many attempts will be made to deliver a message.
	//
	// The default is to try at most 10 times.
	MaxAttempts int

	// Timeout for write operation performed by the Writer.
	//
	// Defaults to 10 seconds.
	WriteTimeout time.Duration

	// Timeout for read operations performed by the Writer.
	//
	// Defaults to 10 seconds.
	ReadTimeout time.Duration

	// Limit on how many messages will be buffered before being sent to a
	// partition.
	//
	// The default is to use a target batch size of 100 messages.
	BatchSize int

	// Time limit on how often incomplete message batches will be flushed to
	// kafka.
	//
	// The default is to flush at least every second.
	BatchTimeout time.Duration

	// CompressionCodec set the codec to be used to compress Kafka messages.
	Compression kafka.CompressionCodec

	// Number of acknowledges from partition replicas required before receiving
	// a response to a produce request. The default is -1, which means to wait for
	// all replicas, and a value above 0 is required to indicate how many replicas
	// should acknowledge a message to be considered successful.
	RequiredAcks kafka.RequiredAcks

	// If not nil, specifies a logger used to report internal changes within the
	// writer.
	Logger Logger

	// ErrorLogger is the logger used to report errors. If nil, the writer falls
	// back to using Logger instead.
	ErrorLogger Logger

	// SecurityConfig holds the security configuration for the producer.
	// If not nil, it will be used to configure the security settings for the producer.
	SecurityConfig *SecurityConfig

	// Setting this flag to true causes the WriteMessages method to never block.
	// It also means that errors are ignored since the caller will not receive
	// the returned value. Use this only if you don't care about guarantees of
	// whether the messages were written to kafka.
	Async bool
}

// DefaultProducerConfig returns a producerOptions with default values
func DefaultProducerConfig() ProducerConfig {
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

func NewProducerConfig(options ...ProducerOption) ProducerConfig {
	cfg := DefaultProducerConfig()
	for _, option := range options {
		option(&cfg)
	}
	return cfg
}

// WithProducerBrokers sets the Kafka brokers
func WithProducerBrokers(brokers ...string) ProducerOption {
	return func(po *ProducerConfig) {
		po.Brokers = brokers
	}
}

// WithProducerTopic sets the Kafka topic
func WithProducerTopic(topic string) ProducerOption {
	return func(po *ProducerConfig) {
		po.Topic = topic
	}
}

// WithProducerBalancer sets the balancer to use
func WithProducerBalancer(balancer kafka.Balancer) ProducerOption {
	return func(po *ProducerConfig) {
		po.Balancer = balancer
	}
}

// WithWriteTimeout sets the write timeout
func WithWriteTimeout(timeout time.Duration) ProducerOption {
	return func(po *ProducerConfig) {
		po.WriteTimeout = timeout
	}
}

// WithReadTimeout sets the read timeout
func WithReadTimeout(timeout time.Duration) ProducerOption {
	return func(po *ProducerConfig) {
		po.ReadTimeout = timeout
	}
}

// WithMaxAttempts sets the maximum number of attempts
func WithMaxAttempts(attempts int) ProducerOption {
	return func(po *ProducerConfig) {
		po.MaxAttempts = attempts
	}
}

// WithBatchSize sets the batch size
func WithBatchSize(size int) ProducerOption {
	return func(po *ProducerConfig) {
		po.BatchSize = size
	}
}

// WithBatchTimeout sets the batch timeout
func WithBatchTimeout(timeout time.Duration) ProducerOption {
	return func(po *ProducerConfig) {
		po.BatchTimeout = timeout
	}
}

// WithProducerAsync sets the async mode
func WithProducerAsync(async bool) ProducerOption {
	return func(po *ProducerConfig) {
		po.Async = async
	}
}

// WithProducerCompression sets the compression codec
func WithProducerCompression(compression kafka.Compression) ProducerOption {
	return func(po *ProducerConfig) {
		po.Compression = compression.Codec()
	}
}

// WithRequiredAcks sets the required acknowledgments
func WithRequiredAcks(acks kafka.RequiredAcks) ProducerOption {
	return func(po *ProducerConfig) {
		po.RequiredAcks = acks
	}
}

// WithProducerLogger sets the logger
func WithProducerLogger(logger Logger) ProducerOption {
	return func(po *ProducerConfig) {
		po.Logger = logger
	}
}

// WithProducerSecurity sets the security configuration
func WithProducerSecurity(securityConfig *SecurityConfig) ProducerOption {
	return func(po *ProducerConfig) {
		po.SecurityConfig = securityConfig
	}
}
