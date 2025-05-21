package kafka

import (
	"time"

	"github.com/segmentio/kafka-go"
)

// ConsumerOption defines a function type that modifies ConsumerConfig.
// It is used in a functional options pattern to configure consumer settings.
// Each option function takes a pointer to ConsumerConfig and applies specific modifications to it.
type ConsumerOption func(*ConsumerConfig)

// ConsumerHandlerOption defines a function type that modifies ConsumerHandlerOptions.
// It follows the functional options pattern for configuring the consumer handler.
type ConsumerHandlerOption func(*ConsumerHandlerOptions)

type ConsumerHandlerOptions struct {
	// Concurrency is the number of concurrent goroutines to use for consuming messages.
	// If Concurrency is greater than 1, the consumer will use a worker pool to process messages.
	Concurrency int
}

// ConsumerConfig is a configuration object used to create new instances of
// Consumer.
type ConsumerConfig struct {
	// The list of broker addresses used to connect to the kafka cluster.
	Brokers []string

	// GroupID holds the optional consumer group id.  If GroupID is specified, then
	// Partition should NOT be specified e.g. 0
	GroupID string

	// GroupTopics allows specifying multiple topics, but can only be used in
	// combination with GroupID, as it is a consumer-group feature. As such, if
	// GroupID is set, then either Topic or GroupTopics must be defined.
	GroupTopics []string

	// The topic to read messages from.
	Topic string

	// StartOffset determines from whence the consumer group should begin
	// consuming when it finds a partition without a committed offset.  If
	// non-zero, it must be set to one of FirstOffset or LastOffset.
	//
	// Default: FirstOffset
	//
	// Only used when GroupID is set
	StartOffset int64

	// CommitInterval indicates the interval at which offsets are committed to
	// the broker.  If 0, commits will be handled synchronously.
	//
	// Default: 0
	//
	// Only used when GroupID is set
	CommitInterval time.Duration

	// HeartbeatInterval sets the optional frequency at which the reader sends the consumer
	// group heartbeat update.
	//
	// Default: 3s
	//
	// Only used when GroupID is set
	HeartbeatInterval time.Duration

	// SessionTimeout optionally sets the length of time that may pass without a heartbeat
	// before the coordinator considers the consumer dead and initiates a rebalance.
	//
	// Default: 30s
	//
	// Only used when GroupID is set
	SessionTimeout time.Duration

	// RebalanceTimeout optionally sets the length of time the coordinator will wait
	// for members to join as part of a rebalance.  For kafka servers under higher
	// load, it may be useful to set this value higher.
	//
	// Default: 30s
	//
	// Only used when GroupID is set
	RebalanceTimeout time.Duration

	// RetentionTime optionally sets the length of time the consumer group will be saved
	// by the broker. -1 will disable the setting and leave the
	// retention up to the broker's offsets.retention.minutes property. By
	// default, that setting is 1 day for kafka < 2.0 and 7 days for kafka >= 2.0.
	//
	// Default: -1
	//
	// Only used when GroupID is set
	RetentionTime time.Duration

	// MinBytes indicates to the broker the minimum batch size that the consumer
	// will accept. Setting a high minimum when consuming from a low-volume topic
	// may result in delayed delivery when the broker does not have enough data to
	// satisfy the defined minimum.
	//
	// Default: 10KB
	MinBytes int

	// MaxBytes indicates to the broker the maximum batch size that the consumer
	// will accept. The broker will truncate a message to satisfy this maximum, so
	// choose a value that is high enough for your largest message size.
	//
	// Default: 10MB
	MaxBytes int

	// Maximum amount of time to wait for new data to come when fetching batches
	// of messages from kafka.
	//
	// Default: 10s
	MaxWait time.Duration

	// Limit of how many attempts to connect will be made before returning the error.
	//
	// The default is to try 3 times.
	MaxAttempts int

	// BackoffDelayMin optionally sets the smallest amount of time the reader will wait before
	// polling for new messages
	//
	// Default: 100ms
	ReadBackoffMin time.Duration

	// BackoffDelayMax optionally sets the maximum amount of time the reader will wait before
	// polling for new messages
	//
	// Default: 1s
	ReadBackoffMax time.Duration

	// Logger is the logger used to report errors. If nil, the reader falls
	// back to using Logger instead.
	Logger Logger

	// SecurityConfig is the security configuration for the consumer.
	SecurityConfig *SecurityConfig

	// IsolationLevel controls the visibility of transactional records.
	// ReadUncommitted makes all records visible. With ReadCommitted only
	// non-transactional and committed records are visible.
	IsolationLevel kafka.IsolationLevel

	// An dialer used to open connections to the kafka server. This field is
	// optional, if nil, the default dialer is used instead.
	Dialer *kafka.Dialer
}

func DefaultConsumerConfig() ConsumerConfig {
	return ConsumerConfig{
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
		MaxWait:  1 * time.Second,
		Dialer: &kafka.Dialer{
			Timeout:   10 * time.Second,
			DualStack: true,
		},
		Logger:            NewNoOpLogger(),
		HeartbeatInterval: 3 * time.Second,
		SessionTimeout:    30 * time.Second,
		RebalanceTimeout:  30 * time.Second,
	}
}

func NewConsumerConfig(options ...ConsumerOption) ConsumerConfig {
	config := DefaultConsumerConfig()
	for _, option := range options {
		option(&config)
	}
	return config
}

func WithConsumerBrokers(brokers ...string) ConsumerOption {
	return func(co *ConsumerConfig) {
		co.Brokers = brokers
	}
}

func WithConsumerTopics(topics ...string) ConsumerOption {
	return func(co *ConsumerConfig) {
		if len(topics) == 1 {
			co.Topic = topics[0]
		} else {
			co.GroupTopics = topics
		}
	}
}

func WithConsumerGroup(group string) ConsumerOption {
	return func(co *ConsumerConfig) {
		co.GroupID = group
	}
}

func WithStartOffset(offset int64) ConsumerOption {
	return func(co *ConsumerConfig) {
		co.StartOffset = offset
	}
}

func WithCommitInterval(interval time.Duration) ConsumerOption {
	return func(co *ConsumerConfig) {
		co.CommitInterval = interval
	}
}

func WithHeartbeatInterval(interval time.Duration) ConsumerOption {
	return func(co *ConsumerConfig) {
		co.HeartbeatInterval = interval
	}
}

func WithSessionTimeout(timeout time.Duration) ConsumerOption {
	return func(co *ConsumerConfig) {
		co.SessionTimeout = timeout
	}
}

func WithRebalanceTimeout(timeout time.Duration) ConsumerOption {
	return func(co *ConsumerConfig) {
		co.RebalanceTimeout = timeout
	}
}

func WithRetentionTime(retention time.Duration) ConsumerOption {
	return func(co *ConsumerConfig) {
		co.RetentionTime = retention
	}
}

func WithMinBytes(minBytes int) ConsumerOption {
	return func(co *ConsumerConfig) {
		co.MinBytes = minBytes
	}
}

func WithMaxBytes(maxBytes int) ConsumerOption {
	return func(co *ConsumerConfig) {
		co.MaxBytes = maxBytes
	}
}

func WithMaxWait(maxWait time.Duration) ConsumerOption {
	return func(co *ConsumerConfig) {
		co.MaxWait = maxWait
	}
}

func WithReadBackoffMin(backoff time.Duration) ConsumerOption {
	return func(co *ConsumerConfig) {
		co.ReadBackoffMin = backoff
	}
}

// WithReadBackoffMax sets the maximum backoff duration between read attempts when
// no messages are available to consume. This option helps control the rate of
// retry attempts when the consumer finds no messages, preventing excessive CPU
// usage while still maintaining responsiveness.
//
// The backoff duration is the maximum time the consumer will wait before attempting
// to read messages again. Setting this to a higher value reduces CPU usage during
// quiet periods, while setting it lower increases responsiveness at the cost of
// more frequent read attempts.
func WithReadBackoffMax(backoff time.Duration) ConsumerOption {
	return func(co *ConsumerConfig) {
		co.ReadBackoffMax = backoff
	}
}

// WithConsumerLogger sets a custom logger for the consumer.
// The provided logger must implement the Logger interface.
// If not set, a default logger will be used.
func WithConsumerLogger(logger Logger) ConsumerOption {
	return func(co *ConsumerConfig) {
		co.Logger = logger
	}
}

// WithConsumerSecurity sets the security configuration for the consumer.
func WithConsumerSecurity(config *SecurityConfig) ConsumerOption {
	return func(co *ConsumerConfig) {
		co.SecurityConfig = config
	}
}

// WithIsolationLevel sets the isolation level for the consumer.
func WithIsolationLevel(level kafka.IsolationLevel) ConsumerOption {
	return func(co *ConsumerConfig) {
		co.IsolationLevel = level
	}
}
