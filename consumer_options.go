package kafka

import (
	"time"

	"github.com/segmentio/kafka-go"
)

// ConsumerOption representa uma opção funcional para configurar um Consumer
type ConsumerOption func(*ConsumerConfig)

// ConsumerConfig mantém todos os parâmetros configuráveis para o Consumer
type ConsumerConfig struct {
	Brokers           []string
	Topic             string
	GroupID           string
	StartOffset       int64
	CommitInterval    time.Duration
	HeartbeatInterval time.Duration
	SessionTimeout    time.Duration
	RebalanceTimeout  time.Duration
	RetentionTime     time.Duration
	MinBytes          int
	MaxBytes          int
	MaxAttempts       int
	MaxWait           time.Duration
	ReadBackoffMin    time.Duration
	ReadBackoffMax    time.Duration
	Logger            Logger
	SecurityConfig    *SecurityConfig
	IsolationLevel    kafka.IsolationLevel
	Dialer            *kafka.Dialer
}

// DefaultConsumerConfig retorna um consumerOptions com valores padrão
func DefaultConsumerConfig() ConsumerConfig {
	return ConsumerConfig{
		StartOffset: kafka.FirstOffset,
		MinBytes:    10e3, // 10KB
		MaxBytes:    10e6, // 10MB
		MaxWait:     1 * time.Second,
		Dialer: &kafka.Dialer{
			Timeout:   10 * time.Second,
			DualStack: true,
		},
		Logger:            NewNoOpLogger(),
		IsolationLevel:    kafka.ReadUncommitted,
		CommitInterval:    time.Second,
		HeartbeatInterval: 3 * time.Second,
		SessionTimeout:    30 * time.Second,
		RebalanceTimeout:  30 * time.Second,
	}
}

// WithConsumerBrokers define os brokers Kafka
func WithConsumerBrokers(brokers ...string) ConsumerOption {
	return func(co *ConsumerConfig) {
		co.Brokers = brokers
	}
}

// WithConsumerTopic define o tópico Kafka
func WithConsumerTopic(topic string) ConsumerOption {
	return func(co *ConsumerConfig) {
		co.Topic = topic
	}
}

// WithConsumerGroupID define o ID do grupo de consumidores
func WithConsumerGroupID(groupID string) ConsumerOption {
	return func(co *ConsumerConfig) {
		co.GroupID = groupID
	}
}

// WithStartOffset define o offset inicial (FirstOffset ou LastOffset)
func WithStartOffset(offset int64) ConsumerOption {
	return func(co *ConsumerConfig) {
		co.StartOffset = offset
	}
}

// WithCommitInterval define o intervalo de commit
func WithCommitInterval(interval time.Duration) ConsumerOption {
	return func(co *ConsumerConfig) {
		co.CommitInterval = interval
	}
}

// WithHeartbeatInterval define o intervalo de heartbeat
func WithHeartbeatInterval(interval time.Duration) ConsumerOption {
	return func(co *ConsumerConfig) {
		co.HeartbeatInterval = interval
	}
}

// WithSessionTimeout define o timeout da sessão
func WithSessionTimeout(timeout time.Duration) ConsumerOption {
	return func(co *ConsumerConfig) {
		co.SessionTimeout = timeout
	}
}

// WithRebalanceTimeout define o timeout de rebalanceamento
func WithRebalanceTimeout(timeout time.Duration) ConsumerOption {
	return func(co *ConsumerConfig) {
		co.RebalanceTimeout = timeout
	}
}

// WithRetentionTime define o tempo de retenção
func WithRetentionTime(retention time.Duration) ConsumerOption {
	return func(co *ConsumerConfig) {
		co.RetentionTime = retention
	}
}

// WithMinBytes define o tamanho mínimo de bytes
func WithMinBytes(minBytes int) ConsumerOption {
	return func(co *ConsumerConfig) {
		co.MinBytes = minBytes
	}
}

// WithMaxBytes define o tamanho máximo de bytes
func WithMaxBytes(maxBytes int) ConsumerOption {
	return func(co *ConsumerConfig) {
		co.MaxBytes = maxBytes
	}
}

// WithMaxWait define o tempo máximo de espera
func WithMaxWait(maxWait time.Duration) ConsumerOption {
	return func(co *ConsumerConfig) {
		co.MaxWait = maxWait
	}
}

// WithReadBackoffMin define o backoff mínimo de leitura
func WithReadBackoffMin(backoff time.Duration) ConsumerOption {
	return func(co *ConsumerConfig) {
		co.ReadBackoffMin = backoff
	}
}

// WithReadBackoffMax define o backoff máximo de leitura
func WithReadBackoffMax(backoff time.Duration) ConsumerOption {
	return func(co *ConsumerConfig) {
		co.ReadBackoffMax = backoff
	}
}

// WithLogger define o logger
func WithLogger(logger Logger) ConsumerOption {
	return func(co *ConsumerConfig) {
		co.Logger = logger
	}
}

// WithSecurity define a configuração de segurança
func WithSecurity(config *SecurityConfig) ConsumerOption {
	return func(co *ConsumerConfig) {
		co.SecurityConfig = config
	}
}

// WithIsolationLevel define o nível de isolamento (ReadUncommitted ou ReadCommitted)
func WithIsolationLevel(level kafka.IsolationLevel) ConsumerOption {
	return func(co *ConsumerConfig) {
		co.IsolationLevel = level
	}
}
