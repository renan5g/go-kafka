package kafka

import (
	"context"

	"github.com/segmentio/kafka-go"
)

// Producer define a interface para um produtor Kafka
type Producer interface {
	// ProduceMessages envia múltiplas mensagens para o Kafka
	ProduceMessages(ctx context.Context, messages ...kafka.Message) error

	// Stats retorna estatísticas do produtor
	Stats() kafka.WriterStats

	// Close fecha o produtor
	Close() error
}

// producer encapsula um produtor Kafka
type producer struct {
	writer *kafka.Writer
	logger Logger
}

// NewProducer creates a new Kafka producer with the provided options
func NewProducer(opts ...Option) Producer {
	options := DefaultConfig()

	for _, opt := range opts {
		opt(&options)
	}

	logger := options.Logger
	dialer := options.Dialer

	if options.SecurityConfig != nil {
		sec := options.SecurityConfig
		if sec.SASL {
			mechanism, err := getSASLMechanism(sec)
			if err != nil {
				logger.Printf("Erro ao configurar SASL: %v", err)
			} else {
				dialer.SASLMechanism = mechanism
			}
		}
		if sec.EnableTLS {
			tlsConfig, err := createTLSConfig(sec)
			if err != nil {
				logger.Printf("Erro ao configurar TLS: %v", err)
			} else {
				dialer.TLS = tlsConfig
			}
		}
	}
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:          options.Brokers,
		Topic:            options.Topic,
		Balancer:         options.Balancer,
		Dialer:           dialer,
		WriteTimeout:     options.WriteTimeout,
		ReadTimeout:      options.ReadTimeout,
		MaxAttempts:      options.MaxAttempts,
		BatchSize:        options.BatchSize,
		BatchTimeout:     options.BatchTimeout,
		Async:            options.Async,
		CompressionCodec: options.Compression,
		RequiredAcks:     int(options.RequiredAcks),
		Logger:           logger,
		ErrorLogger:      logger,
	})
	return &producer{
		writer: writer,
		logger: logger,
	}
}

func (p *producer) ProduceMessages(ctx context.Context, messages ...kafka.Message) error {
	return p.writer.WriteMessages(ctx, messages...)
}

func (p *producer) Stats() kafka.WriterStats {
	return p.writer.Stats()
}

// Close fecha o produtor
func (p *producer) Close() error {
	return p.writer.Close()
}
