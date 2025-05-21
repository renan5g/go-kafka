package kafka

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Producer interface {
	// WriteMessages writes a batch of messages to the kafka topic configured on this
	// writer.
	//
	// Unless the writer was configured to write messages asynchronously, the method
	// blocks until all messages have been written, or until the maximum number of
	// attempts was reached.
	//
	// When sending synchronously and the writer's batch size is configured to be
	// greater than 1, this method blocks until either a full batch can be assembled
	// or the batch timeout is reached.  The batch size and timeouts are evaluated
	// per partition, so the choice of Balancer can also influence the flushing
	// behavior.  For example, the Hash balancer will require on average N * batch
	// size messages to trigger a flush where N is the number of partitions.  The
	// best way to achieve good batching behavior is to share one Writer amongst
	// multiple go routines.
	//
	// When the method returns an error, it may be of type kafka.WriteError to allow
	// the caller to determine the status of each message.
	//
	// The context passed as first argument may also be used to asynchronously
	// cancel the operation. Note that in this case there are no guarantees made on
	// whether messages were written to kafka, they might also still be written
	// after this method has already returned, therefore it is important to not
	// modify byte slices of passed messages if WriteMessages returned early due
	// to a canceled context.
	// The program should assume that the whole batch failed and re-write the
	// messages later (which could then cause duplicates).
	ProduceMessages(ctx context.Context, messages ...kafka.Message) error

	// Stats returns a snapshot of the writer stats since the last time the method
	// was called, or since the writer was created if it is called for the first
	// time.
	//
	// A typical use of this method is to spawn a goroutine that will periodically
	// call Stats on a kafka writer and report the metrics to a stats collection
	// system.
	Stats() kafka.WriterStats

	// Close flushes pending writes, and waits for all writes to complete before
	// returning. Calling Close also prevents new writes from being submitted to
	// the writer, further calls to WriteMessages and the like will fail with
	// io.ErrClosedPipe.
	Close() error
}

type producer struct {
	writer      *kafka.Writer
	logger      Logger
	errorLogger Logger
}

// NewProducer creates a new Kafka producer with the provided options
func NewProducer(options ...ProducerOption) Producer {
	return NewProducerFromConfig(NewProducerConfig(options...))
}

// NewProducerFromConfig creates a new Kafka producer from a config
func NewProducerFromConfig(config ProducerConfig) Producer {
	errLogger := config.ErrorLogger
	dialer := config.Dialer

	if config.SecurityConfig != nil {
		sec := config.SecurityConfig
		if sec.SASL {
			mechanism, err := getSASLMechanism(sec)
			if err != nil {
				errLogger.Printf("error configuring SASL: %v", err)
			} else {
				dialer.SASLMechanism = mechanism
			}
		}
		if sec.EnableTLS {
			tlsConfig, err := createTLSConfig(sec)
			if err != nil {
				errLogger.Printf("error configuring TLS: %v", err)
			} else {
				dialer.TLS = tlsConfig
			}
		}
	}
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:          config.Brokers,
		Topic:            config.Topic,
		Balancer:         config.Balancer,
		Dialer:           dialer,
		WriteTimeout:     config.WriteTimeout,
		ReadTimeout:      config.ReadTimeout,
		MaxAttempts:      config.MaxAttempts,
		BatchSize:        config.BatchSize,
		BatchTimeout:     config.BatchTimeout,
		Async:            config.Async,
		CompressionCodec: config.Compression,
		RequiredAcks:     int(config.RequiredAcks),
		Logger:           errLogger,
		ErrorLogger:      errLogger,
	})
	return &producer{
		writer:      writer,
		logger:      config.Logger,
		errorLogger: errLogger,
	}
}

func (p *producer) ProduceMessages(ctx context.Context, messages ...kafka.Message) error {
	return p.writer.WriteMessages(ctx, messages...)
}

func (p *producer) Stats() kafka.WriterStats {
	return p.writer.Stats()
}

func (p *producer) Close() error {
	return p.writer.Close()
}
