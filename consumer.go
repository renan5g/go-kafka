package kafka

import (
	"context"
	"sync"

	"github.com/segmentio/kafka-go"
)

// Consumer provides a high-level API for consuming messages from kafka.
//
// A Consumer automatically manages reconnections to a kafka server, and
// blocking methods have context support for asynchronous cancellations.
//
// Note that it is important to call `Close()` on a `Consumer` when a process exits.
// The kafka server needs a graceful disconnect to stop it from continuing to
// attempt to send messages to the connected clients. The given example will not
// call `Close()` if the process is terminated with SIGINT (ctrl-c at the shell) or
// SIGTERM (as docker stop or a kubernetes restart does). This can result in a
// delay when a new reader on the same topic connects (e.g. new process started
// or new container running). Use a `signal.Notify` handler to close the reader on
// process shutdown.
type Consumer interface {
	// Consume reads and return the next message from the reader. The method call
	// blocks until a message becomes available, or an error occurs. The program
	// may also specify a context to asynchronously cancel the blocking operation.
	//
	// The method returns io.EOF to indicate that the reader has been closed.
	//
	// If consumer groups are used, ReadMessage will automatically commit the
	// offset when called. Note that this could result in an offset being committed
	// before the message is fully processed.
	//
	// If more fine-grained control of when offsets are committed is required, it
	// is recommended to use FetchMessage with CommitMessages instead.
	Consume(ctx context.Context) (Message, error)

	// ConsumeWithHandler consumes messages from the reader and calls the handler function
	// for each message. The handler function is executed in a separate goroutine for
	// each message.
	//
	// The context passed to the handler function is the same context used to consume messages.
	// If the context is canceled, the handler function will return immediately.
	//
	// The handler function is responsible for processing the message. If the handler
	// function returns an error, the message will be retried. If the handler function
	// returns nil, the message will be committed.
	ConsumeWithHandler(ctx context.Context, handler MessageHandler, opts ...ConsumerHandlerOption)

	// ConsumeWithWorkerPool consumes messages from the reader and calls the handler function
	// for each message. The handler function is executed in a separate goroutine for
	// each message.
	//
	// The context passed to the handler function is the same context used to consume messages.
	// If the context is canceled, the handler function will return immediately.
	//
	// The handler function is responsible for processing the message. If the handler
	// function returns an error, the message will be retried. If the handler function
	// returns nil, the message will be committed.
	ConsumeWithWorkerPool(ctx context.Context, handler MessageHandler, concurrency int)

	// CommitMessages commits the list of messages passed as argument. The program
	// may pass a context to asynchronously cancel the commit operation when it was
	// configured to be blocking.
	//
	// Because kafka consumer groups track a single offset per partition, the
	// highest message offset passed to CommitMessages will cause all previous
	// messages to be committed. Applications need to account for these Kafka
	// limitations when committing messages, and maintain message ordering if they
	// need strong delivery guarantees. This property makes it valid to pass only
	// the last message seen to CommitMessages in order to move the offset of the
	// topic/partition it belonged to forward, effectively committing all previous
	// messages in the partition.
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error

	// Stats returns a snapshot of the reader stats since the last time the method
	// was called, or since the reader was created if it is called for the first
	// time.
	//
	// A typical use of this method is to spawn a goroutine that will periodically
	// call Stats on a kafka reader and report the metrics to a stats collection
	// system.
	Stats() kafka.ReaderStats

	// SetOffset changes the offset from which the next batch of messages will be
	// read. The method fails with io.ErrClosedPipe if the reader has already been closed.
	//
	// From version 0.2.0, FirstOffset and LastOffset can be used to indicate the first
	// or last available offset in the partition. Please note while -1 and -2 were accepted
	// to indicate the first or last offset in previous versions, the meanings of the numbers
	// were swapped in 0.2.0 to match the meanings in other libraries and the Kafka protocol
	// specification.
	SetOffset(offset int64) error

	// Close closes the stream, preventing the program from reading any more
	// messages from it.
	Close() error
}

// MessageHandler is a function that handles a kafka message
type MessageHandler func(kafka.Message) error

type consumer struct {
	reader *kafka.Reader
	logger Logger
}

// NewConsumer creates a new consumer with the given options
func NewConsumer(options ...ConsumerOption) Consumer {
	return NewConsumerFromConfig(NewConsumerConfig(options...))
}

// NewConsumerFromConfig creates a new kafka consumer from a config
func NewConsumerFromConfig(config ConsumerConfig) Consumer {
	logger := config.Logger
	dialer := config.Dialer
	if config.SecurityConfig != nil {
		sec := config.SecurityConfig
		if sec.EnableSASL {
			mechanism, err := DefaultSASLMechanismFactory(sec.SASL)
			if err != nil {
				logger.Printf("error configuring SASL mechanism: %v", err)
			} else {
				dialer.SASLMechanism = mechanism
			}
		}
		if sec.EnableTLS {
			dialer.TLS = sec.TLSConfig
		}
	}
	readerConfig := kafka.ReaderConfig{
		Brokers:           config.Brokers,
		GroupID:           config.GroupID,
		GroupTopics:       config.GroupTopics,
		Topic:             config.Topic,
		StartOffset:       config.StartOffset,
		MinBytes:          config.MinBytes,
		MaxBytes:          config.MaxBytes,
		MaxWait:           config.MaxWait,
		IsolationLevel:    config.IsolationLevel,
		MaxAttempts:       config.MaxAttempts,
		HeartbeatInterval: config.HeartbeatInterval,
		SessionTimeout:    config.SessionTimeout,
		RebalanceTimeout:  config.RebalanceTimeout,
		RetentionTime:     config.RetentionTime,
		CommitInterval:    config.CommitInterval,
		ReadBackoffMin:    config.ReadBackoffMin,
		ReadBackoffMax:    config.ReadBackoffMax,
		Dialer:            dialer,
	}
	reader := kafka.NewReader(readerConfig)
	return &consumer{
		reader: reader,
		logger: logger,
	}
}

func (c *consumer) Consume(ctx context.Context) (kafka.Message, error) {
	return c.reader.ReadMessage(ctx)
}

func (c *consumer) ConsumeWithHandler(ctx context.Context, handler MessageHandler, opts ...ConsumerHandlerOption) {
	options := ConsumerHandlerOptions{
		Concurrency: 1,
	}
	for _, opt := range opts {
		opt(&options)
	}

	if options.Concurrency > 1 {
		c.ConsumeWithWorkerPool(ctx, handler, options.Concurrency)
		return
	}

	for {
		select {
		case <-ctx.Done():
			c.logger.Printf("context canceled - stopping consumption of messages")
			return
		default:
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				c.logger.Printf("error fetch message: %v", err)
				continue
			}
			err = handler(msg)
			if err != nil {
				c.logger.Printf("error processing message: %v", err)
				continue
			}
			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				c.logger.Printf("error commit message: %v", err)
			}
		}
	}
}

func (c *consumer) ConsumeWithWorkerPool(ctx context.Context, handler MessageHandler, concurrency int) {
	if concurrency < 1 {
		concurrency = 1
	}

	jobs := make(chan Message, concurrency)
	wg := sync.WaitGroup{}

	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case msg, ok := <-jobs:
					if !ok {
						return
					}
					if err := handler(msg); err != nil {
						c.logger.Printf("error processing message: %v", err)
						continue
					}
					if err := c.reader.CommitMessages(ctx, msg); err != nil {
						c.logger.Printf("error commit message: %v", err)
					}
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for {
			select {
			case <-ctx.Done():
				c.logger.Printf("context canceled - stopping consumption of messages")
				return
			default:
				msg, err := c.reader.FetchMessage(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					c.logger.Printf("error fetching message: %v", err)
					continue
				}
				select {
				case jobs <- msg:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	wg.Wait()
}

func (c *consumer) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	return c.reader.CommitMessages(ctx, msgs...)
}

func (c *consumer) Stats() kafka.ReaderStats {
	return c.reader.Stats()
}

func (c *consumer) SetOffset(offset int64) error {
	return c.reader.SetOffset(offset)
}

func (c *consumer) Close() error {
	return c.reader.Close()
}
