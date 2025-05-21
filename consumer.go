package kafka

import (
	"context"
	"sync"

	"github.com/segmentio/kafka-go"
)

type ConsumerHandlerOptions struct {
	Concurrency int
}

type ConsumerHandlerOption func(*ConsumerHandlerOptions)

// Consumer define a interface para consumidor Kafka
type Consumer interface {
	Consume(ctx context.Context) (Message, error)
	ConsumeWithHandler(ctx context.Context, handler MessageHandler, opts ...ConsumerHandlerOption)
	ConsumeWithWorkerPool(ctx context.Context, handler MessageHandler, concurrency int)
	CommitMessages(ctx context.Context, msgs ...Message) error
	Stats() kafka.ReaderStats
	SetOffset(offset int64) error
	Close() error
}

// MessageHandler é um callback para processamento de mensagens
type MessageHandler func(Message) error

// consumer encapsula um consumidor Kafka
type consumer struct {
	reader *kafka.Reader
	logger Logger
}

// NewConsumer cria um novo consumidor Kafka com as opções fornecidas
func NewConsumer(opts ...ConsumerOption) Consumer {
	// Iniciar com opções padrão
	options := DefaultConsumerConfig()

	// Aplicar todas as opções fornecidas
	for _, opt := range opts {
		opt(&options)
	}

	// Configurar logger
	logger := options.Logger

	// Configurar dialer com opções de segurança
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

	readerConfig := kafka.ReaderConfig{
		Brokers:           options.Brokers,
		Topic:             options.Topic,
		GroupID:           options.GroupID,
		StartOffset:       options.StartOffset,
		MinBytes:          options.MinBytes,
		MaxBytes:          options.MaxBytes,
		MaxWait:           options.MaxWait,
		IsolationLevel:    options.IsolationLevel,
		MaxAttempts:       options.MaxAttempts,
		HeartbeatInterval: options.HeartbeatInterval,
		SessionTimeout:    options.SessionTimeout,
		RebalanceTimeout:  options.RebalanceTimeout,
		RetentionTime:     options.RetentionTime,
		CommitInterval:    options.CommitInterval,
		ReadBackoffMin:    options.ReadBackoffMin,
		ReadBackoffMax:    options.ReadBackoffMax,
		Dialer:            dialer,
	}

	reader := kafka.NewReader(readerConfig)

	return &consumer{
		reader: reader,
		logger: logger,
	}
}

// Consume consome uma mensagem do Kafka
func (c *consumer) Consume(ctx context.Context) (kafka.Message, error) {
	return c.reader.ReadMessage(ctx)
}

// ConsumeWithHandler consome mensagens usando um handler
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
			c.logger.Println("Contexto cancelado, parando consumo")
			return
		default:
			msg, err := c.reader.FetchMessage(ctx)
			if err != nil {
				c.logger.Printf("Erro ao buscar mensagem: %v", err)
				continue
			}
			err = handler(msg)
			if err != nil {
				c.logger.Printf("Erro ao processar mensagem: %v", err)
				continue
			}
			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				c.logger.Printf("Erro ao confirmar mensagem: %v", err)
			}
		}
	}
}

// ConsumeWithHandlerConcurrent consome mensagens usando um handler com concorrência
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
						c.logger.Printf("Erro ao processar mensagem: %v", err)
						continue
					}
					if err := c.reader.CommitMessages(ctx, msg); err != nil {
						c.logger.Printf("Erro ao confirmar mensagem: %v", err)
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
				c.logger.Println("Contexto cancelado, parando consumo")
				return
			default:
				msg, err := c.reader.FetchMessage(ctx)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					c.logger.Printf("Erro ao buscar mensagem: %v", err)
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

// CommitMessages confirma múltiplas mensagens
func (c *consumer) CommitMessages(ctx context.Context, msgs ...Message) error {
	return c.reader.CommitMessages(ctx, msgs...)
}

// Stats retorna estatísticas do consumidor
func (c *consumer) Stats() kafka.ReaderStats {
	return c.reader.Stats()
}

// SetOffset define a posição inicial para leitura
func (c *consumer) SetOffset(offset int64) error {
	return c.reader.SetOffset(offset)
}

// Close fecha o consumidor
func (c *consumer) Close() error {
	return c.reader.Close()
}
