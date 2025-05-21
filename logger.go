package kafka

import (
	"log"
	"os"
)

// Logger interface para permitir qualquer tipo de logger
type Logger interface {
	Printf(format string, v ...any)
}

// StdLogger é uma implementação simples da interface Logger
type StdLogger struct {
	logger *log.Logger
	prefix string
}

// NewStdLogger cria um novo logger para stdout com prefixo
func NewStdLogger(prefix string) *StdLogger {
	return &StdLogger{
		logger: log.New(os.Stdout, prefix, log.LstdFlags),
		prefix: prefix,
	}
}

func (l *StdLogger) Printf(format string, v ...any) {
	l.logger.Printf(format, v...)
}

// NoOpLogger é um logger que não faz nada
type NoOpLogger struct{}

// NewNoOpLogger cria um logger que não registra nada
func NewNoOpLogger() *NoOpLogger {
	return &NoOpLogger{}
}

// Printf implementa a interface Logger sem fazer nada
func (l *NoOpLogger) Printf(format string, v ...any) {}
