package kafka

import (
	"log"
	"os"
)

// Logger  is an interface that defines a logging method for printing formatted strings.
type Logger interface {
	Printf(format string, v ...any)
}

// StdLogger implements a Logger that writes to stdout
type StdLogger struct {
	logger *log.Logger
	prefix string
}

func NewStdLogger(prefix string) *StdLogger {
	return &StdLogger{
		logger: log.New(os.Stdout, prefix, log.LstdFlags),
		prefix: prefix,
	}
}

func (l *StdLogger) Printf(format string, v ...any) {
	l.logger.Printf(format, v...)
}

// NoOpLogger implements a Logger that does nothing
type NoOpLogger struct{}

func NewNoOpLogger() *NoOpLogger {
	return &NoOpLogger{}
}

func (l *NoOpLogger) Printf(format string, v ...any) {}
