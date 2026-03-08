package wecomaibot

import (
	"fmt"
	"log"
	"os"
	"time"
)

type Logger interface {
	Debug(message string, args ...any)
	Info(message string, args ...any)
	Warn(message string, args ...any)
	Error(message string, args ...any)
}

type DefaultLogger struct {
	prefix string
	logger *log.Logger
}

func NewDefaultLogger(prefix string) *DefaultLogger {
	if prefix == "" {
		prefix = "AiBotSDK"
	}
	return &DefaultLogger{
		prefix: prefix,
		logger: log.New(os.Stdout, "", 0),
	}
}

func (l *DefaultLogger) Debug(message string, args ...any) {
	l.printf("DEBUG", message, args...)
}

func (l *DefaultLogger) Info(message string, args ...any) {
	l.printf("INFO", message, args...)
}

func (l *DefaultLogger) Warn(message string, args ...any) {
	l.printf("WARN", message, args...)
}

func (l *DefaultLogger) Error(message string, args ...any) {
	l.printf("ERROR", message, args...)
}

func (l *DefaultLogger) printf(level string, message string, args ...any) {
	if len(args) > 0 {
		message = fmt.Sprintf(message, args...)
	}
	l.logger.Printf("[%s] [%s] [%s] %s", time.Now().UTC().Format(time.RFC3339), l.prefix, level, message)
}
