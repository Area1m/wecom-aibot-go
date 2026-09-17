package wecomaibot

import (
	"fmt"
	"log"
	"os"
	"time"
)

// Logger 是 SDK 的日志接口，可用 Config.Logger 注入自己的实现。
type Logger interface {
	Debug(message string, args ...any)
	Info(message string, args ...any)
	Warn(message string, args ...any)
	Error(message string, args ...any)
}

// DefaultLogger 是默认日志实现，按 RFC3339 时间戳 + 前缀打印到标准输出。
type DefaultLogger struct {
	prefix string
	logger *log.Logger
}

// NewDefaultLogger 创建默认日志器，prefix 为空时用 "AiBotSDK"。
func NewDefaultLogger(prefix string) *DefaultLogger {
	if prefix == "" {
		prefix = "AiBotSDK"
	}
	return &DefaultLogger{
		prefix: prefix,
		logger: log.New(os.Stdout, "", 0),
	}
}

// Debug 打印调试日志。
func (l *DefaultLogger) Debug(message string, args ...any) {
	l.printf("DEBUG", message, args...)
}

// Info 打印普通日志。
func (l *DefaultLogger) Info(message string, args ...any) {
	l.printf("INFO", message, args...)
}

// Warn 打印告警日志。
func (l *DefaultLogger) Warn(message string, args ...any) {
	l.printf("WARN", message, args...)
}

// Error 打印错误日志。
func (l *DefaultLogger) Error(message string, args ...any) {
	l.printf("ERROR", message, args...)
}

func (l *DefaultLogger) printf(level string, message string, args ...any) {
	if len(args) > 0 {
		message = fmt.Sprintf(message, args...)
	}
	l.logger.Printf("[%s] [%s] [%s] %s", time.Now().UTC().Format(time.RFC3339), l.prefix, level, message)
}
