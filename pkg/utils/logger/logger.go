package logger

import (
	"context"
	"errors"
)

var (
	ErrNoLoggerInContext = errors.New("no logger found in context")
)

// loggerKey is a custom type to avoid collisions in context.
type loggerKey struct{}

type Logger interface {
	Debug(msg string, keyvals ...any)
	Info(msg string, keyvals ...any)
	Warn(msg string, keyvals ...any)
	Error(msg string, keyvals ...any)
}

// WithLogger returns a new context with the given logger.
func WithLogger(ctx context.Context, logger Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, logger)
}

// LoggerFromContext retrieves the logger from context.
// If none is found, returns a fallback logger.
func LoggerFromContext(ctx context.Context) (Logger, error) {
	logger, ok := ctx.Value(loggerKey{}).(Logger)
	if !ok {
		return nil, ErrNoLoggerInContext
	}
	return logger, nil
}
