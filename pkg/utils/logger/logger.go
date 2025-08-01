package logger

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

const DEFAULT_LOG_FILE_PATH = "logs/app.log"

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

func GetSlogLogger() *slog.Logger {
	logLevel := &slog.LevelVar{}

	logLevel.Set(slog.LevelInfo)
	if os.Getenv("INFRA") == "local" {
		logLevel.Set(slog.LevelDebug)
	}

	opts := &slog.HandlerOptions{
		Level: logLevel,
	}

	handler := slog.NewTextHandler(os.Stdout, opts)

	l := slog.New(handler)
	slog.SetDefault(l)
	return l
}

func GetSlogMultiLogger(dir string) *slog.Logger {
	filePath := DEFAULT_LOG_FILE_PATH
	if dir != "" {
		filePath = filepath.Join(dir, filePath)
	}

	logLevel := &slog.LevelVar{}
	logLevel.Set(slog.LevelInfo)
	if os.Getenv("INFRA") == "local" {
		logLevel.Set(slog.LevelDebug)
	}

	opts := &slog.HandlerOptions{
		Level: logLevel,
	}

	// Create lumberjack writer
	logWriter := &lumberjack.Logger{
		Filename:   filePath,
		MaxSize:    100, // megabytes
		MaxBackups: 5,
		MaxAge:     28,   // days
		Compress:   true, // compress rotated logs
	}

	// Create a MultiWriter if you want logs in both file and console
	multiWriter := io.MultiWriter(os.Stdout, logWriter)

	// Use TextHandler or JSONHandler
	handler := slog.NewTextHandler(multiWriter, opts)

	l := slog.New(handler)
	slog.SetDefault(l)
	return l
}

func GetZapLogger(dir, namedAs string) *zap.Logger {
	filePath := DEFAULT_LOG_FILE_PATH
	if dir != "" {
		filePath = filepath.Join(dir, filePath)
	}

	cfg := zap.NewDevelopmentEncoderConfig()
	cfg.EncodeTime = zapcore.ISO8601TimeEncoder

	fileEncoder := zapcore.NewJSONEncoder(cfg)
	consoleEncoder := zapcore.NewConsoleEncoder(cfg)

	writer := zapcore.AddSync(&lumberjack.Logger{
		Filename:   filePath,
		MaxSize:    10, // megabytes
		MaxBackups: 3,
		MaxAge:     28, // days
	})

	logLevel := zapcore.InfoLevel
	if os.Getenv("INFRA") == "local" {
		logLevel = zapcore.DebugLevel
	}

	core := zapcore.NewTee(
		zapcore.NewCore(fileEncoder, writer, logLevel),
		zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), logLevel),
	)
	logger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel)).Named(namedAs)
	return logger
}
