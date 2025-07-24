package utils

import (
	"log/slog"
	"os"
)

func GetLogger() *slog.Logger {
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
