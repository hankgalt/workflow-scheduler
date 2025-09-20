package main

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"

	"github.com/comfforts/logger"
	envutils "github.com/hankgalt/workflow-scheduler/pkg/utils/environment"
)

func main() {

	_, otelEndpoint := envutils.BuildMetricsConfig()

	l := logger.GetSlogLogger()
	ctx := logger.WithLogger(context.Background(), l)

	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(otelEndpoint), // or otel-collector:4317 if running inside Docker
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		l.Error("failed to create OTLP trace exporter", "error", err.Error())
		return
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(resource.NewSchemaless(
			semconv.ServiceNameKey.String("otel-smoketest"),
		)),
	)
	otel.SetTracerProvider(tp)
	defer func() {
		// Ensure pending spans are flushed before exit
		err := tp.Shutdown(context.Background())
		if err != nil {
			l.Error("Error shutting down tracer provider", "error", err.Error())
		}
		l.Info("OTel TracerProvider shutdown successfully")
	}()

	_, span := otel.Tracer("smoketest").Start(ctx, "emit-one-span")
	time.Sleep(200 * time.Millisecond)
	span.End()
	l.Info("emitted one span to", "endpoint", otelEndpoint)
}
