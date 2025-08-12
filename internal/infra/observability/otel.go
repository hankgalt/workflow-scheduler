package observability

import (
	"context"
	"log"
	"net/http"
	"os"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
)

const DEFAULT_METRICS_ADDR = ":9464"

type InitOptions struct {
	ServiceName  string
	MetricsAddr  string // e.g. ":9464" (Prom’s Prometheus exporter defaults to 9464)
	OTLPEndpoint string // e.g. "otel-collector:4317" or "" to skip tracing exporter
}

func Init(ctx context.Context, opt InitOptions) (shutdown func(context.Context) error, err error) {
	host, _ := os.Hostname()
	res, _ := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(opt.ServiceName),
			semconv.ServiceInstanceIDKey.String(host),
		),
	)

	// --- Metrics: Prometheus exporter (scrape /metrics) ---
	promExp, err := prometheus.New()
	if err != nil {
		return nil, err
	}
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(promExp),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)

	// serve /metrics
	if opt.MetricsAddr == "" {
		opt.MetricsAddr = DEFAULT_METRICS_ADDR
	}
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Printf("Prometheus metrics on %s/metrics", opt.MetricsAddr)
		if err := http.ListenAndServe(opt.MetricsAddr, nil); err != nil {
			log.Printf("metrics server error: %v", err)
		}
	}()

	// --- Traces: OTLP (via collector or Jaeger OTLP) ---
	var tp *sdktrace.TracerProvider
	if opt.OTLPEndpoint != "" {
		exp, err := otlptracegrpc.New(ctx, otlptracegrpc.WithEndpoint(opt.OTLPEndpoint), otlptracegrpc.WithInsecure())
		if err != nil {
			return nil, err
		}
		tp = sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(exp),
			sdktrace.WithResource(res),
		)
		otel.SetTracerProvider(tp)
		otel.SetTextMapPropagator(propagation.TraceContext{})
	}

	return func(ctx context.Context) error {
		// close trace provider if used
		if tp != nil {
			if err := tp.Shutdown(ctx); err != nil {
				return err
			}
		}
		return nil
	}, nil
}
