package temporal

import (
	"context"
	"errors"
	"fmt"

	"github.com/hankgalt/workflow-scheduler/internal/domain/infra"
	"github.com/hankgalt/workflow-scheduler/internal/infra/observability"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/interceptor"
)

const (
	ERR_REQUIRED_PARAMS   = "namespace & host port are required"
	ERR_MISSING_NAMESPACE = "missing namespace information"
	ERR_MISSING_HOST      = "missing server host information"
	ERR_TEMPORAL_CLIENT   = "error creating temporal client"
)

var (
	ErrRequiredParams   = errors.New(ERR_REQUIRED_PARAMS)
	ErrMissingNamespace = errors.New(ERR_MISSING_NAMESPACE)
	ErrMissingHost      = errors.New(ERR_MISSING_HOST)
	ErrTemporalClient   = errors.New(ERR_TEMPORAL_CLIENT)
)

type TemporalConfig struct {
	namespace    string
	host         string
	clientName   string
	metricsAddr  string
	otelEndpoint string
}

func NewTemporalConfig(namespace, host, clientName, metricsAddr, otelEndpoint string) TemporalConfig {
	return TemporalConfig{
		namespace:    namespace,
		host:         host,
		clientName:   clientName,
		metricsAddr:  metricsAddr,
		otelEndpoint: otelEndpoint,
	}
}

func (rc TemporalConfig) Namespace() string    { return rc.namespace }
func (rc TemporalConfig) Host() string         { return rc.host }
func (rc TemporalConfig) ClientName() string   { return rc.clientName }
func (rc TemporalConfig) MetricsAddr() string  { return rc.metricsAddr }
func (rc TemporalConfig) OtelEndpoint() string { return rc.otelEndpoint }

type TemporalConnectionBuilder interface {
	Build(ctx context.Context) (client.Options, infra.ShutdownFunc, interceptor.Interceptor, error)
	WithMetrics(clientName, metricsAddr, otelEndpoint string) TemporalConnectionBuilder
}

// temporalClientConnectionBuilder is a ConnectionBuilder implementation for creating Temporal client options.
type temporalClientConnectionBuilder struct {
	namespace    string
	hostPort     string
	clientName   string
	metricsAddr  string
	otelEndpoint string
}

func NewTemporalClientConnectionBuilder(namespace, hostPort string) temporalClientConnectionBuilder {
	return temporalClientConnectionBuilder{
		namespace: namespace,
		hostPort:  hostPort,
	}
}

func (b temporalClientConnectionBuilder) WithMetrics(clientName, metricsAddr, otelEndpoint string) TemporalConnectionBuilder {
	b.clientName = clientName
	b.metricsAddr = metricsAddr
	b.otelEndpoint = otelEndpoint
	return b
}

func (b temporalClientConnectionBuilder) Build(ctx context.Context) (client.Options, infra.ShutdownFunc, interceptor.Interceptor, error) {
	if b.namespace == "" || b.hostPort == "" {
		return client.Options{}, nil, nil, ErrRequiredParams
	}

	opts := client.Options{
		HostPort:  b.hostPort,
		Namespace: b.namespace,
	}

	if b.clientName != "" && b.metricsAddr != "" && b.otelEndpoint != "" {
		// 1) Init OTel
		shutdown, err := observability.Init(ctx, observability.InitOptions{
			ServiceName:  b.clientName,
			MetricsAddr:  b.metricsAddr,  // Prometheus scrape port
			OTLPEndpoint: b.otelEndpoint, // or "" to skip traces, or "jaeger:4317" or "otel-collector:4317"
		})
		if err != nil {
			return opts, nil, nil, fmt.Errorf("error initializing observability: %w", err)
		}

		// 2) Temporal OTel integrations
		tracingInt, err := opentelemetry.NewTracingInterceptor(opentelemetry.TracerOptions{})
		if err != nil {
			return opts, shutdown, nil, fmt.Errorf("error creating tracing interceptor: %w", err)
		}
		mh := opentelemetry.NewMetricsHandler(opentelemetry.MetricsHandlerOptions{})

		// 3) Update client options with metrics and tracing
		opts.Interceptors = []interceptor.ClientInterceptor{tracingInt}
		opts.MetricsHandler = mh

		return opts, shutdown, tracingInt, nil
	}

	return opts, nil, nil, nil
}
