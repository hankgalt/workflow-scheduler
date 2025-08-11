package temporal

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/sdk/client"

	"github.com/comfforts/logger"
	"github.com/hankgalt/workflow-scheduler/internal/domain/infra"
)

const (
	ERR_MISSING_NAMESPACE = "missing namespace information"
	ERR_MISSING_HOST      = "missing server host information"
	ERR_TEMPORAL_CLIENT   = "error creating temporal client"
)

var (
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

func (rc TemporalConfig) Namespace() string    { return rc.namespace }
func (rc TemporalConfig) Host() string         { return rc.host }
func (rc TemporalConfig) ClientName() string   { return rc.clientName }
func (rc TemporalConfig) MetricsAddr() string  { return rc.metricsAddr }
func (rc TemporalConfig) OtelEndpoint() string { return rc.otelEndpoint }

func NewTemporalConfig(namespace, host, clientName, metricsAddr, otelEndpoint string) TemporalConfig {
	return TemporalConfig{
		namespace:    namespace,
		host:         host,
		clientName:   clientName,
		metricsAddr:  metricsAddr,
		otelEndpoint: otelEndpoint,
	}
}

type registryOption struct {
	registry any
	alias    string
}

type TemporalClient struct {
	// client is the client used for temporal
	client             client.Client
	oTelShutdown       infra.ShutdownFunc
	workflowRegistries []registryOption
	activityRegistries []registryOption
}

func NewTemporalClient(ctx context.Context, cfg TemporalConfig) (*TemporalClient, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("temporalClient:NewTemporalClient - %w", err)
	}

	connBuilder := NewTemporalClientConnectionBuilder(cfg.Namespace(), cfg.Host()).WithMetrics(cfg.ClientName(), cfg.MetricsAddr(), cfg.OtelEndpoint())

	clOpts, shutdown, _, err := connBuilder.Build(ctx)
	if err != nil {
		l.Error("error building temporal client options", "error", err.Error())
		if shutdown != nil {
			sErr := shutdown(ctx)
			if sErr != nil {
				l.Error("error shutting down OTel", "error", sErr.Error())
				err = errors.Join(sErr, err)
			}
		}
		return nil, fmt.Errorf("error building temporal client options: %w", err)
	}

	tClient, err := client.Dial(clOpts)
	if err != nil {
		l.Error("error connecting temporal server", "error", err.Error())
		if shutdown != nil {
			sErr := shutdown(ctx)
			if sErr != nil {
				l.Error("error shutting down OTel", "error", sErr.Error())
				err = errors.Join(sErr, err)
			}
		}
		return nil, err
	}

	temporalCl := TemporalClient{
		client:       tClient,
		oTelShutdown: shutdown,
	}

	return &temporalCl, nil
}

// Close closes the temporal service client
func (tc *TemporalClient) Close(ctx context.Context) error {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return fmt.Errorf("temporalClient:NewTemporalClient - %w", err)
	}

	tc.client.Close()

	if tc.oTelShutdown != nil {
		err := tc.oTelShutdown(ctx)
		if err != nil {
			l.Error("error shutting down OTel", "error", err.Error())
			return fmt.Errorf("error shutting down OTel: %w", err)
		}
	}
	return nil
}

// RegisterWorkflow registers a workflow
func (cc *TemporalClient) RegisterWorkflow(workflow any) {
	cc.RegisterWorkflowWithAlias(workflow, "")
}

// RegisterWorkflowWithAlias registers a workflow with alias
func (cc *TemporalClient) RegisterWorkflowWithAlias(workflow any, alias string) {
	registryOption := registryOption{
		registry: workflow,
		alias:    alias,
	}
	cc.workflowRegistries = append(cc.workflowRegistries, registryOption)
}

// RegisterActivity registers a activity
func (cc *TemporalClient) RegisterActivity(activity any) {
	cc.RegisterActivityWithAlias(activity, "")
}

// RegisterActivityWithAlias registers a activity with alias
func (cc *TemporalClient) RegisterActivityWithAlias(activity any, alias string) {
	registryOption := registryOption{
		registry: activity,
		alias:    alias,
	}
	cc.activityRegistries = append(cc.activityRegistries, registryOption)
}

// StartWorkflow starts a workflow
func (cc *TemporalClient) StartWorkflow(
	options client.StartWorkflowOptions,
	workflow any,
	args ...any,
) (client.WorkflowRun, error) {
	return cc.StartWorkflowWithCtx(context.Background(), options, workflow, args...)
}

// StartWorkflowWithCtx starts a workflow with the provided context
func (cc *TemporalClient) StartWorkflowWithCtx(
	ctx context.Context,
	options client.StartWorkflowOptions,
	workflow any,
	args ...any,
) (client.WorkflowRun, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("TemporalClient:StartWorkflowWithCtx - %w", err)
	}

	we, err := cc.client.ExecuteWorkflow(ctx, options, workflow, args...)
	if err != nil {
		l.Error("failed to create workflow", "error", err.Error())
		return nil, fmt.Errorf("failed to create workflow: %w", err)
	}

	l.Info("started Workflow", "WorkflowID", we.GetID(), "RunID", we.GetRunID())
	return we, nil
}

func (cc *TemporalClient) SignalWorkflow(ctx context.Context, workflowID, signal string, data any) error {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return fmt.Errorf("TemporalClient:SignalWorkflow - %w", err)
	}

	err = cc.client.SignalWorkflow(context.Background(), workflowID, "", signal, data)
	if err != nil {
		l.Error("failed to signal workflow", "error", err.Error())
		return fmt.Errorf("failed to signal workflow: %w", err)
	}
	return nil
}

// SignalWithStartWorkflowWithCtx signals workflow and starts it if it's not yet started
func (cc *TemporalClient) SignalWithStartWorkflowWithCtx(
	ctx context.Context,
	workflowID string,
	signalName string,
	signalArg any,
	options client.StartWorkflowOptions,
	workflow any,
	workflowArgs ...any,
) (client.WorkflowRun, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("temporalClient:SignalWithStartWorkflowWithCtx - %w", err)
	}

	we, err := cc.client.SignalWithStartWorkflow(ctx, workflowID, signalName, signalArg, options, workflow, workflowArgs...)
	if err != nil {
		l.Error("failed to signal with start workflow", "error", err.Error())
		return nil, fmt.Errorf("failed to signal with start workflow: %w", err)
	}

	l.Info("signaled and started Workflow", "WorkflowID", we.GetID(), "RunID", we.GetRunID())
	return we, nil
}

func (cc *TemporalClient) CancelWorkflow(ctx context.Context, workflowID string) error {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return fmt.Errorf("TemporalClient:CancelWorkflow - error getting logger from context: %w", err)
	}

	err = cc.client.CancelWorkflow(ctx, workflowID, "")
	if err != nil {
		l.Error("failed to cancel workflow", "error", err.Error())
		return fmt.Errorf("failed to cancel workflow: %w", err)
	}
	return nil
}

func (cc *TemporalClient) QueryWorkflow(
	ctx context.Context,
	workflowID, runID, queryType string,
	args ...any,
) (any, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("TemporalClient:QueryWorkflow - %w", err)
	}

	resp, err := cc.client.QueryWorkflow(ctx, workflowID, runID, queryType, args...)
	if err != nil {
		l.Error("failed to query workflow", "error", err.Error())
		return nil, fmt.Errorf("failed to query workflow: %w", err)
	}
	var result any
	if err := resp.Get(&result); err != nil {
		l.Error("failed to decode query result", "error", err.Error())
		return nil, fmt.Errorf("failed to decode query result: %w", err)
	}
	// l.Debug("received query result", "result", result)
	return result, nil
}
