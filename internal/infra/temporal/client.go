package temporal

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/sdk/client"

	"github.com/comfforts/logger"
	"github.com/hankgalt/workflow-scheduler/internal/domain/infra"
)

type registryOption struct {
	registry any
	alias    string
}

type TemporalClient struct {
	client             client.Client
	oTelShutdown       infra.ShutdownFunc
	workflowRegistries []registryOption
	activityRegistries []registryOption
}

func NewTemporalClient(ctx context.Context, cfg TemporalConfig) (*TemporalClient, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
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
		tErr := ErrTemporalClient
		if shutdown != nil {
			sErr := shutdown(ctx)
			if sErr != nil {
				l.Error("error shutting down OTel", "error", sErr.Error())
				err = errors.Join(sErr, tErr)
			}
		}
		return nil, tErr
	}

	temporalCl := TemporalClient{
		client:       tClient,
		oTelShutdown: shutdown,
	}

	return &temporalCl, nil
}

func (tc *TemporalClient) RegisteredWorkflows() []registryOption {
	return tc.workflowRegistries
}

func (tc *TemporalClient) RegisteredActivities() []registryOption {
	return tc.activityRegistries
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
		l = logger.GetSlogLogger()
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
		l = logger.GetSlogLogger()
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
		l = logger.GetSlogLogger()
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
		l = logger.GetSlogLogger()
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
		l = logger.GetSlogLogger()
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

// Close closes the temporal service client
func (tc *TemporalClient) Close(ctx context.Context) error {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		l = logger.GetSlogLogger()
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
