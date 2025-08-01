package temporal

import (
	"context"
	"fmt"
	"os"

	"go.temporal.io/sdk/client"

	"github.com/comfforts/errors"
	"github.com/hankgalt/workflow-scheduler/pkg/utils/logger"
)

const (
	ERR_MISSING_NAMESPACE = "error: missing namespace information"
	ERR_MISSING_HOST      = "error: missing server host information"
	ERR_TEMPORAL_CLIENT   = "error: creating temporal client"
)

var (
	ErrMissingNamespace = errors.NewAppError(ERR_MISSING_NAMESPACE)
	ErrMissingHost      = errors.NewAppError(ERR_MISSING_HOST)
	ErrTemporalClient   = errors.NewAppError(ERR_TEMPORAL_CLIENT)
)

type Configuration struct {
	Namespace       string `yaml:"namespace"`
	HostNameAndPort string `yaml:"host"`
}

type registryOption struct {
	registry any
	alias    string
}

type TemporalClient struct {
	// client is the client used for temporal
	client             client.Client
	workflowRegistries []registryOption
	activityRegistries []registryOption
}

func NewTemporalClient(ctx context.Context) (*TemporalClient, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("temporalClient:NewTemporalClient - failed to get logger from context: %w", err)
	}

	namespace := os.Getenv("WORKFLOW_DOMAIN")
	host := os.Getenv("TEMPORAL_HOST")

	if namespace == "" {
		l.Error(ERR_MISSING_NAMESPACE)
		return nil, ErrMissingNamespace
	}
	if host == "" {
		l.Error(ERR_MISSING_HOST)
		return nil, ErrMissingHost
	}

	clientOptions := client.Options{
		Namespace: namespace,
		HostPort:  host,
	}
	tClient, err := client.Dial(clientOptions)
	if err != nil {
		l.Error(ERR_TEMPORAL_CLIENT)
		return nil, ErrTemporalClient
	}

	temporalCl := TemporalClient{
		client: tClient,
	}

	return &temporalCl, nil
}

// Close closes the temporal service client
func (tc *TemporalClient) Close() {
	tc.client.Close()
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
		return nil, fmt.Errorf("TemporalClient:StartWorkflowWithCtx - error getting logger from context: %w", err)
	}

	we, err := cc.client.ExecuteWorkflow(ctx, options, workflow, args...)
	if err != nil {
		l.Error("failed to create workflow", "error", err.Error())
		return nil, errors.WrapError(err, "failed to create workflow")
	}

	l.Info("started Workflow", "WorkflowID", we.GetID(), "RunID", we.GetRunID())
	return we, nil
}

func (cc *TemporalClient) SignalWorkflow(ctx context.Context, workflowID, signal string, data any) error {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return err
	}

	err = cc.client.SignalWorkflow(context.Background(), workflowID, "", signal, data)
	if err != nil {
		l.Error("failed to signal workflow", "error", err.Error())
		return errors.WrapError(err, "failed to signal workflow")
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
		return nil, fmt.Errorf("temporalClient:SignalWithStartWorkflowWithCtx - failed to get logger from context: %w", err)
	}

	we, err := cc.client.SignalWithStartWorkflow(ctx, workflowID, signalName, signalArg, options, workflow, workflowArgs...)
	if err != nil {
		l.Error("failed to signal with start workflow", "error", err.Error())
		return nil, errors.WrapError(err, "failed to signal with start workflow")
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
		return errors.WrapError(err, "failed to cancel workflow")
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
		return nil, fmt.Errorf("TemporalClient:QueryWorkflow - error getting logger from context: %w", err)
	}

	resp, err := cc.client.QueryWorkflow(ctx, workflowID, runID, queryType, args...)
	if err != nil {
		l.Error("failed to query workflow", "error", err.Error())
		return nil, errors.WrapError(err, "failed to query workflow")
	}
	var result any
	if err := resp.Get(&result); err != nil {
		l.Error("failed to decode query result", "error", err.Error())
		return nil, errors.WrapError(err, "failed to decode query result")
	}
	// l.Debug("received query result", "result", result)
	return result, nil
}
