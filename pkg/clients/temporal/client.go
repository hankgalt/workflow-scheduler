package temporal

import (
	"context"
	"os"

	"go.temporal.io/sdk/client"
	"go.uber.org/zap"

	"github.com/comfforts/errors"
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
	registry interface{}
	alias    string
}

type TemporalClient struct {
	*zap.Logger
	// client is the client used for temporal
	client             client.Client
	workflowRegistries []registryOption
	activityRegistries []registryOption
}

func NewTemporalClient(l *zap.Logger) (*TemporalClient, error) {
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
		Logger: l,
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
	workflow interface{},
	args ...interface{},
) (client.WorkflowRun, error) {
	return cc.StartWorkflowWithCtx(context.Background(), options, workflow, args...)
}

// StartWorkflowWithCtx starts a workflow with the provided context
func (cc *TemporalClient) StartWorkflowWithCtx(
	ctx context.Context,
	options client.StartWorkflowOptions,
	workflow interface{},
	args ...interface{},
) (client.WorkflowRun, error) {
	we, err := cc.client.ExecuteWorkflow(ctx, options, workflow, args...)
	if err != nil {
		cc.Error("failed to create workflow", zap.Error(err))
		return nil, errors.WrapError(err, "failed to create workflow")
	}

	cc.Info("started Workflow", zap.String("WorkflowID", we.GetID()), zap.String("RunID", we.GetRunID()))
	return we, nil
}

func (cc *TemporalClient) SignalWorkflow(workflowID, signal string, data any) error {
	err := cc.client.SignalWorkflow(context.Background(), workflowID, "", signal, data)
	if err != nil {
		cc.Error("failed to signal workflow", zap.Error(err))
		return errors.WrapError(err, "failed to signal workflow")
	}
	return nil
}

// SignalWithStartWorkflowWithCtx signals workflow and starts it if it's not yet started
func (cc *TemporalClient) SignalWithStartWorkflowWithCtx(
	ctx context.Context,
	workflowID string,
	signalName string,
	signalArg interface{},
	options client.StartWorkflowOptions,
	workflow interface{},
	workflowArgs ...interface{},
) (client.WorkflowRun, error) {
	we, err := cc.client.SignalWithStartWorkflow(ctx, workflowID, signalName, signalArg, options, workflow, workflowArgs...)
	if err != nil {
		cc.Error("failed to signal with start workflow", zap.Error(err))
		return nil, errors.WrapError(err, "failed to signal with start workflow")
	}

	cc.Info("signaled and started Workflow", zap.String("WorkflowID", we.GetID()), zap.String("RunID", we.GetRunID()))
	return we, nil
}

func (cc *TemporalClient) CancelWorkflow(ctx context.Context, workflowID string) error {
	err := cc.client.CancelWorkflow(ctx, workflowID, "")
	if err != nil {
		cc.Error("failed to cancel workflow", zap.Error(err))
		return errors.WrapError(err, "failed to cancel workflow")
	}
	return nil
}

func (cc *TemporalClient) QueryWorkflow(
	ctx context.Context,
	workflowID, runID, queryType string,
	args ...interface{},
) (interface{}, error) {
	resp, err := cc.client.QueryWorkflow(ctx, workflowID, runID, queryType, args...)
	if err != nil {
		cc.Error("failed to query workflow", zap.Error(err))
		return nil, errors.WrapError(err, "failed to query workflow")
	}
	var result interface{}
	if err := resp.Get(&result); err != nil {
		cc.Error("failed to decode query result", zap.Error(err))
		return nil, errors.WrapError(err, "failed to decode query result")
	}
	// cc.logger.Debug("received query result", zap.Any("result", result))
	return result, nil
}
