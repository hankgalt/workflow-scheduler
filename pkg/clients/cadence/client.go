package cadence

import (
	"context"
	"os"

	"github.com/opentracing/opentracing-go"
	"github.com/uber-go/tally/prometheus"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/client"
	"go.uber.org/cadence/encoded"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"

	"github.com/comfforts/errors"

	lprom "github.com/hankgalt/workflow-scheduler/pkg/clients/prometheus"
)

const (
	defaultConfigFile = "config/development.yaml"
)

type Configuration struct {
	DomainName      string                    `yaml:"domain"`
	ServiceName     string                    `yaml:"service"`
	HostNameAndPort string                    `yaml:"host"`
	Prometheus      *prometheus.Configuration `yaml:"prometheus"`
}

type registryOption struct {
	registry interface{}
	alias    string
}

type CadenceClient struct {
	logger *zap.Logger
	// client is the client used for cadence
	client             client.Client
	workflowRegistries []registryOption
	activityRegistries []registryOption
	Tracer             opentracing.Tracer
	DataConverter      encoded.DataConverter
	CtxPropagators     []workflow.ContextPropagator
}

func NewCadenceClient(cfgPath string, logger *zap.Logger) (*CadenceClient, error) {
	if cfgPath == "" {
		cfgPath = os.Getenv("CADENCE_CONFIG_PATH")
	}
	if cfgPath == "" {
		cfgPath = defaultConfigFile
	}

	configData, err := os.ReadFile(cfgPath)
	if err != nil {
		logger.Error("failed to read cadence config file", zap.Error(err), zap.String("cfgFile", cfgPath))
		return nil, errors.WrapError(err, "failed to read cadence config fil")
	}

	var cadenceCfg Configuration
	if err := yaml.Unmarshal(configData, &cadenceCfg); err != nil {
		logger.Error("error initializing cadence configuration", zap.Error(err))
		return nil, errors.WrapError(err, "error initializing cadence configuration")
	}

	cadenceCl := CadenceClient{
		logger: logger,
	}

	reporter, err := lprom.NewPrometheusReporter("127.0.0.1:9099", logger)
	if err != nil {
		logger.Error("error initializing prometheus reporter", zap.Error(err))
		return nil, errors.WrapError(err, "error initializing prometheus reporter")
	}

	svcMetricsScope := lprom.NewServiceScope(reporter)
	builder := NewBuilder(logger).
		SetHostPort(cadenceCfg.HostNameAndPort).
		SetDomain(cadenceCfg.DomainName).
		SetMetricsScope(svcMetricsScope).
		SetDataConverter(cadenceCl.DataConverter).
		SetTracer(cadenceCl.Tracer).
		SetContextPropagators(cadenceCl.CtxPropagators)

	client, err := builder.BuildCadenceClient()
	if err != nil {
		logger.Error("error initializing cadence service client", zap.Error(err))
		return nil, errors.WrapError(err, "error initializing cadence service client")
	}
	cadenceCl.client = client

	return &cadenceCl, nil
}

// RegisterWorkflow registers a workflow
func (cc *CadenceClient) RegisterWorkflow(workflow interface{}) {
	cc.RegisterWorkflowWithAlias(workflow, "")
}

// RegisterWorkflowWithAlias registers a workflow with alias
func (cc *CadenceClient) RegisterWorkflowWithAlias(workflow interface{}, alias string) {
	registryOption := registryOption{
		registry: workflow,
		alias:    alias,
	}
	cc.workflowRegistries = append(cc.workflowRegistries, registryOption)
}

// RegisterActivity registers a activity
func (cc *CadenceClient) RegisterActivity(activity interface{}) {
	cc.RegisterActivityWithAlias(activity, "")
}

// RegisterActivityWithAlias registers a activity with alias
func (cc *CadenceClient) RegisterActivityWithAlias(activity interface{}, alias string) {
	registryOption := registryOption{
		registry: activity,
		alias:    alias,
	}
	cc.activityRegistries = append(cc.activityRegistries, registryOption)
}

// StartWorkflow starts a workflow
func (cc *CadenceClient) StartWorkflow(
	options client.StartWorkflowOptions,
	workflow interface{},
	args ...interface{},
) (*workflow.Execution, error) {
	return cc.StartWorkflowWithCtx(context.Background(), options, workflow, args...)
}

// StartWorkflowWithCtx starts a workflow with the provided context
func (cc *CadenceClient) StartWorkflowWithCtx(
	ctx context.Context,
	options client.StartWorkflowOptions,
	workflow interface{},
	args ...interface{},
) (*workflow.Execution, error) {
	we, err := cc.client.StartWorkflow(ctx, options, workflow, args...)
	if err != nil {
		cc.logger.Error("failed to create workflow", zap.Error(err))
		return nil, errors.WrapError(err, "failed to create workflow")
	}

	cc.logger.Info("started Workflow", zap.String("WorkflowID", we.ID), zap.String("RunID", we.RunID))
	return we, nil
}

func (cc *CadenceClient) SignalWorkflow(workflowID, signal string, data interface{}) error {
	err := cc.client.SignalWorkflow(context.Background(), workflowID, "", signal, data)
	if err != nil {
		cc.logger.Error("failed to signal workflow", zap.Error(err))
		return errors.WrapError(err, "failed to signal workflow")
	}
	return nil
}

// SignalWithStartWorkflowWithCtx signals workflow and starts it if it's not yet started
func (cc *CadenceClient) SignalWithStartWorkflowWithCtx(
	ctx context.Context,
	workflowID string,
	signalName string,
	signalArg interface{},
	options client.StartWorkflowOptions,
	workflow interface{},
	workflowArgs ...interface{},
) (*workflow.Execution, error) {
	we, err := cc.client.SignalWithStartWorkflow(ctx, workflowID, signalName, signalArg, options, workflow, workflowArgs...)
	if err != nil {
		cc.logger.Error("failed to signal with start workflow", zap.Error(err))
		return nil, errors.WrapError(err, "failed to signal with start workflow")
	}

	cc.logger.Info("signaled and started Workflow", zap.String("WorkflowID", we.ID), zap.String("RunID", we.RunID))
	return we, nil
}

func (cc *CadenceClient) CancelWorkflow(ctx context.Context, workflowID string) error {
	err := cc.client.CancelWorkflow(ctx, workflowID, "")
	if err != nil {
		cc.logger.Error("failed to cancel workflow", zap.Error(err))
		return errors.WrapError(err, "failed to cancel workflow")
	}
	return nil
}

func (cc *CadenceClient) QueryWorkflow(
	ctx context.Context,
	workflowID, runID, queryType string,
	args ...interface{},
) (interface{}, error) {
	resp, err := cc.client.QueryWorkflow(ctx, workflowID, runID, queryType, args...)
	if err != nil {
		cc.logger.Error("failed to query workflow", zap.Error(err))
		return nil, errors.WrapError(err, "failed to query workflow")
	}
	var result interface{}
	if err := resp.Get(&result); err != nil {
		cc.logger.Error("failed to decode query result", zap.Error(err))
		return nil, errors.WrapError(err, "failed to decode query result")
	}
	// cc.logger.Debug("received query result", zap.Any("result", result))
	return result, nil
}

func (cc *CadenceClient) ConsistentQueryWorkflow(
	ctx context.Context,
	valuePtr interface{},
	workflowID, runID, queryType string,
	args ...interface{},
) (interface{}, error) {
	resp, err := cc.client.QueryWorkflowWithOptions(
		ctx,
		&client.QueryWorkflowWithOptionsRequest{
			WorkflowID:            workflowID,
			RunID:                 runID,
			QueryType:             queryType,
			QueryConsistencyLevel: shared.QueryConsistencyLevelStrong.Ptr(),
			Args:                  args,
		})
	if err != nil {
		cc.logger.Error("failed to query workflow", zap.Error(err))
		return nil, errors.WrapError(err, "failed to query workflow")
	}
	if err := resp.QueryResult.Get(&valuePtr); err != nil {
		cc.logger.Error("failed to decode query result", zap.Error(err))
		return nil, errors.WrapError(err, "failed to decode query result")
	}
	cc.logger.Info("received consistent query result.", zap.Any("Result", valuePtr))
	return valuePtr, nil
}
