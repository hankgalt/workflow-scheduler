package scheduler

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"go.temporal.io/sdk/client"

	"github.com/comfforts/logger"

	"github.com/hankgalt/batch-orchestra/pkg/domain"
	"github.com/hankgalt/workflow-scheduler/internal/domain/batch"
	"github.com/hankgalt/workflow-scheduler/internal/domain/infra"
	"github.com/hankgalt/workflow-scheduler/internal/domain/stores"
	"github.com/hankgalt/workflow-scheduler/internal/infra/mongostore"
	"github.com/hankgalt/workflow-scheduler/internal/infra/temporal"
	"github.com/hankgalt/workflow-scheduler/internal/repo/daud"
	btchwkfl "github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch"
	bsinks "github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch/sinks"
	"github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch/sources"
	btchutils "github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch/utils"
)

type SchedulerService interface {
	ProcessLocalCSVToMongoWorkflow(ctx context.Context, req batch.LocalCSVMongoBatchRequest) (*stores.WorkflowRun, error)
	ProcessCloudCSVToMongoWorkflow(ctx context.Context, req batch.CloudCSVMongoBatchRequest) (*stores.WorkflowRun, error)
	CreateRun(ctx context.Context, params *stores.WorkflowRun) (*stores.WorkflowRun, error)
	GetRun(ctx context.Context, runId string) (*stores.WorkflowRun, error)
	DeleteRun(ctx context.Context, runId string) error
	Close(ctx context.Context) error
}

type schedulerServiceConfig struct {
	TemporalConfig temporal.TemporalConfig
	MongoConfig    infra.StoreConfig
}

func NewSchedulerServiceConfig(temporalCfg temporal.TemporalConfig, mongoCfg infra.StoreConfig) schedulerServiceConfig {
	return schedulerServiceConfig{
		TemporalConfig: temporalCfg,
		MongoConfig:    mongoCfg,
	}
}

type schedulerService struct {
	temporal *temporal.TemporalClient
	daud     daud.DaudRepo
}

// NewSchedulerService initializes a new SchedulerService instance
func NewSchedulerService(ctx context.Context, cfg schedulerServiceConfig) (*schedulerService, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("SchedulerService:NewSchedulerService - error getting logger from context: %w", err)
	}

	// Create a new Temporal client
	tc, err := temporal.NewTemporalClient(ctx, cfg.TemporalConfig)
	if err != nil {
		l.Error("error creating temporal client", "error", err.Error())
		return nil, err
	}
	bs := &schedulerService{
		temporal: tc,
	}

	// Initialize MongoDB store for daud repository
	dms, err := mongostore.NewMongoStore(ctx, cfg.MongoConfig)
	if err != nil {
		l.Error("error getting MongoDB store", "error", err.Error())

		// Close the temporal client before returning
		if tErr := tc.Close(ctx); tErr != nil {
			l.Error("error closing temporal client", "error", tErr.Error())
			err = errors.Join(err, tErr)
		}
		return nil, err
	}

	// Initialize Daud repository
	dr, err := daud.NewDaudRepo(ctx, dms)
	if err != nil {
		l.Error("error creating Daud repository", "error", err.Error())

		// Close the temporal client before returning
		if tErr := tc.Close(ctx); tErr != nil {
			l.Error("error closing temporal client", "error", tErr.Error())
			err = errors.Join(err, tErr)
		}

		// Close the daud MongoDB store before returning
		if dErr := dms.Close(ctx); dErr != nil {
			l.Error("error closing daud MongoDB store", "error", dErr.Error())
			err = errors.Join(err, dErr)
		}

		return nil, err
	}
	bs.daud = dr

	return bs, nil
}

// Workflows

func (bs *schedulerService) ProcessCloudCSVToMongoWorkflow(ctx context.Context, req batch.CloudCSVMongoBatchRequest) (*stores.WorkflowRun, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("SchedulerService:ProcessCloudCSVToMongoWorkflow - error getting logger from context: %w", err)
	}

	runId, err := btchutils.GenerateRunID(req.Config)
	if err != nil {
		l.Error("error generating run ID", "error", err.Error())
		return nil, err
	}

	workflowOptions := client.StartWorkflowOptions{
		ID:                       fmt.Sprintf("cloud-csv-mongo-%s", runId),
		TaskQueue:                btchwkfl.ApplicationName,
		WorkflowExecutionTimeout: time.Duration(24 * time.Hour),
		WorkflowTaskTimeout:      time.Minute * 5, // set to max, as there are decision tasks that'll take as long as max
		WorkflowIDReusePolicy:    1,
	}

	// Build source & sink configurations
	// Source - local CSV
	fCfg := req.Config.CloudCSVBatchConfig
	path := filepath.Join(fCfg.Path, fCfg.Name)
	sourceCfg := sources.CloudCSVConfig{
		Path:         path,
		Bucket:       fCfg.Bucket,
		Provider:     string(sources.CloudSourceGCS),
		Delimiter:    '|',
		HasHeader:    true,
		MappingRules: req.RequestConfig.MappingRules,
	}

	// Sink - MongoDB
	mdbCfg := req.Config.MongoBatchConfig
	sinkCfg := bsinks.MongoSinkConfig[domain.CSVRow]{
		Protocol:   mdbCfg.Protocol,
		Host:       mdbCfg.Host,
		DBName:     mdbCfg.Name,
		User:       mdbCfg.User,
		Pwd:        mdbCfg.Pwd,
		Params:     mdbCfg.Params,
		Collection: req.Config.Collection,
	}

	batReq := &btchwkfl.CloudCSVMongoBatchRequest{
		JobID:      fmt.Sprintf("cloud-csv-mongo-%s", runId),
		BatchSize:  req.CSVBatchRequest.BatchSize,
		MaxBatches: req.CSVBatchRequest.MaxBatches,
		Source:     sourceCfg,
		Sink:       sinkCfg,
	}

	// we, err := bs.temporal.StartWorkflowWithCtx(ctx, workflowOptions, btchwkfl.ProcessCloudCSVMongo, &req)
	we, err := bs.temporal.StartWorkflowWithCtx(ctx, workflowOptions, btchwkfl.ProcessCloudCSVMongoWorkflowAlias, &batReq)
	if err != nil {
		l.Error("error starting workflow", "error", err.Error())
		return nil, err
	}

	// TODO - add logic to create workflow run db record
	// if workflow starts successfully, there should be a record created in the database
	// handle failure, retries, etc.

	return &stores.WorkflowRun{
		RunId:      we.GetRunID(),
		WorkflowId: we.GetID(),
	}, nil

}

func (bs *schedulerService) ProcessLocalCSVToMongoWorkflow(ctx context.Context, req batch.LocalCSVMongoBatchRequest) (*stores.WorkflowRun, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("SchedulerService:ProcessLocalCSVToMongoWorkflow - error getting logger from context: %w", err)
	}

	runId, err := btchutils.GenerateRunID(req.Config)
	if err != nil {
		l.Error("error generating run ID", "error", err.Error())
		return nil, err
	}

	workflowOptions := client.StartWorkflowOptions{
		ID:                       fmt.Sprintf("local-csv-mongo-%s", runId),
		TaskQueue:                btchwkfl.ApplicationName,
		WorkflowExecutionTimeout: time.Duration(24 * time.Hour),
		WorkflowTaskTimeout:      time.Minute * 5, // set to max, as there are decision tasks that'll take as long as max
		WorkflowIDReusePolicy:    1,
	}

	// Build source & sink configurations
	// Source - local CSV
	fCfg := req.Config.LocalCSVBatchConfig
	path := filepath.Join(fCfg.Path, fCfg.Name)
	sourceCfg := sources.LocalCSVConfig{
		Path:         path,
		Delimiter:    '|',
		HasHeader:    true,
		MappingRules: req.RequestConfig.MappingRules,
	}
	// Sink - MongoDB
	mdbCfg := req.Config.MongoBatchConfig
	sinkCfg := bsinks.MongoSinkConfig[domain.CSVRow]{
		Protocol:   mdbCfg.Protocol,
		Host:       mdbCfg.Host,
		DBName:     mdbCfg.Name,
		User:       mdbCfg.User,
		Pwd:        mdbCfg.Pwd,
		Params:     mdbCfg.Params,
		Collection: req.Config.Collection,
	}

	batReq := &btchwkfl.LocalCSVMongoBatchRequest{
		JobID:      fmt.Sprintf("local-csv-mongo-%s", runId),
		BatchSize:  req.CSVBatchRequest.BatchSize,
		MaxBatches: req.CSVBatchRequest.MaxBatches,
		Source:     sourceCfg,
		Sink:       sinkCfg,
	}

	// we, err := bs.temporal.StartWorkflowWithCtx(ctx, workflowOptions, btchwkfl.ProcessLocalCSVMongo, &req)
	we, err := bs.temporal.StartWorkflowWithCtx(ctx, workflowOptions, btchwkfl.ProcessLocalCSVMongoWorkflowAlias, &batReq)
	if err != nil {
		l.Error("SchedulerService:ProcessLocalCSVToMongoWorkflow - error starting workflow", "error", err.Error())
		return nil, err
	}

	// TODO - add logic to create workflow run db record
	// if workflow starts successfully, there should be a record created in the database
	// handle failure, retries, etc.

	return &stores.WorkflowRun{
		RunId:      we.GetRunID(),
		WorkflowId: we.GetID(),
	}, nil
}

func (ss *schedulerService) QueryWorkflowState(ctx context.Context, params *batch.WorkflowQueryParams) (any, error) {
	if params.WorkflowId == "" || params.RunId == "" {
		return nil, errors.New("missing required workflowId or runId")
	}

	if state, err := ss.temporal.QueryWorkflow(ctx, params.WorkflowId, params.RunId, "state"); err != nil {
		return nil, err
	} else {
		return state, nil
	}
}

// Workflow Runs

func (bs *schedulerService) CreateRun(ctx context.Context, params *stores.WorkflowRun) (*stores.WorkflowRun, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("SchedulerService:CreateRun - error getting logger from context: %w", err)
	}

	runId, err := bs.daud.CreateRun(ctx, params)
	if err != nil {
		l.Error("error creating workflow run record", "error", err.Error(), "params", params)
		return nil, err
	}

	wkflRun, err := bs.daud.GetRunById(ctx, runId)
	if err != nil {
		l.Error("error getting workflow run by ID", "error", err.Error(), "runId", runId)
		return nil, err
	}

	return wkflRun, nil
}

func (bs *schedulerService) GetRun(ctx context.Context, runId string) (*stores.WorkflowRun, error) {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("SchedulerService:GetRun - error getting logger from context: %w", err)
	}

	wkflRun, err := bs.daud.GetRun(ctx, runId)
	if err != nil {
		l.Error("error getting workflow run", "error", err.Error(), "runId", runId)
		return nil, err
	}

	return wkflRun, nil
}

func (bs *schedulerService) DeleteRun(ctx context.Context, runId string) error {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return fmt.Errorf("SchedulerService:DeleteRun - error getting logger from context: %w", err)
	}

	if err := bs.daud.DeleteRun(ctx, runId); err != nil {
		l.Error("error deleting workflow run", "error", err.Error(), "runId", runId)
		return err
	}

	return nil
}

// Close closes scheduler upstream client connections
func (bs *schedulerService) Close(ctx context.Context) error {
	l, err := logger.LoggerFromContext(ctx)
	if err != nil {
		return fmt.Errorf("SchedulerService:Close - error getting logger from context: %w", err)
	}

	err = bs.temporal.Close(ctx)
	if err != nil {
		l.Error("error closing temporal client", "error", err.Error())
		return err
	}

	if err := bs.daud.Close(ctx); err != nil {
		l.Error("error closing Daud repository connection", "error", err.Error())
		return err
	}

	return nil
}
