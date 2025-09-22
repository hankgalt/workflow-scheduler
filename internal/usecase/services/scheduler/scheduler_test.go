package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/comfforts/logger"
	"github.com/hankgalt/batch-orchestra/pkg/domain"

	"github.com/hankgalt/workflow-scheduler/internal/domain/batch"
	"github.com/hankgalt/workflow-scheduler/internal/usecase/services/scheduler"
	envutils "github.com/hankgalt/workflow-scheduler/pkg/utils/environment"
)

func TestProcessLocalCSVToMongoWorkflow(t *testing.T) {
	// Initialize logger
	l := logger.GetSlogLogger()
	l.Info("SchedulerService - TestProcessLocalCSVToMongoWorkflow initialized logger")

	mCfg := envutils.BuildMongoStoreConfig(true)
	require.NotEmpty(t, mCfg.Host, "MongoDB host should not be empty")

	tCfg := envutils.BuildTemporalConfig("TestProcessLocalCSVToMongoWorkflow")
	require.NotEmpty(t, tCfg.Host, "Temporal host should not be empty")

	svcCfg := scheduler.NewSchedulerServiceConfig(tCfg, mCfg)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	ss, err := scheduler.NewSchedulerService(ctx, svcCfg)
	require.NoError(t, err)
	defer func() {
		err := ss.Close(ctx)
		require.NoError(t, err)
	}()

	reqCfg, err := envutils.BuildLocalCSVMongoBatchConfig(false)
	require.NoError(t, err)

	req := batch.LocalCSVMongoBatchRequest{
		CSVBatchRequest: batch.CSVBatchRequest{
			RequestConfig: &batch.RequestConfig{
				MaxInProcessBatches: 2,
				BatchSize:           400,
				MappingRules:        domain.BuildBusinessModelTransformRules(),
			},
		},
		Config: reqCfg,
	}

	we, err := ss.ProcessLocalCSVToMongoWorkflow(ctx, req)
	require.NoError(t, err)

	l.Info("SchedulerService - TestProcessLocalCSVToMongoWorkflow started workflow successfully", "workflow-run", we)
}

func TestProcessCloudCSVToMongoWorkflow(t *testing.T) {
	// Initialize logger
	l := logger.GetSlogLogger()
	l.Info("SchedulerService - TestProcessCloudCSVToMongoWorkflow initialized logger")

	mCfg := envutils.BuildMongoStoreConfig(true)
	require.NotEmpty(t, mCfg.Host, "MongoDB host should not be empty")

	tCfg := envutils.BuildTemporalConfig("TestProcessCloudCSVToMongoWorkflow")
	require.NotEmpty(t, tCfg.Host, "Temporal host should not be empty")

	svcCfg := scheduler.NewSchedulerServiceConfig(tCfg, mCfg)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	ss, err := scheduler.NewSchedulerService(ctx, svcCfg)
	require.NoError(t, err)
	defer func() {
		err := ss.Close(ctx)
		require.NoError(t, err)
	}()

	reqCfg, err := envutils.BuildCloudCSVMongoBatchConfig(false)
	require.NoError(t, err)

	req := batch.CloudCSVMongoBatchRequest{
		CSVBatchRequest: batch.CSVBatchRequest{
			RequestConfig: &batch.RequestConfig{
				MaxInProcessBatches: 2,
				BatchSize:           400,
				MappingRules:        domain.BuildBusinessModelTransformRules(),
			},
		},
		Config: reqCfg,
	}

	we, err := ss.ProcessCloudCSVToMongoWorkflow(ctx, req)
	require.NoError(t, err)

	l.Info("SchedulerService - TestProcessCloudCSVToMongoWorkflow started workflow successfully", "workflow-run", we)
}

func TestQueryWorkflowState(t *testing.T) {
	// Initialize logger
	l := logger.GetSlogLogger()
	l.Info("SchedulerService - TestQueryWorkflowState initialized logger")

	mCfg := envutils.BuildMongoStoreConfig(true)
	require.NotEmpty(t, mCfg.Host, "MongoDB host should not be empty")

	tCfg := envutils.BuildTemporalConfig("TestQueryWorkflowState")
	require.NotEmpty(t, tCfg.Host, "Temporal host should not be empty")

	svcCfg := scheduler.NewSchedulerServiceConfig(tCfg, mCfg)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	ss, err := scheduler.NewSchedulerService(ctx, svcCfg)
	require.NoError(t, err)
	defer func() {
		err := ss.Close(ctx)
		require.NoError(t, err)
	}()

	// Simulate querying a workflow state
	workflowID := "local-csv-mongo-data-scheduler-Agents.csv-0"
	state, err := ss.QueryWorkflowState(ctx, &batch.WorkflowQueryParams{
		WorkflowId: workflowID,
		RunId:      "eebc25ad-5760-4048-a1dc-20631bc61788",
	})
	require.NoError(t, err)
	st, ok := state.(map[string]any)
	require.True(t, ok, "expected state to be of type map[string]any")
	l.Info("SchedulerService - TestQueryWorkflowState retrieved workflow state", "workflow-id", workflowID, "state", st)
}
