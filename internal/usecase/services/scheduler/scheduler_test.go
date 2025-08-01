package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hankgalt/workflow-scheduler/internal/domain/batch"
	"github.com/hankgalt/workflow-scheduler/internal/usecase/services/scheduler"
	envutils "github.com/hankgalt/workflow-scheduler/pkg/utils/env"
	"github.com/hankgalt/workflow-scheduler/pkg/utils/logger"
)

func TestProcessLocalCSVToMongoWorkflow(t *testing.T) {
	// Initialize logger
	l := logger.GetSlogLogger()
	l.Info("SchedulerService - TestProcessLocalCSVToMongoWorkflow initialized logger")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	ss, err := scheduler.NewSchedulerService(ctx)
	require.NoError(t, err)
	defer func() {
		err := ss.Close(ctx)
		require.NoError(t, err)
	}()

	reqCfg, err := envutils.BuildLocalCSVMongoBatchConfig()
	require.NoError(t, err)

	req := batch.LocalCSVMongoBatchRequest{
		CSVBatchRequest: batch.CSVBatchRequest{
			RequestConfig: &batch.RequestConfig{
				MaxBatches: 2,
				BatchSize:  400,
				Offsets:    []uint64{},
				Headers:    []string{},
			},
		},
		Config: reqCfg,
	}

	we, err := ss.ProcessLocalCSVToMongoWorkflow(ctx, req)
	require.NoError(t, err)

	l.Info("SchedulerService - TestProcessLocalCSVToMongoWorkflow started workflow successfully", "workflow-run", we)
}
