package daud_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/comfforts/logger"

	"github.com/hankgalt/workflow-scheduler/internal/domain/stores"
	"github.com/hankgalt/workflow-scheduler/internal/infra/mongostore"
	"github.com/hankgalt/workflow-scheduler/internal/repo/daud"
	envutils "github.com/hankgalt/workflow-scheduler/pkg/utils/environment"
)

func TestDaudRepoWorkflowCRUD(t *testing.T) {
	// Initialize logger
	l := logger.GetSlogLogger()
	t.Log("TestVyparRepo Logger initialized")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	// Get MongoDB configuration
	nmCfg := envutils.BuildMongoStoreConfig(true)
	ms, err := mongostore.NewMongoStore(ctx, nmCfg)
	require.NoError(t, err)

	defer func() {
		err := ms.Close(ctx)
		require.NoError(t, err)
	}()

	// Initialize Daud repository
	dr, err := daud.NewDaudRepo(ctx, ms)
	require.NoError(t, err)

	wkflId, runId := "T3s73k7l0w", "T3s7Ru41d"
	wfRun := &stores.WorkflowRun{
		WorkflowId: wkflId,
		RunId:      runId,
	}

	wkflRunId, err := dr.CreateRun(ctx, wfRun)
	require.NoError(t, err)
	require.NotEmpty(t, wkflRunId)
	t.Logf("Workflow run created successfully with ID: %s", wkflRunId)

	// Test getting the workflow run
	// fetch by object ID
	fetchedRun, err := dr.GetRunById(ctx, wkflRunId)
	require.NoError(t, err)
	require.Equal(t, runId, fetchedRun.RunId)
	t.Logf("Workflow run fetched successfully with obj_id: %+v", fetchedRun)

	// fetch by run ID
	fetchedRun, err = dr.GetRun(ctx, runId)
	require.NoError(t, err)
	require.NotNil(t, fetchedRun)
	require.Equal(t, runId, fetchedRun.RunId)
	t.Logf("Workflow run fetched successfully with run_id: %+v", fetchedRun)

	// Test deleting the workflow run
	err = dr.DeleteRun(ctx, runId)
	require.NoError(t, err)
	t.Logf("Workflow run deleted successfully with run_id: %s", runId)
}
