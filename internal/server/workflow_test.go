package server_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	api "github.com/hankgalt/workflow-scheduler/api/v1"
	"github.com/hankgalt/workflow-scheduler/internal/server"
	"github.com/stretchr/testify/require"
)

const DATA_PATH string = "scheduler"

func TestSchedulerServiceWorkflows(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client api.SchedulerClient,
		nbClient api.SchedulerClient,
		config *server.Config,
	){
		// "process file signal workflow run, succeeds": testProcessFileSignalWorkflowRun,
		"query file signal workflow run, succeeds": testQueryWorkflowState,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, nbClient, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, nbClient, config)
		})

		err := os.RemoveAll(TEST_DIR)
		require.NoError(t, err)
	}
}

func testProcessFileSignalWorkflowRun(t *testing.T, client, nbClient api.SchedulerClient, config *server.Config) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reqstr := "process-file-signal-workflow-server-test@test.com"
	filePath := fmt.Sprintf("%s/%s", DATA_PATH, "Agents-sm.csv")
	wfRun, err := client.ProcessFileSignalWorkflow(ctx, &api.FileSignalRequest{
		FilePath:    filePath,
		Type:        api.EntityType_AGENT,
		RequestedBy: reqstr,
	})

	require.NoError(t, err)
	require.Equal(t, "file-scheduler/Agents-sm.csv", wfRun.Run.WorkflowId)
}

func testQueryWorkflowState(t *testing.T, client, nbClient api.SchedulerClient, config *server.Config) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runId := "c6f63e6a-7cfe-4fa5-a6a6-94282bea143e"
	wkflId := "file-scheduler/Agents-sm.csv"
	wfState, err := client.QueryFileWorkflowState(ctx, &api.QueryWorkflowRequest{
		RunId:      runId,
		WorkflowId: wkflId,
	})
	require.NoError(t, err)
	require.Equal(t, runId, wfState.State.RunId)
	require.Equal(t, wkflId, wfState.State.WorkflowId)
}
