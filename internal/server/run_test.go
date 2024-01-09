package server_test

import (
	"context"
	"os"
	"testing"

	api "github.com/hankgalt/workflow-scheduler/api/v1"
	"github.com/hankgalt/workflow-scheduler/internal/server"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
	"github.com/stretchr/testify/require"
)

func TestWorkflowRuns(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client api.SchedulerClient,
		nbClient api.SchedulerClient,
		config *server.Config,
	){
		"test workflow run CRUD, succeeds":   testWorkflowCRUD,
		"test workflow run search, succeeds": testWorkflowSearch,
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

func testWorkflowCRUD(t *testing.T, client, nbClient api.SchedulerClient, config *server.Config) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	requester := "test-create-run@gmail.com"
	wfRun, err := client.CreateRun(ctx, &api.RunRequest{
		WorkflowId:  "C3r43r-T3s73k7l0w",
		RunId:       "C3r43r-T3s7Ru41d",
		RequestedBy: requester,
	})
	require.NoError(t, err)
	require.Equal(t, wfRun.Run.Status, string(models.STARTED))

	wfRun, err = client.UpdateRun(ctx, &api.UpdateRunRequest{
		WorkflowId: wfRun.Run.WorkflowId,
		RunId:      wfRun.Run.RunId,
		Status:     string(models.UPLOADED),
	})
	require.NoError(t, err)
	require.Equal(t, wfRun.Run.Status, string(models.UPLOADED))

	resp, err := client.DeleteRun(ctx, &api.DeleteRunRequest{
		Id: wfRun.Run.RunId,
	})
	require.NoError(t, err)
	require.Equal(t, resp.Ok, true)
}

func testWorkflowSearch(t *testing.T, client, nbClient api.SchedulerClient, config *server.Config) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	requester := "test-search-run@gmail.com"
	wfRun, err := client.CreateRun(ctx, &api.RunRequest{
		WorkflowId:  "S3r43r-T3s73k7l0w",
		RunId:       "S3r43r-T3s7Ru41d",
		RequestedBy: requester,
	})
	require.NoError(t, err)
	require.Equal(t, wfRun.Run.Status, string(models.STARTED))

	sResp, err := client.SearchRuns(ctx, &api.SearchRunRequest{
		WorkflowId: wfRun.Run.WorkflowId,
	})
	require.NoError(t, err)
	require.Equal(t, sResp.Ok, true)
	require.Equal(t, len(sResp.Runs), 1)

	wkflType := "fileSignalWorkflow"
	fWfRun, err := client.CreateRun(ctx, &api.RunRequest{
		WorkflowId:  "FS3r43r-T3s73k7l0w",
		RunId:       "FS3r43r-T3s7Ru41d",
		RequestedBy: requester,
		Type:        wkflType,
	})
	require.NoError(t, err)
	require.Equal(t, wfRun.Run.Status, string(models.STARTED))

	sResp, err = client.SearchRuns(ctx, &api.SearchRunRequest{
		Type: wkflType,
	})
	require.NoError(t, err)
	require.Equal(t, sResp.Ok, true)
	require.Equal(t, len(sResp.Runs), 1)

	resp, err := client.DeleteRun(ctx, &api.DeleteRunRequest{
		Id: wfRun.Run.RunId,
	})
	require.NoError(t, err)
	require.Equal(t, resp.Ok, true)

	resp, err = client.DeleteRun(ctx, &api.DeleteRunRequest{
		Id: fWfRun.Run.RunId,
	})
	require.NoError(t, err)
	require.Equal(t, resp.Ok, true)
}
