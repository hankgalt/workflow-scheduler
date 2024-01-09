package scheduler_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/comfforts/logger"

	api "github.com/hankgalt/workflow-scheduler/api/v1"
	"github.com/hankgalt/workflow-scheduler/pkg/clients/scheduler"
)

const TEST_DIR = "data"

func TestSchedulerClient(t *testing.T) {
	l := logger.NewTestAppLogger(TEST_DIR)

	cPath := os.Getenv("CERTS_PATH")
	if cPath != "" && !strings.Contains(cPath, "../../../") {
		cPath = fmt.Sprintf("../%s", cPath)
	}
	os.Setenv("CERTS_PATH", cPath)

	for scenario, fn := range map[string]func(
		t *testing.T,
		sc scheduler.Client,
	){
		"test workflow CRUD, succeeds":   testWorkflowCRUD,
		"test workflow search, succeeds": testWorkflowSearch,
	} {
		t.Run(scenario, func(t *testing.T) {
			sc, teardown := setup(t, l)
			defer teardown()
			fn(t, sc)
		})
	}
}

func setup(t *testing.T, l logger.AppLogger) (
	sc scheduler.Client,
	teardown func(),
) {
	t.Helper()

	clientOpts := scheduler.NewDefaultClientOption()
	clientOpts.Caller = "scheduler-client-test"

	sc, err := scheduler.NewClient(l, clientOpts)
	require.NoError(t, err)

	return sc, func() {
		t.Logf(" %s ended, will close deliveries client", t.Name())
		err := sc.Close()
		require.NoError(t, err)
	}
}

func testWorkflowCRUD(t *testing.T, sc scheduler.Client) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	requester := "test-create-run@gmail.com"
	wfRun, err := sc.CreateRun(ctx, &api.RunRequest{
		WorkflowId:  "S3r43r-T3s73k7l0w",
		RunId:       "S3r43r-T3s7Ru41d",
		RequestedBy: requester,
	})
	require.NoError(t, err)
	require.Equal(t, wfRun.Run.Status, "STARTED")

	wfRun, err = sc.UpdateRun(ctx, &api.UpdateRunRequest{
		WorkflowId: wfRun.Run.WorkflowId,
		RunId:      wfRun.Run.RunId,
		Status:     "UPLOADED",
	})
	require.NoError(t, err)
	require.Equal(t, wfRun.Run.Status, string("UPLOADED"))

	wfRun, err = sc.GetRun(ctx, &api.RunRequest{
		RunId: wfRun.Run.RunId,
	})
	require.NoError(t, err)
	require.Equal(t, wfRun.Run.Status, string("UPLOADED"))

	resp, err := sc.DeleteRun(ctx, &api.DeleteRunRequest{
		Id: wfRun.Run.RunId,
	})
	require.NoError(t, err)
	require.Equal(t, resp.Ok, true)
}

func testWorkflowSearch(t *testing.T, sc scheduler.Client) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	requester := "test-search-run@gmail.com"
	wfRun, err := sc.CreateRun(ctx, &api.RunRequest{
		WorkflowId:  "S3r43r-T3s73k7l0w",
		RunId:       "S3r43r-T3s7Ru41d",
		RequestedBy: requester,
	})
	require.NoError(t, err)
	require.Equal(t, wfRun.Run.Status, "STARTED")

	rsResp, err := sc.SearchRuns(ctx, &api.SearchRunRequest{
		WorkflowId: wfRun.Run.WorkflowId,
	})
	require.NoError(t, err)
	require.Equal(t, len(rsResp.Runs), 1)

	runType := "fileSignalWorkflow"
	rResp, err := sc.CreateRun(ctx, &api.RunRequest{
		WorkflowId:  "N3s73k7l0w",
		RunId:       "N3s7Ru41d",
		RequestedBy: requester,
		Type:        runType,
	})
	require.NoError(t, err)

	rsResp, err = sc.SearchRuns(ctx, &api.SearchRunRequest{
		Type: runType,
	})
	require.NoError(t, err)
	require.Equal(t, len(rsResp.Runs), 1)

	resp, err := sc.DeleteRun(ctx, &api.DeleteRunRequest{
		Id: wfRun.Run.RunId,
	})
	require.NoError(t, err)
	require.Equal(t, resp.Ok, true)

	resp, err = sc.DeleteRun(ctx, &api.DeleteRunRequest{
		Id: rResp.Run.RunId,
	})
	require.NoError(t, err)
	require.Equal(t, resp.Ok, true)
}
