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

func TestBusinessClient(t *testing.T) {
	logger := logger.NewTestAppLogger(TEST_DIR)

	cPath := os.Getenv("CERTS_PATH")
	if cPath != "" && !strings.Contains(cPath, "../../../") {
		cPath = fmt.Sprintf("../%s", cPath)
	}
	os.Setenv("CERTS_PATH", cPath)

	for scenario, fn := range map[string]func(
		t *testing.T,
		bc scheduler.Client,
	){
		"test workflow CRUD, succeeds": testWorkflowCRUD,
	} {
		t.Run(scenario, func(t *testing.T) {
			bc, teardown := setup(t, logger)
			defer teardown()
			fn(t, bc)
		})
	}
}

func setup(t *testing.T, logger logger.AppLogger) (
	bc scheduler.Client,
	teardown func(),
) {
	t.Helper()

	clientOpts := scheduler.NewDefaultClientOption()
	clientOpts.Caller = "business-client-test"

	dc, err := scheduler.NewClient(logger, clientOpts)
	require.NoError(t, err)

	return dc, func() {
		t.Logf(" %s ended, will close deliveries client", t.Name())
		err := dc.Close()
		require.NoError(t, err)
	}
}

func testWorkflowCRUD(t *testing.T, bc scheduler.Client) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	requester := "test-create-run@gmail.com"
	wfRun, err := bc.CreateRun(ctx, &api.RunRequest{
		WorkflowId:  "S3r43r-T3s73k7l0w",
		RunId:       "S3r43r-T3s7Ru41d",
		RequestedBy: requester,
	})
	require.NoError(t, err)
	require.Equal(t, wfRun.Run.Status, "STARTED")

	wfRun, err = bc.UpdateRun(ctx, &api.UpdateRunRequest{
		WorkflowId: wfRun.Run.WorkflowId,
		RunId:      wfRun.Run.RunId,
		Status:     "UPLOADED",
	})
	require.NoError(t, err)
	require.Equal(t, wfRun.Run.Status, string("UPLOADED"))

	wfRun, err = bc.GetRun(ctx, &api.RunRequest{
		RunId: wfRun.Run.RunId,
	})
	require.NoError(t, err)
	require.Equal(t, wfRun.Run.Status, string("UPLOADED"))

	resp, err := bc.DeleteRun(ctx, &api.DeleteRunRequest{
		Id: wfRun.Run.RunId,
	})
	require.NoError(t, err)
	require.Equal(t, resp.Ok, true)
}
