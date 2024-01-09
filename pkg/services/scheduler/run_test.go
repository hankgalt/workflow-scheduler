package scheduler_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/comfforts/logger"

	"github.com/hankgalt/workflow-scheduler/pkg/models"
	"github.com/hankgalt/workflow-scheduler/pkg/services/scheduler"
)

const TEST_DIR = "data"
const CADENCE_CONFIG = "../../../cmd/scheduler/config/development.yaml"

func TestWorkflowCRUD(t *testing.T) {
	setupEnv(t)

	l := logger.NewTestAppZapLogger(TEST_DIR)

	for scenario, fn := range map[string]func(
		t *testing.T,
		ss scheduler.SchedulerService,
	){
		"workflow run CRUD, succeeds":   testWorkflowRunCRUD,
		"workflow run search, succeeds": testWorkflowRunSearch,
	} {
		t.Run(scenario, func(t *testing.T) {
			ss, teardown := setupAPITests(t, l)
			defer teardown()
			fn(t, ss)
		})
	}

	err := os.RemoveAll(TEST_DIR)
	require.NoError(t, err)
}

func setupEnv(t *testing.T) {
	t.Helper()

	cPath := os.Getenv("CERTS_PATH")
	if cPath != "" && !strings.Contains(cPath, "../../../") {
		cPath = fmt.Sprintf("../%s", cPath)
	}
	pPath := os.Getenv("POLICY_PATH")
	if pPath != "" && !strings.Contains(pPath, "../../../") {
		pPath = fmt.Sprintf("../%s", pPath)
	}

	os.Setenv("CERTS_PATH", cPath)
	os.Setenv("POLICY_PATH", pPath)
}

func setupAPITests(t *testing.T, l *zap.Logger) (
	ss scheduler.SchedulerService,
	teardown func(),
) {
	t.Helper()

	serviceCfg, err := scheduler.NewServiceConfig("localhost", "", "", "", CADENCE_CONFIG, false)
	require.NoError(t, err)

	ss, err = scheduler.NewSchedulerService(serviceCfg, l)
	require.NoError(t, err)

	return ss, func() {
		t.Logf(" %s ended, will clean up resources", t.Name())
		err = ss.Close()
		require.NoError(t, err)

		// err = os.RemoveAll(TEST_DIR)
		// require.NoError(t, err)
	}
}

func testWorkflowRunCRUD(t *testing.T, ss scheduler.SchedulerService) {
	t.Helper()

	ctx, cancel1 := context.WithCancel(context.Background())
	defer cancel1()

	requester := "test-create-run@gmail.com"
	wfRun, err := ss.CreateRun(ctx, &models.RunParams{
		WorkflowId:  "T3s73k7l0w",
		RunId:       "T3s7Ru41d",
		RequestedBy: requester,
	})
	require.NoError(t, err)
	require.Equal(t, wfRun.Status, string(models.STARTED))

	wfRun, err = ss.UpdateRun(ctx, &models.RunParams{
		WorkflowId:  wfRun.WorkflowId,
		RunId:       wfRun.RunId,
		Status:      string(models.UPLOADED),
		RequestedBy: requester,
	})
	require.NoError(t, err)
	require.Equal(t, wfRun.Status, string(models.UPLOADED))

	err = ss.DeleteRun(ctx, wfRun.RunId)
	require.NoError(t, err)
}

func testWorkflowRunSearch(t *testing.T, ss scheduler.SchedulerService) {
	t.Helper()

	ctx, cancel1 := context.WithCancel(context.Background())
	defer cancel1()

	requester := "test-search-run@gmail.com"
	wfRun, err := ss.CreateRun(ctx, &models.RunParams{
		WorkflowId:  "T3s73k7l0w",
		RunId:       "T3s7Ru41d",
		RequestedBy: requester,
	})
	require.NoError(t, err)
	require.Equal(t, wfRun.Status, string(models.STARTED))

	runType := "fileSignalWorkflow"
	fileWfRun, err := ss.CreateRun(ctx, &models.RunParams{
		WorkflowId:  "N3s73k7l0w",
		RunId:       "N3s7Ru41d",
		RequestedBy: requester,
		Type:        runType,
	})
	require.NoError(t, err)
	require.Equal(t, wfRun.Status, string(models.STARTED))

	runs, err := ss.SearchRuns(ctx, &models.RunParams{
		WorkflowId: wfRun.WorkflowId,
	})
	require.NoError(t, err)
	require.Equal(t, len(runs), 1)

	runs, err = ss.SearchRuns(ctx, &models.RunParams{
		Type: runType,
	})
	require.NoError(t, err)
	require.Equal(t, len(runs), 1)

	err = ss.DeleteRun(ctx, wfRun.RunId)
	require.NoError(t, err)

	err = ss.DeleteRun(ctx, fileWfRun.RunId)
	require.NoError(t, err)
}
