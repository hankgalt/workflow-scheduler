package scheduler_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/comfforts/errors"
	"github.com/comfforts/logger"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	api "github.com/hankgalt/workflow-scheduler/api/v1"
	schclient "github.com/hankgalt/workflow-scheduler/pkg/clients/scheduler"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
	"github.com/hankgalt/workflow-scheduler/pkg/services/scheduler"
)

const DATA_PATH string = "scheduler"

func TestSchedulerServiceWorkflows(t *testing.T) {
	setupEnv(t)

	logger := logger.NewTestAppZapLogger(TEST_DIR)

	for scenario, fn := range map[string]func(
		t *testing.T,
		logger *zap.Logger,
		ss scheduler.SchedulerService,
	){
		"process file signal workflow run, succeeds": testProcessFileSignalWorkflowRun,
		// "get completed workflow run, succeeds": testGetCompletedRun,
	} {
		t.Run(scenario, func(t *testing.T) {
			ss, teardown := setupAPITests(t, logger)
			defer teardown()
			fn(t, logger, ss)
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
	crPath := os.Getenv("CREDS_PATH")
	if crPath != "" && !strings.Contains(crPath, "../../../") {
		crPath = fmt.Sprintf("../%s", crPath)
	}
	os.Setenv("CERTS_PATH", cPath)
	os.Setenv("POLICY_PATH", pPath)
	os.Setenv("CREDS_PATH", crPath)
}

func testProcessFileSignalWorkflowRun(t *testing.T, l *zap.Logger, ss scheduler.SchedulerService) {
	t.Helper()

	var runId string
	defer func() {
		if err := recover(); err != nil {
			l.Debug("testProcessFileSignalWorkflowRun panicked", zap.Any("error", err))
		}

		l.Debug("testProcessFileSignalWorkflowRun run status", zap.String("run-id", runId))

		if runId != "" {
			bOpts := schclient.NewDefaultClientOption()
			bOpts.Caller = "TestSchedulerServiceWorkflows"
			schClient, err := schclient.NewClient(l, bOpts)
			require.NoError(t, err)

			run, err := getCompletedRun(schClient, runId)
			require.NoError(t, err)
			l.Debug("testProcessFileSignalWorkflowRun - run details", zap.Any("status", run.Status), zap.Any("run-id", run.RunId))
			require.Equal(t, string(models.COMPLETED), run.Status)

			err = deleteRun(schClient, runId)
			require.NoError(t, err)

			err = schClient.Close()
			require.NoError(t, err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reqstr := "process-file-signal-workflow-test@gmail.com"
	filePath := fmt.Sprintf("%s/%s", DATA_PATH, "Agents-sm.csv")
	pRun, err := ss.ProcessFileSignalWorkflow(ctx, &models.FileSignalParams{
		FilePath:    filePath,
		RequestedBy: reqstr,
		Type:        models.AGENT,
	})
	require.NoError(t, err)
	l.Debug("testProcessFileSignalWorkflowRun run status", zap.Any("run", pRun))
	runId = pRun.RunId

	for i := 0; i < 10; i++ {
		time.Sleep(200 * time.Millisecond)
		if state, err := ss.QueryWorkflowState(ctx, &models.WorkflowQueryParams{
			RunId:      runId,
			WorkflowId: pRun.WorkflowId,
		}); err != nil {
			l.Debug("testProcessFileSignalWorkflowRun run state error", zap.Error(err))
		} else {
			if runState, ok := state.(models.JSONMapper); ok {
				fmt.Println()
				l.Debug("testProcessFileSignalWorkflowRun run state", zap.Any("runState", runState))
				fmt.Println()
				cRunId, _ := runState["ProcessRunId"].(string)
				cWkFlId, ok := runState["ProcessWorkflowId"].(string)
				if ok && cRunId != "" && cWkFlId != "" {
					if childState, err := ss.QueryWorkflowState(ctx, &models.WorkflowQueryParams{
						RunId:      cRunId,
						WorkflowId: cWkFlId,
					}); err != nil {
						l.Debug("testProcessFileSignalWorkflowRun child run state error", zap.Error(err))
					} else {
						fmt.Println()
						l.Debug("testProcessFileSignalWorkflowRun child run state", zap.Any("childState", childState))
						fmt.Println()
					}
				}
			}
		}
	}
}

func testGetCompletedRun(t *testing.T, l *zap.Logger, ss scheduler.SchedulerService) {
	bOpts := schclient.NewDefaultClientOption()
	bOpts.Caller = "TestSchedulerServiceWorkflows"
	schClient, err := schclient.NewClient(l, bOpts)
	require.NoError(t, err)

	run, err := getCompletedRun(schClient, "c9ee71f2-434d-44e3-a40d-10aa71ba996f")
	require.NoError(t, err)
	l.Info("testGetCompletedRun - run details", zap.Any("status", run.Status), zap.Any("run-id", run.RunId))

	err = schClient.Close()
	require.NoError(t, err)
}

func deleteRun(bCl schclient.Client, runId string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, err := bCl.DeleteRun(ctx, &api.DeleteRunRequest{
		Id: runId,
	})
	if err != nil {
		return err
	}
	return nil
}

func getCompletedRun(bCl schclient.Client, runId string) (*api.WorkflowRun, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	count := 0
	for count < 30 {
		if count > 0 {
			time.Sleep(200 * time.Millisecond)
		}
		if resp, err := bCl.GetRun(ctx, &api.RunRequest{
			RunId: runId,
		}); err == nil && resp.Run.Status == string(models.COMPLETED) {
			return resp.Run, nil
		}
		count++
	}

	return nil, errors.NewAppError("run not found")
}
