package file_test

import (
	"context"
	"fmt"
	"strings"
	"time"

	api "github.com/hankgalt/workflow-scheduler/api/v1"
	"github.com/hankgalt/workflow-scheduler/pkg/clients/scheduler"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
	"github.com/hankgalt/workflow-scheduler/pkg/workflows/common"
	fiwkfl "github.com/hankgalt/workflow-scheduler/pkg/workflows/file"

	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/encoded"
	"go.uber.org/zap"

	"github.com/comfforts/logger"
)

func (s *FileWorkflowTestSuite) Test_FileSignalWorkflow() {
	l := logger.NewTestAppZapLogger(TEST_DIR)

	expectedCall := []string{
		common.SearchRunActivityName,
		common.CreateRunActivityName,
		common.UpdateRunActivityName,
	}

	var activityCalled []string
	var runId string
	s.env.SetOnActivityStartedListener(func(activityInfo *activity.Info, ctx context.Context, args encoded.Values) {
		activityType := activityInfo.ActivityType.Name
		if strings.HasPrefix(activityType, "internalSession") {
			return
		}
		activityCalled = append(activityCalled, activityType)
		switch activityType {
		case expectedCall[0]:
			// search run
			var input models.RunParams
			s.NoError(args.Get(&input))
			l.Info("Test_FileSignalWorkflow search run", zap.Any("type", input.Type))
			s.Equal(fiwkfl.FileSignalWorkflowName, input.Type)
		case expectedCall[1]:
			// create run
			var input models.RunParams
			s.NoError(args.Get(&input))
			l.Info("Test_FileSignalWorkflow create run", zap.Any("run-id", input.RunId))
			s.Equal(fiwkfl.FileSignalWorkflowName, input.RequestedBy)
		case expectedCall[2]:
			// update run
			var input models.RunParams
			s.NoError(args.Get(&input))
			s.Equal(string(models.COMPLETED), input.Status)
			l.Info("Test_FileSignalWorkflow update run", zap.Any("run-id", input.RunId))
			runId = input.RunId
		default:
			panic("unexpected activity call")
		}
	})

	s.env.RegisterDelayedCallback(func() {
		s.env.SignalWorkflow(fiwkfl.FILE_SIG_CHAN, models.FileSignal{
			FilePath:    fmt.Sprintf("%s/%s", DATA_PATH, LIVE_FILE_NAME),
			RequestedBy: "test-file-signal@gmail.com",
		})
	}, 10*time.Millisecond)

	s.env.RegisterDelayedCallback(func() {
		s.env.SignalWorkflow(fiwkfl.FILE_SIG_CHAN, models.FileSignal{
			Done: true,
		})
	}, 20*time.Millisecond)

	defer func() {
		if err := recover(); err != nil {
			l.Info("Test_FileSignalWorkflow panicked", zap.Any("error", err), zap.String("wkfl", fiwkfl.FileSignalWorkflowName))
		}

		err := s.env.GetWorkflowError()
		if err != nil {
			l.Info("Test_FileSignalWorkflow error", zap.Any("error", err))
		}

		l.Info("cleaning up", zap.String("wkfl", fiwkfl.FileSignalWorkflowName))

		sOpts := scheduler.NewDefaultClientOption()
		sOpts.Caller = "FileWorkflowTestSuite"
		schClient, err := scheduler.NewClient(l, sOpts)
		s.NoError(err)

		if _, err = schClient.DeleteRun(context.Background(), &api.DeleteRunRequest{
			Id: runId,
		}); err != nil {
			l.Error("error deleting run", zap.Error(err), zap.String("wkfl", fiwkfl.FileSignalWorkflowName))
		}

		if err = schClient.Close(); err != nil {
			l.Error("error closing scheduler client", zap.Error(err), zap.String("wkfl", fiwkfl.FileSignalWorkflowName))
		}
	}()

	s.env.ExecuteWorkflow(fiwkfl.FileSignalWorkflow)

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *FileWorkflowTestSuite) Test_FileSignalWorkflow_ExistingRun() {
	l := logger.NewTestAppZapLogger(TEST_DIR)

	sOpts := scheduler.NewDefaultClientOption()
	sOpts.Caller = "FileWorkflowTestSuite"
	schClient, err := scheduler.NewClient(l, sOpts)
	s.NoError(err)

	var runId string
	if run, err := createFileSignalRun(schClient, &api.RunRequest{
		RunId:       "default-test-run-id",
		WorkflowId:  "default-test-workflow-id",
		RequestedBy: fiwkfl.FileSignalWorkflowName,
		Type:        fiwkfl.FileSignalWorkflowName,
	}); err != nil {
		l.Error("error creating run", zap.Error(err), zap.String("wkfl", fiwkfl.FileSignalWorkflowName))
	} else {
		runId = run.RunId
	}

	expectedCall := []string{
		common.SearchRunActivityName,
		common.UpdateRunActivityName,
	}

	var activityCalled []string
	s.env.SetOnActivityStartedListener(func(activityInfo *activity.Info, ctx context.Context, args encoded.Values) {
		activityType := activityInfo.ActivityType.Name
		if strings.HasPrefix(activityType, "internalSession") {
			return
		}
		activityCalled = append(activityCalled, activityType)
		switch activityType {
		case expectedCall[0]:
			// search run
			var input models.RunParams
			s.NoError(args.Get(&input))
			l.Info("Test_FileSignalWorkflow search run", zap.Any("type", input.Type))
			s.Equal(fiwkfl.FileSignalWorkflowName, input.Type)
		case expectedCall[1]:
			// update run
			var input models.RunParams
			s.NoError(args.Get(&input))
			s.Equal(string(models.COMPLETED), input.Status)
			l.Info("Test_FileSignalWorkflow update run", zap.Any("run-id", input.RunId))
			runId = input.RunId
		default:
			panic("unexpected activity call")
		}
	})

	s.env.RegisterDelayedCallback(func() {
		s.env.SignalWorkflow(fiwkfl.FILE_SIG_CHAN, models.FileSignal{
			FilePath:    fmt.Sprintf("%s/%s", DATA_PATH, LIVE_FILE_NAME),
			RequestedBy: "test-file-signal@gmail.com",
		})
	}, 10*time.Millisecond)

	s.env.RegisterDelayedCallback(func() {
		s.env.SignalWorkflow(fiwkfl.FILE_SIG_CHAN, models.FileSignal{
			Done: true,
		})
	}, 20*time.Millisecond)

	defer func() {
		if err := recover(); err != nil {
			l.Info("Test_FileSignalWorkflow panicked", zap.Any("error", err), zap.String("wkfl", fiwkfl.FileSignalWorkflowName))
		}

		err := s.env.GetWorkflowError()
		if err != nil {
			l.Info("Test_FileSignalWorkflow error", zap.Any("error", err))
		}

		l.Info("cleaning up", zap.String("wkfl", fiwkfl.FileSignalWorkflowName))

		if err = deleteRun(schClient, runId); err != nil {
			l.Error("error deleting run", zap.Error(err), zap.String("wkfl", fiwkfl.FileSignalWorkflowName))
		}

		if err = schClient.Close(); err != nil {
			l.Error("error closing scheduler client", zap.Error(err), zap.String("wkfl", fiwkfl.FileSignalWorkflowName))
		}
	}()

	s.env.ExecuteWorkflow(fiwkfl.FileSignalWorkflow)

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func deleteRun(cl scheduler.Client, runId string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, err := cl.DeleteRun(ctx, &api.DeleteRunRequest{
		Id: runId,
	}); err != nil {
		return err
	}
	return nil
}

func createFileSignalRun(cl scheduler.Client, req *api.RunRequest) (*api.WorkflowRun, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if resp, err := cl.CreateRun(ctx, req); err != nil {
		return nil, err
	} else {
		return resp.Run, nil
	}
}
