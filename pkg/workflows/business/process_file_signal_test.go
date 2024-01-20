package business_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	api "github.com/hankgalt/workflow-scheduler/api/v1"
	"github.com/hankgalt/workflow-scheduler/pkg/clients/scheduler"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
	bizwkfl "github.com/hankgalt/workflow-scheduler/pkg/workflows/business"
	"github.com/hankgalt/workflow-scheduler/pkg/workflows/common"
	fiwkfl "github.com/hankgalt/workflow-scheduler/pkg/workflows/file"
	"github.com/stretchr/testify/suite"

	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/encoded"
	"go.uber.org/cadence/testsuite"
	"go.uber.org/zap"

	"github.com/comfforts/logger"
)

type BusinessWorkflowTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func TestUnitTestSuite(t *testing.T) {
	suite.Run(t, new(BusinessWorkflowTestSuite))
}

func (s *BusinessWorkflowTestSuite) TestProcessFileSignalWorkflow() {
	l := logger.NewTestAppZapLogger(TEST_DIR)
	reqstr := "process-file-signal-workflow-test@gmail.com"
	filePath := fmt.Sprintf("%s/%s", DATA_PATH, "Agents-sm.csv")
	req := &models.CSVInfo{
		FileName:    filePath,
		RequestedBy: reqstr,
		Type:        models.AGENT,
	}

	expectedCall := []string{
		common.SearchRunActivityName,
		common.CreateRunActivityName,
		fiwkfl.DownloadFileActivityName,
		bizwkfl.GetCSVHeadersActivityName,
		bizwkfl.GetCSVOffsetsActivityName,
		bizwkfl.AddAgentActivityName,
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
			l.Info("Test_ProcessFileSignalWorkflow search run", zap.Any("type", input.Type))
			s.Equal(bizwkfl.ProcessFileSignalWorkflowName, input.Type)
		case expectedCall[1]:
			// create run
			var input models.RunParams
			s.NoError(args.Get(&input))
			l.Info("Test_ProcessFileSignalWorkflow create run", zap.Any("run-id", input.RunId))
			s.Equal(reqstr, input.RequestedBy)
		case expectedCall[2]:
			// download file
			var input models.RequestInfo
			s.NoError(args.Get(&input))
			s.Equal(req.FileName, input.FileName)
		case expectedCall[3]:
			// get headers
			var input models.CSVInfo
			s.NoError(args.Get(&input))
			s.Equal(req.RequestedBy, input.RequestedBy)
			s.Equal(req.FileName, input.FileName)
			l.Debug("Test_ProcessFileSignalWorkflow build headers activity", zap.String("file", input.FileName), zap.Any("headers", input.Headers))
		case expectedCall[4]:
			// build offsets
			var input models.CSVInfo
			s.NoError(args.Get(&input))
			s.Equal(req.RequestedBy, input.RequestedBy)
			s.Equal(req.FileName, input.FileName)
			l.Debug("Test_ProcessFileSignalWorkflow build offsets activity", zap.String("file", input.FileName), zap.Any("headers", input.Headers))
		case expectedCall[5]:
			// add agent
			var fields map[string]string
			s.NoError(args.Get(&fields))
			s.Equal(true, len(fields) > 0)
			l.Debug("Test_ProcessFileSignalWorkflow add agent activity", zap.Any("fields", fields))
		case expectedCall[6]:
			// update run
			var input models.RunParams
			s.NoError(args.Get(&input))
			s.Equal(string(models.COMPLETED), input.Status)
			l.Info("Test_ProcessFileSignalWorkflow update run", zap.Any("run-id", input.RunId))
			runId = input.RunId
		default:
			panic("Test_ProcessFileSignalWorkflow unexpected activity call")
		}
	})

	defer func() {
		if err := recover(); err != nil {
			l.Info("Test_ProcessFileSignalWorkflow panicked", zap.Any("error", err), zap.String("wkfl", bizwkfl.ProcessFileSignalWorkflowName))
		}

		err := s.env.GetWorkflowError()
		if err != nil {
			l.Info("Test_ProcessFileSignalWorkflow error", zap.Any("error", err))
		}

		l.Info("cleaning up", zap.String("wkfl", bizwkfl.ProcessFileSignalWorkflowName))

		sOpts := scheduler.NewDefaultClientOption()
		sOpts.Caller = "BusinessWorkflowTestSuite"
		schClient, err := scheduler.NewClient(l, sOpts)
		s.NoError(err)

		if err = deleteRun(schClient, runId); err != nil {
			l.Error("Test_ProcessFileSignalWorkflow error deleting run", zap.Error(err), zap.String("wkfl", bizwkfl.ProcessFileSignalWorkflowName))
		}

		if err = schClient.Close(); err != nil {
			l.Error("error closing scheduler client", zap.Error(err), zap.String("wkfl", bizwkfl.ProcessFileSignalWorkflowName))
		}
	}()

	s.env.ExecuteWorkflow(bizwkfl.ProcessFileSignalWorkflow, req)

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *BusinessWorkflowTestSuite) TestFileSignalWorkflowExistingRun() {
	l := logger.NewTestAppZapLogger(TEST_DIR)
	reqstr := "process-file-signal-workflow-test@gmail.com"
	filePath := fmt.Sprintf("%s/%s", DATA_PATH, "Agents-sm.csv")
	req := &models.CSVInfo{
		FileName:    filePath,
		RequestedBy: reqstr,
		Type:        models.AGENT,
	}

	sOpts := scheduler.NewDefaultClientOption()
	sOpts.Caller = "BusinessWorkflowTestSuite"
	schClient, err := scheduler.NewClient(l, sOpts)
	s.NoError(err)

	var runId string
	if run, err := createFileSignalRun(schClient, &api.RunRequest{
		RunId:       "default-test-run-id",
		WorkflowId:  "default-test-workflow-id",
		RequestedBy: reqstr,
		Type:        bizwkfl.ProcessFileSignalWorkflowName,
		ExternalRef: req.FileName,
	}); err != nil {
		l.Error("Test_FileSignalWorkflow_ExistingRun error creating run", zap.Error(err), zap.String("wkfl", fiwkfl.FileSignalWorkflowName))
	} else {
		l.Debug("Test_FileSignalWorkflow_ExistingRun run created", zap.String("ref", run.ExternalRef), zap.String("run", run.RunId), zap.String("wkfl", fiwkfl.FileSignalWorkflowName))
		runId = run.RunId
	}

	expectedCall := []string{
		common.SearchRunActivityName,
		fiwkfl.DownloadFileActivityName,
		bizwkfl.GetCSVHeadersActivityName,
		bizwkfl.GetCSVOffsetsActivityName,
		bizwkfl.AddAgentActivityName,
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
			s.Equal(bizwkfl.ProcessFileSignalWorkflowName, input.Type)
			s.Equal(filePath, input.ExternalRef)
			l.Debug("Test_FileSignalWorkflow_ExistingRun search run activity", zap.String("type", input.Type), zap.String("ex-ref", input.ExternalRef))
		case expectedCall[1]:
			// download file
			var input models.RequestInfo
			s.NoError(args.Get(&input))
			s.Equal(req.FileName, input.FileName)
			l.Debug("Test_FileSignalWorkflow_ExistingRun file download activity", zap.String("file", input.FileName))
		case expectedCall[2]:
			// get headers
			var input models.CSVInfo
			s.NoError(args.Get(&input))
			s.Equal(req.RequestedBy, input.RequestedBy)
			s.Equal(req.FileName, input.FileName)
			l.Debug("Test_FileSignalWorkflow_ExistingRun build headers activity", zap.String("file", input.FileName), zap.Any("headers", input.Headers))
		case expectedCall[3]:
			// build offsets
			var input models.CSVInfo
			s.NoError(args.Get(&input))
			s.Equal(req.RequestedBy, input.RequestedBy)
			s.Equal(req.FileName, input.FileName)
			l.Debug("Test_FileSignalWorkflow_ExistingRun build offsets activity", zap.String("file", input.FileName), zap.Any("headers", input.Headers))
		case expectedCall[4]:
			// add agent
			var fields map[string]string
			s.NoError(args.Get(&fields))
			s.Equal(true, len(fields) > 0)
			l.Debug("Test_FileSignalWorkflow_ExistingRun add agent activity", zap.Any("fields", fields))
		case expectedCall[5]:
			// update run
			l.Debug("Test_FileSignalWorkflow_ExistingRun update run called")
			// var input models.RunParams
			// s.NoError(args.Get(&input))
			// s.Equal(string(models.COMPLETED), input.Status)
			// l.Debug("Test_FileSignalWorkflow_ExistingRun update run", zap.String("run-id", input.RunId), zap.String("status", input.Status))
			// runId = input.RunId
		default:
			panic("Test_FileSignalWorkflow_ExistingRun unexpected activity call")
		}
	})

	defer func() {
		if err := recover(); err != nil {
			l.Debug("Test_FileSignalWorkflow_ExistingRun panicked", zap.Any("error", err), zap.String("wkfl", bizwkfl.ProcessFileSignalWorkflowName))
		}

		err := s.env.GetWorkflowError()
		if err != nil {
			l.Error("Test_FileSignalWorkflow_ExistingRun error", zap.Any("error", err))
		}

		l.Debug("Test_FileSignalWorkflow_ExistingRun cleaning up", zap.String("wkfl", bizwkfl.ProcessFileSignalWorkflowName))

		if err = deleteRun(schClient, runId); err != nil {
			l.Error("Test_FileSignalWorkflow_ExistingRun error deleting run", zap.Error(err), zap.String("wkfl", bizwkfl.ProcessFileSignalWorkflowName))
		}

		if err = schClient.Close(); err != nil {
			l.Error("Test_FileSignalWorkflow_ExistingRun error closing scheduler client", zap.Error(err), zap.String("wkfl", bizwkfl.ProcessFileSignalWorkflowName))
		}
	}()

	s.env.ExecuteWorkflow(bizwkfl.ProcessFileSignalWorkflow, req)

	result, err := s.env.QueryWorkflow("state")
	s.NoError(err)
	var state models.CSVInfo
	err = result.Get(&state)
	s.NoError(err)
	// require.Equal(t, expectedState, state)
	fmt.Println()
	l.Debug("Test_FileSignalWorkflow_ExistingRun state", zap.Any("state", state))
	fmt.Println()

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
