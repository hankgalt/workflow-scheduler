package business_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.uber.org/zap"

	"github.com/comfforts/logger"

	api "github.com/hankgalt/workflow-scheduler/api/v1"
	"github.com/hankgalt/workflow-scheduler/pkg/clients/cloud"
	"github.com/hankgalt/workflow-scheduler/pkg/clients/scheduler"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
	bizwkfl "github.com/hankgalt/workflow-scheduler/pkg/workflows/temporal/business"
	comwkfl "github.com/hankgalt/workflow-scheduler/pkg/workflows/temporal/common"
	fiwkfl "github.com/hankgalt/workflow-scheduler/pkg/workflows/temporal/file"
)

type ProcessFileSignalWorkflowTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func TestProcessFileSignalWorkflowTestSuite(t *testing.T) {
	suite.Run(t, new(ProcessFileSignalWorkflowTestSuite))
}

func (s *ProcessFileSignalWorkflowTestSuite) SetupTest() {
	// get test logger
	l := getTestLogger()

	// set environment logger
	s.SetLogger(l)

	s.env = s.NewTestWorkflowEnvironment()

	s.env.RegisterWorkflow(bizwkfl.ProcessCSVWorkflow)
	s.env.RegisterWorkflow(bizwkfl.ReadCSVWorkflow)
	s.env.RegisterWorkflow(bizwkfl.ReadCSVRecordsWorkflow)
	s.env.RegisterWorkflow(bizwkfl.ProcessFileSignalWorkflow)

	s.env.RegisterActivityWithOptions(comwkfl.SearchRunActivity, activity.RegisterOptions{
		Name: comwkfl.SearchRunActivityName,
	})
	s.env.RegisterActivityWithOptions(comwkfl.CreateRunActivity, activity.RegisterOptions{
		Name: comwkfl.CreateRunActivityName,
	})
	s.env.RegisterActivityWithOptions(fiwkfl.DownloadFileActivity, activity.RegisterOptions{
		Name: fiwkfl.DownloadFileActivityName,
	})
	s.env.RegisterActivityWithOptions(bizwkfl.GetCSVHeadersActivity, activity.RegisterOptions{
		Name: bizwkfl.GetCSVHeadersActivityName,
	})
	s.env.RegisterActivityWithOptions(bizwkfl.GetCSVOffsetsActivity, activity.RegisterOptions{
		Name: bizwkfl.GetCSVOffsetsActivityName,
	})
	s.env.RegisterActivityWithOptions(bizwkfl.AddAgentActivity, activity.RegisterOptions{
		Name: bizwkfl.AddAgentActivityName,
	})
	s.env.RegisterActivityWithOptions(bizwkfl.AddPrincipalActivity, activity.RegisterOptions{
		Name: bizwkfl.AddPrincipalActivityName,
	})
	s.env.RegisterActivityWithOptions(bizwkfl.AddFilingActivity, activity.RegisterOptions{
		Name: bizwkfl.AddFilingActivityName,
	})
	s.env.RegisterActivityWithOptions(comwkfl.UpdateRunActivity, activity.RegisterOptions{
		Name: comwkfl.UpdateRunActivityName,
	})

	clog := logger.NewTestAppZapLogger(TEST_DIR)

	cloudCfg, err := cloud.NewCloudConfig("", TEST_DIR)
	if err != nil {
		l.Error(fiwkfl.ERR_CLOUD_CFG_INIT, slog.Any("error", err))
		panic(err)
	}

	cloudClient, err := cloud.NewGCPCloudClient(cloudCfg, clog)
	if err != nil {
		l.Error(fiwkfl.ERR_CLOUD_CLIENT_INIT, slog.Any("error", err))
		panic(err)
	}

	bucket := os.Getenv("BUCKET")
	if bucket == "" {
		l.Error(fiwkfl.ERR_MISSING_CLOUD_BUCKET)
		panic(err)
	}

	schClient, err := scheduler.NewClient(clog, scheduler.NewDefaultClientOption())
	if err != nil {
		l.Error("ProcessCSVWorkflowTestSuite - error initializing scheduler grpc client", slog.Any("error", err))
		panic(err)
	}

	ctx := context.WithValue(context.Background(), scheduler.SchedulerClientContextKey, schClient)
	ctx = context.WithValue(ctx, cloud.CloudClientContextKey, cloudClient)
	ctx = context.WithValue(ctx, cloud.CloudBucketContextKey, bucket)
	ctx = context.WithValue(ctx, fiwkfl.DataPathContextKey, TEST_DIR)

	s.env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	s.env.SetTestTimeout(comwkfl.ONE_DAY)
}

func (s *ProcessFileSignalWorkflowTestSuite) TearDownTest() {
	s.env.AssertExpectations(s.T())

	// err := os.RemoveAll(TEST_DIR)
	// s.NoError(err)
}

func (s *ProcessFileSignalWorkflowTestSuite) Test_ProcessFileSignalWorkflow_NewRun() {
	start := time.Now()
	l := s.GetLogger()

	reqstr := "process-file-signal-workflow-test@test.com"
	filePath := fmt.Sprintf("%s/%s", DATA_PATH, "Agents-sm.csv")
	req := &models.CSVInfo{
		FileName:    filePath,
		RequestedBy: reqstr,
		Type:        models.AGENT,
	}

	expectedCall := []string{
		comwkfl.SearchRunActivityName,
		comwkfl.CreateRunActivityName,
		fiwkfl.DownloadFileActivityName,
		bizwkfl.GetCSVHeadersActivityName,
		bizwkfl.GetCSVOffsetsActivityName,
		bizwkfl.AddAgentActivityName,
		comwkfl.UpdateRunActivityName,
	}

	var activityCalled []string
	var runId string
	s.env.SetOnActivityStartedListener(func(activityInfo *activity.Info, ctx context.Context, args converter.EncodedValues) {
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
			l.Info("Test_ProcessFileSignalWorkflow_NewRun search run", slog.Any("type", input.Type))
			s.Equal(bizwkfl.ProcessFileSignalWorkflowName, input.Type)
		case expectedCall[1]:
			// create run
			var input models.RunParams
			s.NoError(args.Get(&input))
			l.Info("Test_ProcessFileSignalWorkflow_NewRun create run", slog.Any("run-id", input.RunId))
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
			l.Debug("Test_ProcessFileSignalWorkflow_NewRun build headers activity", slog.String("file", input.FileName), slog.Any("headers", input.Headers))
		case expectedCall[4]:
			// build offsets
			var input models.CSVInfo
			s.NoError(args.Get(&input))
			s.Equal(req.RequestedBy, input.RequestedBy)
			s.Equal(req.FileName, input.FileName)
			l.Debug("Test_ProcessFileSignalWorkflow_NewRun build offsets activity", slog.String("file", input.FileName), slog.Any("headers", input.Headers))
		case expectedCall[5]:
			// add agent
			var fields map[string]string
			s.NoError(args.Get(&fields))
			s.Equal(true, len(fields) > 0)
			l.Debug("Test_ProcessFileSignalWorkflow_NewRun add agent activity", slog.Any("fields", fields))
		case expectedCall[6]:
			// update run
			var input models.RunParams
			s.NoError(args.Get(&input))
			s.Equal(string(models.COMPLETED), input.Status)
			l.Info("Test_ProcessFileSignalWorkflow_NewRun update run", slog.Any("run-id", input.RunId))
			runId = input.RunId
		default:
			panic("Test_ProcessFileSignalWorkflow_NewRun unexpected activity call")
		}
	})

	defer func() {
		if err := recover(); err != nil {
			l.Info("Test_ProcessFileSignalWorkflow_NewRun panicked", slog.Any("error", err), slog.String("wkfl", bizwkfl.ProcessFileSignalWorkflowName))
		}

		err := s.env.GetWorkflowError()
		if err != nil {
			l.Info("Test_ProcessFileSignalWorkflow_NewRun error", slog.Any("error", err))
		} else {
			var result models.CSVInfo
			s.env.GetWorkflowResult(&result)

			l.Info("cleaning up", slog.String("wkfl", bizwkfl.ProcessFileSignalWorkflowName))

			cLog := logger.NewTestAppZapLogger(TEST_DIR)
			sOpts := scheduler.NewDefaultClientOption()
			sOpts.Caller = "ProcessFileSignalWorkflowTestSuite"
			schClient, err := scheduler.NewClient(cLog, sOpts)
			s.NoError(err)

			errCount := 0
			resultCount := 0
			recCount := 0
			for _, v := range result.Results {
				errCount = errCount + len(v.Errors)
				resultCount = resultCount + len(v.Results)
				recCount = recCount + len(v.Errors) + len(v.Results)
				for _, res := range v.Results {
					deleteEntity(schClient, &api.EntityRequest{
						Type: models.MapEntityTypeToProto(req.Type),
						Id:   res.Id,
					})
				}
			}

			if err = deleteRun(schClient, runId); err != nil {
				l.Error("Test_ProcessFileSignalWorkflow_NewRun error deleting run", slog.Any("error", err), slog.String("wkfl", bizwkfl.ProcessFileSignalWorkflowName))
			}

			if err = schClient.Close(); err != nil {
				l.Error("Test_ProcessFileSignalWorkflow_NewRun error closing scheduler client", slog.Any("error", err), slog.String("wkfl", bizwkfl.ProcessFileSignalWorkflowName))
			}

			timeTaken := time.Since(start)
			l.Info("Test_ProcessFileSignalWorkflow_NewRun - time taken", slog.Any("time-taken", timeTaken))
		}
	}()

	s.env.ExecuteWorkflow(bizwkfl.ProcessFileSignalWorkflow, req)

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
}

func (s *ProcessFileSignalWorkflowTestSuite) Test_FileSignalWorkflow_ExistingRun() {
	start := time.Now()
	l := s.GetLogger()

	reqstr := "process-file-signal-workflow-test@test.com"
	filePath := fmt.Sprintf("%s/%s", DATA_PATH, "Agents-sm.csv")
	req := &models.CSVInfo{
		FileName:    filePath,
		RequestedBy: reqstr,
		Type:        models.AGENT,
	}

	cLog := logger.NewTestAppZapLogger(TEST_DIR)
	sOpts := scheduler.NewDefaultClientOption()
	sOpts.Caller = "ProcessFileSignalWorkflowTestSuite"
	schClient, err := scheduler.NewClient(cLog, sOpts)
	s.NoError(err)

	var runId string
	if run, err := createFileSignalRun(schClient, &api.RunRequest{
		RunId:       "default-test-run-id",
		WorkflowId:  "default-test-workflow-id",
		RequestedBy: reqstr,
		Type:        bizwkfl.ProcessFileSignalWorkflowName,
		ExternalRef: req.FileName,
	}); err != nil {
		l.Error("Test_FileSignalWorkflow_ExistingRun error creating run", slog.Any("error", err), slog.String("wkfl", bizwkfl.ProcessFileSignalWorkflowName))
	} else {
		l.Debug("Test_FileSignalWorkflow_ExistingRun run created", slog.String("ref", run.ExternalRef), slog.String("run", run.RunId), slog.String("wkfl", bizwkfl.ProcessFileSignalWorkflowName))
		runId = run.RunId
	}

	expectedCall := []string{
		comwkfl.SearchRunActivityName,
		fiwkfl.DownloadFileActivityName,
		bizwkfl.GetCSVHeadersActivityName,
		bizwkfl.GetCSVOffsetsActivityName,
		bizwkfl.AddAgentActivityName,
		comwkfl.UpdateRunActivityName,
	}

	var activityCalled []string
	s.env.SetOnActivityStartedListener(func(activityInfo *activity.Info, ctx context.Context, args converter.EncodedValues) {
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
			l.Debug("Test_FileSignalWorkflow_ExistingRun search run activity", slog.String("type", input.Type), slog.String("ex-ref", input.ExternalRef))
		case expectedCall[1]:
			// download file
			var input models.RequestInfo
			s.NoError(args.Get(&input))
			s.Equal(req.FileName, input.FileName)
			l.Debug("Test_FileSignalWorkflow_ExistingRun file download activity", slog.String("file", input.FileName))
		case expectedCall[2]:
			// get headers
			var input models.CSVInfo
			s.NoError(args.Get(&input))
			s.Equal(req.RequestedBy, input.RequestedBy)
			s.Equal(req.FileName, input.FileName)
			l.Debug("Test_FileSignalWorkflow_ExistingRun build headers activity", slog.String("file", input.FileName), slog.Any("headers", input.Headers))
		case expectedCall[3]:
			// build offsets
			var input models.CSVInfo
			s.NoError(args.Get(&input))
			s.Equal(req.RequestedBy, input.RequestedBy)
			s.Equal(req.FileName, input.FileName)
			l.Debug("Test_FileSignalWorkflow_ExistingRun build offsets activity", slog.String("file", input.FileName), slog.Any("headers", input.Headers))
		case expectedCall[4]:
			// add agent
			var fields map[string]string
			s.NoError(args.Get(&fields))
			s.Equal(true, len(fields) > 0)
			l.Debug("Test_FileSignalWorkflow_ExistingRun add agent activity", slog.Any("fields", fields))
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
			l.Debug("Test_FileSignalWorkflow_ExistingRun panicked", slog.Any("error", err), slog.String("wkfl", bizwkfl.ProcessFileSignalWorkflowName))
		}

		err := s.env.GetWorkflowError()
		if err != nil {
			l.Error("Test_FileSignalWorkflow_ExistingRun error", slog.Any("error", err))
		} else {
			var result models.CSVInfo
			s.env.GetWorkflowResult(&result)

			l.Info("cleaning up", slog.String("wkfl", bizwkfl.ProcessFileSignalWorkflowName))

			errCount := 0
			resultCount := 0
			recCount := 0
			for _, v := range result.Results {
				errCount = errCount + len(v.Errors)
				resultCount = resultCount + len(v.Results)
				recCount = recCount + len(v.Errors) + len(v.Results)
				for _, res := range v.Results {
					deleteEntity(schClient, &api.EntityRequest{
						Type: models.MapEntityTypeToProto(req.Type),
						Id:   res.Id,
					})
				}
			}

			l.Debug("Test_FileSignalWorkflow_ExistingRun cleaning up", zap.String("wkfl", bizwkfl.ProcessFileSignalWorkflowName))

			if err = deleteRun(schClient, runId); err != nil {
				l.Error("Test_FileSignalWorkflow_ExistingRun error deleting run", zap.Error(err), zap.String("wkfl", bizwkfl.ProcessFileSignalWorkflowName))
			}

			if err = schClient.Close(); err != nil {
				l.Error("Test_FileSignalWorkflow_ExistingRun error closing scheduler client", zap.Error(err), zap.String("wkfl", bizwkfl.ProcessFileSignalWorkflowName))
			}

			timeTaken := time.Since(start)
			l.Info("Test_FileSignalWorkflow_ExistingRun - time taken", slog.Any("time-taken", timeTaken))
		}
	}()

	s.env.ExecuteWorkflow(bizwkfl.ProcessFileSignalWorkflow, req)

	result, err := s.env.QueryWorkflow("state")
	s.NoError(err)
	var state models.CSVInfoState
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
