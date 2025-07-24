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

	"github.com/comfforts/logger"

	api "github.com/hankgalt/workflow-scheduler/api/v1"
	"github.com/hankgalt/workflow-scheduler/pkg/clients/scheduler"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
	bizwkfl "github.com/hankgalt/workflow-scheduler/pkg/workflows/business"
	comwkfl "github.com/hankgalt/workflow-scheduler/pkg/workflows/common"
)

type ProcessCSVWorkflowTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func TestProcessCSVWorkflowTestSuite(t *testing.T) {
	suite.Run(t, new(ProcessCSVWorkflowTestSuite))
}

func (s *ProcessCSVWorkflowTestSuite) SetupTest() {
	// get test logger
	l := getTestLogger()

	// set environment logger
	s.SetLogger(l)

	s.env = s.NewTestWorkflowEnvironment()

	s.env.RegisterWorkflow(bizwkfl.ProcessCSVWorkflow)
	s.env.RegisterWorkflow(bizwkfl.ReadCSVWorkflow)
	s.env.RegisterWorkflow(bizwkfl.ReadCSVRecordsWorkflow)

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

	clog := logger.NewTestAppZapLogger(TEST_DIR)

	schClient, err := scheduler.NewClient(clog, scheduler.NewDefaultClientOption())
	if err != nil {
		l.Error("ProcessCSVWorkflowTestSuite - error initializing scheduler grpc client", slog.Any("error", err))
		panic(err)
	}

	ctx := context.WithValue(context.Background(), scheduler.SchedulerClientContextKey, schClient)

	s.env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	s.env.SetTestTimeout(comwkfl.ONE_DAY)
}

func (s *ProcessCSVWorkflowTestSuite) TearDownTest() {
	s.env.AssertExpectations(s.T())

	// err := os.RemoveAll(TEST_DIR)
	// s.NoError(err)
}

func (s *ProcessCSVWorkflowTestSuite) Test_ProcessCSVWorkflow_Agent() {
	start := time.Now()
	l := s.GetLogger()

	reqstr := "process-csv-workflow-agent-test@test.com"
	filePath := fmt.Sprintf("%s/%s", DATA_PATH, "Agents-sm.csv")
	req := &models.CSVInfo{
		FileName:    filePath,
		RequestedBy: reqstr,
		Type:        models.AGENT,
		BatchSize:   500,
		NumBatches:  3,
	}

	expectedCall := []string{
		bizwkfl.GetCSVHeadersActivityName,
		bizwkfl.GetCSVOffsetsActivityName,
		bizwkfl.AddAgentActivityName,
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
			// get headers
			var input models.CSVInfo
			s.NoError(args.Get(&input))
			s.Equal(req.RequestedBy, input.RequestedBy)
			s.Equal(req.FileName, input.FileName)
		case expectedCall[1]:
			// build offsets
			var input models.CSVInfo
			s.NoError(args.Get(&input))
			s.Equal(req.RequestedBy, input.RequestedBy)
			s.Equal(req.FileName, input.FileName)
		case expectedCall[2]:
			// add agent
			var fields map[string]string
			s.NoError(args.Get(&fields))
			s.Equal(true, len(fields) > 0)
		default:
			panic("Test_ProcessCSVWorkflow_Agent - unexpected activity call")
		}
	})

	defer func() {
		if err := recover(); err != nil {
			l.Info("Test_ProcessCSVWorkflow_Agent - panicked", slog.Any("error", err), slog.String("wkfl", bizwkfl.ProcessCSVWorkflowName))
		}

		err := s.env.GetWorkflowError()
		if err != nil {
			l.Info("Test_ProcessCSVWorkflow_Agent - error", slog.Any("error", err))
		} else {
			var result models.CSVInfo
			err := s.env.GetWorkflowResult(&result)
			s.NoError(err)

			cLog := logger.NewTestAppZapLogger(TEST_DIR)
			sOpts := scheduler.NewDefaultClientOption()
			sOpts.Caller = "ProcessCSVWorkflowTestSuite"
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
					err := deleteEntity(schClient, &api.EntityRequest{
						Type: models.MapEntityTypeToProto(req.Type),
						Id:   res.Id,
					})
					s.NoError(err)
				}
			}
			timeTaken := time.Since(start)
			l.Info("Test_ProcessCSVWorkflow_Agent - time taken", slog.Any("time-taken", timeTaken))
		}

	}()

	s.env.ExecuteWorkflow(bizwkfl.ProcessCSVWorkflow, req)

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
	// s.Equal(expectedCall, activityCalled)
}

func (s *ProcessCSVWorkflowTestSuite) Test_ProcessCSVWorkflow_Principal() {
	start := time.Now()
	l := s.GetLogger()

	reqstr := "process-csv-workflow-principal-test@test.com"
	filePath := fmt.Sprintf("%s/%s", DATA_PATH, "Principals-sm.csv")
	req := &models.CSVInfo{
		FileName:    filePath,
		RequestedBy: reqstr,
		Type:        models.PRINCIPAL,
	}

	expectedCall := []string{
		bizwkfl.GetCSVHeadersActivityName,
		bizwkfl.GetCSVOffsetsActivityName,
		bizwkfl.AddPrincipalActivityName,
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
			// get headers
			var input models.CSVInfo
			s.NoError(args.Get(&input))
			s.Equal(req.RequestedBy, input.RequestedBy)
			s.Equal(req.FileName, input.FileName)
		case expectedCall[1]:
			// build offsets
			var input models.CSVInfo
			s.NoError(args.Get(&input))
			s.Equal(req.RequestedBy, input.RequestedBy)
			s.Equal(req.FileName, input.FileName)
		case expectedCall[2]:
			// add principals
			var fields map[string]string
			s.NoError(args.Get(&fields))
			s.Equal(true, len(fields) > 0)
		default:
			panic("Test_ProcessCSVWorkflow_Principal - unexpected activity call")
		}
	})

	defer func() {
		if err := recover(); err != nil {
			l.Info("Test_ProcessCSVWorkflow_Principal - panicked", slog.Any("error", err), slog.String("wkfl", bizwkfl.ProcessCSVWorkflowName))
		}

		err := s.env.GetWorkflowError()
		if err != nil {
			l.Info("Test_ProcessCSVWorkflow_Principal - error", slog.Any("error", err))
		} else {
			var result models.CSVInfo
			err := s.env.GetWorkflowResult(&result)
			s.NoError(err)

			cLog := logger.NewTestAppZapLogger(TEST_DIR)
			sOpts := scheduler.NewDefaultClientOption()
			sOpts.Caller = "ProcessCSVWorkflowTestSuite"
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
					err := deleteEntity(schClient, &api.EntityRequest{
						Type: models.MapEntityTypeToProto(req.Type),
						Id:   res.Id,
					})
					s.NoError(err)
				}
			}

			timeTaken := time.Since(start)
			l.Info("Test_ProcessCSVWorkflow_Principal - time taken", slog.Any("time-taken", timeTaken))
		}

	}()

	s.env.ExecuteWorkflow(bizwkfl.ProcessCSVWorkflow, req)

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
	// s.Equal(expectedCall, activityCalled)
}

func (s *ProcessCSVWorkflowTestSuite) Test_ProcessCSVWorkflow_Filing() {
	start := time.Now()
	l := s.GetLogger()

	reqstr := "process-csv-workflow-filing-test@test.com"
	filePath := fmt.Sprintf("%s/%s", DATA_PATH, "filings-sm.csv")
	req := &models.CSVInfo{
		FileName:    filePath,
		RequestedBy: reqstr,
		Type:        models.FILING,
	}

	expectedCall := []string{
		bizwkfl.GetCSVHeadersActivityName,
		bizwkfl.GetCSVOffsetsActivityName,
		bizwkfl.AddFilingActivityName,
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
			// get headers
			var input models.CSVInfo
			s.NoError(args.Get(&input))
			s.Equal(req.RequestedBy, input.RequestedBy)
			s.Equal(req.FileName, input.FileName)
		case expectedCall[1]:
			// build offsets
			var input models.CSVInfo
			s.NoError(args.Get(&input))
			s.Equal(req.RequestedBy, input.RequestedBy)
			s.Equal(req.FileName, input.FileName)
		case expectedCall[2]:
			// add fillings
			var fields map[string]string
			s.NoError(args.Get(&fields))
			s.Equal(true, len(fields) > 0)
		default:
			panic("Test_ProcessCSVWorkflow_Filing - unexpected activity call")
		}
	})

	defer func() {
		if err := recover(); err != nil {
			l.Info("Test_ProcessCSVWorkflow_Filing - panicked", slog.Any("error", err), slog.String("wkfl", bizwkfl.ProcessCSVWorkflowName))
		}

		err := s.env.GetWorkflowError()
		if err != nil {
			l.Info("Test_ProcessCSVWorkflow_Filing - error", slog.Any("error", err))
		} else {
			var result models.CSVInfo
			err := s.env.GetWorkflowResult(&result)
			s.NoError(err)

			cLog := logger.NewTestAppZapLogger(TEST_DIR)
			sOpts := scheduler.NewDefaultClientOption()
			sOpts.Caller = "ProcessCSVWorkflowTestSuite"
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
					err := deleteEntity(schClient, &api.EntityRequest{
						Type: models.MapEntityTypeToProto(req.Type),
						Id:   res.Id,
					})
					s.NoError(err)
				}
			}

			timeTaken := time.Since(start)
			l.Info("Test_ProcessCSVWorkflow_Filing - time taken", slog.Any("time-taken", timeTaken))
		}

	}()

	s.env.ExecuteWorkflow(bizwkfl.ProcessCSVWorkflow, req)

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
	// s.Equal(expectedCall, activityCalled)
}

func deleteEntity(cl scheduler.Client, req *api.EntityRequest) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if _, err := cl.DeleteEntity(ctx, req); err != nil {
		return err
	}
	return nil
}

func getTestLogger() *slog.Logger {
	// create log level var
	logLevel := &slog.LevelVar{}
	// set log level
	logLevel.Set(slog.LevelDebug)
	// create log handler options
	opts := &slog.HandlerOptions{
		Level: logLevel,
	}
	// create log handler
	handler := slog.NewJSONHandler(os.Stdout, opts)
	// create logger
	l := slog.New(handler)
	// set default logger
	slog.SetDefault(l)

	return l
}
