package business_test

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/encoded"
	"go.uber.org/zap"

	"github.com/comfforts/logger"

	api "github.com/hankgalt/workflow-scheduler/api/v1"
	"github.com/hankgalt/workflow-scheduler/pkg/clients/scheduler"
	"github.com/hankgalt/workflow-scheduler/pkg/models"
	bizwkfl "github.com/hankgalt/workflow-scheduler/pkg/workflows/business"
)

func (s *BusinessWorkflowTestSuite) Test_ProcessCSVWorkflow_Agent() {
	start := time.Now()
	l := logger.NewTestAppZapLogger(TEST_DIR)

	reqstr := "process-csv-workflow-test@gmail.com"
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
	s.env.SetOnActivityStartedListener(func(activityInfo *activity.Info, ctx context.Context, args encoded.Values) {
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
			panic("unexpected activity call")
		}
	})

	defer func() {
		if err := recover(); err != nil {
			l.Info("Test_ProcessCSVWorkflow panicked", zap.Any("error", err), zap.String("wkfl", bizwkfl.ProcessCSVWorkflowName))
		}

		err := s.env.GetWorkflowError()
		if err != nil {
			l.Info("Test_ProcessCSVWorkflow error", zap.Any("error", err))
		} else {
			var result models.CSVInfo
			s.env.GetWorkflowResult(&result)

			sOpts := scheduler.NewDefaultClientOption()
			sOpts.Caller = "BusinessWorkflowTestSuite"
			schClient, err := scheduler.NewClient(l, sOpts)
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
						Type: api.EntityType_AGENT,
						Id:   res.Id,
					})
				}
			}
			timeTaken := time.Since(start)
			l.Info("Test_ProcessCSVWorkflow time taken", zap.Any("time-taken", timeTaken))
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

func (s *BusinessWorkflowTestSuite) Test_ProcessCSVWorkflow_Principal() {
	l := logger.NewTestAppZapLogger(TEST_DIR)

	reqstr := "process-csv-workflow-test@gmail.com"
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
	s.env.SetOnActivityStartedListener(func(activityInfo *activity.Info, ctx context.Context, args encoded.Values) {
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
			panic("unexpected activity call")
		}
	})

	defer func() {
		if err := recover(); err != nil {
			l.Info("Test_ProcessCSVWorkflow panicked", zap.Any("error", err), zap.String("wkfl", bizwkfl.ProcessCSVWorkflowName))
		}

		err := s.env.GetWorkflowError()
		if err != nil {
			l.Info("Test_ProcessCSVWorkflow error", zap.Any("error", err))
		} else {
			var result models.CSVInfo
			s.env.GetWorkflowResult(&result)
			l.Info(
				"Test_ProcessCSVWorkflow result",
				zap.Any("file", result.FileName),
				zap.Any("batches", len(result.OffSets)),
				zap.Any("headers", result.Headers.Headers),
				zap.Any("batchesProcessed", len(result.Results)))
			errCount := 0
			resultCount := 0
			recCount := 0
			for _, v := range result.Results {
				errCount = errCount + len(v.Errors)
				resultCount = resultCount + len(v.Results)
				recCount = recCount + len(v.Errors) + len(v.Results)
			}
			l.Info(
				"batch info",
				zap.Int("resultCount", resultCount),
				zap.Int("errCount", errCount),
				zap.Int("recCount", recCount))
		}

	}()

	s.env.ExecuteWorkflow(bizwkfl.ProcessCSVWorkflow, req)

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
	// s.Equal(expectedCall, activityCalled)
}

func (s *BusinessWorkflowTestSuite) Test_ProcessCSVWorkflow_Filing() {
	l := logger.NewTestAppZapLogger(TEST_DIR)

	reqstr := "process-csv-workflow-test@gmail.com"
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
	s.env.SetOnActivityStartedListener(func(activityInfo *activity.Info, ctx context.Context, args encoded.Values) {
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
			panic("unexpected activity call")
		}
	})

	defer func() {
		if err := recover(); err != nil {
			l.Info("Test_ProcessCSVWorkflow panicked", zap.Any("error", err), zap.String("wkfl", bizwkfl.ProcessCSVWorkflowName))
		}

		err := s.env.GetWorkflowError()
		if err != nil {
			l.Info("Test_ProcessCSVWorkflow error", zap.Any("error", err))
		} else {
			var result models.CSVInfo
			s.env.GetWorkflowResult(&result)
			l.Info(
				"Test_ProcessCSVWorkflow result",
				zap.Any("file", result.FileName),
				zap.Any("batches", len(result.OffSets)),
				zap.Any("headers", result.Headers.Headers),
				zap.Any("batchesProcessed", len(result.Results)))
			errCount := 0
			resultCount := 0
			recCount := 0
			for _, v := range result.Results {
				errCount = errCount + len(v.Errors)
				resultCount = resultCount + len(v.Results)
				recCount = recCount + len(v.Errors) + len(v.Results)
			}
			l.Info(
				"batch info",
				zap.Int("resultCount", resultCount),
				zap.Int("errCount", errCount),
				zap.Int("recCount", recCount))
		}

	}()

	s.env.ExecuteWorkflow(bizwkfl.ProcessCSVWorkflow, req)

	s.True(s.env.IsWorkflowCompleted())
	s.NoError(s.env.GetWorkflowError())
	// s.Equal(expectedCall, activityCalled)
}
