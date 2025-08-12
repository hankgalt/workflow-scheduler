package batch_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"

	"github.com/comfforts/logger"

	"github.com/hankgalt/workflow-scheduler/internal/domain/batch"
	btchwkfl "github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch"
	envutils "github.com/hankgalt/workflow-scheduler/pkg/utils/environment"
)

type ProcessLocalCSVMongoWorkflowTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func TestProcessLocalCSVMongoWorkflowTestSuite(t *testing.T) {
	suite.Run(t, new(ProcessLocalCSVMongoWorkflowTestSuite))
}

func (s *ProcessLocalCSVMongoWorkflowTestSuite) SetupTest() {
	// get test logger
	l := logger.GetSlogLogger()

	// set environment logger
	s.SetLogger(l)
}

func (s *ProcessLocalCSVMongoWorkflowTestSuite) TearDownTest() {
	s.env.AssertExpectations(s.T())

	// err := os.RemoveAll(TEST_DIR)
	// s.NoError(err)
}

func (s *ProcessLocalCSVMongoWorkflowTestSuite) Test_ProcessLocalCSVMongoWorkflow() {
	l := s.GetLogger()

	s.env = s.NewTestWorkflowEnvironment()

	// register workflow
	s.env.RegisterWorkflow(btchwkfl.ProcessLocalCSVMongo)

	// register activities
	s.env.RegisterActivityWithOptions(
		btchwkfl.SetupLocalCSVMongoBatch,
		activity.RegisterOptions{Name: btchwkfl.SetupLocalCSVMongoBatchActivity},
	)
	s.env.RegisterActivityWithOptions(
		btchwkfl.HandleLocalCSVMongoBatchData,
		activity.RegisterOptions{Name: btchwkfl.HandleLocalCSVMongoBatchDataActivity},
	)

	s.env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.Background(),
		DeadlockDetectionTimeout:  24 * time.Hour, // set a long timeout to avoid deadlock detection during tests
	})

	s.env.SetTestTimeout(24 * time.Hour)

	s.Run("valid csv to mongo request", func() {
		start := time.Now()

		req, err := envutils.BuildLocalCSVMongoBatchRequest(2, 700)
		s.NoError(err)

		expectedCall := []string{
			btchwkfl.SetupLocalCSVMongoBatchActivity,
			btchwkfl.HandleLocalCSVMongoBatchDataActivity,
		}

		var activityCalled []string
		s.env.SetOnActivityStartedListener(
			func(
				activityInfo *activity.Info,
				ctx context.Context,
				args converter.EncodedValues,
			) {
				activityType := activityInfo.ActivityType.Name
				if strings.HasPrefix(activityType, "internalSession") {
					return
				}
				activityCalled = append(activityCalled, activityType)
				switch activityType {
				case expectedCall[0]:
					l.Debug(
						"Test_ProcessLocalCSVMongoWorkflow - expected activity call",
						"activity-type", activityType)
					var input batch.RequestConfig
					s.NoError(args.Get(&input))
					l.Debug(
						"Test_ProcessLocalCSVMongoWorkflow - received activity input",
						"activity-type", activityType,
						"offsets", input.Offsets)
				case expectedCall[1]:
					l.Debug(
						"Test_ProcessLocalCSVMongoWorkflow - expected activity call",
						"activity-type", activityType)
					var input batch.Batch
					s.NoError(args.Get(&input))
					l.Debug(
						"Test_ProcessLocalCSVMongoWorkflow - received activity input",
						"activity-type", activityType,
						"batch-id", input.BatchID,
						"start", input.Start,
						"end", input.End)
				default:
					l.Debug(
						"Test_ProcessLocalCSVMongoWorkflow - unexpected activity call",
						"activity-type", activityType)
					panic("Test_ProcessLocalCSVMongoWorkflow - unexpected activity call")

				}
			})

		defer func() {
			if err := recover(); err != nil {
				l.Error(
					"Test_ProcessLocalCSVMongoWorkflow - panicked",
					"error", err,
					"wkfl", btchwkfl.ProcessLocalCSVWorkflow)
			}

			err := s.env.GetWorkflowError()
			if err != nil {
				l.Error("Test_ProcessLocalCSVMongoWorkflow - error", "error", err.Error())
			} else {
				var result batch.LocalCSVMongoBatchRequest
				err := s.env.GetWorkflowResult(&result)
				s.NoError(err, "Test_ProcessLocalCSVMongoWorkflow - failed to get workflow result")

				timeTaken := time.Since(start)
				batches := [][]uint64{}
				recordCount := 0
				for _, v := range result.Batches {
					batches = append(batches, []uint64{v.Start, v.End, uint64(len(v.Records))})
					recordCount += len(v.Records)

					for _, rec := range v.Records {
						l.Debug(
							"Test_ProcessLocalCSVMongoWorkflow",
							"recordId", rec.RecordID,
							"result", rec.Record)
					}
				}

				l.Info(
					"Test_ProcessLocalCSVMongoWorkflow result",
					"time-taken", fmt.Sprintf("%dms", timeTaken.Milliseconds()),
					"record-count", recordCount)
			}
		}()

		s.env.ExecuteWorkflow(btchwkfl.ProcessLocalCSVMongo, req)

		s.True(s.env.IsWorkflowCompleted())
		s.NoError(s.env.GetWorkflowError())
	})
}
