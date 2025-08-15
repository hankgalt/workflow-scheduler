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

type ProcessCloudCSVMongoWorkflowTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func TestProcessCloudCSVMongoWorkflowTestSuite(t *testing.T) {
	suite.Run(t, new(ProcessCloudCSVMongoWorkflowTestSuite))
}

func (s *ProcessCloudCSVMongoWorkflowTestSuite) SetupTest() {
	// get test logger
	l := logger.GetSlogLogger()

	// set environment logger
	s.SetLogger(l)
}

func (s *ProcessCloudCSVMongoWorkflowTestSuite) TearDownTest() {
	s.env.AssertExpectations(s.T())

	// err := os.RemoveAll(TEST_DIR)
	// s.NoError(err)
}

func (s *ProcessCloudCSVMongoWorkflowTestSuite) Test_ProcessCloudCSVMongoWorkflow() {
	l := s.GetLogger()

	s.env = s.NewTestWorkflowEnvironment()

	// register workflow
	s.env.RegisterWorkflow(btchwkfl.ProcessCloudCSVMongo)

	// register activities
	s.env.RegisterActivityWithOptions(
		btchwkfl.SetupCloudCSVMongoBatch,
		activity.RegisterOptions{Name: btchwkfl.SetupCloudCSVMongoBatchActivity},
	)
	s.env.RegisterActivityWithOptions(
		btchwkfl.HandleCloudCSVMongoBatchData,
		activity.RegisterOptions{Name: btchwkfl.HandleCloudCSVMongoBatchDataActivity},
	)

	s.env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: context.Background(),
		DeadlockDetectionTimeout:  24 * time.Hour, // set a long timeout to avoid deadlock detection during tests
	})

	s.env.SetTestTimeout(24 * time.Hour)

	s.Run("valid cloud csv to mongo request", func() {
		start := time.Now()
		mappingRules := map[string]batch.Rule{}

		req, err := envutils.BuildCloudCSVMongoBatchRequest(2, 500, mappingRules)
		s.NoError(err)

		expectedCall := []string{
			btchwkfl.SetupCloudCSVMongoBatchActivity,
			btchwkfl.HandleCloudCSVMongoBatchDataActivity,
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
						"Test_ProcessCloudCSVMongoWorkflow - expected activity call",
						"activity-type", activityType)
					var input batch.RequestConfig
					s.NoError(args.Get(&input))
					l.Debug(
						"Test_ProcessCloudCSVMongoWorkflow - received activity input",
						"activity-type", activityType,
						"offsets", input.Offsets,
					)
				case expectedCall[1]:
					l.Debug(
						"Test_ProcessCloudCSVMongoWorkflow - expected activity call",
						"activity-type", activityType)
					var input batch.Batch
					s.NoError(args.Get(&input))
					l.Debug(
						"Test_ProcessCloudCSVMongoWorkflow - received activity input",
						"activity-type", activityType,
						"batch-id", input.BatchID,
						"start", input.Start,
						"end", input.End,
					)
				default:
					l.Debug(
						"Test_ProcessCloudCSVMongoWorkflow - unexpected activity call",
						"activity-type", activityType)
					panic("Test_ProcessCloudCSVMongoWorkflow - unexpected activity call")

				}
			},
		)

		defer func() {
			if err := recover(); err != nil {
				l.Error(
					"Test_ProcessCloudCSVMongoWorkflow - panicked",
					"error", err,
					"wkfl", btchwkfl.ProcessCloudCSVMongoWorkflow,
				)
			}

			err := s.env.GetWorkflowError()
			if err != nil {
				l.Error("Test_ProcessCloudCSVMongoWorkflow - error", "error", err.Error())
			} else {
				var result batch.CloudCSVMongoBatchRequest
				err := s.env.GetWorkflowResult(&result)
				s.NoError(err, "Test_ProcessCloudCSVMongoWorkflow - failed to get workflow result")

				timeTaken := time.Since(start)
				batches := [][]uint64{}
				recordCount := 0
				for _, v := range result.Batches {
					batches = append(batches, []uint64{v.Start, v.End, uint64(len(v.Records))})
					recordCount += len(v.Records)

					for _, rec := range v.Records {
						l.Debug(
							"Test_ProcessCloudCSVMongoWorkflow",
							"recordId", rec.RecordID,
							"result", rec.Record)
					}
				}

				l.Info(
					"Test_ProcessCloudCSVMongoWorkflow result",
					"time-taken", fmt.Sprintf("%dms", timeTaken.Milliseconds()),
					"record-count", recordCount,
				)
			}
		}()

		s.env.ExecuteWorkflow(btchwkfl.ProcessCloudCSVMongo, req)

		s.True(s.env.IsWorkflowCompleted())
		s.NoError(s.env.GetWorkflowError())
	})
}
