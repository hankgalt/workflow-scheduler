package batch_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/comfforts/logger"
	"github.com/hankgalt/workflow-scheduler/internal/domain/batch"
	btchwkfl "github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch"
	envutils "github.com/hankgalt/workflow-scheduler/pkg/utils/environment"
)

type ProcessBatchWorkflowTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	env *testsuite.TestWorkflowEnvironment
}

func TestProcessBatchWorkflowTestSuite(t *testing.T) {
	suite.Run(t, new(ProcessBatchWorkflowTestSuite))
}

func (s *ProcessBatchWorkflowTestSuite) SetupTest() {
	// get test logger
	l := logger.GetSlogLogger()

	// set environment logger
	s.SetLogger(l)
}

func (s *ProcessBatchWorkflowTestSuite) TearDownTest() {
	s.env.AssertExpectations(s.T())

	// err := os.RemoveAll(TEST_DIR)
	// s.NoError(err)

}

func (s *ProcessBatchWorkflowTestSuite) Test_ProcessBatchWorkflow_CloudCSV_Mongo_HappyPath() {
	l := s.GetLogger()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	s.env = s.NewTestWorkflowEnvironment()
	s.env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
		DeadlockDetectionTimeout:  24 * time.Hour, // set a long timeout to avoid deadlock detection during tests
	})

	s.env.SetTestTimeout(24 * time.Hour)

	s.Run("valid cloud csv to mongo migration request", func() {
		// Register the workflow
		s.env.RegisterWorkflowWithOptions(
			btchwkfl.ProcessBatchWorkflow[batch.CSVRow, batch.CloudCSVConfig, batch.MongoSinkConfig[batch.CSVRow]],
			workflow.RegisterOptions{
				Name: btchwkfl.ProcessCloudCSVMongoWorkflowAlias,
			},
		)

		// Register activities.
		s.env.RegisterActivityWithOptions(
			btchwkfl.FetchNextActivity[batch.CSVRow, batch.CloudCSVConfig],
			activity.RegisterOptions{
				Name: btchwkfl.FetchNextCloudCSVSourceBatchAlias,
			},
		)
		s.env.RegisterActivityWithOptions(
			btchwkfl.WriteActivity[batch.CSVRow, batch.MongoSinkConfig[batch.CSVRow]],
			activity.RegisterOptions{
				Name: btchwkfl.WriteNextMongoSinkBatchAlias,
			},
		)

		// Build source & sink configurations
		// Source - cloud CSV
		envCfg, err := envutils.BuildCloudCSVBatchConfig()
		s.NoError(err, "error building cloud CSV config for test environment")
		path := filepath.Join(envCfg.Path, envCfg.Name)
		sourceCfg := batch.CloudCSVConfig{
			Path:         path,
			Bucket:       envCfg.Bucket,
			Provider:     string(batch.CloudSourceGCS),
			Delimiter:    '|',
			HasHeader:    true,
			MappingRules: batch.BuildBusinessModelTransformRules(),
		}

		// Sink - MongoDB
		mCfg := envutils.BuildMongoStoreConfig()
		s.Require().NotEmpty(mCfg.Name(), "MongoDB name should not be empty")
		s.Require().NotEmpty(mCfg.Host(), "MongoDB host should not be empty")
		sinkCfg := batch.MongoSinkConfig[batch.CSVRow]{
			Protocol:   mCfg.Protocol(),
			Host:       mCfg.Host(),
			DBName:     mCfg.Name(),
			User:       mCfg.User(),
			Pwd:        mCfg.Pwd(),
			Params:     mCfg.Params(),
			Collection: "vypar.agents",
		}

		req := &batch.BatchRequest[batch.CSVRow, batch.CloudCSVConfig, batch.MongoSinkConfig[batch.CSVRow]]{
			JobID:     "cloud-csv-mongo-happy",
			BatchSize: 400,
			StartAt:   0,
			Source:    sourceCfg,
			Sink:      sinkCfg,
		}

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

				l.Debug(
					"Test_ProcessBatchWorkflow_CloudCSV_Mongo_HappyPath - Activity started",
					"activityType", activityType,
				)

			})

		defer func() {
			if err := recover(); err != nil {
				l.Error(
					"Test_ProcessBatchWorkflow_CloudCSV_Mongo_HappyPath - panicked",
					"error", err,
					"wkfl", btchwkfl.ProcessCloudCSVMongoWorkflowAlias)
			}

			err := s.env.GetWorkflowError()
			if err != nil {
				l.Error(
					"Test_ProcessBatchWorkflow_CloudCSV_Mongo_HappyPath - error",
					"error", err.Error(),
				)
			} else {
				var result batch.BatchRequest[batch.CSVRow, batch.CloudCSVConfig, batch.MongoSinkConfig[batch.CSVRow]]
				err := s.env.GetWorkflowResult(&result)
				if err != nil {
					l.Error(
						"Test_ProcessBatchWorkflow_CloudCSV_Mongo_HappyPath - error",
						"error", err.Error(),
					)
				} else {
					l.Debug(
						"Test_ProcessBatchWorkflow_CloudCSV_Mongo_HappyPath - success",
						"result-offsets", result.Offsets,
					)
				}
			}

		}()

		l.Debug("Test_ProcessBatchWorkflow_CloudCSV_Mongo_HappyPath - Starting workflow test")
		s.env.ExecuteWorkflow(btchwkfl.ProcessCloudCSVMongoWorkflowAlias, req)
		s.True(s.env.IsWorkflowCompleted())
		l.Debug("Test_ProcessBatchWorkflow_CloudCSV_Mongo_HappyPath - test completed")
	})
}

func Test_ProcessBatchWorkflow_LocalCSV_Mongo_HappyPath(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	l := logger.GetSlogLogger()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
		DeadlockDetectionTimeout:  24 * time.Hour, // set a long timeout to avoid deadlock detection during tests
	})

	env.SetTestTimeout(24 * time.Hour)

	// Register the workflow
	env.RegisterWorkflowWithOptions(
		btchwkfl.ProcessBatchWorkflow[batch.CSVRow, batch.LocalCSVConfig, batch.MongoSinkConfig[batch.CSVRow]],
		workflow.RegisterOptions{
			Name: btchwkfl.ProcessLocalCSVMongoWorkflowAlias,
		},
	)

	// Register activities.
	env.RegisterActivityWithOptions(
		btchwkfl.FetchNextActivity[batch.CSVRow, batch.LocalCSVConfig],
		activity.RegisterOptions{
			Name: btchwkfl.FetchNextLocalCSVSourceBatchAlias,
		},
	)
	env.RegisterActivityWithOptions(
		btchwkfl.WriteActivity[batch.CSVRow, batch.MongoSinkConfig[batch.CSVRow]],
		activity.RegisterOptions{
			Name: btchwkfl.WriteNextMongoSinkBatchAlias,
		},
	)

	// Build source & sink configurations
	// Source - local CSV
	fileName := envutils.BuildFileName()
	filePath, err := envutils.BuildFilePath()
	require.NoError(t, err, "error building csv file path for test")
	path := filepath.Join(filePath, fileName)
	sourceCfg := batch.LocalCSVConfig{
		Path:         path,
		Delimiter:    '|',
		HasHeader:    true,
		MappingRules: batch.BuildBusinessModelTransformRules(),
	}

	// Sink - MongoDB
	mCfg := envutils.BuildMongoStoreConfig()
	require.NotEmpty(t, mCfg.Name(), "MongoDB name should not be empty")
	require.NotEmpty(t, mCfg.Host(), "MongoDB host should not be empty")
	sinkCfg := batch.MongoSinkConfig[batch.CSVRow]{
		Protocol:   mCfg.Protocol(),
		Host:       mCfg.Host(),
		DBName:     mCfg.Name(),
		User:       mCfg.User(),
		Pwd:        mCfg.Pwd(),
		Params:     mCfg.Params(),
		Collection: "vypar.agents",
	}

	req := &batch.BatchRequest[batch.CSVRow, batch.LocalCSVConfig, batch.MongoSinkConfig[batch.CSVRow]]{
		JobID:     "job-happy",
		BatchSize: 400,
		StartAt:   0,
		Source:    sourceCfg,
		Sink:      sinkCfg,
	}

	env.SetOnActivityStartedListener(
		func(
			activityInfo *activity.Info,
			ctx context.Context,
			args converter.EncodedValues,
		) {
			activityType := activityInfo.ActivityType.Name
			if strings.HasPrefix(activityType, "internalSession") {
				return
			}

			l.Debug(
				"Test_ProcessBatchWorkflow_LocalCSV_Mongo_HappyPath - Activity started",
				"activityType", activityType,
			)

		})

	defer func() {
		if err := recover(); err != nil {
			l.Error(
				"Test_ProcessBatchWorkflow_LocalCSV_Mongo_HappyPath - panicked",
				"error", err,
				"wkfl", btchwkfl.ProcessLocalCSVMongoWorkflowAlias)
		}

		err := env.GetWorkflowError()
		if err != nil {
			l.Error(
				"Test_ProcessBatchWorkflow_LocalCSV_Mongo_HappyPath - error",
				"error", err.Error(),
			)
		} else {
			var result batch.BatchRequest[batch.CSVRow, batch.LocalCSVConfig, batch.MongoSinkConfig[batch.CSVRow]]
			err := env.GetWorkflowResult(&result)
			if err != nil {
				l.Error(
					"Test_ProcessBatchWorkflow_LocalCSV_Mongo_HappyPath - error",
					"error", err.Error(),
				)
			} else {
				l.Debug(
					"Test_ProcessBatchWorkflow_LocalCSV_Mongo_HappyPath - success",
					"result-offsets", result.Offsets,
				)
			}
		}

	}()

	env.ExecuteWorkflow(btchwkfl.ProcessLocalCSVMongoWorkflowAlias, req)

	require.True(t, env.IsWorkflowCompleted())

}
