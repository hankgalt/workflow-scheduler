package batch_test

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/comfforts/logger"
	bo "github.com/hankgalt/batch-orchestra"
	"github.com/hankgalt/batch-orchestra/pkg/domain"

	btchwkfl "github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch"
	sinks "github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch/sinks"
	"github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch/sources"
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
			bo.ProcessBatchWorkflow[domain.CSVRow, sources.CloudCSVConfig, sinks.MongoSinkConfig[domain.CSVRow]],
			workflow.RegisterOptions{
				Name: btchwkfl.ProcessCloudCSVMongoWorkflowAlias,
			},
		)

		// Register activities.
		s.env.RegisterActivityWithOptions(
			bo.FetchNextActivity[domain.CSVRow, sources.CloudCSVConfig],
			activity.RegisterOptions{
				Name: btchwkfl.FetchNextCloudCSVSourceBatchAlias,
			},
		)
		s.env.RegisterActivityWithOptions(
			bo.WriteActivity[domain.CSVRow, sinks.MongoSinkConfig[domain.CSVRow]],
			activity.RegisterOptions{
				Name: btchwkfl.WriteNextMongoSinkBatchAlias,
			},
		)

		// Build source & sink configurations
		// Source - cloud CSV
		envCfg, err := envutils.BuildCloudCSVBatchConfig()
		s.NoError(err, "error building cloud CSV config for test environment")
		path := filepath.Join(envCfg.Path, envCfg.Name)
		sourceCfg := sources.CloudCSVConfig{
			Path:         path,
			Bucket:       envCfg.Bucket,
			Provider:     string(sources.CloudSourceGCS),
			Delimiter:    '|',
			HasHeader:    true,
			MappingRules: domain.BuildBusinessModelTransformRules(),
		}

		// Sink - MongoDB
		mCfg := envutils.BuildMongoStoreConfig()
		s.Require().NotEmpty(mCfg.Name(), "MongoDB name should not be empty")
		s.Require().NotEmpty(mCfg.Host(), "MongoDB host should not be empty")
		sinkCfg := sinks.MongoSinkConfig[domain.CSVRow]{
			Protocol:   mCfg.Protocol(),
			Host:       mCfg.Host(),
			DBName:     mCfg.Name(),
			User:       mCfg.User(),
			Pwd:        mCfg.Pwd(),
			Params:     mCfg.Params(),
			Collection: "vypar.agents",
		}

		req := &btchwkfl.CloudCSVMongoBatchRequest{
			JobID:               "cloud-csv-mongo-happy",
			BatchSize:           400,
			MaxInProcessBatches: 2,
			Source:              sourceCfg,
			Sink:                sinkCfg,
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
					"wkfl", bo.ProcessCloudCSVMongoWorkflowAlias)
			}

			err := s.env.GetWorkflowError()
			if err != nil {
				l.Error(
					"Test_ProcessBatchWorkflow_CloudCSV_Mongo_HappyPath - error",
					"error", err.Error(),
				)
			} else {
				var result btchwkfl.CloudCSVMongoBatchRequest
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
		s.env.ExecuteWorkflow(bo.ProcessCloudCSVMongoWorkflowAlias, req)
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
		bo.ProcessBatchWorkflow[domain.CSVRow, sources.LocalCSVConfig, sinks.MongoSinkConfig[domain.CSVRow]],
		workflow.RegisterOptions{
			Name: bo.ProcessLocalCSVMongoWorkflowAlias,
		},
	)

	// Register activities.
	env.RegisterActivityWithOptions(
		bo.FetchNextActivity[domain.CSVRow, sources.LocalCSVConfig],
		activity.RegisterOptions{
			Name: btchwkfl.FetchNextLocalCSVSourceBatchAlias,
		},
	)
	env.RegisterActivityWithOptions(
		bo.WriteActivity[domain.CSVRow, sinks.MongoSinkConfig[domain.CSVRow]],
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
	sourceCfg := sources.LocalCSVConfig{
		Path:         path,
		Delimiter:    '|',
		HasHeader:    true,
		MappingRules: domain.BuildBusinessModelTransformRules(),
	}

	// Sink - MongoDB
	mCfg := envutils.BuildMongoStoreConfig()
	require.NotEmpty(t, mCfg.Name(), "MongoDB name should not be empty")
	require.NotEmpty(t, mCfg.Host(), "MongoDB host should not be empty")
	sinkCfg := sinks.MongoSinkConfig[domain.CSVRow]{
		Protocol:   mCfg.Protocol(),
		Host:       mCfg.Host(),
		DBName:     mCfg.Name(),
		User:       mCfg.User(),
		Pwd:        mCfg.Pwd(),
		Params:     mCfg.Params(),
		Collection: "vypar.agents",
	}

	req := &btchwkfl.LocalCSVMongoBatchRequest{
		JobID:               "job-happy",
		BatchSize:           400,
		MaxInProcessBatches: 2,
		Source:              sourceCfg,
		Sink:                sinkCfg,
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
				"wkfl", bo.ProcessLocalCSVMongoWorkflowAlias)
		}

		err := env.GetWorkflowError()
		if err != nil {
			l.Error(
				"Test_ProcessBatchWorkflow_LocalCSV_Mongo_HappyPath - error",
				"error", err.Error(),
			)
		} else {
			var result btchwkfl.LocalCSVMongoBatchRequest
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

	env.ExecuteWorkflow(bo.ProcessLocalCSVMongoWorkflowAlias, req)

	require.True(t, env.IsWorkflowCompleted())

}

// Integration test for ProcessBatchWorkflow on a dev Temporal server.
// Start temporal dev server before running this test.
func Test_ProcessBatchWorkflow_LocalCSV_Mongo_HappyPath_Server(t *testing.T) {
	// get test logger
	l := logger.GetSlogLogger()

	// setup context with logger
	ctx := logger.WithLogger(context.Background(), l)

	// Temporal client connection
	c, err := client.Dial(
		client.Options{
			HostPort: client.DefaultHostPort,
			Logger:   l,
		},
	)
	if err != nil {
		l.Error("Temporal dev server not running (localhost:7233)", "error", err)
		return
	}
	defer c.Close()

	// workflow task queue
	const tq = "process-batch-workflow-local-csv-mongo-tq"

	// Temporal worker for the task queue
	w := worker.New(c, tq, worker.Options{
		BackgroundActivityContext: ctx,            // Global worker context for activities
		DeadlockDetectionTimeout:  24 * time.Hour, // set a long timeout to avoid deadlock detection during tests
	})

	// Register the workflow function
	w.RegisterWorkflowWithOptions(
		bo.ProcessBatchWorkflow[domain.CSVRow, sources.LocalCSVConfig, sinks.MongoSinkConfig[domain.CSVRow]],
		workflow.RegisterOptions{
			Name: bo.ProcessLocalCSVMongoWorkflowAlias,
		},
	)

	// Register activities.
	w.RegisterActivityWithOptions(
		bo.FetchNextActivity[domain.CSVRow, sources.LocalCSVConfig],
		activity.RegisterOptions{
			Name: btchwkfl.FetchNextLocalCSVSourceBatchAlias,
		},
	)
	w.RegisterActivityWithOptions(
		bo.WriteActivity[domain.CSVRow, sinks.MongoSinkConfig[domain.CSVRow]],
		activity.RegisterOptions{
			Name: btchwkfl.WriteNextMongoSinkBatchAlias,
		},
	)

	// Start the worker in a goroutine
	go func() {
		// Stop worker when test ends.
		if err := w.Run(worker.InterruptCh()); err != nil {
			l.Error("Test_ProcessBatchWorkflow_LocalCSV_Mongo_HappyPath_Server - worker stopped:", "error", err.Error())
		}
	}()

	// Build source & sink configurations
	// Source - local CSV
	fileName := envutils.BuildFileName()
	filePath, err := envutils.BuildFilePath()
	require.NoError(t, err, "error building csv file path for test")
	path := filepath.Join(filePath, fileName)
	sourceCfg := sources.LocalCSVConfig{
		Path:         path,
		Delimiter:    '|',
		HasHeader:    true,
		MappingRules: domain.BuildBusinessModelTransformRules(),
	}

	// Sink - MongoDB
	mCfg := envutils.BuildMongoStoreConfig()
	require.NotEmpty(t, mCfg.Name(), "MongoDB name should not be empty")
	require.NotEmpty(t, mCfg.Host(), "MongoDB host should not be empty")
	sinkCfg := sinks.MongoSinkConfig[domain.CSVRow]{
		Protocol:   mCfg.Protocol(),
		Host:       mCfg.Host(),
		DBName:     mCfg.Name(),
		User:       mCfg.User(),
		Pwd:        mCfg.Pwd(),
		Params:     mCfg.Params(),
		Collection: "vypar.agents",
	}

	req := &btchwkfl.LocalCSVMongoBatchRequest{
		JobID:               "process-batch-workflow-local-csv-mongo-server-happy",
		BatchSize:           400,
		MaxInProcessBatches: 2,
		Source:              sourceCfg,
		Sink:                sinkCfg,
	}

	// Create workflow execution context
	ctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)

	// Kick off the workflow.
	run, err := c.ExecuteWorkflow(
		ctx, // context
		client.StartWorkflowOptions{ // workflow options
			ID:        "pbwlcmsh-" + time.Now().Format("150405.000"),
			TaskQueue: tq,
		},
		bo.ProcessLocalCSVMongoWorkflowAlias, // workflow function
		req,                                  // workflow input arg1
	)
	require.NoError(t, err)

	// With a real server, Continue-As-New chains transparently; Get waits for the final run.
	var out btchwkfl.LocalCSVMongoBatchRequest
	require.NoError(t, run.Get(ctx, &out))
	require.NotNil(t, out)

	l.Debug("Test_ProcessBatchWorkflow_LocalCSV_Mongo_HappyPath_Server - workflow completed successfully",
		"workflow-id", run.GetID(),
		"workflow-run-id", run.GetRunID(),
		"offsets", out.Offsets,
	)

}

func Test_ProcessBatchWorkflow_LocalCSV_Mongo_ContinueAsNewError(t *testing.T) {
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
		bo.ProcessBatchWorkflow[domain.CSVRow, sources.LocalCSVConfig, sinks.MongoSinkConfig[domain.CSVRow]],
		workflow.RegisterOptions{
			Name: bo.ProcessLocalCSVMongoWorkflowAlias,
		},
	)

	// Register activities.
	env.RegisterActivityWithOptions(
		bo.FetchNextActivity[domain.CSVRow, sources.LocalCSVConfig],
		activity.RegisterOptions{
			Name: bo.FetchNextLocalCSVSourceBatchAlias,
		},
	)
	env.RegisterActivityWithOptions(
		bo.WriteActivity[domain.CSVRow, sinks.MongoSinkConfig[domain.CSVRow]],
		activity.RegisterOptions{
			Name: bo.WriteNextMongoSinkBatchAlias,
		},
	)

	// Build source & sink configurations
	// Source - local CSV
	fileName := envutils.BuildFileName()
	filePath, err := envutils.BuildFilePath()
	require.NoError(t, err, "error building csv file path for test")
	path := filepath.Join(filePath, fileName)
	sourceCfg := sources.LocalCSVConfig{
		Path:         path,
		Delimiter:    '|',
		HasHeader:    true,
		MappingRules: domain.BuildBusinessModelTransformRules(),
	}

	// Sink - MongoDB
	mCfg := envutils.BuildMongoStoreConfig()
	require.NotEmpty(t, mCfg.Name(), "MongoDB name should not be empty")
	require.NotEmpty(t, mCfg.Host(), "MongoDB host should not be empty")
	sinkCfg := sinks.MongoSinkConfig[domain.CSVRow]{
		Protocol:   mCfg.Protocol(),
		Host:       mCfg.Host(),
		DBName:     mCfg.Name(),
		User:       mCfg.User(),
		Pwd:        mCfg.Pwd(),
		Params:     mCfg.Params(),
		Collection: "vypar.agents",
	}

	req := &btchwkfl.LocalCSVMongoBatchRequest{
		JobID:               "job-happy",
		BatchSize:           400,
		MaxInProcessBatches: 2,
		MaxBatches:          6,
		StartAt:             0,
		Source:              sourceCfg,
		Sink:                sinkCfg,
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
				"Test_ProcessBatchWorkflow_LocalCSV_Mongo_ContinueAsNewError - Activity started",
				"activity-type", activityType,
			)

		})

	defer func() {
		if err := recover(); err != nil {
			l.Error(
				"Test_ProcessBatchWorkflow_LocalCSV_Mongo_ContinueAsNewError - panicked",
				"workflow", bo.ProcessLocalCSVMongoWorkflowAlias,
				"error", err,
			)
		}

		err := env.GetWorkflowError()
		var ca *workflow.ContinueAsNewError
		require.True(t, errors.As(err, &ca), "expected ContinueAsNewError, got: %v", err)
		require.Equal(t, bo.ProcessLocalCSVMongoWorkflowAlias, ca.WorkflowType.Name, "expected workflow type to match")

		var next btchwkfl.LocalCSVMongoBatchRequest
		ok, decErr := extractContinueAsNewInput(err, converter.GetDefaultDataConverter(), &next)
		require.True(t, ok, "expected to extract continue-as-new input, got error: %v", decErr)
		require.NoError(t, decErr, "error extracting continue-as-new input")
		require.NotNil(t, &next, "expected non-nil continue-as-new input")
		require.True(t, next.StartAt > req.StartAt, "expected next.StartAt > req.StartAt")
	}()

	env.ExecuteWorkflow(bo.ProcessLocalCSVMongoWorkflowAlias, req)
	require.True(t, env.IsWorkflowCompleted())

}

func extractContinueAsNewInput[T any](err error, dc converter.DataConverter, out *T) (ok bool, _ error) {
	var cae *workflow.ContinueAsNewError
	if !errors.As(err, &cae) || cae == nil {
		return false, nil
	}
	// Decode the single argument you continued with.
	if e := dc.FromPayloads(cae.Input, out); e != nil {
		return true, e
	}
	return true, nil
}
