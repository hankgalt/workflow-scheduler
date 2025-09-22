package batch_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/api/iterator"

	"github.com/comfforts/logger"
	bo "github.com/hankgalt/batch-orchestra"
	"github.com/hankgalt/batch-orchestra/pkg/domain"

	btchwkfl "github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch"
	sinks "github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch/sinks"
	"github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch/snapshotters"
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

func (s *ProcessBatchWorkflowTestSuite) Test_ProcessBatchWorkflow_CloudCSV_Mongo_Local_HappyPath() {
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
			bo.ProcessBatchWorkflow[domain.CSVRow, *sources.CloudCSVConfig, *sinks.MongoSinkConfig[domain.CSVRow], *snapshotters.LocalSnapshotterConfig],
			workflow.RegisterOptions{
				Name: btchwkfl.ProcessCloudCSVMongoLocalWorkflowAlias,
			},
		)

		// Register activities.
		s.env.RegisterActivityWithOptions(
			bo.FetchNextActivity[domain.CSVRow, *sources.CloudCSVConfig],
			activity.RegisterOptions{
				Name: btchwkfl.FetchNextCloudCSVSourceBatchActivityAlias,
			},
		)
		s.env.RegisterActivityWithOptions(
			bo.WriteActivity[domain.CSVRow, *sinks.MongoSinkConfig[domain.CSVRow]],
			activity.RegisterOptions{
				Name: btchwkfl.WriteNextMongoSinkBatchActivityAlias,
			},
		)
		s.env.RegisterActivityWithOptions(
			bo.SnapshotActivity[*snapshotters.LocalSnapshotterConfig],
			activity.RegisterOptions{
				Name: btchwkfl.SnapshotLocalBatchActivityAlias,
			},
		)

		// Build source & sink configurations
		// Source - cloud CSV
		envCfg, err := envutils.BuildCloudCSVBatchConfig()
		s.NoError(err, "error building cloud CSV config for test environment")
		path := filepath.Join(envCfg.Path, envCfg.Name)
		sourceCfg := &sources.CloudCSVConfig{
			Path:         path,
			Bucket:       envCfg.Bucket,
			Provider:     string(sources.CloudSourceGCS),
			Delimiter:    '|',
			HasHeader:    true,
			MappingRules: domain.BuildBusinessModelTransformRules(),
		}

		// Sink - MongoDB
		mCfg := envutils.BuildMongoStoreConfig(true)
		s.Require().NotEmpty(mCfg.Name(), "MongoDB name should not be empty")
		s.Require().NotEmpty(mCfg.Host(), "MongoDB host should not be empty")
		sinkCfg := &sinks.MongoSinkConfig[domain.CSVRow]{
			Protocol:   mCfg.Protocol(),
			Host:       mCfg.Host(),
			DBName:     mCfg.Name(),
			User:       mCfg.User(),
			Pwd:        mCfg.Pwd(),
			Params:     mCfg.Params(),
			Collection: "vypar.agents",
		}

		filePath, err := envutils.BuildFilePath()
		s.NoError(err, "error building csv file path for test")
		ssCfg := &snapshotters.LocalSnapshotterConfig{
			Path: filePath,
		}

		req := &btchwkfl.CloudCSVMongoLocalBatchRequest{
			JobID:               "job-cloud-csv-mongo-local-happy",
			BatchSize:           400,
			MaxInProcessBatches: 2,
			StartAt:             "0",
			Source:              sourceCfg,
			Sink:                sinkCfg,
			Snapshotter:         ssCfg,
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
					"Test_ProcessBatchWorkflow_CloudCSV_Mongo_Local_HappyPath - Activity started",
					"activityType", activityType,
				)

			})

		defer func() {
			if err := recover(); err != nil {
				l.Error(
					"Test_ProcessBatchWorkflow_CloudCSV_Mongo_Local_HappyPath - panicked",
					"error", err,
					"wkfl", btchwkfl.ProcessCloudCSVMongoLocalWorkflowAlias)
			}

			err := s.env.GetWorkflowError()
			if err != nil {
				l.Error(
					"Test_ProcessBatchWorkflow_CloudCSV_Mongo_Local_HappyPath - error",
					"error", err.Error(),
				)
			} else {
				var result btchwkfl.CloudCSVMongoLocalBatchRequest
				err := s.env.GetWorkflowResult(&result)
				if err != nil {
					l.Error(
						"Test_ProcessBatchWorkflow_CloudCSV_Mongo_Local_HappyPath - error",
						"error", err.Error(),
					)
				} else {
					l.Debug(
						"Test_ProcessBatchWorkflow_CloudCSV_Mongo_Local_HappyPath - success",
						"result-offsets", result.Offsets,
					)
					s.EqualValues(10, result.Snapshot.NumBatches)
					errorCount := 0
					for _, errs := range result.Snapshot.Errors {
						errorCount += len(errs)
					}
					s.EqualValues(25, result.Snapshot.NumRecords)
					s.EqualValues(0, errorCount)
				}
			}

		}()

		l.Debug("Test_ProcessBatchWorkflow_CloudCSV_Mongo_Local_HappyPath - Starting workflow test")
		s.env.ExecuteWorkflow(btchwkfl.ProcessCloudCSVMongoLocalWorkflowAlias, req)
		s.True(s.env.IsWorkflowCompleted())
		l.Debug("Test_ProcessBatchWorkflow_CloudCSV_Mongo_Local_HappyPath - test completed")
	})
}

func (s *ProcessBatchWorkflowTestSuite) Test_ProcessBatchWorkflow_CloudCSV_Mongo_Cloud_HappyPath() {
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
			bo.ProcessBatchWorkflow[domain.CSVRow, *sources.CloudCSVConfig, *sinks.MongoSinkConfig[domain.CSVRow], *snapshotters.CloudSnapshotterConfig],
			workflow.RegisterOptions{
				Name: btchwkfl.ProcessCloudCSVMongoCloudWorkflowAlias,
			},
		)

		// Register activities.
		s.env.RegisterActivityWithOptions(
			bo.FetchNextActivity[domain.CSVRow, *sources.CloudCSVConfig],
			activity.RegisterOptions{
				Name: btchwkfl.FetchNextCloudCSVSourceBatchActivityAlias,
			},
		)
		s.env.RegisterActivityWithOptions(
			bo.WriteActivity[domain.CSVRow, *sinks.MongoSinkConfig[domain.CSVRow]],
			activity.RegisterOptions{
				Name: btchwkfl.WriteNextMongoSinkBatchActivityAlias,
			},
		)
		s.env.RegisterActivityWithOptions(
			bo.SnapshotActivity[*snapshotters.CloudSnapshotterConfig],
			activity.RegisterOptions{
				Name: btchwkfl.SnapshotCloudBatchActivityAlias,
			},
		)

		// Build source & sink configurations
		// Source - cloud CSV
		envCfg, err := envutils.BuildCloudCSVBatchConfig()
		s.NoError(err, "error building cloud CSV config for test environment")
		path := filepath.Join(envCfg.Path, envCfg.Name)
		sourceCfg := &sources.CloudCSVConfig{
			Path:         path,
			Bucket:       envCfg.Bucket,
			Provider:     string(sources.CloudSourceGCS),
			Delimiter:    '|',
			HasHeader:    true,
			MappingRules: domain.BuildBusinessModelTransformRules(),
		}

		// Sink - MongoDB
		mCfg := envutils.BuildMongoStoreConfig(true)
		s.Require().NotEmpty(mCfg.Name(), "MongoDB name should not be empty")
		s.Require().NotEmpty(mCfg.Host(), "MongoDB host should not be empty")
		sinkCfg := &sinks.MongoSinkConfig[domain.CSVRow]{
			Protocol:   mCfg.Protocol(),
			Host:       mCfg.Host(),
			DBName:     mCfg.Name(),
			User:       mCfg.User(),
			Pwd:        mCfg.Pwd(),
			Params:     mCfg.Params(),
			Collection: "vypar.agents",
		}

		// Snapshotter - Cloud
		ssCfg := &snapshotters.CloudSnapshotterConfig{
			Path:   envCfg.Path,
			Bucket: envCfg.Bucket,
		}

		req := &btchwkfl.CloudCSVMongoCloudBatchRequest{
			JobID:               "job-cloud-csv-mongo-cloud-happy",
			BatchSize:           400,
			MaxInProcessBatches: 2,
			StartAt:             "0",
			Source:              sourceCfg,
			Sink:                sinkCfg,
			Snapshotter:         ssCfg,
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
					"Test_ProcessBatchWorkflow_CloudCSV_Mongo_Cloud_HappyPath - Activity started",
					"activityType", activityType,
				)

			})

		defer func() {
			if err := recover(); err != nil {
				l.Error(
					"Test_ProcessBatchWorkflow_CloudCSV_Mongo_Cloud_HappyPath - panicked",
					"error", err,
					"wkfl", btchwkfl.ProcessCloudCSVMongoCloudWorkflowAlias)
			}

			err := s.env.GetWorkflowError()
			if err != nil {
				l.Error(
					"Test_ProcessBatchWorkflow_CloudCSV_Mongo_Cloud_HappyPath - error",
					"error", err.Error(),
				)
			} else {
				var result btchwkfl.CloudCSVMongoCloudBatchRequest
				err := s.env.GetWorkflowResult(&result)
				if err != nil {
					l.Error(
						"Test_ProcessBatchWorkflow_CloudCSV_Mongo_Cloud_HappyPath - error",
						"error", err.Error(),
					)
				} else {
					l.Debug(
						"Test_ProcessBatchWorkflow_CloudCSV_Mongo_Cloud_HappyPath - success",
						"result-offsets", result.Offsets,
					)
					s.EqualValues(10, uint(result.Snapshot.NumBatches))
					errorCount := 0
					for _, errs := range result.Snapshot.Errors {
						errorCount += len(errs)
					}
					s.EqualValues(25, uint(result.Snapshot.NumRecords))
					s.EqualValues(0, errorCount)

					client, err := storage.NewClient(ctx)
					s.NoError(err, "error creating GCS client")
					defer func() {
						if err := client.Close(); err != nil {
							l.Error("cloud storage: failed to close client", "error", err.Error())
						}
					}()

					bucket := client.Bucket(envCfg.Bucket)
					it := bucket.Objects(ctx, nil)
					names := []string{}
					for {
						objAttrs, err := it.Next()
						if err != nil {
							if err == iterator.Done {
								break
							} else {
								l.Error("cloud storage: failed to list objects", "error", err.Error())
								break
							}
						}
						names = append(names, objAttrs.Name)
					}
					l.Debug("cloud storage: list of objects", "names", names)

					snapshotFileName := fmt.Sprintf("%s/%s-0.json", envCfg.Path, req.JobID)
					s.Contains(names, snapshotFileName, "snapshot file not found in cloud storage")

					s.NoError(bucket.Object(snapshotFileName).Delete(ctx), "error deleting snapshot file from cloud storage")
				}
			}

		}()

		l.Debug("Test_ProcessBatchWorkflow_CloudCSV_Mongo_Cloud_HappyPath - Starting workflow test")
		s.env.ExecuteWorkflow(btchwkfl.ProcessCloudCSVMongoCloudWorkflowAlias, req)
		s.True(s.env.IsWorkflowCompleted())
		l.Debug("Test_ProcessBatchWorkflow_CloudCSV_Mongo_Cloud_HappyPath - test completed")
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
		bo.ProcessBatchWorkflow[domain.CSVRow, *sources.LocalCSVConfig, *sinks.MongoSinkConfig[domain.CSVRow], *snapshotters.LocalSnapshotterConfig],
		workflow.RegisterOptions{
			Name: btchwkfl.ProcessLocalCSVMongoLocalWorkflowAlias,
		},
	)

	// Register activities.
	env.RegisterActivityWithOptions(
		bo.FetchNextActivity[domain.CSVRow, *sources.LocalCSVConfig],
		activity.RegisterOptions{
			Name: btchwkfl.FetchNextLocalCSVSourceBatchActivityAlias,
		},
	)
	env.RegisterActivityWithOptions(
		bo.WriteActivity[domain.CSVRow, *sinks.MongoSinkConfig[domain.CSVRow]],
		activity.RegisterOptions{
			Name: btchwkfl.WriteNextMongoSinkBatchActivityAlias,
		},
	)
	env.RegisterActivityWithOptions(
		bo.SnapshotActivity[*snapshotters.LocalSnapshotterConfig],
		activity.RegisterOptions{
			Name: btchwkfl.SnapshotLocalBatchActivityAlias,
		},
	)

	// Build source & sink configurations
	// Source - local CSV
	fileName := envutils.BuildFileName()
	filePath, err := envutils.BuildFilePath()
	require.NoError(t, err, "error building csv file path for test")
	path := filepath.Join(filePath, fileName)
	sourceCfg := &sources.LocalCSVConfig{
		Path:         path,
		Delimiter:    '|',
		HasHeader:    true,
		MappingRules: domain.BuildBusinessModelTransformRules(),
	}

	// Sink - MongoDB
	mCfg := envutils.BuildMongoStoreConfig(true)
	require.NotEmpty(t, mCfg.Name(), "MongoDB name should not be empty")
	require.NotEmpty(t, mCfg.Host(), "MongoDB host should not be empty")
	sinkCfg := &sinks.MongoSinkConfig[domain.CSVRow]{
		Protocol:   mCfg.Protocol(),
		Host:       mCfg.Host(),
		DBName:     mCfg.Name(),
		User:       mCfg.User(),
		Pwd:        mCfg.Pwd(),
		Params:     mCfg.Params(),
		Collection: "vypar.agents",
	}

	ssCfg := &snapshotters.LocalSnapshotterConfig{
		Path: filePath,
	}

	req := &btchwkfl.LocalCSVMongoLocalBatchRequest{
		JobID:               "job-local-csv-mongo-happy",
		BatchSize:           400,
		MaxInProcessBatches: 2,
		StartAt:             "0",
		Source:              sourceCfg,
		Sink:                sinkCfg,
		Snapshotter:         ssCfg,
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
				"wkfl", btchwkfl.ProcessLocalCSVMongoLocalWorkflowAlias)
		}

		err := env.GetWorkflowError()
		if err != nil {
			l.Error(
				"Test_ProcessBatchWorkflow_LocalCSV_Mongo_HappyPath - error",
				"error", err.Error(),
			)
		} else {
			var result btchwkfl.LocalCSVMongoLocalBatchRequest
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
				require.EqualValues(t, 11, int(result.Snapshot.NumBatches))
				errorCount := 0
				for _, errs := range result.Snapshot.Errors {
					errorCount += len(errs)
				}
				require.EqualValues(t, 20, int(result.Snapshot.NumRecords))
				require.EqualValues(t, 0, errorCount)
			}
		}

	}()

	env.ExecuteWorkflow(btchwkfl.ProcessLocalCSVMongoLocalWorkflowAlias, req)

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
		bo.ProcessBatchWorkflow[domain.CSVRow, *sources.LocalCSVConfig, *sinks.MongoSinkConfig[domain.CSVRow], *snapshotters.LocalSnapshotterConfig],
		workflow.RegisterOptions{
			Name: btchwkfl.ProcessLocalCSVMongoLocalWorkflowAlias,
		},
	)

	// Register activities.
	w.RegisterActivityWithOptions(
		bo.FetchNextActivity[domain.CSVRow, *sources.LocalCSVConfig],
		activity.RegisterOptions{
			Name: btchwkfl.FetchNextLocalCSVSourceBatchActivityAlias,
		},
	)
	w.RegisterActivityWithOptions(
		bo.WriteActivity[domain.CSVRow, *sinks.MongoSinkConfig[domain.CSVRow]],
		activity.RegisterOptions{
			Name: btchwkfl.WriteNextMongoSinkBatchActivityAlias,
		},
	)
	w.RegisterActivityWithOptions(
		bo.SnapshotActivity[*snapshotters.LocalSnapshotterConfig],
		activity.RegisterOptions{
			Name: btchwkfl.SnapshotLocalBatchActivityAlias,
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
	sourceCfg := &sources.LocalCSVConfig{
		Path:         path,
		Delimiter:    '|',
		HasHeader:    true,
		MappingRules: domain.BuildBusinessModelTransformRules(),
	}

	// Sink - MongoDB
	mCfg := envutils.BuildMongoStoreConfig(true)
	require.NotEmpty(t, mCfg.Name(), "MongoDB name should not be empty")
	require.NotEmpty(t, mCfg.Host(), "MongoDB host should not be empty")
	sinkCfg := &sinks.MongoSinkConfig[domain.CSVRow]{
		Protocol:   mCfg.Protocol(),
		Host:       mCfg.Host(),
		DBName:     mCfg.Name(),
		User:       mCfg.User(),
		Pwd:        mCfg.Pwd(),
		Params:     mCfg.Params(),
		Collection: "vypar.agents",
	}

	ssCfg := &snapshotters.LocalSnapshotterConfig{
		Path: filePath,
	}

	req := &btchwkfl.LocalCSVMongoLocalBatchRequest{
		JobID:               "job-local-csv-mongo-server-happy",
		BatchSize:           400,
		MaxInProcessBatches: 2,
		MaxBatches:          3,
		StartAt:             "0",
		Source:              sourceCfg,
		Sink:                sinkCfg,
		Snapshotter:         ssCfg,
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
		btchwkfl.ProcessLocalCSVMongoLocalWorkflowAlias, // workflow function
		req, // workflow input arg1
	)
	require.NoError(t, err)

	// With a real server, Continue-As-New chains transparently; Get waits for the final run.
	var out btchwkfl.LocalCSVMongoLocalBatchRequest
	require.NoError(t, run.Get(ctx, &out))
	require.NotNil(t, out)

	l.Debug("Test_ProcessBatchWorkflow_LocalCSV_Mongo_HappyPath_Server - workflow completed successfully",
		"workflow-id", run.GetID(),
		"workflow-run-id", run.GetRunID(),
		"offsets", out.Offsets,
	)
	require.EqualValues(t, 11, int(out.Snapshot.NumBatches))
	errorCount := 0
	for _, errs := range out.Snapshot.Errors {
		errorCount += len(errs)
	}
	require.EqualValues(t, 20, int(out.Snapshot.NumRecords))
	require.EqualValues(t, 0, errorCount)

}

// Integration test for ProcessBatchWorkflow on a dev Temporal server.
// Start temporal dev server before running this test.
func Test_ProcessBatchWorkflow_CloudCSV_Mongo_HappyPath_Server(t *testing.T) {
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
	const tq = "process-batch-workflow-cloud-csv-mongo-cloud-tq"

	// Temporal worker for the task queue
	w := worker.New(c, tq, worker.Options{
		BackgroundActivityContext: ctx,            // Global worker context for activities
		DeadlockDetectionTimeout:  24 * time.Hour, // set a long timeout to avoid deadlock detection during tests
	})

	// Register the workflow function
	w.RegisterWorkflowWithOptions(
		bo.ProcessBatchWorkflow[domain.CSVRow, *sources.CloudCSVConfig, *sinks.MongoSinkConfig[domain.CSVRow], *snapshotters.CloudSnapshotterConfig],
		workflow.RegisterOptions{
			Name: btchwkfl.ProcessCloudCSVMongoCloudWorkflowAlias,
		},
	)

	// Register activities.
	w.RegisterActivityWithOptions(
		bo.FetchNextActivity[domain.CSVRow, *sources.CloudCSVConfig],
		activity.RegisterOptions{
			Name: btchwkfl.FetchNextCloudCSVSourceBatchActivityAlias,
		},
	)
	w.RegisterActivityWithOptions(
		bo.WriteActivity[domain.CSVRow, *sinks.MongoSinkConfig[domain.CSVRow]],
		activity.RegisterOptions{
			Name: btchwkfl.WriteNextMongoSinkBatchActivityAlias,
		},
	)
	w.RegisterActivityWithOptions(
		bo.SnapshotActivity[*snapshotters.CloudSnapshotterConfig],
		activity.RegisterOptions{
			Name: btchwkfl.SnapshotCloudBatchActivityAlias,
		},
	)

	// Start the worker in a goroutine
	go func() {
		// Stop worker when test ends.
		if err := w.Run(worker.InterruptCh()); err != nil {
			l.Error("Test_ProcessBatchWorkflow_CloudCSV_Mongo_HappyPath_Server - worker stopped:", "error", err.Error())
		}
	}()

	// Build source & sink configurations
	// Source - cloud CSV
	envCfg, err := envutils.BuildCloudCSVBatchConfig()
	require.NoError(t, err, "error building cloud CSV config for test environment")
	path := filepath.Join(envCfg.Path, envCfg.Name)
	sourceCfg := &sources.CloudCSVConfig{
		Path:         path,
		Bucket:       envCfg.Bucket,
		Provider:     string(sources.CloudSourceGCS),
		Delimiter:    '|',
		HasHeader:    true,
		MappingRules: domain.BuildBusinessModelTransformRules(),
	}

	// Sink - MongoDB
	mCfg := envutils.BuildMongoStoreConfig(true)
	require.NotEmpty(t, mCfg.Name(), "MongoDB name should not be empty")
	require.NotEmpty(t, mCfg.Host(), "MongoDB host should not be empty")
	sinkCfg := &sinks.MongoSinkConfig[domain.CSVRow]{
		Protocol:   mCfg.Protocol(),
		Host:       mCfg.Host(),
		DBName:     mCfg.Name(),
		User:       mCfg.User(),
		Pwd:        mCfg.Pwd(),
		Params:     mCfg.Params(),
		Collection: "vypar.agents",
	}

	// Snapshotter - Cloud
	ssCfg := &snapshotters.CloudSnapshotterConfig{
		Path:   envCfg.Path,
		Bucket: envCfg.Bucket,
	}

	req := &btchwkfl.CloudCSVMongoCloudBatchRequest{
		JobID:               "job-cloud-csv-mongo-cloud-server-happy",
		BatchSize:           400,
		MaxInProcessBatches: 2,
		MaxBatches:          3,
		StartAt:             "0",
		Source:              sourceCfg,
		Sink:                sinkCfg,
		Snapshotter:         ssCfg,
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
		btchwkfl.ProcessCloudCSVMongoCloudWorkflowAlias, // workflow function
		req, // workflow input arg1
	)
	require.NoError(t, err)

	// With a real server, Continue-As-New chains transparently; Get waits for the final run.
	var out btchwkfl.CloudCSVMongoCloudBatchRequest
	require.NoError(t, run.Get(ctx, &out))
	require.NotNil(t, out)

	l.Debug("Test_ProcessBatchWorkflow_CloudCSV_Mongo_HappyPath_Server - workflow completed successfully",
		"workflow-id", run.GetID(),
		"workflow-run-id", run.GetRunID(),
		"num-processed", out.Snapshot.NumBatches,
	)
	require.EqualValues(t, 10, int(out.Snapshot.NumBatches))
	errorCount := 0
	for _, errs := range out.Snapshot.Errors {
		errorCount += len(errs)
	}
	require.EqualValues(t, 25, int(out.Snapshot.NumRecords))
	require.EqualValues(t, 0, errorCount)

	client, err := storage.NewClient(ctx)
	require.NoError(t, err, "error creating GCS client")
	defer func() {
		if err := client.Close(); err != nil {
			l.Error("cloud storage: failed to close client", "error", err.Error())
		}
	}()

	bucket := client.Bucket(envCfg.Bucket)
	it := bucket.Objects(ctx, nil)
	names := []string{}
	for {
		objAttrs, err := it.Next()
		if err != nil {
			if err == iterator.Done {
				break
			} else {
				l.Error("cloud storage: failed to list objects", "error", err.Error())
				break
			}
		}
		names = append(names, objAttrs.Name)
	}
	l.Debug("cloud storage: list of objects", "names", names)

	// snapshotFileName := fmt.Sprintf("%s/%s-0.json", envCfg.Path, req.JobID)
	// s.Contains(names, snapshotFileName, "snapshot file not found in cloud storage")

	// s.NoError(bucket.Object(snapshotFileName).Delete(ctx), "error deleting snapshot file from cloud storage")
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
		bo.ProcessBatchWorkflow[domain.CSVRow, *sources.LocalCSVConfig, *sinks.MongoSinkConfig[domain.CSVRow], *snapshotters.LocalSnapshotterConfig],
		workflow.RegisterOptions{
			Name: btchwkfl.ProcessLocalCSVMongoLocalWorkflowAlias,
		},
	)

	// Register activities.
	env.RegisterActivityWithOptions(
		bo.FetchNextActivity[domain.CSVRow, *sources.LocalCSVConfig],
		activity.RegisterOptions{
			Name: btchwkfl.FetchNextLocalCSVSourceBatchActivityAlias,
		},
	)
	env.RegisterActivityWithOptions(
		bo.WriteActivity[domain.CSVRow, *sinks.MongoSinkConfig[domain.CSVRow]],
		activity.RegisterOptions{
			Name: btchwkfl.WriteNextMongoSinkBatchActivityAlias,
		},
	)
	env.RegisterActivityWithOptions(
		bo.SnapshotActivity[*snapshotters.LocalSnapshotterConfig],
		activity.RegisterOptions{
			Name: btchwkfl.SnapshotLocalBatchActivityAlias,
		},
	)

	// Build source & sink configurations
	// Source - local CSV
	fileName := envutils.BuildFileName()
	filePath, err := envutils.BuildFilePath()
	require.NoError(t, err, "error building csv file path for test")
	path := filepath.Join(filePath, fileName)
	sourceCfg := &sources.LocalCSVConfig{
		Path:         path,
		Delimiter:    '|',
		HasHeader:    true,
		MappingRules: domain.BuildBusinessModelTransformRules(),
	}

	// Sink - MongoDB
	mCfg := envutils.BuildMongoStoreConfig(true)
	require.NotEmpty(t, mCfg.Name(), "MongoDB name should not be empty")
	require.NotEmpty(t, mCfg.Host(), "MongoDB host should not be empty")
	sinkCfg := &sinks.MongoSinkConfig[domain.CSVRow]{
		Protocol:   mCfg.Protocol(),
		Host:       mCfg.Host(),
		DBName:     mCfg.Name(),
		User:       mCfg.User(),
		Pwd:        mCfg.Pwd(),
		Params:     mCfg.Params(),
		Collection: "vypar.agents",
	}

	ssCfg := &snapshotters.LocalSnapshotterConfig{
		Path: filePath,
	}

	req := &btchwkfl.LocalCSVMongoLocalBatchRequest{
		JobID:               "job-local-csv-mongo-continue-as-new-error",
		BatchSize:           400,
		MaxInProcessBatches: 2,
		MaxBatches:          6,
		StartAt:             "0",
		Source:              sourceCfg,
		Sink:                sinkCfg,
		Snapshotter:         ssCfg,
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
				"workflow", btchwkfl.ProcessLocalCSVMongoLocalWorkflowAlias,
				"error", err,
			)
		}

		err := env.GetWorkflowError()
		var ca *workflow.ContinueAsNewError
		require.True(t, errors.As(err, &ca), "expected ContinueAsNewError, got: %v", err)
		require.Equal(t, btchwkfl.ProcessLocalCSVMongoLocalWorkflowAlias, ca.WorkflowType.Name, "expected workflow type to match")

		var next btchwkfl.LocalCSVMongoLocalBatchRequest
		ok, decErr := extractContinueAsNewInput(err, converter.GetDefaultDataConverter(), &next)
		require.True(t, ok, "expected to extract continue-as-new input, got error: %v", decErr)
		require.NoError(t, decErr, "error extracting continue-as-new input")
		require.NotNil(t, &next, "expected non-nil continue-as-new input")
		require.True(t, next.StartAt > req.StartAt, "expected next.StartAt > req.StartAt")
	}()

	env.ExecuteWorkflow(btchwkfl.ProcessLocalCSVMongoLocalWorkflowAlias, req)
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
