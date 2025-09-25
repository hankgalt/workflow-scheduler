package batch_test

import (
	"container/list"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/comfforts/logger"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"

	bo "github.com/hankgalt/batch-orchestra"
	"github.com/hankgalt/batch-orchestra/pkg/domain"
	"github.com/hankgalt/batch-orchestra/pkg/utils"

	btchwkfl "github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch"
	"github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch/sinks"
	"github.com/hankgalt/workflow-scheduler/internal/usecase/workflows/batch/sources"
	envutils "github.com/hankgalt/workflow-scheduler/pkg/utils/environment"
)

type ETLRequest[T any] struct {
	MaxBatches uint
	BatchSize  uint
	Done       bool
	Offsets    []any
	Batches    map[string]*domain.BatchProcess
}

type assertErr string

func (e assertErr) Error() string { return string(e) }

func Test_FetchNext_LocalTempCSV(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()

	l := logger.GetSlogLogger()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	// Register the generic activity instantiation to call.
	env.RegisterActivityWithOptions(
		bo.FetchNextActivity[domain.CSVRow, *sources.LocalCSVConfig],
		activity.RegisterOptions{
			Name: btchwkfl.FetchNextLocalCSVSourceBatchActivityAlias,
		},
	)

	// Build a small CSV
	path := writeTempCSV(t,
		[]string{"id", "name"},
		[][]string{
			{"1", "alpha"},
			{"2", "beta"},
			{"3", "gamma"},
			{"4", "delta"},
			{"5", "epsilon"},
		},
	)
	defer func() {
		require.NoError(t, os.Remove(path), "cleanup temp CSV file")
	}()

	// Create a local CSV config
	cfg := &sources.LocalCSVConfig{
		Path:      path,
		HasHeader: true,
	}

	// initialize batch size & next offset
	// Use a batch size larger than the largest row size in bytes to ensure more than one row is fetched.
	batchSize := uint(30)
	nextOffset := "0"

	fIn := &domain.FetchInput[domain.CSVRow, *sources.LocalCSVConfig]{
		Source:    cfg,
		Offset:    nextOffset,
		BatchSize: batchSize,
	}
	val, err := env.ExecuteActivity(btchwkfl.FetchNextLocalCSVSourceBatchActivityAlias, fIn)
	require.NoError(t, err)

	var out domain.FetchOutput[domain.CSVRow]
	require.NoError(t, val.Get(&out))
	require.False(t, out.Batch.Done)
	require.Len(t, out.Batch.Records, 2)
	require.EqualValues(t, "23", out.Batch.NextOffset)
	require.Equal(t, map[string]any{"id": "1", "name": "alpha"}, out.Batch.Records[0].Data)
	require.Equal(t, map[string]any{"id": "2", "name": "beta"}, out.Batch.Records[1].Data)

	// Fetch the next batch of records till done
	for out.Batch.Done == false {
		fIn = &domain.FetchInput[domain.CSVRow, *sources.LocalCSVConfig]{
			Source:    cfg,
			Offset:    out.Batch.NextOffset,
			BatchSize: batchSize,
		}
		val, err := env.ExecuteActivity(btchwkfl.FetchNextLocalCSVSourceBatchActivityAlias, fIn)
		require.NoError(t, err)
		require.NoError(t, val.Get(&out))

		nextOffsetStr, ok := out.Batch.NextOffset.(string)
		if !ok {
			l.Error("invalid offset type", "type", fmt.Sprintf("%T", out.Batch.NextOffset))
			t.FailNow()
		}

		nOffset, err := utils.ParseInt64(nextOffsetStr)
		require.NoError(t, err)

		sOffset, err := utils.ParseInt64(nextOffset)
		require.NoError(t, err)

		require.Equal(t, true, nOffset > sOffset, "next offset should be greater than start offset")
	}

	require.True(t, out.Batch.Done)
	require.EqualValues(t, "49", out.Batch.NextOffset)

	l.Debug("FetchNext_LocalTempCSV completed", "next-offset", out.Batch.NextOffset)
}

func Test_FetchNext_LocalCSV(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()

	l := logger.GetSlogLogger()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	// Register the generic activity instantiation we will call.
	// In test env, this is optional for function activities, but explicit is fine.
	env.RegisterActivityWithOptions(
		bo.FetchNextActivity[domain.CSVRow, *sources.LocalCSVConfig],
		activity.RegisterOptions{
			Name: btchwkfl.FetchNextLocalCSVSourceBatchActivityAlias,
		},
	)

	fileName := envutils.BuildFileName()
	filePath, err := envutils.BuildFilePath()
	require.NoError(t, err, "error building file path for test CSV")

	path := filepath.Join(filePath, fileName)

	cfg := &sources.LocalCSVConfig{
		Path:         path,
		Delimiter:    '|',
		HasHeader:    true,
		MappingRules: domain.BuildBusinessModelTransformRules(),
	}

	batchSize := uint(400) // larger than the largest row size in bytes
	nextOffset := "0"

	fIn := &domain.FetchInput[domain.CSVRow, *sources.LocalCSVConfig]{
		Source:    cfg,
		Offset:    nextOffset,
		BatchSize: batchSize,
	}
	val, err := env.ExecuteActivity(btchwkfl.FetchNextLocalCSVSourceBatchActivityAlias, fIn)
	require.NoError(t, err)

	var out domain.FetchOutput[domain.CSVRow]
	require.NoError(t, val.Get(&out))

	nextOffsetStr, ok := out.Batch.NextOffset.(string)
	if !ok {
		l.Error("invalid offset type", "type", fmt.Sprintf("%T", out.Batch.NextOffset))
		t.FailNow()
	}

	nOffset, err := utils.ParseInt64(nextOffsetStr)
	require.NoError(t, err)

	sOffset, err := utils.ParseInt64(nextOffset)
	require.NoError(t, err)

	require.Equal(t, true, nOffset > sOffset, "next offset should be greater than start offset")
	l.Debug("Test_FetchNext_LocalCSV first batch completed", "next-offset", out.Batch.NextOffset)

	for out.Batch.Done == false {
		fIn = &domain.FetchInput[domain.CSVRow, *sources.LocalCSVConfig]{
			Source:    cfg,
			Offset:    out.Batch.NextOffset,
			BatchSize: batchSize,
		}
		val, err = env.ExecuteActivity(btchwkfl.FetchNextLocalCSVSourceBatchActivityAlias, fIn)
		require.NoError(t, err)

		require.NoError(t, val.Get(&out))

		nextOffsetStr, ok := out.Batch.NextOffset.(string)
		if !ok {
			l.Error("invalid offset type", "type", fmt.Sprintf("%T", out.Batch.NextOffset))
			t.FailNow()
		}

		nOffset, err := utils.ParseInt64(nextOffsetStr)
		require.NoError(t, err)

		sOffset, err := utils.ParseInt64(nextOffset)
		require.NoError(t, err)

		require.Equal(t, true, nOffset > sOffset, "next offset should be greater than start offset")
		l.Debug("Test_FetchNext_LocalCSV batch completed", "next-offset", out.Batch.NextOffset)
	}
	l.Debug("Test_FetchNext_LocalCSV Done", "final-offset", out.Batch.NextOffset)
}

func Test_FetchNext_CloudCSV(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()

	l := logger.GetSlogLogger()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	// Register activity
	env.RegisterActivityWithOptions(
		bo.FetchNextActivity[domain.CSVRow, *sources.CloudCSVConfig],
		activity.RegisterOptions{
			Name: btchwkfl.FetchNextCloudCSVSourceBatchActivityAlias,
		},
	)

	envCfg, err := envutils.BuildCloudCSVBatchConfig()
	require.NoError(t, err, "error building cloud CSV config for test environment")

	path := filepath.Join(envCfg.Path, envCfg.Name)

	cfg := &sources.CloudCSVConfig{
		Path:         path,
		Bucket:       envCfg.Bucket,
		Provider:     string(sources.CloudSourceGCS),
		Delimiter:    '|',
		HasHeader:    true,
		MappingRules: domain.BuildBusinessModelTransformRules(),
	}

	batchSize := uint(400) // larger than the largest row size in bytes
	nextOffset := "0"

	fIn := &domain.FetchInput[domain.CSVRow, *sources.CloudCSVConfig]{
		Source:    cfg,
		Offset:    nextOffset,
		BatchSize: batchSize,
	}
	val, err := env.ExecuteActivity(btchwkfl.FetchNextCloudCSVSourceBatchActivityAlias, fIn)
	require.NoError(t, err)

	var out domain.FetchOutput[domain.CSVRow]
	require.NoError(t, val.Get(&out))

	nextOffsetStr, ok := out.Batch.NextOffset.(string)
	if !ok {
		l.Error("invalid offset type", "type", fmt.Sprintf("%T", out.Batch.NextOffset))
		t.FailNow()
	}

	nOffset, err := utils.ParseInt64(nextOffsetStr)
	require.NoError(t, err)

	sOffset, err := utils.ParseInt64(nextOffset)
	require.NoError(t, err)

	require.Equal(t, true, nOffset > sOffset, "next offset should be greater than start offset")
	l.Debug("Test_FetchNext_CloudCSV first batch", "next-offset", out.Batch.NextOffset)

	for out.Batch.Done == false {
		fIn = &domain.FetchInput[domain.CSVRow, *sources.CloudCSVConfig]{
			Source:    cfg,
			Offset:    out.Batch.NextOffset,
			BatchSize: batchSize,
		}
		val, err := env.ExecuteActivity(btchwkfl.FetchNextCloudCSVSourceBatchActivityAlias, fIn)
		require.NoError(t, err)

		require.NoError(t, val.Get(&out))
		nextOffsetStr, ok := out.Batch.NextOffset.(string)
		if !ok {
			l.Error("invalid offset type", "type", fmt.Sprintf("%T", out.Batch.NextOffset))
			t.FailNow()
		}

		nOffset, err := utils.ParseInt64(nextOffsetStr)
		require.NoError(t, err)

		sOffset, err := utils.ParseInt64(nextOffset)
		require.NoError(t, err)

		require.Equal(t, true, nOffset > sOffset, "next offset should be greater than start offset")
		l.Debug("Test_FetchNext_CloudCSV batch completed", "next-offset", out.Batch.NextOffset)
	}

	l.Debug("Test_FetchNext_CloudCSV Done", "final-offset", out.Batch.NextOffset)
}

func Test_Write_NoopSink(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()

	l := logger.GetSlogLogger()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
	})

	env.RegisterActivityWithOptions(
		bo.WriteActivity[domain.CSVRow, *sinks.NoopSinkConfig[domain.CSVRow]],
		activity.RegisterOptions{
			Name: btchwkfl.WriteNextNoopSinkBatchActivityAlias,
		},
	)

	recs := []*domain.BatchRecord{
		{
			Start: "0",
			End:   "10",
			Data:  domain.CSVRow{"id": "1", "name": "alpha"},
		},
		{
			Start: "10",
			End:   "20",
			Data:  domain.CSVRow{"id": "2", "name": "beta"},
		},
	}
	b := &domain.BatchProcess{
		Records:    recs,
		NextOffset: "30",
		Done:       false,
	}

	in := &domain.WriteInput[domain.CSVRow, *sinks.NoopSinkConfig[domain.CSVRow]]{
		Sink:  &sinks.NoopSinkConfig[domain.CSVRow]{},
		Batch: b,
	}

	val, err := env.ExecuteActivity(btchwkfl.WriteNextNoopSinkBatchActivityAlias, in)
	require.NoError(t, err)

	var out domain.WriteOutput[domain.CSVRow]
	require.NoError(t, val.Get(&out))
	require.Equal(t, 2, len(out.Batch.Records))
}

func Test_Write_MongoSink(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()

	l := logger.GetSlogLogger()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
		DeadlockDetectionTimeout:  24 * time.Hour, // set a long timeout to avoid deadlock detection during tests
	})

	env.SetTestTimeout(24 * time.Hour)

	env.RegisterActivityWithOptions(
		bo.WriteActivity[domain.CSVRow, *sinks.MongoSinkConfig[domain.CSVRow]],
		activity.RegisterOptions{
			Name: btchwkfl.WriteNextMongoSinkBatchActivityAlias,
		},
	)

	recs := []*domain.BatchRecord{
		{
			Start: "0",
			End:   "10",
			Data:  domain.CSVRow{"id": "1", "name": "alpha"},
		},
		{
			Start: "10",
			End:   "20",
			Data:  domain.CSVRow{"id": "2", "name": "beta"},
		},
	}

	b := &domain.BatchProcess{
		Records:    recs,
		NextOffset: "30",
		Done:       false,
	}

	nmCfg := envutils.BuildMongoStoreConfig(true)
	require.NotEmpty(t, nmCfg.Name(), "MongoDB name should not be empty")
	require.NotEmpty(t, nmCfg.Host(), "MongoDB host should not be empty")

	cfg := &sinks.MongoSinkConfig[domain.CSVRow]{
		Protocol:   nmCfg.Protocol(),
		Host:       nmCfg.Host(),
		DBName:     nmCfg.Name(),
		User:       nmCfg.User(),
		Pwd:        nmCfg.Pwd(),
		Params:     nmCfg.Params(),
		Collection: "test.people",
	}

	in := &domain.WriteInput[domain.CSVRow, *sinks.MongoSinkConfig[domain.CSVRow]]{
		Sink:  cfg,
		Batch: b,
	}

	val, err := env.ExecuteActivity(btchwkfl.WriteNextMongoSinkBatchActivityAlias, in)
	require.NoError(t, err)

	var out domain.WriteOutput[domain.CSVRow]
	require.NoError(t, val.Get(&out))
	require.Equal(t, 2, len(out.Batch.Records))
}

func Test_FetchAndWrite_LocalCSVSource_MongoSink_Queue(t *testing.T) {
	// setup test environment
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()

	// setup logger & global activity execution context
	l := logger.GetSlogLogger()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
		DeadlockDetectionTimeout:  24 * time.Hour, // set a long timeout to avoid deadlock detection during tests
	})

	env.SetTestTimeout(24 * time.Hour)

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

	// Build ETL request
	// batchSize should be larger than the largest row size in bytes
	etlReq := &ETLRequest[domain.CSVRow]{
		MaxBatches: 2,
		BatchSize:  400,
		Done:       false,
		Offsets:    []any{},
		Batches:    map[string]*domain.BatchProcess{},
	}

	etlReq.Offsets = append(etlReq.Offsets, "0")

	// initiate a new queue
	q := list.New()

	// setup first batch request
	fIn := &domain.FetchInput[domain.CSVRow, *sources.LocalCSVConfig]{
		Source:    sourceCfg,
		Offset:    etlReq.Offsets[len(etlReq.Offsets)-1],
		BatchSize: etlReq.BatchSize,
	}

	// Execute the fetch activity for first batch
	fVal, err := env.ExecuteActivity(btchwkfl.FetchNextLocalCSVSourceBatchActivityAlias, fIn)
	require.NoError(t, err)

	var fOut domain.FetchOutput[domain.CSVRow]
	require.NoError(t, fVal.Get(&fOut))
	require.Equal(t, true, len(fOut.Batch.Records) > 0)

	// Store the fetched batch in ETL request
	etlReq.Offsets = append(etlReq.Offsets, fOut.Batch.NextOffset)
	etlReq.Done = fOut.Batch.Done

	// setup first write request
	wIn := &domain.WriteInput[domain.CSVRow, *sinks.MongoSinkConfig[domain.CSVRow]]{
		Sink:  sinkCfg,
		Batch: fOut.Batch,
	}

	// Execute async the write activity for first batch
	wVal, err := env.ExecuteActivity(btchwkfl.WriteNextMongoSinkBatchActivityAlias, wIn)
	require.NoError(t, err)

	// Push the write activity future to the queue
	q.PushBack(wVal)

	// while there are items in queue
	count := 0
	for q.Len() > 0 {
		// if queue has less than max batches and batches are not done
		if q.Len() < int(etlReq.MaxBatches) && !etlReq.Done {
			fIn = &domain.FetchInput[domain.CSVRow, *sources.LocalCSVConfig]{
				Source:    sourceCfg,
				Offset:    etlReq.Offsets[len(etlReq.Offsets)-1],
				BatchSize: etlReq.BatchSize,
			}

			fVal, err := env.ExecuteActivity(btchwkfl.FetchNextLocalCSVSourceBatchActivityAlias, fIn)
			require.NoError(t, err)

			var fOut domain.FetchOutput[domain.CSVRow]
			require.NoError(t, fVal.Get(&fOut))
			require.Equal(t, true, len(fOut.Batch.Records) > 0)

			etlReq.Done = fOut.Batch.Done
			etlReq.Offsets = append(etlReq.Offsets, fOut.Batch.NextOffset)

			batchId, err := domain.GetBatchId(fOut.Batch.StartOffset, fOut.Batch.NextOffset, "", "")
			if err != nil {
				l.Error("failed to get batch id", "error", err)
				t.FailNow()
			}

			etlReq.Batches[batchId] = fOut.Batch

			wIn = &domain.WriteInput[domain.CSVRow, *sinks.MongoSinkConfig[domain.CSVRow]]{
				Sink:  sinkCfg,
				Batch: fOut.Batch,
			}

			wVal, err := env.ExecuteActivity(btchwkfl.WriteNextMongoSinkBatchActivityAlias, wIn)
			require.NoError(t, err)

			q.PushBack(wVal)
		} else {
			if count < int(etlReq.MaxBatches) {
				count++
			} else {
				count = 0
				// Pause execution for 1 second
				time.Sleep(1 * time.Second)
			}
			wVal := q.Remove(q.Front()).(converter.EncodedValue)
			var wOut domain.WriteOutput[domain.CSVRow]
			require.NoError(t, wVal.Get(&wOut))
			require.Equal(t, true, len(wOut.Batch.Records) > 0)

			batchId, err := domain.GetBatchId(wOut.Batch.StartOffset, wOut.Batch.NextOffset, "", "")
			if err != nil {
				l.Error("failed to get batch id", "error", err)
				t.FailNow()
			}

			if _, ok := etlReq.Batches[batchId]; !ok {
				etlReq.Batches[batchId] = wOut.Batch
			} else {
				etlReq.Batches[batchId] = wOut.Batch
			}
		}
	}

	require.Equal(t, true, len(etlReq.Offsets) > 0)
}

func Test_FetchAndWrite_CloudCSVSource_MongoSink_Queue(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()

	l := logger.GetSlogLogger()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
		DeadlockDetectionTimeout:  24 * time.Hour, // set a long timeout to avoid deadlock detection during tests
	})
	env.SetTestTimeout(24 * time.Hour)

	// Register concrete generic instantiations used by the test.
	env.RegisterActivityWithOptions(
		bo.FetchNextActivity[domain.CSVRow, *sources.CloudCSVConfig],
		activity.RegisterOptions{
			Name: btchwkfl.FetchNextCloudCSVSourceBatchActivityAlias,
		},
	)
	env.RegisterActivityWithOptions(
		bo.WriteActivity[domain.CSVRow, *sinks.MongoSinkConfig[domain.CSVRow]],
		activity.RegisterOptions{
			Name: btchwkfl.WriteNextMongoSinkBatchActivityAlias,
		},
	)

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

	// batchSize should be larger than the largest row size in bytes
	etlReq := &ETLRequest[domain.CSVRow]{
		MaxBatches: 2,
		BatchSize:  400,
		Done:       false,
		Offsets:    []any{},
		Batches:    map[string]*domain.BatchProcess{},
	}

	etlReq.Offsets = append(etlReq.Offsets, "0")

	// initiate a new queue
	q := list.New()

	fIn := &domain.FetchInput[domain.CSVRow, *sources.CloudCSVConfig]{
		Source:    sourceCfg,
		Offset:    etlReq.Offsets[len(etlReq.Offsets)-1],
		BatchSize: etlReq.BatchSize,
	}

	fVal, err := env.ExecuteActivity(btchwkfl.FetchNextCloudCSVSourceBatchActivityAlias, fIn)
	require.NoError(t, err)

	var fOut domain.FetchOutput[domain.CSVRow]
	require.NoError(t, fVal.Get(&fOut))
	require.Equal(t, true, len(fOut.Batch.Records) > 0)

	// Store the fetched batch in ETL request
	etlReq.Offsets = append(etlReq.Offsets, fOut.Batch.NextOffset)
	etlReq.Done = fOut.Batch.Done

	wIn := &domain.WriteInput[domain.CSVRow, *sinks.MongoSinkConfig[domain.CSVRow]]{
		Sink:  sinkCfg,
		Batch: fOut.Batch,
	}

	wVal, err := env.ExecuteActivity(btchwkfl.WriteNextMongoSinkBatchActivityAlias, wIn)
	require.NoError(t, err)

	q.PushBack(wVal)

	count := 0
	// while there are items in queue
	for q.Len() > 0 {
		if q.Len() < int(etlReq.MaxBatches) && !etlReq.Done {
			fIn = &domain.FetchInput[domain.CSVRow, *sources.CloudCSVConfig]{
				Source:    sourceCfg,
				Offset:    etlReq.Offsets[len(etlReq.Offsets)-1],
				BatchSize: etlReq.BatchSize,
			}

			fVal, err := env.ExecuteActivity(btchwkfl.FetchNextCloudCSVSourceBatchActivityAlias, fIn)
			require.NoError(t, err)

			var fOut domain.FetchOutput[domain.CSVRow]
			require.NoError(t, fVal.Get(&fOut))
			require.Equal(t, true, len(fOut.Batch.Records) > 0)

			etlReq.Done = fOut.Batch.Done
			etlReq.Offsets = append(etlReq.Offsets, fOut.Batch.NextOffset)

			batchId, err := domain.GetBatchId(fOut.Batch.StartOffset, fOut.Batch.NextOffset, "", "")
			if err != nil {
				l.Error("failed to get batch id", "error", err)
				t.FailNow()
			}

			etlReq.Batches[batchId] = fOut.Batch

			wIn = &domain.WriteInput[domain.CSVRow, *sinks.MongoSinkConfig[domain.CSVRow]]{
				Sink:  sinkCfg,
				Batch: fOut.Batch,
			}

			wVal, err := env.ExecuteActivity(btchwkfl.WriteNextMongoSinkBatchActivityAlias, wIn)
			require.NoError(t, err)

			q.PushBack(wVal)
		} else {
			if count < int(etlReq.MaxBatches) {
				count++
			} else {
				count = 0
				// Pause execution for 1 second
				time.Sleep(1 * time.Second)
			}
			wVal := q.Remove(q.Front()).(converter.EncodedValue)
			var wOut domain.WriteOutput[domain.CSVRow]
			require.NoError(t, wVal.Get(&wOut))
			require.Equal(t, true, len(wOut.Batch.Records) > 0)

			batchId, err := domain.GetBatchId(wOut.Batch.StartOffset, wOut.Batch.NextOffset, "", "")
			if err != nil {
				l.Error("failed to get batch id", "error", err)
				t.FailNow()
			}

			if _, ok := etlReq.Batches[batchId]; !ok {
				etlReq.Batches[batchId] = wOut.Batch
			} else {
				etlReq.Batches[batchId] = wOut.Batch
			}
		}
	}
}

func Test_FetchAndWrite_LocalJSONSource_LocalJSONSink_Queue(t *testing.T) {
	// setup test environment
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestActivityEnvironment()

	// setup logger & global activity execution context
	l := logger.GetSlogLogger()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	ctx = logger.WithLogger(ctx, l)

	env.SetWorkerOptions(worker.Options{
		BackgroundActivityContext: ctx,
		DeadlockDetectionTimeout:  24 * time.Hour, // set a long timeout to avoid deadlock detection during tests
	})

	env.SetTestTimeout(24 * time.Hour)

	// Register activities.
	env.RegisterActivityWithOptions(
		bo.FetchNextActivity[any, *sources.LocalJSONSourceConfig],
		activity.RegisterOptions{
			Name: btchwkfl.FetchNextLocalJSONSourceBatchActivityAlias,
		},
	)
	env.RegisterActivityWithOptions(
		bo.WriteActivity[any, *sinks.LocalJSONSinkConfig[any]],
		activity.RegisterOptions{
			Name: btchwkfl.WriteNextLocalJSONSinkBatchActivityAlias,
		},
	)

	// Build source & sink configurations
	// Source - local JSON
	sourceCfg := &sources.LocalJSONSourceConfig{
		Path:    "sources/data/scheduler",
		FileKey: "dummy-job-multiple-key",
	}

	// Sink - Local JSON
	sinkCfg := &sinks.LocalJSONSinkConfig[any]{}

	// Build ETL request
	// batchSize should be larger than the largest row size in bytes
	etlReq := &ETLRequest[any]{
		MaxBatches: 2,
		BatchSize:  1,
		Done:       false,
		Offsets:    []any{},
		Batches:    map[string]*domain.BatchProcess{},
	}

	startOffset := map[string]any{
		"id": "0",
	}

	etlReq.Offsets = append(etlReq.Offsets, startOffset)

	// initiate a new queue
	q := list.New()

	// setup first batch request
	fIn := &domain.FetchInput[any, *sources.LocalJSONSourceConfig]{
		Source:    sourceCfg,
		Offset:    etlReq.Offsets[len(etlReq.Offsets)-1],
		BatchSize: etlReq.BatchSize,
	}

	// Execute the fetch activity for first batch
	fVal, err := env.ExecuteActivity(btchwkfl.FetchNextLocalJSONSourceBatchActivityAlias, fIn)
	require.NoError(t, err)

	var fOut domain.FetchOutput[any]
	require.NoError(t, fVal.Get(&fOut))
	require.Equal(t, true, len(fOut.Batch.Records) > 0)

	// Store the fetched batch in ETL request
	etlReq.Offsets = append(etlReq.Offsets, fOut.Batch.NextOffset)
	etlReq.Done = fOut.Batch.Done

	// setup first write request
	wIn := &domain.WriteInput[any, *sinks.LocalJSONSinkConfig[any]]{
		Sink:  sinkCfg,
		Batch: fOut.Batch,
	}

	// Execute async the write activity for first batch
	wVal, err := env.ExecuteActivity(btchwkfl.WriteNextLocalJSONSinkBatchActivityAlias, wIn)
	require.NoError(t, err)

	// Push the write activity future to the queue
	q.PushBack(wVal)

	// while there are items in queue
	count := 0
	for q.Len() > 0 {
		// if queue has less than max batches and batches are not done
		if q.Len() < int(etlReq.MaxBatches) && !etlReq.Done {
			fIn = &domain.FetchInput[any, *sources.LocalJSONSourceConfig]{
				Source:    sourceCfg,
				Offset:    etlReq.Offsets[len(etlReq.Offsets)-1],
				BatchSize: etlReq.BatchSize,
			}

			fVal, err := env.ExecuteActivity(btchwkfl.FetchNextLocalJSONSourceBatchActivityAlias, fIn)
			require.NoError(t, err)

			var fOut domain.FetchOutput[any]
			require.NoError(t, fVal.Get(&fOut))
			require.Equal(t, true, len(fOut.Batch.Records) > 0)

			etlReq.Done = fOut.Batch.Done
			etlReq.Offsets = append(etlReq.Offsets, fOut.Batch.NextOffset)

			batchId, err := domain.GetBatchId(fOut.Batch.StartOffset, fOut.Batch.NextOffset, "", "")
			if err != nil {
				l.Error("failed to get batch id", "error", err)
				t.FailNow()
			}

			etlReq.Batches[batchId] = fOut.Batch

			wIn = &domain.WriteInput[any, *sinks.LocalJSONSinkConfig[any]]{
				Sink:  sinkCfg,
				Batch: fOut.Batch,
			}

			wVal, err := env.ExecuteActivity(btchwkfl.WriteNextLocalJSONSinkBatchActivityAlias, wIn)
			require.NoError(t, err)

			q.PushBack(wVal)
		} else {
			if count < int(etlReq.MaxBatches) {
				count++
			} else {
				count = 0
				// Pause execution for 1 second
				time.Sleep(1 * time.Second)
			}
			wVal := q.Remove(q.Front()).(converter.EncodedValue)
			var wOut domain.WriteOutput[any]
			require.NoError(t, wVal.Get(&wOut))
			require.Equal(t, true, len(wOut.Batch.Records) > 0)

			batchId, err := domain.GetBatchId(wOut.Batch.StartOffset, wOut.Batch.NextOffset, "", "")
			if err != nil {
				l.Error("failed to get batch id", "error", err)
				t.FailNow()
			}

			if _, ok := etlReq.Batches[batchId]; !ok {
				etlReq.Batches[batchId] = wOut.Batch
			} else {
				etlReq.Batches[batchId] = wOut.Batch
			}
		}
	}

	require.Equal(t, true, len(etlReq.Offsets) > 0)
}

func writeTempCSV(t *testing.T, header []string, rows [][]string) string {
	t.Helper()

	// get current dir path
	dir, err := os.Getwd()
	require.NoError(t, err)

	fp := filepath.Join(dir, "data", "data.csv")
	f, err := os.Create(fp)
	require.NoError(t, err)
	defer f.Close()

	write := func(cols []string) {
		for i, c := range cols {
			if i > 0 {
				_, _ = f.WriteString(",")
			}
			_, _ = f.WriteString(c)
		}
		_, _ = f.WriteString("\n")
	}

	if len(header) > 0 {
		write(header)
	}
	for _, r := range rows {
		write(r)
	}
	require.NoError(t, f.Sync())
	return fp
}
